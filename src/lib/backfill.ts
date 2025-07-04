import type { Database, Post, PostInclusionReason } from "../util/db.ts";
import { BackgroundQueue } from "../util/queue.ts";
import { XRPCManager } from "../util/xrpc.ts";
import {
	AppBskyEmbedExternal,
	AppBskyEmbedRecord,
	AppBskyEmbedRecordWithMedia,
	AppBskyFeedDefs,
	AppBskyFeedLike,
	AppBskyFeedPost,
	AppBskyFeedRepost,
	AppBskyGraphFollow,
} from "@atcute/bluesky";
import type {} from "@atcute/atproto";
import type { Did, ResourceUri } from "@atcute/lexicons/syntax";
import { is } from "@atcute/lexicons/validations";
import { RepoReader } from "@atcute/car/v4";
import { MultiBar, Presets as BarPresets, type SingleBar } from "cli-progress";
import {
	extractAltTexts,
	getRepoRev,
	logarithmicScale,
	parseAtUri,
	toDateOrNull,
} from "../util/util.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";
import { arrayBuffer } from "node:stream/consumers";

export const MAX_DEPTH = 6;
const MANY_FOLLOWS_MAX_DEPTH = 4;
const MANY_FOLLOWS_THRESHOLD = 250;
const WRITE_POSTS_BATCH_SIZE = 20;

const PUBLIC_APPVIEW_URL = "https://public.api.bsky.app";

type PostInclusion = {
	reason: PostInclusionReason;
	context?: string;
};

export interface BackfillOptions {
	embeddings?: boolean;
	depth?: number;
}

export class Backfill {
	progress: ProgressTracker;
	xrpc = new XRPCManager();
	postQueue = new BackgroundQueue(
		(uri: ResourceUri, options: ProcessPostOptions, sourceCollection?: string) =>
			this.processPost(uri, options).finally(() =>
				this.progress.incrementCompleted(sourceCollection)
			),
		{ concurrency: 100 },
	);
	repoQueue = new BackgroundQueue(
		(did: Did, collections?: string[]) =>
			this.processRepo(did, collections).finally(() =>
				this.progress.incrementCompleted("app.bsky.graph.follow")
			),
		{ concurrency: 25 },
	);
	embeddingsQueue = new BackgroundQueue(
		(posts: Post[]) =>
			this.generateEmbeddings(posts).catch((e) => console.error("error generating embeddings:", e)),
		{ concurrency: 1 },
	);

	seenPosts = new Set<string>();
	toWrite: Post[] = [];

	embeddingsEnabled: boolean;
	maxDepth: number;

	constructor(public userDid: string, public db: Database, options: BackfillOptions = {}) {
		this.embeddingsEnabled = options.embeddings ?? true;
		this.maxDepth = options.depth ?? MAX_DEPTH;

		const multibarKeys = Object.keys(this.processors);
		multibarKeys.push("posts");
		if (this.embeddingsEnabled) multibarKeys.push("embeddings");

		this.progress = new ProgressTracker(multibarKeys);

		this.xrpc.createClient(PUBLIC_APPVIEW_URL, {
			concurrency: 25,
			interval: 300 * 1000,
			intervalCap: 10_000,
		});
	}

	async backfill() {
		const { handle, followsCount } = await this.xrpc.query(
			PUBLIC_APPVIEW_URL,
			(c) => c.get("app.bsky.actor.getProfile", { params: { actor: this.userDid as Did } }),
		).catch((e) => {
			console.error(`error fetching profile for ${this.userDid}: ${e}`);
			return { handle: undefined, followsCount: 0 };
		});
		if (!handle) return;

		if (this.maxDepth === MAX_DEPTH && (followsCount ?? 0) > MANY_FOLLOWS_THRESHOLD) {
			console.warn(
				`high follow count detected, reducing search depth from ${MAX_DEPTH} to ${MANY_FOLLOWS_MAX_DEPTH} — pass the --depth flag to override`,
			);
		}

		if (this.embeddingsEnabled) {
			console.log("loading embeddings model...");
			await loadEmbeddingsModel();
			console.log("loaded embeddings model");
		}

		console.log(`backfilling index for user ${handle}...`);
		{
			using _logging = this.progress.start();
			await this.processRepo(this.userDid as Did, [
				"app.bsky.feed.post",
				"app.bsky.feed.repost",
				"app.bsky.feed.like",
				"app.bsky.graph.follow",
			]);
			await Promise.allSettled([
				this.repoQueue.processAll(),
				this.postQueue.processAll(),
				this.embeddingsQueue.processAll(),
			]);
			await this.writePosts();
		}
		console.log(`backfill complete`);
	}

	async processRepo(
		did: Did,
		collections = ["app.bsky.feed.post", "app.bsky.feed.repost"],
	): Promise<void> {
		try {
			const isOwnRepo = did === this.userDid;
			// always fetch full repo for self
			const rev = isOwnRepo ? "" : await this.db.getRepoRev(did);

			const stream = await this.xrpc.queryByDid(
				did,
				(c) => c.get("com.atproto.sync.getRepo", { params: { did, since: rev }, as: "stream" }),
			);
			const [repoStream, toBufferStream] = stream.tee();

			await using repo = RepoReader.fromStream(repoStream);
			for await (const { collection, rkey, record } of repo) {
				if (collections.includes(collection) && collection in this.processors) {
					const uri = `at://${did}/${collection}/${rkey}` as ResourceUri;

					this.progress.incrementTotal(collection);

					if (collection === "app.bsky.feed.post") {
						if (isOwnRepo) {
							this.processors[collection](uri, record, { reason: "self" });
						} else {
							this.processors[collection](uri, record, { reason: "by_follow" });
						}
					} else {
						this.processors[collection](uri, record);
					}
				}
			}

			const repoBytes = new Uint8Array(await arrayBuffer(toBufferStream));
			const latestRev = getRepoRev(repoBytes);
			if (latestRev) await this.db.setRepoRev(did, latestRev);
		} catch (e) {
			console.error(`failed to process repo ${did}: ${e}`);
		}
	}

	async processPost(
		uri: ResourceUri,
		{ inclusion, record, depth = 0 }: ProcessPostOptions,
	): Promise<void> {
		try {
			this.progress.incrementTotal("posts");

			if (depth > this.maxDepth) return;

			const { repo, rkey } = parseAtUri(uri);
			if (this.seenPosts.has(`${repo}/${rkey}`)) return;
			this.seenPosts.add(`${repo}/${rkey}`);

			if (!record) {
				const res = await this.xrpc.queryByDid(
					repo,
					(c) =>
						c.get("com.atproto.repo.getRecord", {
							params: { collection: "app.bsky.feed.post", repo, rkey },
						}),
				);
				if (!is(AppBskyFeedPost.mainSchema, res.value)) {
					throw new Error(`invalid post record: ${uri}`);
				}
				record = res.value;
			}

			const createdAt = toDateOrNull(record.createdAt)?.getTime();
			if (!createdAt) throw new Error(`invalid post createdAt (${uri}): ${record.createdAt}`);

			const altText = extractAltTexts(record.embed)?.map((alt, i) => `---image ${i + 1}---\n${alt}`)
				.join("\n\n");
			const embed = is(AppBskyEmbedExternal.mainSchema, record.embed)
				? record.embed?.external
				: null;
			const quoted = is(AppBskyEmbedRecord.mainSchema, record.embed)
				? record.embed.record.uri
				: is(AppBskyEmbedRecordWithMedia.mainSchema, record.embed)
				? record.embed.record.record.uri
				: null;

			const post: Post = {
				creator: repo,
				rkey,
				createdAt,
				text: record.text,
				altText,
				embedUrl: embed?.uri,
				embedTitle: embed?.title,
				embedDescription: embed?.description,
				replyParent: record.reply?.parent?.uri,
				replyRoot: record.reply?.root?.uri,
				quoted,
				inclusionReason: inclusion.reason,
				inclusionContext: inclusion.context,
			};
			await this.writePost(post);

			if (quoted) {
				this.postQueue.add(quoted, {
					depth: depth + 1,
					inclusion: { reason: "quoted_by", context: uri },
				});
			}

			if (record.reply) {
				this.postQueue.add(record.reply.root.uri, {
					depth, // navigating upthread then down to siblings should only add 1 to depth
					inclusion: { reason: "same_thread_as", context: uri },
				});
			} else {
				const { thread } = await this.xrpc.query(
					PUBLIC_APPVIEW_URL,
					(c) => c.get("app.bsky.feed.getPostThread", { params: { uri, depth: 20 } }),
				).catch((e) => {
					console.error(`error fetching thread for ${uri}`, e);
					return { thread: null };
				});
				if (!is(AppBskyFeedDefs.threadViewPostSchema, thread)) return;

				// for a thread with 50 replies, go up to 20 levels deep
				// for a thread with 500 replies, go up to 4 levels deep
				// in between, scale logarithmically
				const maxReplyDepth = Math.round(
					logarithmicScale([50, 500], [20, 4], thread.post.replyCount ?? 0),
				);

				const { postQueue } = this;
				thread.replies?.forEach((reply) =>
					(function processReply(reply, replyDepth) {
						if (replyDepth > maxReplyDepth) return;
						if (
							!is(AppBskyFeedDefs.threadViewPostSchema, reply) ||
							!is(AppBskyFeedPost.mainSchema, reply.post.record)
						) return;
						postQueue.add(reply.post.uri, {
							record: reply.post.record,
							depth: depth + 1,
							inclusion: { reason: "same_thread_as", context: uri },
						});
						reply.replies?.forEach(processReply, replyDepth + 1);
					})(reply, 1)
				);
			}
		} catch (e) {
			console.warn(`failed to process post ${uri}`, e);
		} finally {
			this.progress.incrementCompleted("posts");
		}
	}

	async writePost(post: Post): Promise<void> {
		this.toWrite.push(post);
		await this.writePosts();
	}

	async writePosts(): Promise<void> {
		while (this.toWrite.length > 0) {
			const batch = this.toWrite.splice(0, WRITE_POSTS_BATCH_SIZE);
			await this.db.insertPosts(batch).catch((e) => console.error("error inserting posts:", e));
			if (this.embeddingsEnabled) this.embeddingsQueue.add(batch);
		}
	}

	async generateEmbeddings(posts: Post[]) {
		posts.forEach(() => this.progress.incrementTotal("embeddings"));
		const [hasText, hasTextIndices] = posts.reduce<[Post[], number[]]>(
			([posts, indices], post, i) => {
				post.text && (posts.push(post), indices.push(i));
				return [posts, indices];
			},
			[[], []],
		);
		const [hasAltText, hasAltTextIndices] = posts.reduce<[Post[], number[]]>(
			([posts, indices], post, i) => {
				post.altText && (posts.push(post), indices.push(i));
				return [posts, indices];
			},
			[[], []],
		);

		try {
			const [textEmbeddings, altTextEmbeddings] = await Promise.all([
				hasText.length ? extractEmbeddings(hasText.map((p) => p.text)) : [],
				hasAltText.length ? extractEmbeddings(hasAltText.map((p) => p.altText!)) : [],
			]);

			textEmbeddings.forEach((embedding, i) => {
				posts[hasTextIndices[i]].embedding = embedding;
			});
			altTextEmbeddings.forEach((embedding, i) => {
				posts[hasAltTextIndices[i]].altTextEmbedding = embedding;
			});
		} catch (error) {
			console.error("error generating embeddings:", error);
		} finally {
			posts.forEach(() => this.progress.incrementCompleted("embeddings"));
		}
	}

	processors: Record<
		string,
		(uri: ResourceUri, record: unknown, inclusion?: PostInclusion) => void
	> = {
		"app.bsky.feed.post": (uri, record, inclusion) => {
			if (!is(AppBskyFeedPost.mainSchema, record)) return;
			if (!inclusion) throw new Error("inclusion reason is required for app.bsky.feed.post");
			this.postQueue.add(uri, { record, inclusion }, "app.bsky.feed.post");
		},
		"app.bsky.feed.repost": (uri, record) => {
			if (!is(AppBskyFeedRepost.mainSchema, record)) return;
			const { repo: reposter } = parseAtUri(uri);
			this.postQueue.add(record.subject.uri, {
				inclusion: { reason: "reposted_by", context: reposter },
			}, "app.bsky.feed.repost");
		},
		"app.bsky.feed.like": (_uri, record) => {
			if (!is(AppBskyFeedLike.mainSchema, record)) return;
			this.postQueue.add(record.subject.uri, {
				inclusion: { reason: "liked_by_self" },
			}, "app.bsky.feed.like");
		},
		"app.bsky.graph.follow": (_uri, record) => {
			if (!is(AppBskyGraphFollow.mainSchema, record)) return;
			this.repoQueue.add(record.subject);
		},
	};
}

interface ProcessPostOptions {
	inclusion: PostInclusion;
	record?: AppBskyFeedPost.Main;
	depth?: number;
}

class ProgressTracker {
	progress: Record<string, { completed: number; total: number }> = {};
	multibar = new MultiBar(
		{ format: " {key}  {bar}  {value}/{total}", fps: 30 },
		BarPresets.shades_classic,
	);
	bars: Record<string, SingleBar> = {};

	constructor(private keys: string[]) {}

	incrementCompleted(key: string | undefined) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.progress[key].completed++;
		this.bars[key].update(this.progress[key].completed);
	}

	incrementTotal(key: string | undefined) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.progress[key].total++;
		this.bars[key].setTotal(this.progress[key].total);
	}

	start() {
		for (const key of this.keys) {
			this.progress[key] = { completed: 0, total: 0 };
			this.bars[key] = this.multibar.create(100, 0, { key });
		}

		const { multibar } = this;
		const consoleLog = console.log, consoleWarn = console.warn, consoleError = console.error;
		console.log = console.warn = console.error = (...data: string[]) =>
			multibar.log(data.join(" ") + "\n");

		return {
			[Symbol.dispose]() {
				multibar.stop();
				console.log = consoleLog;
				console.warn = consoleWarn;
				console.error = consoleError;
			},
		};
	}
}
