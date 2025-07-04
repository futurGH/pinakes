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
import { CarReader, RepoReader } from "@atcute/car/v4";
import { MultiBar, Presets as BarPresets, type SingleBar } from "cli-progress";
import {
	errorToString,
	extractAltTexts,
	logarithmicScale,
	parseAtUri,
	toDateOrNull,
	tryExtractRootPostFromThreadView,
} from "../util/util.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";
import { ClientResponseError } from "@atcute/client";
import { toCidLink } from "@atcute/cid";
import { decode as decodeCbor } from "@atcute/cbor";
import { isCommit } from "@atcute/car/v4/repo-reader";
import xxhash from "xxhash-wasm";

const { h32 } = await xxhash();

export const MAX_DEPTH = 5;
const MANY_FOLLOWS_MAX_DEPTH = 3;
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
		// sourceCollection, if provided, is the collection this post "came from"
		(uri: ResourceUri, options: ProcessPostOptions, sourceCollection?: string) =>
			this.processPost(uri, options).finally(() =>
				this.progress.incrementCompleted(sourceCollection)
			),
		{ softConcurrency: 25, hardConcurrency: 100 },
	);
	repoQueue = new BackgroundQueue(
		(did: Did, collections?: string[]) =>
			this.processRepo(did, collections).finally(() =>
				this.progress.incrementCompleted("app.bsky.graph.follow")
			),
		{ softConcurrency: 10, hardConcurrency: 20 },
	);
	embeddingsQueue = new BackgroundQueue(
		(posts: Post[]) =>
			this.generateEmbeddings(posts).catch((e) => console.error("error generating embeddings:", e)),
		{ hardConcurrency: 1 },
	);

	seenPosts = new Set<number>();
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
		this.postQueue.on("queued", () => this.progress.incrementTotal("posts"));
		this.postQueue.on("completed", () => this.progress.incrementCompleted("posts"));
		this.postQueue.on("error", console.error);
		this.repoQueue.on("error", console.error);
	}

	async backfill() {
		const { handle, followsCount } = await this.xrpc.query(
			PUBLIC_APPVIEW_URL,
			(c) => c.get("app.bsky.actor.getProfile", { params: { actor: this.userDid as Did } }),
		).catch((e) => {
			console.error(`error fetching profile for ${this.userDid}: ${errorToString(e)}`);
			return { handle: undefined, followsCount: 0 };
		});
		if (!handle) return;

		// post count scales massively with follows
		if (this.maxDepth === MAX_DEPTH && (followsCount ?? 0) > MANY_FOLLOWS_THRESHOLD) {
			console.warn(
				`high follow count detected, reducing search depth from ${MAX_DEPTH} to ${MANY_FOLLOWS_MAX_DEPTH} — pass the --depth flag to override`,
			);
			this.maxDepth = MANY_FOLLOWS_MAX_DEPTH;
		}

		if (this.embeddingsEnabled) {
			console.log("loading embeddings model...");
			await loadEmbeddingsModel();
			console.log("loaded embeddings model");
		}

		console.log(`backfilling index for user ${handle}...`);
		const startTime = performance.now();
		{
			using _logging = this.progress.start(); // auto resets console.* at the end of the block

			await this.processRepo(this.userDid as Did, [
				"app.bsky.feed.post",
				"app.bsky.feed.repost",
				"app.bsky.feed.like",
				"app.bsky.graph.follow",
			]);
			while (this.repoQueue.size > 0 || this.postQueue.size > 0 || this.embeddingsQueue.size > 0) {
				await Promise.allSettled([
					(async () => {
						while (this.repoQueue.size > 0) await this.repoQueue.processAll();
					})(),
					(async () => {
						while (this.postQueue.size > 0) await this.postQueue.processAll();
					})(),
					this.embeddingsEnabled && (async () => {
						while (this.embeddingsQueue.size > 0) await this.embeddingsQueue.processAll();
					})(),
				]);
			}
			await this.writePosts();
		}
		const endTime = performance.now();
		const mins = Math.floor((endTime - startTime) / 60_000);
		const secs = Math.floor((endTime - startTime) / 1000) % 60;
		console.log(`backfill complete in ${mins}m ${secs}s`);
	}

	async processRepo(
		did: Did,
		collections = ["app.bsky.feed.post", "app.bsky.feed.repost"], // for anyone but the user, only process posts and reposts
	): Promise<void> {
		try {
			const isOwnRepo = did === this.userDid;
			// always fetch full repo for self
			const rev = isOwnRepo ? "" : await this.db.getRepoRev(did);

			const stream = await this.xrpc.queryByDid(
				did,
				(c) => c.get("com.atproto.sync.getRepo", { params: { did, since: rev }, as: "stream" }),
			);
			// split into two streams
			const [repoStream, carStream] = stream.tee();

			await Promise.allSettled([
				(async () => { // first stream is used to parse out records
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
				})(),
				(async () => { // second stream is just used to get the repo rev
					if (isOwnRepo) return;
					await using car = CarReader.fromStream(carStream);
					const rootCid = (await car.roots())[0].$link;
					for await (const entry of car) {
						if (toCidLink(entry.cid).$link === rootCid) {
							const decoded = decodeCbor(entry.bytes);
							if (!isCommit(decoded)) break;
							await this.db.setRepoRev(did, decoded.rev);
							break;
						}
					}
				})(),
			]);
		} catch (e) {
			console.error(`failed to process repo ${did}: ${errorToString(e)}`);
		}
	}

	async processPost(
		uri: ResourceUri,
		{ inclusion, record, threadView, depth = 0 }: ProcessPostOptions,
	): Promise<void> {
		if (depth > this.maxDepth) return;

		const uriHash = h32(uri);
		if (this.seenPosts.has(uriHash)) return;

		if (!record) {
			try {
				({ record, threadView } = await this.fetchPost(uri, threadView));
			} catch (e) {
				if (e instanceof DOMException && e.name === "AbortError") {
					throw e; // handled by BackgroundQueue
				} else {
					console.error(`failed to fetch post record for ${uri}: ${errorToString(e)}`);
					return;
				}
			}
		}

		this.seenPosts.add(uriHash);

		const createdAt = toDateOrNull(record.createdAt)?.getTime();
		if (!createdAt) throw new Error(`invalid post createdAt (${uri}): ${record.createdAt}`);

		const altText = extractAltTexts(record.embed)?.map((alt, i) => `---image ${i + 1}---\n${alt}`)
			.join("\n\n");
		const embed = is(AppBskyEmbedExternal.mainSchema, record.embed) ? record.embed?.external : null;
		const quoted = is(AppBskyEmbedRecord.mainSchema, record.embed)
			? record.embed.record.uri
			: is(AppBskyEmbedRecordWithMedia.mainSchema, record.embed)
			? record.embed.record.record.uri
			: null;

		const { repo, rkey } = parseAtUri(uri);

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
		this.queueWritePost(post);

		if (quoted) {
			let quotedRecordView;
			if (threadView) {
				if (
					is(AppBskyEmbedRecord.viewSchema, threadView.post.embed) &&
					is(AppBskyEmbedRecord.viewRecordSchema, threadView.post.embed.record)
				) {
					quotedRecordView = threadView.post.embed.record;
				} else if (
					is(AppBskyEmbedRecordWithMedia.viewSchema, threadView.post.embed) &&
					is(AppBskyEmbedRecord.viewRecordSchema, threadView.post.embed.record.record)
				) {
					quotedRecordView = threadView.post.embed.record.record;
				}
			}
			if (is(AppBskyFeedPost.mainSchema, quotedRecordView?.value)) {
				this.postQueue.add(quoted, {
					depth: depth + 1,
					record: quotedRecordView.value,
					inclusion: { reason: "quoted_by", context: uri },
				});
			}
		}

		// if the post is a reply, queue the root
		// but only if this post is the entrypoint to the thread
		if (record.reply && inclusion.reason !== "same_thread_as") {
			const rootThreadView = threadView
				? tryExtractRootPostFromThreadView(threadView, record.reply.root.uri)
				: null;
			this.postQueue.add(record.reply.root.uri, {
				depth, // navigating upthread then down to siblings should only add 1 to depth
				threadView: rootThreadView ?? undefined,
				inclusion: { reason: "same_thread_as", context: uri },
			});
		} else { // if the post is a top-level post, recursively queue its replies
			if (!threadView) {
				const { thread } = await this.xrpc.query(
					PUBLIC_APPVIEW_URL,
					(c) =>
						c.get("app.bsky.feed.getPostThread", {
							params: { uri, depth: 20 },
							signal: AbortSignal.timeout(10_000),
						}),
				).catch((e) => {
					console.error(`error fetching thread for ${uri}`, e);
					return { thread: null };
				});
				if (!is(AppBskyFeedDefs.threadViewPostSchema, thread)) return;
				threadView = thread;
			}

			// for a thread with 50 replies, go up to 20 levels deep
			// for a thread with 500 replies, go up to 4 levels deep
			// in between, scale logarithmically
			const maxReplyDepth = Math.round(
				logarithmicScale([50, 500], [20, 4], threadView.post.replyCount ?? 0),
			);

			const { postQueue } = this;
			threadView.replies?.forEach((reply) =>
				(function processReply(reply, replyDepth) {
					if (replyDepth > maxReplyDepth) return;
					if (
						!is(AppBskyFeedDefs.threadViewPostSchema, reply) ||
						!is(AppBskyFeedPost.mainSchema, reply.post.record)
					) return;
					postQueue.add(reply.post.uri, {
						// threadView is available but unnecessary; replies don't use it
						depth: depth + 1,
						inclusion: { reason: "same_thread_as", context: uri },
					});
					reply.replies?.forEach(processReply, replyDepth + 1);
				})(reply, 1)
			);
		}
	}

	queueWritePost(post: Post) {
		this.toWrite.push(post);
		if (this.toWrite.length > WRITE_POSTS_BATCH_SIZE) void this.writePosts();
	}

	async writePosts(): Promise<void> {
		while (this.toWrite.length > 0) {
			const batch = this.toWrite.splice(0, WRITE_POSTS_BATCH_SIZE);
			await this.db.insertPosts(batch).catch((e) => console.error("error inserting posts:", e));
			if (this.embeddingsEnabled) this.embeddingsQueue.add(batch);
		}
	}

	async fetchPost(
		uri: ResourceUri,
		threadView?: AppBskyFeedDefs.ThreadViewPost,
	): Promise<{ record: AppBskyFeedPost.Main; threadView?: AppBskyFeedDefs.ThreadViewPost }> {
		if (threadView && is(AppBskyFeedPost.mainSchema, threadView.post.record)) {
			return { threadView, record: threadView.post.record };
		}

		// first try fetching thread view; gets us more info in one query
		try {
			const { thread } = await this.xrpc.queryNoRetry(
				PUBLIC_APPVIEW_URL,
				(c) =>
					c.get("app.bsky.feed.getPostThread", {
						params: { uri },
						signal: AbortSignal.timeout(10_000),
					}),
			);
			if (!is(AppBskyFeedDefs.threadViewPostSchema, thread)) {
				throw new Error(`invalid thread view for ${uri}`);
			}
			if (!is(AppBskyFeedPost.mainSchema, thread.post.record)) {
				throw new Error(`invalid post record for ${uri}`);
			}
			return { threadView: thread, record: thread.post.record };
		} catch (e) {
			// if the appview says a post doesn't exist, trust it
			if (e instanceof ClientResponseError && e.error === "NotFound") throw e;
			// if it's an AbortError, rethrow it to be handled by BackgroundQueue
			if (e instanceof DOMException && e.name === "AbortError") throw e;
			console.warn(
				`failed to fetch thread view for ${uri}, falling back to getRecord: ${errorToString(e)}`,
			);
		}

		// if that fails, fetch the post record directly
		const { repo, rkey } = parseAtUri(uri);

		const res = await this.xrpc.queryByDid(
			repo,
			(c) =>
				c.get("com.atproto.repo.getRecord", {
					params: { collection: "app.bsky.feed.post", repo, rkey },
					signal: AbortSignal.timeout(15_000),
				}),
		);
		if (!is(AppBskyFeedPost.mainSchema, res.value)) {
			throw new Error(`invalid post record for ${uri}`);
		}

		return { threadView, record: res.value };
	}

	// generates text & alt text embeddings for each post then re-writes them to the db
	// writing the same posts twice is way faster than blocking on generating embeddings
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
	threadView?: AppBskyFeedDefs.ThreadViewPost;
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

	setCompleted(key: string | undefined, completed: number | ((prev: number) => number)) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.progress[key].completed = typeof completed === "number"
			? completed
			: completed(this.progress[key].completed);
		this.bars[key].update(this.progress[key].completed);
	}

	incrementTotal(key: string | undefined) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.progress[key].total++;
		this.bars[key].setTotal(this.progress[key].total);
	}

	setTotal(key: string | undefined, total: number | ((prev: number) => number)) {
		if (!key || !this.progress[key] || !this.bars[key]) return;
		this.progress[key].total = typeof total === "number" ? total : total(this.progress[key].total);
		this.bars[key].setTotal(this.progress[key].total);
	}

	start() {
		for (const key of this.keys) {
			this.progress[key] = { completed: 0, total: 0 };
			this.bars[key] = this.multibar.create(100, 0, { key }, { clearOnComplete: false });
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
