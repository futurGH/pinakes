import type {} from "@atcute/atproto";
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
import { CarReader } from "@atcute/car/v4";
import { collectBlock, isCommit, readBlock, walkMstEntries } from "@atcute/car/v4/repo-reader";
import { decode as decodeCbor } from "@atcute/cbor";
import { Client, ClientResponseError } from "@atcute/client";
import { type Did, isTid, type ResourceUri } from "@atcute/lexicons/syntax";
import { is } from "@atcute/lexicons/validations";
import assert from "node:assert";
import pc from "picocolors";
import xxhash from "xxhash-wasm";
import type { Database, Post, PostInclusionReason } from "../util/db.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";
import { ProgressTracker } from "../util/progress.ts";
import { BackgroundQueue } from "../util/queue.ts";
import {
	errorToString,
	extractAltTexts,
	logarithmicScale,
	parseAtUri,
	toDateOrNull,
} from "../util/util.ts";
import { XRPCManager } from "../util/xrpc.ts";

const { h32 } = await xxhash();

export const MAX_DEPTH = 5;
const MANY_FOLLOWS_MAX_DEPTH = 2;
const MANY_FOLLOWS_THRESHOLD = 250;
const WRITE_POSTS_BATCH_SIZE = 20;

const BSKY_APP_DID = "did:plc:z72i7hdynmk6r22z27h6tvur";
export const PUBLIC_APPVIEW_URL = "https://public.api.bsky.app";

type PostInclusion = { reason: PostInclusionReason; context?: string };

export interface BackfillOptions {
	embeddings?: boolean;
	depth?: number;
	appview?: string;
}

export class Backfill {
	progress: ProgressTracker;
	xrpc = new XRPCManager();
	postQueue = new BackgroundQueue(
		// sourceCollection, if provided, is the collection this post "came from"
		(uri: ResourceUri, options: ProcessPostOptions, sourceCollection?: string) =>
			this.processPost(uri, options).then(() =>
				this.progress.incrementCompleted(sourceCollection)
			),
		{ softConcurrency: 25, hardConcurrency: 100, maxQueueSize: 100_000 },
	);
	repoQueue = new BackgroundQueue(
		(did: Did, collections?: Set<string>) =>
			this.processRepo(did, collections).then(() =>
				this.progress.incrementCompleted("app.bsky.graph.follow")
			),
		{ softConcurrency: 10, hardConcurrency: 20, softTimeoutMs: 60_000, maxQueueSize: 1000 },
	);
	embeddingsQueue = new BackgroundQueue(
		(posts: Post[]) =>
			this.generateEmbeddings(posts).catch((e) =>
				console.error("error generating embeddings:", e)
			),
		{ hardConcurrency: 1 },
	);

	seenPosts = new Set<number>();
	toWrite: Post[] = [];

	embeddingsEnabled: boolean;
	maxDepth: number;
	appview: string;

	constructor(public userDid: string, public db: Database, options: BackfillOptions = {}) {
		this.embeddingsEnabled = options.embeddings ?? false;
		this.maxDepth = options.depth ?? MAX_DEPTH;
		this.appview = options.appview ?? PUBLIC_APPVIEW_URL;

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
			this.appview,
			(c) => c.get("app.bsky.actor.getProfile", { params: { actor: this.userDid as Did } }),
		).catch((e) => {
			console.error(`error fetching profile for ${this.userDid}: ${errorToString(e)}`);
			return { handle: undefined, followsCount: 0 };
		});
		if (!handle) return;

		// post count scales massively with follows
		if (this.maxDepth === MAX_DEPTH && (followsCount ?? 0) > MANY_FOLLOWS_THRESHOLD) {
			console.warn(
				`${
					pc.blue("high follow count detected!")
				} reducing search depth from ${MAX_DEPTH} to ${MANY_FOLLOWS_MAX_DEPTH} — pass the ${
					pc.green("--depth")
				} flag to override`,
			);
			this.maxDepth = MANY_FOLLOWS_MAX_DEPTH;
		}

		if (this.embeddingsEnabled) {
			console.log("loading embeddings model...");
			await loadEmbeddingsModel();
			console.log("loaded embeddings model");
		}

		const startTime = performance.now();
		{
			using _logging = this.progress.start();// auto resets console.* at the end of the block

			console.log(`fetching repo for ${pc.underline(handle)}...`);
			await this.processRepo(
				this.userDid as Did,
				new Set([
					"app.bsky.feed.post",
					"app.bsky.feed.repost",
					"app.bsky.feed.like",
					"app.bsky.graph.follow",
				]),
			);

			while (
				this.repoQueue.size > 0 || this.postQueue.size > 0 || this.embeddingsQueue.size > 0
			) {
				await Promise.allSettled([
					(async () => {
						while (this.repoQueue.size > 0) await this.repoQueue.processAll();
					})(),
					(async () => {
						while (this.postQueue.size > 0) await this.postQueue.processAll();
					})(),
					this.embeddingsEnabled && (async () => {
						while (this.embeddingsQueue.size > 0) {
							await this.embeddingsQueue.processAll();
						}
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
		collections: ReadonlySet<string> = new Set(["app.bsky.feed.post", "app.bsky.feed.repost"]), // for anyone but the user, only process posts and reposts
	): Promise<void> {
		try {
			const isOwnRepo = did === this.userDid;
			const rev = await this.db.getRepoRev(did);

			const repoBytes = await this.xrpc.queryByDid(
				did,
				(c) => c.get("com.atproto.sync.getRepo", { params: { did }, as: "bytes" }),
			);

			const car = CarReader.fromUint8Array(repoBytes);
			assert(
				car.roots.length === 1,
				`expected only 1 root in the car archive; got=${car.roots.length}`,
			);

			const blockmap = collectBlock(car);
			assert(
				blockmap.size > 0,
				`expected at least 1 block in the archive; got=${blockmap.size}`,
			);

			const commit = readBlock(blockmap, car.roots[0], isCommit);

			const records: Array<
				{ uri: ResourceUri; collection: string; record: unknown; inclusion?: PostInclusion }
			> = [];

			for (const { key, cid } of walkMstEntries(blockmap, commit.data)) {
				const [collection, rkey] = key.split("/");

				if (!collections.has(collection) || !(collection in this.processors)) continue;

				const carEntry = blockmap.get(cid.$link);
				assert(carEntry != null, `cid not found in blockmap; cid=${cid}`);

				const record = decodeCbor(carEntry.bytes);

				// for repos that aren't the user's own, ignore records created prior to the last known rev
				// for the user's repo, ignore old records unless they're a follow
				// we always want to process follows, since we don't know whether they've created new records
				if (
					rev && isTid(rev) && rkey < rev
					&& (!isOwnRepo || collection !== "app.bsky.graph.follow")
				) {
					continue;
				}

				const uri = `at://${did}/${collection}/${rkey}` as ResourceUri;

				this.progress.incrementTotal(collection);

				if (collection === "app.bsky.feed.post") {
					if (isOwnRepo) {
						records.push({ uri, collection, record, inclusion: { reason: "self" } });
					} else {
						records.push({
							uri,
							collection,
							record,
							inclusion: { reason: "by_follow" },
						});
					}
				} else {
					records.push({ uri, collection, record });
				}
			}

			// first loop runs synchronously just to update the collection progress bar totals
			// then this loop awaits on every queue add
			for (const { uri, collection, record, inclusion } of records) {
				await this.processors[collection](uri, record, inclusion);
			}

			await this.db.setRepoRev(did, commit.rev);
		} catch (e) {
			console.error(`failed to process repo ${did}: ${errorToString(e)}`);
		}
	}

	async processPost(
		uri: ResourceUri,
		{ inclusion, record, depth = 0 }: ProcessPostOptions,
	): Promise<void> {
		if (depth > this.maxDepth) return;

		const uriHash = h32(uri);
		if (this.seenPosts.has(uriHash)) return;

		let threadView: AppBskyFeedDefs.ThreadViewPost | undefined;
		if (!record) {
			try {
				({ record, threadView } = await this.fetchPost(uri));
			} catch (e) {
				if (e instanceof DOMException && e.name === "TimeoutError") {
					throw e; // handled by BackgroundQueue
				} else if (e instanceof ClientResponseError && e.error === "NotFound") {
					return; // logging this is just noise
				} else {
					console.error(`failed to fetch post record for ${uri}: ${errorToString(e)}`);
					return;
				}
			}
		}
		if (!record) return;

		this.seenPosts.add(uriHash);

		const createdAt = toDateOrNull(record.createdAt)?.getTime();
		if (!createdAt) {
			return console.error(`invalid post createdAt (${uri}): ${record.createdAt}`);
		}

		const altTexts = extractAltTexts(record.embed);
		const altText = altTexts && altTexts.length > 1
			? altTexts.map((alt, i) => `---image ${i + 1}---\n${alt}`).join("\n\n")
			: altTexts?.[0];
		const embed = is(AppBskyEmbedExternal.mainSchema, record.embed)
			? record.embed?.external
			: null;
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
			let quotedRecordView: AppBskyEmbedRecord.ViewRecord | undefined;
			if (threadView?.post.embed) {
				if (
					is(AppBskyEmbedRecord.viewSchema, threadView.post.embed)
					&& is(AppBskyEmbedRecord.viewRecordSchema, threadView.post.embed.record)
				) {
					quotedRecordView = threadView.post.embed.record;
				} else if (
					is(AppBskyEmbedRecordWithMedia.viewSchema, threadView.post.embed)
					&& is(AppBskyEmbedRecord.viewRecordSchema, threadView.post.embed.record.record)
				) {
					quotedRecordView = threadView.post.embed.record.record;
				}
			}
			if (quotedRecordView && is(AppBskyFeedPost.mainSchema, quotedRecordView.value)) {
				void this.postQueue.prepend(quoted, { // prepend so record doesn't stick around in the heap
					depth: depth + 1,
					record: quotedRecordView?.value,
					inclusion: { reason: "quoted_by", context: uri },
				});
			} else {
				void this.postQueue.add(quoted, {
					depth: depth + 1,
					inclusion: { reason: "quoted_by", context: uri },
				});
			}
		}

		// if this post is being processed as a descendant,
		// its ancestors and descendants are already being queued
		if (inclusion.reason === "descendant_of") return;

		// if the post is a reply, queue the root and/or its ancestors
		if (record.reply) {
			// but only if this post is the reason we're looking at this thread;
			// we don't want to re-queue the root for every ancestor in between
			if (inclusion.reason === "ancestor_of") return;

			// if we have enough depth budget to navigate up to the root then down to its descendants,
			// we can just queue the root and it'll handle the rest
			if (depth + 1 < this.maxDepth) {
				void this.postQueue.add(record.reply.root.uri, {
					depth: depth + 1,
					inclusion: { reason: "ancestor_of", context: uri },
				});
				return;
			}
		}

		// otherwise, either the post is a top-level post or there isn't enough depth budget to navigate to siblings & descendants
		// in both cases we'll fetch the thread so that we can queue up replies and, if the post itself is a reply, any ancestors
		if (!threadView) {
			const { thread } = await this.xrpc.query(this.appview, this.getPostThreadQuery(uri))
				.catch((e) => {
					console.error(`error fetching thread for ${uri}`, e);
					return { thread: null };
				});
			if (is(AppBskyFeedDefs.threadViewPostSchema, thread)) threadView = thread;
		}

		// if we couldn't get the thread or it was invalid, just queue up the parent & root uris if we have them
		if (!threadView) {
			if (record.reply?.parent) {
				void this.postQueue.add(record.reply.parent.uri, {
					depth: depth + 1,
					inclusion: { reason: "ancestor_of", context: uri },
				});
			}
			if (record.reply?.root) {
				void this.postQueue.add(record.reply.root.uri, {
					depth: depth + 1,
					inclusion: { reason: "ancestor_of", context: uri },
				});
			}
			return;
		}

		// if we do have the parent, queue up all ancestors
		if (threadView.parent) {
			let parent = threadView.parent;
			while (parent) {
				if (is(AppBskyFeedDefs.threadViewPostSchema, parent)) {
					void this.postQueue.prepend(parent.post.uri, { // prepend so record doesn't stick around in the heap
						record: is(AppBskyFeedPost.mainSchema, parent.post.record)
							? parent.post.record
							: undefined,
						depth: depth + 1,
						inclusion: { reason: "ancestor_of", context: uri },
					});
					parent = parent.parent!; // we don't actually know it's non-null but the while loop will exit anyways if it's not
				} else {
					if (is(AppBskyFeedDefs.blockedPostSchema, parent)) {
						void this.postQueue.add(parent.uri, {
							depth: depth + 1,
							inclusion: { reason: "ancestor_of", context: uri },
						});
					}
					break;
				}
			}
		}

		// for a thread with 5 replies, go up to 20 levels deep
		// for a thread with 200 replies, go up to 3 levels deep
		// in between, scale logarithmically
		const maxThreadDepth = Math.round(
			logarithmicScale([5, 200], [20, 3], threadView.post.replyCount ?? 0),
		);

		this.processPostReplies({
			sourceUri: uri,
			replies: threadView.replies,
			backfillDepth: depth,
			maxThreadDepth,
		});
	}

	private queueWritePost(post: Post) {
		this.toWrite.push(post);
		if (this.toWrite.length > WRITE_POSTS_BATCH_SIZE) void this.writePosts();
	}

	private async writePosts(): Promise<void> {
		while (this.toWrite.length > 0) {
			const batch = this.toWrite.splice(0, WRITE_POSTS_BATCH_SIZE);
			await this.db.insertPosts(batch).catch((e) =>
				console.error("error inserting posts:", e)
			);
			if (this.embeddingsEnabled) this.embeddingsQueue.add(batch);
		}
	}

	private async fetchPost(
		uri: ResourceUri,
	): Promise<{ record?: AppBskyFeedPost.Main; threadView?: AppBskyFeedDefs.ThreadViewPost }> {
		const { repo, collection, rkey } = parseAtUri(uri);
		if (collection !== "app.bsky.feed.post") return {};
		if (repo === BSKY_APP_DID) return {}; // no one looks at these and the replies are full of quote spam

		// first try fetching thread view; gets us more info in one query
		try {
			const { thread } = await this.xrpc.queryNoRetry(
				this.appview,
				this.getPostThreadQuery(uri),
			);
			if (thread.$type === "app.bsky.feed.defs#threadViewPost") {
				if (is(AppBskyFeedPost.mainSchema, thread?.post?.record)) {
					const threadView = is(AppBskyFeedDefs.threadViewPostSchema, thread)
						? thread
						: undefined;
					return { threadView, record: thread.post.record };
				} else {
					return {}; // if a valid thread view was returned, containing an invalid post record, don't bother to fetch it
				}
			} else if (thread.$type === "app.bsky.feed.defs#blockedPost") {
				// fall through to getRecord
			} else if (thread.$type === "app.bsky.feed.defs#notFoundPost") {
				throw new ClientResponseError({ status: 404, data: { error: "NotFound" } });
			} else {
				throw new Error(`unexpected thread type: ${(thread as any).$type}`);
			}
		} catch (e) {
			// if the appview says a post doesn't exist, trust it
			if (e instanceof ClientResponseError && e.error === "NotFound") throw e;
			// if it's a TimeoutError, rethrow it to be handled by BackgroundQueue
			if (e instanceof DOMException && e.name === "TimeoutError") throw e;
			console.warn(
				`failed to fetch thread view for ${uri}, falling back to getRecord: ${
					errorToString(e)
				}`,
			);
		}

		// if that fails, fetch the post record directly
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

		return { record: res.value };
	}

	private processPostReplies(
		{ sourceUri, backfillDepth, replies = [], threadDepth = 1, maxThreadDepth = 20 }: {
			sourceUri: string;
			backfillDepth: number;
			replies?: AppBskyFeedDefs.ThreadViewPost["replies"];
			threadDepth?: number;
			maxThreadDepth?: number;
		},
	) {
		if (threadDepth > maxThreadDepth) return;
		for (const reply of replies) {
			if (!is(AppBskyFeedDefs.threadViewPostSchema, reply)) continue;
			void this.postQueue.prepend(reply.post.uri, { // prepend so record doesn't stick around in the heap
				record: is(AppBskyFeedPost.mainSchema, reply.post.record)
					? reply.post.record
					: undefined,
				depth: backfillDepth + 1,
				inclusion: { reason: "descendant_of", context: sourceUri },
			});
			this.processPostReplies({
				replies: reply.replies,
				sourceUri,
				backfillDepth,
				threadDepth: threadDepth + 1,
				maxThreadDepth,
			});
		}
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

	private getPostThreadQuery = (uri: ResourceUri) => (client: Client) =>
		client.get("app.bsky.feed.getPostThread", {
			params: { uri, depth: 20, parentHeight: 20 },
			signal: AbortSignal.timeout(10_000),
		});

	processors: Record<
		string,
		(uri: ResourceUri, record: unknown, inclusion?: PostInclusion) => Promise<void>
	> = {
		"app.bsky.feed.post": async (uri, record, inclusion) => {
			if (!is(AppBskyFeedPost.mainSchema, record)) return;
			if (!inclusion) throw new Error("inclusion reason is required for app.bsky.feed.post");
			await this.postQueue.add(uri, { record, inclusion }, "app.bsky.feed.post");
		},
		"app.bsky.feed.repost": async (uri, record) => {
			if (!is(AppBskyFeedRepost.mainSchema, record)) return;
			const { repo: reposter } = parseAtUri(uri);
			await this.postQueue.add(record.subject.uri, {
				inclusion: { reason: "reposted_by", context: reposter },
			}, "app.bsky.feed.repost");
		},
		"app.bsky.feed.like": async (_uri, record) => {
			if (!is(AppBskyFeedLike.mainSchema, record)) return;
			await this.postQueue.add(
				record.subject.uri,
				{ inclusion: { reason: "liked_by_self" } },
				"app.bsky.feed.like",
			);
		},
		"app.bsky.graph.follow": async (_uri, record) => {
			if (!is(AppBskyGraphFollow.mainSchema, record)) return;
			await this.repoQueue.add(record.subject);
		},
	};
}

interface ProcessPostOptions {
	inclusion: PostInclusion;
	record?: AppBskyFeedPost.Main;
	depth?: number;
}
