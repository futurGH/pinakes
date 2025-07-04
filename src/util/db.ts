import { Kysely, type SelectQueryBuilder, sql } from "kysely";
import { LibsqlDialect } from "kysely-libsql";
import { createClient } from "@libsql/client/node";
import { toDateOrNull } from "./util.ts";

export type PostInclusionReason =
	| "self"
	| "liked_by_self"
	| "reposted_by"
	| "same_thread_as"
	| "quoted_by"
	| "linked_by"
	| "by_follow";

export interface Post {
	creator: string;
	rkey: string;
	createdAt: number;
	text: string;
	embedding?: Float32Array | null;
	altText?: string | null;
	altTextEmbedding?: Float32Array | null;
	replyParent?: string | null;
	replyRoot?: string | null;
	quoted?: string | null;
	embedTitle?: string | null;
	embedDescription?: string | null;
	embedUrl?: string | null;
	inclusionReason: PostInclusionReason;
	inclusionContext?: string | null;
}

export interface Repo {
	did: string;
	rev: string;
}

export interface Config {
	key: string;
	value: string;
}

export interface Tables {
	post: Post;
	repo: Repo;
	config: Config;
}

export class Database {
	url: string;
	db: Kysely<Tables>;

	constructor(path: string) {
		this.url = path.includes(":") ? path : `file:${path}`;
		this.db = new Kysely({
			dialect: new LibsqlDialect({ client: createClient({ url: this.url, concurrency: 100 }) }),
		});
	}

	async init() {
		if (this.url.includes("file:") || this.url === ":memory:") {
			await sql`PRAGMA journal_mode=WAL;`.execute(this.db);
		}

		await this.db.schema.createTable("post")
			.ifNotExists()
			.addColumn("creator", "varchar", (col) => col.notNull())
			.addColumn("rkey", "varchar", (col) => col.notNull())
			.addColumn("createdAt", "integer", (col) => col.notNull())
			.addColumn("text", "varchar", (col) => col.notNull())
			.addColumn("embedding", sql`F32_BLOB(384)`)
			.addColumn("altText", "varchar")
			.addColumn("altTextEmbedding", sql`F32_BLOB(384)`)
			.addColumn("replyParent", "varchar")
			.addColumn("replyRoot", "varchar")
			.addColumn("quoted", "varchar")
			.addColumn("embedTitle", "varchar")
			.addColumn("embedDescription", "varchar")
			.addColumn("embedUrl", "varchar")
			.addColumn("inclusionReason", "varchar", (col) => col.notNull())
			.addColumn("inclusionContext", "varchar")
			.addPrimaryKeyConstraint("pk_post", ["creator", "rkey"])
			.execute();
		await this.db.schema.createIndex("post_creator_idx").ifNotExists().on("post").column("creator")
			.execute();
		await sql`CREATE INDEX IF NOT EXISTS post_embedding_idx ON post (libsql_vector_idx(embedding, 'compress_neighbors=float8'))`
			.execute(this.db);

		await this.db.schema.createTable("repo")
			.ifNotExists()
			.addColumn("did", "varchar", (col) => col.primaryKey())
			.addColumn("rev", "varchar", (col) => col.notNull())
			.execute();

		await this.db.schema.createTable("config")
			.ifNotExists()
			.addColumn("key", "varchar", (col) => col.primaryKey())
			.addColumn("value", "varchar", (col) => col.notNull())
			.execute();
	}

	async insertPost(
		post: Omit<Post, "embedding" | "altTextEmbedding"> & {
			embedding?: Float32Array | null;
			altTextEmbedding?: Float32Array | null;
		},
	) {
		return this.insertPosts([post]);
	}

	async insertPosts(posts: (Omit<Post, "embedding" | "altTextEmbedding"> & {
		embedding?: Float32Array | null;
		altTextEmbedding?: Float32Array | null;
	})[]) {
		await this.db.insertInto("post")
			.values(posts.map((post) => ({
				creator: post.creator,
				rkey: post.rkey,
				createdAt: post.createdAt,
				text: post.text,
				embedding: post.embedding ? formatVector(post.embedding) : null,
				altText: post.altText,
				altTextEmbedding: post.altTextEmbedding ? formatVector(post.altTextEmbedding) : null,
				replyParent: post.replyParent,
				replyRoot: post.replyRoot,
				quoted: post.quoted,
				embedTitle: post.embedTitle,
				embedDescription: post.embedDescription,
				embedUrl: post.embedUrl,
				inclusionReason: post.inclusionReason,
				inclusionContext: post.inclusionContext,
			})))
			.onConflict((oc) => oc.doNothing())
			.execute();
	}

	async getPost(creator: string, rkey: string) {
		return await this.db.selectFrom("post")
			.selectAll()
			.where("creator", "=", creator)
			.where("rkey", "=", rkey)
			.executeTakeFirst();
	}

	async searchPostsText(text: string, options: SearchPostsOptions) {
		const { includeAltText } = options;

		const qb = this.db.selectFrom("post")
			.selectAll()
			.where((eb) => {
				const q = eb("text", "ilike", `%${text}%`);
				if (includeAltText) return q.or("altText", "ilike", `%${text}%`);
				return q;
			})
			.orderBy("createdAt", "desc");

		return await this.applySearchPostsOptions(qb, options).execute();
	}

	async searchPostsVector(embedding: Float32Array, options: SearchPostsOptions) {
		const { includeAltText } = options;

		const distanceExpr = sql<number>`vector_distance_cos(embedding, ${formatVector(embedding)})`;
		const altDistanceExpr = sql<number>`vector_distance_cos("altTextEmbedding", ${
			formatVector(embedding)
		})`;

		const bestDistanceExpr = includeAltText
			? sql<number>`
				case
					when "altTextEmbedding" is not null
					then min(${distanceExpr}, ${altDistanceExpr})
					else ${distanceExpr}
				end
			`
			: distanceExpr;

		const qb = this.db
			.selectFrom("post")
			.select([
				"rkey",
				"createdAt",
				"creator",
				"text",
				"embedding",
				"altText",
				"altTextEmbedding",
				"replyParent",
				"replyRoot",
				"quoted",
				"embedTitle",
				"embedDescription",
				"embedUrl",
				"inclusionReason",
				"inclusionContext",
				distanceExpr.as("textDistance"),
				...(includeAltText ? [altDistanceExpr.as("altTextDistance")] : []),
				bestDistanceExpr.as("bestDistance"),
			])
			.orderBy("bestDistance", "asc");

		return await this.applySearchPostsOptions(qb, options).execute();
	}

	private applySearchPostsOptions<T extends SelectQueryBuilder<Tables, "post", Post>>(
		qb: T,
		options: SearchPostsOptions,
	): T {
		const { results, creator, parentAuthor, before, after } = options;

		const beforeTs = toDateOrNull(before);
		if (before && !beforeTs) throw new Error(`invalid 'before' date: ${before}`);
		const afterTs = toDateOrNull(after);
		if (after && !afterTs) throw new Error(`invalid 'after' date: ${after}`);

		// avoiding several "not assignable to type T" errors on following lines
		// deno-lint-ignore no-explicit-any
		let _qb: any = qb;

		if (creator) _qb = _qb.where("creator", "=", creator);
		if (parentAuthor) _qb = _qb.where("replyParent", "like", `at://${parentAuthor}%`);
		if (beforeTs) _qb = _qb.where("createdAt", "<", beforeTs.getTime());
		if (afterTs) _qb = _qb.where("createdAt", ">", afterTs.getTime());
		return _qb.limit(results);
	}

	async getRepoRev(did: string) {
		return await this.db.selectFrom("repo")
			.select("rev")
			.where("did", "=", did)
			.executeTakeFirst()
			.then((r) => r?.rev);
	}

	async setRepoRev(did: string, rev: string) {
		await this.db.insertInto("repo")
			.values({
				did,
				rev,
			})
			.onConflict((oc) => oc.column("did").doUpdateSet({ rev }))
			.execute();
	}

	async getConfig<K extends keyof ConfigSettings>(key: K): Promise<ConfigSettings[K] | undefined> {
		return await this.db.selectFrom("config")
			.select("value")
			.where("key", "=", key)
			.executeTakeFirst()
			.then((r) => r?.value);
	}

	async setConfig<K extends keyof ConfigSettings>(
		key: K,
		value: NonNullable<ConfigSettings[K]>,
	): Promise<void> {
		await this.db.insertInto("config")
			.values({
				key,
				value,
			})
			.onConflict((oc) => oc.column("key").doUpdateSet({ value }))
			.execute();
	}

	async deleteConfig<K extends keyof ConfigSettings>(key: K): Promise<void> {
		await this.db.deleteFrom("config")
			.where("key", "=", key)
			.execute();
	}
}

export const ConfigSettings = {
	did: {
		description: "The user's DID",
		default: "" as string | undefined,
	},
} as const;
export type ConfigSettings = {
	[K in keyof typeof ConfigSettings]: (typeof ConfigSettings)[K]["default"];
};

export interface SearchPostsOptions {
	results: number;
	creator?: string;
	parentAuthor?: string;
	before?: string;
	after?: string;
	includeAltText?: boolean;
}

const formatVector = (embedding: Float32Array) => {
	return sql<Float32Array>`vector32(${`[${embedding.join(",")}]`})`;
};
