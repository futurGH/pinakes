import { createClient } from "@libsql/client/node";
import { Kysely, type SelectQueryBuilder, sql } from "kysely";
import { Expression } from "kysely";
import { SqlBool } from "kysely";
import { LibsqlDialect } from "kysely-libsql";
import { PUBLIC_APPVIEW_URL } from "../lib/backfill.ts";
import { toDateOrNull } from "./util.ts";

export type PostInclusionReason =
	| "self"
	| "liked_by_self"
	| "reposted_by"
	| "ancestor_of"
	| "descendant_of"
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
			dialect: new LibsqlDialect({
				client: createClient({ url: this.url, concurrency: 100 }),
			}),
		});
	}

	async init() {
		if (this.url.includes("file:") || this.url === ":memory:") {
			await sql`PRAGMA journal_mode=WAL;`.execute(this.db);
		}

		await /* dprint-ignore */ this.db.schema
			.createTable("post")
			.ifNotExists()
			.addColumn("creator", "varchar", (col) => col.notNull())
			.addColumn("rkey", "varchar", (col) => col.notNull())
			.addColumn("createdAt", "integer", (col) => col.notNull())
			.addColumn("text", "varchar", (col) => col.notNull())
			.addColumn("embedding", sql`F16_BLOB(384)`)
			.addColumn("altText", "varchar")
			.addColumn("altTextEmbedding", sql`F16_BLOB(384)`)
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
		await /* dprint-ignore */ this.db.schema
			.createIndex("post_creator_idx")
			.ifNotExists()
			.on("post")
			.column("creator")
			.execute();
		await sql`CREATE INDEX IF NOT EXISTS post_embedding_idx ON post (libsql_vector_idx(embedding, 'max_neighbors=5', 'compress_neighbors=float8'))`
			.execute(this.db);
		await sql`CREATE INDEX IF NOT EXISTS "post_altTextEmbedding_idx" ON post (libsql_vector_idx("altTextEmbedding", 'max_neighbors=5', 'compress_neighbors=float8'))`
			.execute(this.db);

		await /* dprint-ignore */ this.db.schema
			.createTable("repo")
			.ifNotExists()
			.addColumn("did", "varchar", (col) => col.primaryKey())
			.addColumn("rev", "varchar", (col) => col.notNull())
			.execute();

		await /* dprint-ignore */ this.db.schema
			.createTable("config")
			.ifNotExists()
			.addColumn("key", "varchar", (col) => col.primaryKey())
			.addColumn("value", "varchar", (col) => col.notNull())
			.execute();
	}

	insertPost(
		post: Omit<Post, "embedding" | "altTextEmbedding"> & {
			embedding?: Float32Array | null;
			altTextEmbedding?: Float32Array | null;
		},
	) {
		return this.insertPosts([post]);
	}

	async insertPosts(
		posts:
			(Omit<Post, "embedding" | "altTextEmbedding"> & {
				embedding?: Float32Array | null;
				altTextEmbedding?: Float32Array | null;
			})[],
	) {
		await this.db.insertInto("post").values(
			posts.map((post) => ({
				creator: post.creator,
				rkey: post.rkey,
				createdAt: post.createdAt,
				text: post.text,
				embedding: post.embedding ? formatVector(post.embedding) : null,
				altText: post.altText,
				altTextEmbedding: post.altTextEmbedding
					? formatVector(post.altTextEmbedding)
					: null,
				replyParent: post.replyParent,
				replyRoot: post.replyRoot,
				quoted: post.quoted,
				embedTitle: post.embedTitle,
				embedDescription: post.embedDescription,
				embedUrl: post.embedUrl,
				inclusionReason: post.inclusionReason,
				inclusionContext: post.inclusionContext,
			})),
		).onConflict((oc) =>
			oc.doUpdateSet((eb) => ({
				creator: eb.ref("excluded.creator"),
				rkey: eb.ref("excluded.rkey"),
				createdAt: eb.ref("excluded.createdAt"),
				text: eb.ref("excluded.text"),
				embedding: eb.ref("excluded.embedding"),
				altText: eb.ref("excluded.altText"),
				altTextEmbedding: eb.ref("excluded.altTextEmbedding"),
				replyParent: eb.ref("excluded.replyParent"),
				replyRoot: eb.ref("excluded.replyRoot"),
				quoted: eb.ref("excluded.quoted"),
				embedTitle: eb.ref("excluded.embedTitle"),
				embedDescription: eb.ref("excluded.embedDescription"),
				embedUrl: eb.ref("excluded.embedUrl"),
				inclusionReason: eb.ref("excluded.inclusionReason"),
				inclusionContext: eb.ref("excluded.inclusionContext"),
			}))
		).execute();
	}

	async getPost(creator: string, rkey: string) {
		return await this.db.selectFrom("post").selectAll().where("creator", "=", creator).where(
			"rkey",
			"=",
			rkey,
		).executeTakeFirst();
	}

	async searchPostsText(text: string | undefined, options: SearchPostsOptions) {
		const { includeAltText, order = "desc" } = options;

		let qb = this.db.selectFrom("post").selectAll().orderBy("createdAt", order);
		if (text || includeAltText) {
			qb = qb.where((eb) => {
				const q: Expression<SqlBool>[] = [];
				if (text) q.push(eb("text", "like", `%${text}%`));
				if (includeAltText) q.push(eb("altText", "like", `%${text}%`));
				return eb.or(q);
			});
		}

		return await this.applySearchPostsOptions(qb, options).execute();
	}

	async searchPostsVector(
		embedding: Float32Array,
		options: SearchPostsOptions,
	): Promise<PostWithDistance[]> {
		const { includeAltText, threshold = 0.5, order = "asc" } = options;

		const distanceExpr = sql<number>`vector_distance_cos(embedding, ${
			formatVector(embedding)
		})`;
		const altDistanceExpr = sql<
			number
		>`case when "altTextEmbedding" is not null then vector_distance_cos("altTextEmbedding", ${
			formatVector(embedding)
		}) end`;

		const bestDistanceExpr = includeAltText
			? sql<number>`
				case
					when "altTextEmbedding" is not null
					then min(${distanceExpr}, ${altDistanceExpr})
					else ${distanceExpr}
				end
			`
			: distanceExpr;

		const qb = this.db.with(
			"results",
			(eb) =>
				this.applySearchPostsOptions(
					eb.selectFrom("post").selectAll().select([
						distanceExpr.as("textDistance"),
						...(includeAltText ? [altDistanceExpr.as("altTextDistance")] : []),
						bestDistanceExpr.as("bestDistance"),
					]).where("embedding", "is not", null).orderBy("bestDistance", order),
					options,
				),
		).selectFrom("results").selectAll().where("bestDistance", "<=", threshold);

		return await qb.execute();
	}

	private applySearchPostsOptions<T extends SelectQueryBuilder<Tables, "post", Post>>(
		qb: T,
		options: SearchPostsOptions,
	): T {
		const { results, creator, parentAuthor, rootAuthor, before, after } = options;

		const beforeTs = toDateOrNull(before);
		if (before && !beforeTs) throw new Error(`invalid 'before' date: ${before}`);
		const afterTs = toDateOrNull(after);
		if (after && !afterTs) throw new Error(`invalid 'after' date: ${after}`);

		return qb.where((eb) => {
			const conditions: Expression<SqlBool>[] = [];

			if (creator) {
				conditions.push(eb.or(creator.map((creator) => eb("creator", "=", creator))));
			}
			if (parentAuthor?.length) {
				conditions.push(
					eb.or(
						parentAuthor.map((parent) => eb("replyParent", "like", `at://${parent}%`)),
					),
				);
			}
			if (rootAuthor?.length) {
				conditions.push(
					eb.or(rootAuthor.map((root) => eb("replyRoot", "like", `at://${root}%`))),
				);
			}
			if (beforeTs) conditions.push(eb("createdAt", "<", beforeTs.getTime()));
			if (afterTs) conditions.push(eb("createdAt", ">", afterTs.getTime()));

			return eb.and(conditions);
		}).limit(results) as T;
	}

	async getRepoRev(did: string) {
		return await this.db.selectFrom("repo").select("rev").where("did", "=", did)
			.executeTakeFirst().then((r) => r?.rev);
	}

	async setRepoRev(did: string, rev: string) {
		await this.db.insertInto("repo").values({ did, rev }).onConflict((oc) =>
			oc.column("did").doUpdateSet({ rev })
		).execute();
	}

	async getConfig<K extends keyof ConfigSettings>(
		key: K,
	): Promise<ConfigSettings[K] | undefined> {
		return await this.db.selectFrom("config").select("value").where("key", "=", key)
			.executeTakeFirst().then((r) => r?.value);
	}

	async setConfig<K extends keyof ConfigSettings>(
		key: K,
		value: NonNullable<ConfigSettings[K]>,
	): Promise<void> {
		await this.db.insertInto("config").values({ key, value }).onConflict((oc) =>
			oc.column("key").doUpdateSet({ value })
		).execute();
	}

	async deleteConfig<K extends keyof ConfigSettings>(key: K): Promise<void> {
		await this.db.deleteFrom("config").where("key", "=", key).execute();
	}
}

export const ConfigSettings = {
	did: { description: "the user's DID", default: "" as string | undefined },
	appview: {
		description: "the appview to query",
		default: PUBLIC_APPVIEW_URL as string | undefined,
	},
} as const;
export type ConfigSettings = {
	[K in keyof typeof ConfigSettings]: (typeof ConfigSettings)[K]["default"];
};

export type PostWithDistance = Post & {
	textDistance?: number;
	altTextDistance?: number;
	bestDistance?: number;
};

export interface SearchPostsOptions {
	results: number;
	creator?: string[];
	parentAuthor?: string[];
	rootAuthor?: string[];
	before?: string;
	after?: string;
	includeAltText?: boolean;
	order?: "asc" | "desc";
	threshold?: number;
}

export const formatVector = (embedding: Float32Array) => {
	return sql<Float32Array>`vector16(vector32(${`[${embedding.join(",")}]`}))`;
};
