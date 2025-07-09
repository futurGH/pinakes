import { IdResolver } from "@atproto/identity";
import { buildCommand, numberParser } from "@stricli/core";
import pc from "picocolors";
import type { AppContext } from "../context.ts";
import { Post, PostWithDistance, SearchPostsOptions } from "../util/db.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";

export const searchCommand = buildCommand({
	func: searchCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [{
				placeholder: "query",
				brief: "search query",
				parse: String,
				optional: true,
			}],
		},
		flags: {
			vector: { kind: "boolean", brief: "whether to use vector search", default: false },
			results: {
				kind: "parsed",
				parse: numberParser,
				brief: "number of results to return",
				default: "25",
			},
			creator: {
				kind: "parsed",
				parse: String,
				brief: "filter by post creator (handle or did) (pass multiple times for OR)",
				optional: true,
				variadic: true,
			},
			parentAuthor: {
				kind: "parsed",
				parse: String,
				brief: "filter by parent post author (handle or did) (pass multiple times for OR)",
				optional: true,
				variadic: true,
			},
			rootAuthor: {
				kind: "parsed",
				parse: String,
				brief: "filter by root post author (handle or did) (pass multiple times for OR)",
				optional: true,
				variadic: true,
			},
			before: {
				kind: "parsed",
				parse: String,
				brief: "filter for posts before a certain date",
				optional: true,
			},
			after: {
				kind: "parsed",
				parse: String,
				brief: "filter for posts after a certain date",
				optional: true,
			},
			order: {
				kind: "parsed",
				brief:
					"asc(ending) or desc(ending); defaults to descending createdAt for text search, ascending distance for vector search",
				optional: true,
				parse: (value): "asc" | "desc" => {
					if (value === "asc" || value === "desc") return value;
					throw new SyntaxError(`invalid order: "${value}"; pass asc or desc`);
				},
			},
			threshold: {
				kind: "parsed",
				parse: numberParser,
				brief:
					"(only for vector search) maximum query distance to include (0-2, 0 is a perfect match)",
				optional: true,
			},
			includeAlt: {
				kind: "boolean",
				brief: "whether to include alt text in search",
				default: false,
			},
		},
	},
	docs: { brief: "search the index for posts" },
});

async function searchCommandImpl(
	this: AppContext,
	{
		vector,
		results,
		creator,
		parentAuthor,
		rootAuthor,
		before,
		after,
		order,
		threshold,
		includeAlt,
	}: {
		vector: boolean;
		results: number;
		creator?: string[];
		parentAuthor?: string[];
		rootAuthor?: string[];
		before?: string;
		after?: string;
		order?: "asc" | "desc";
		threshold?: number;
		includeAlt: boolean;
	},
	query?: string,
) {
	for (const i in creator) {
		if (creator[i].startsWith("did:")) continue;
		if (creator[i].startsWith("@")) creator[i] = creator[i].slice(1);
		const did = await idResolver.handle.resolve(creator[i]);
		if (!did) throw new Error(`invalid creator: ${creator[i]}`);
		creator[i] = did;
	}

	for (const i in parentAuthor) {
		if (parentAuthor[i].startsWith("did:")) continue;
		if (parentAuthor[i].startsWith("@")) parentAuthor[i] = parentAuthor[i].slice(1);
		const did = await idResolver.handle.resolve(parentAuthor[i]);
		if (!did) throw new Error(`invalid parent author: ${parentAuthor[i]}`);
		parentAuthor[i] = did;
	}

	for (const i in rootAuthor) {
		if (rootAuthor[i].startsWith("did:")) continue;
		if (rootAuthor[i].startsWith("@")) rootAuthor[i] = rootAuthor[i].slice(1);
		const did = await idResolver.handle.resolve(rootAuthor[i]);
		if (!did) throw new Error(`invalid root author: ${rootAuthor[i]}`);
		rootAuthor[i] = did;
	}

	const searchOptions: SearchPostsOptions = {
		results,
		creator,
		parentAuthor,
		rootAuthor,
		before,
		after,
		order,
		threshold,
		includeAltText: includeAlt,
	};
	const posts: Array<
		Post & { textDistance?: number; altTextDistance?: number; bestDistance?: number }
	> = vector && query?.length
		? await (async () => {
			console.log("loading embeddings model...");
			await loadEmbeddingsModel();
			console.log("generating embedding for query...");
			const embedding = await extractEmbeddings(query);
			console.log("searching...");
			return this.db.searchPostsVector(embedding, searchOptions);
		})()
		: await this.db.searchPostsText(query, searchOptions);

	if (posts.length === 0) {
		console.log("no results found");
		return;
	}

	for (const post of posts) {
		console.log(await formatPost(post, includeAlt));
		console.log();
	}
}

async function formatPost(post: PostWithDistance, includeAlt: boolean): Promise<string> {
	let result = "";

	const handle = await getHandle(post.creator) ?? post.creator;
	const uri = `at://${post.creator}/app.bsky.feed.post/${post.rkey}`;

	let title = `${pc.blue(`@${handle}`)} ${pc.gray(`(${uri})`)} - ${
		pc.dim(new Date(post.createdAt).toLocaleString())
	}`;
	if (post.bestDistance) {
		title += pc.gray(` (distance: ${post.bestDistance.toFixed(2)})`);
	}

	result += title;

	if (post.replyParent) {
		result += pc.gray(`\n↳  to: ${post.replyParent}`);
	}

	result += `\n${post.text}`;

	if (post.altText) {
		if (includeAlt) {
			let alt = "alt text:";
			if (post.altText.startsWith("---")) {
				alt += `\n${post.altText}`;
			} else {
				alt += ` ${post.altText}`;
			}
			result += `\n${pc.gray(alt)}`;
		} else {
			result += `\n${pc.gray(`alt text omitted (pass --include-alt)`)}`;
		}
	}

	if (post.quoted) {
		result += `\n${pc.gray(`╰──  quoted: ${post.quoted}`)}`;
	}
	return result;
}

const idResolver = new IdResolver();
const handles = new Map<string, string>();
const getHandle = async (did: string) => {
	if (handles.has(did)) return handles.get(did)!;
	const handle = (await idResolver.did.resolveAtprotoData(did))?.handle;
	handles.set(did, handle);
	return handle;
};
