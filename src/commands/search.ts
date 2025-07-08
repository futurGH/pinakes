import { IdResolver } from "@atproto/identity";
import { buildCommand, numberParser } from "@stricli/core";
import pc from "picocolors";
import type { AppContext } from "../context.ts";
import { SearchPostsOptions } from "../util/db.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";

export const searchCommand = buildCommand({
	func: searchCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [{ placeholder: "query", brief: "search query", parse: String }],
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
				brief: "filter by post creator (handle or did)",
				optional: true,
			},
			parentAuthor: {
				kind: "parsed",
				parse: String,
				brief: "filter by parent post author (handle or did)",
				optional: true,
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
	{ vector, results, creator, parentAuthor, before, after, includeAlt }: {
		vector: boolean;
		results: number;
		creator?: string;
		parentAuthor?: string;
		before?: string;
		after?: string;
		includeAlt: boolean;
	},
	query: string,
) {
	if (creator && !creator.startsWith("did:")) {
		if (creator.startsWith("@")) creator = creator.slice(1);
		const did = await idResolver.handle.resolve(creator);
		if (!did) throw new Error(`invalid creator: ${creator}`);
		creator = did;
	}

	if (parentAuthor && !parentAuthor.startsWith("did:")) {
		if (parentAuthor.startsWith("@")) parentAuthor = parentAuthor.slice(1);
		const did = await idResolver.handle.resolve(parentAuthor);
		if (!did) throw new Error(`invalid parent author: ${parentAuthor}`);
		parentAuthor = did;
	}

	const searchOptions: SearchPostsOptions = {
		results,
		creator,
		parentAuthor,
		before,
		after,
		includeAltText: includeAlt,
	};
	const posts = vector
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
		const handle = await getHandle(post.creator) ?? post.creator;
		const uri = `at://${post.creator}/app.bsky.feed.post/${post.rkey}`;
		console.log(
			`\n${pc.blue(`@${handle}`)} ${pc.gray(`(${uri})`)} - ${
				pc.dim(new Date(post.createdAt).toLocaleString())
			}`,
		);
		if (post.replyParent) {
			console.log(pc.gray(`↳  to: ${post.replyParent}`));
		}
		console.log(post.text);
		if (post.altText) {
			if (includeAlt) {
				console.log(pc.gray(`alt text: ${post.altText}`));
			} else {
				console.log(pc.gray(`alt text omitted (pass --include-alt)`));
			}
		}
		if (post.quoted) {
			console.log(pc.gray(`quoted: ${post.quoted}`));
		}
	}
}

const idResolver = new IdResolver();
const handles = new Map<string, string>();
const getHandle = async (did: string) => {
	if (handles.has(did)) return handles.get(did)!;
	const handle = (await idResolver.did.resolveAtprotoData(did))?.handle;
	handles.set(did, handle);
	return handle;
};
