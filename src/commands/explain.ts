import { parseCanonicalResourceUri } from "@atcute/lexicons";
import { IdResolver } from "@atproto/identity";
import { buildCommand } from "@stricli/core";
import pc from "picocolors";
import type { AppContext } from "../context.ts";
import { linkAtUri, parseAtUri } from "../util/util.ts";

export const explainCommand = buildCommand({
	func: explainCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [{ placeholder: "uri", brief: "post uri to explain", parse: String }],
		},
	},
	docs: { brief: "explain why a post is in the index" },
});

async function explainCommandImpl(this: AppContext, _: {}, uri: string) {
	const seen = new Set<string>();

	const idResolver = new IdResolver();

	const explainUri = async (currentUri: string, prefix = "") => {
		if (seen.has(currentUri)) {
			console.log(`${prefix}╰─ ${pc.yellow("recursion detected:")} ${pc.blue(currentUri)}`);
			return;
		}
		seen.add(currentUri);

		try {
			const { repo, rkey, collection } = parseAtUri(currentUri);

			// if the context isn't a post, we can't look it up further
			if (collection !== "app.bsky.feed.post") {
				console.log(`${prefix}╰─ ${pc.magenta(collection)} by ${pc.blue(repo)}`);
				return;
			}

			const post = await this.db.getPost(repo, rkey);

			if (!post) {
				console.log(`${prefix}╰─ ${pc.red("post not found in index")}`);
				return;
			}

			let reasonText = pc.green(post.inclusionReason);
			if (post.inclusionContext) {
				const parsedInclusionContextUri = parseCanonicalResourceUri(post.inclusionContext);
				if (parsedInclusionContextUri.ok) {
					const handle =
						(await idResolver.did.resolveAtprotoData(
							parsedInclusionContextUri.value.repo,
						).catch(() => null))?.handle;
					if (handle) reasonText += ` ${pc.cyan(handle)}`;
				}
				reasonText += ` ${pc.yellow(linkAtUri(post.inclusionContext))}`;
			}

			console.log(`${prefix}${post.inclusionContext ? "├" : "╰"}─ ${reasonText}`);

			if (post.inclusionContext) {
				await explainUri(post.inclusionContext!, `${prefix}│  `);
			}
		} catch (e) {
			console.log(`${prefix}╰─ ${pc.red("invalid uri:")} ${pc.blue(currentUri)}`);
		}
	};

	console.log(`╭─ ${pc.blue(linkAtUri(uri))}`);
	await explainUri(uri, "│  ");
}
