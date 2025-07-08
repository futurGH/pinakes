import { buildCommand, numberParser } from "@stricli/core";
import pc from "picocolors";
import type { AppContext } from "../context.ts";
import { Backfill, MAX_DEPTH } from "../lib/backfill.ts";

export const backfillCommand = buildCommand({
	func: backfillCommandImpl,
	parameters: {
		flags: {
			depth: {
				kind: "parsed",
				parse: numberParser,
				brief: "backfill depth",
				default: `${MAX_DEPTH}`,
			},
			embeddings: {
				kind: "boolean",
				brief: "whether to generate vector embeddings",
				default: false,
			},
			appview: {
				kind: "parsed",
				parse: String,
				brief: "custom appview to use for fetching posts",
				optional: true,
			},
		},
	},
	docs: { brief: "backfill the index with all posts you might've seen" },
});

async function backfillCommandImpl(
	this: AppContext,
	{ depth, embeddings, appview }: { depth: number; embeddings: boolean; appview?: string },
) {
	const did = await this.db.getConfig("did");
	if (!did) {
		console.error(
			"did not found, please run `pinakes config set did <did>` then run this command again",
		);
		return;
	}

	appview ??= await this.db.getConfig("appview");

	if (!embeddings) {
		console.warn(
			`backfilling without generating embeddings; you can run ${
				pc.green("`pinakes embeddings`")
			} later to generate embeddings`,
		);
	}

	const backfill = new Backfill(did, this.db, { embeddings, depth, appview });
	await backfill.backfill().catch(console.error);
}
