import { buildCommand, numberParser } from "@stricli/core";
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
				default: true,
			},
		},
	},
	docs: {
		brief: "backfill the index with all posts you might've seen",
	},
});

async function backfillCommandImpl(
	this: AppContext,
	{ depth, embeddings }: { depth: number; embeddings: boolean },
) {
	const did = await this.db.getConfig("did");
	if (!did) {
		console.error(
			"did not found, please run `pinakes config set did <did>` then run this command again",
		);
		return;
	}

	if (!embeddings) {
		console.warn(
			"backfilling without generating embeddings; you can run `pinakes embeddings` later to generate embeddings",
		);
	}

	const backfill = new Backfill(did, this.db, { generateEmbeddings: embeddings, depth });
	await backfill.backfill().catch(console.error);
}
