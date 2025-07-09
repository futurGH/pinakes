import { Did, isTid, ResourceUri } from "@atcute/lexicons/syntax";
import { buildCommand, numberParser } from "@stricli/core";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import pc from "picocolors";
import type { AppContext } from "../context.ts";
import { Backfill, MAX_DEPTH } from "../lib/backfill.ts";
import { errorToString } from "../util/util.ts";

export const importCommand = buildCommand({
	func: importCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [{
				placeholder: "source",
				brief: "did, handle, or path to a CAR file",
				parse: String,
			}],
		},
		flags: {
			did: {
				kind: "parsed",
				parse: String,
				brief: "(required if CAR file is provided) did of the repository to import",
				optional: true,
			},
			depth: {
				kind: "parsed",
				parse: numberParser,
				brief: "depth of records to process",
				default: `${MAX_DEPTH}`,
			},
			force: {
				kind: "boolean",
				brief: "force re-import of all records, ignoring last sync state",
				default: false,
			},
		},
	},
	docs: { brief: "import a repository from a file, DID, or handle" },
});

async function importCommandImpl(
	this: AppContext,
	{ did, depth, force }: { did?: string; depth: number; force: boolean },
	source: string,
) {
	const userDid = await this.db.getConfig("did");
	if (!userDid) {
		console.error(
			"did not found, please run `pinakes config set did <did>` then run this command again",
		);
		return;
	}

	const backfill = new Backfill(userDid, this.db);
	const { xrpc } = backfill;
	const { idResolver } = xrpc;

	let repoBytes: Uint8Array;
	let lastKnownRev: string | undefined;

	try {
		const stat = await readFile(source).catch(() => null);
		if (stat) {
			if (!did) throw new Error("must pass the `--did` option if importing from CAR file");
			console.log(`importing from CAR file at ${pc.underline(source)}`);
			repoBytes = stat;
		} else {
			if (source.startsWith("did:")) {
				did = source;
			} else {
				const handle = source.startsWith("@") ? source.slice(1) : source;
				did = await idResolver.handle.resolve(handle);
				if (!did) throw new Error(`could not resolve handle: ${handle}`);
			}
			console.log(`fetching repo for ${pc.underline(did)}...`);

			lastKnownRev = force ? undefined : await this.db.getRepoRev(did);
			if (lastKnownRev) {
				console.log(`only including records since last sync (${pc.gray(lastKnownRev)})`);
			}

			repoBytes = await xrpc.queryByDid(
				did,
				(c) =>
					c.get("com.atproto.sync.getRepo", { params: { did: did as Did }, as: "bytes" }),
			);
		}

		console.log("processing repository...");

		{
			using _logging = backfill.progress.start();

			const { rev, iterator } = backfill.processRepoBytes(repoBytes);

			if (!force && lastKnownRev && rev && isTid(rev) && lastKnownRev >= rev) {
				console.log(`no new records since last sync (${pc.gray(lastKnownRev)})`);
				return;
			}

			for await (const { record, rkey, collection } of iterator()) {
				if (collection !== "app.bsky.feed.post" && collection !== "app.bsky.feed.repost") {
					continue;
				}

				if (!force && lastKnownRev && isTid(lastKnownRev) && rkey < lastKnownRev) {
					continue;
				}

				const uri = `at://${did}/${collection}/${rkey}` as ResourceUri;
				backfill.progress.incrementTotal(collection);

				await backfill.processors[collection](uri, record, { reason: "repo_import" });
			}

			await this.db.setRepoRev(did, rev);

			await backfill.postQueue.processAll();

			await backfill.writePosts();
		}

		console.log("import complete!");
		process.exit(0);
	} catch (e) {
		console.error(`failed to import: ${errorToString(e)}`);
	}
}
