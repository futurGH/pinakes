import type { CommandContext } from "@stricli/core";
import process from "node:process";
import { Database } from "./util/db.ts";

export interface AppContext extends CommandContext {
	readonly db: Database;
}

export async function buildContext(): Promise<AppContext> {
	const db = new Database("pinakes.db");
	await db.init();

	return { db, process };
}
