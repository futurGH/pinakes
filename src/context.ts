import type { CommandContext } from "@stricli/core";
import { Database } from "./util/db.ts";
import process from "node:process";

export interface AppContext extends CommandContext {
	readonly db: Database;
}

export function buildContext(): AppContext {
	const db = new Database("pinakes.db");
	return {
		db,
		process,
	};
}
