import { buildRouteMap } from "@stricli/core";
import { configGetCommand } from "./get.ts";
import { configSetCommand } from "./set.ts";
import { configDeleteCommand } from "./delete.ts";

export const configRoute = buildRouteMap({
	routes: {
		get: configGetCommand,
		set: configSetCommand,
		delete: configDeleteCommand,
	},
	docs: {
		brief: "manage configuration",
	},
});
