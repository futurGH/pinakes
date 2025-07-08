import { buildRouteMap } from "@stricli/core";
import { configDeleteCommand } from "./delete.ts";
import { configGetCommand } from "./get.ts";
import { configSetCommand } from "./set.ts";

export const configRoute = buildRouteMap({
	routes: { get: configGetCommand, set: configSetCommand, delete: configDeleteCommand },
	docs: { brief: "manage configuration" },
});
