import { buildApplication, buildRouteMap } from "@stricli/core";
import { subdirCommand } from "./commands/subdir/command.ts";
import { nestedRoutes } from "./commands/nested/commands.ts";

const { name, version, description } =
	(await import("../deno.json", { with: { type: "json" } })).default;

const routes = buildRouteMap({
	routes: {
		subdir: subdirCommand,
		nested: nestedRoutes,
	},
	docs: {
		brief: description,
		hideRoute: {
			install: true,
			uninstall: true,
		},
	},
});

export const app = buildApplication(routes, {
	name,
	versionInfo: {
		currentVersion: version,
	},
	scanner: {
		caseStyle: "allow-kebab-for-camel",
	},
});
