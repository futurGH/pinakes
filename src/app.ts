import { buildApplication, buildRouteMap } from "@stricli/core";
import { backfillCommand } from "./commands/backfill.ts";
import { configRoute } from "./commands/config/index.ts";
import { embeddingsCommand } from "./commands/embeddings.ts";
import { explainCommand } from "./commands/explain.ts";
import { importCommand } from "./commands/import.ts";
import { searchCommand } from "./commands/search.ts";
import { formatException } from "./util/util.ts";

const { name, version, description } =
	(await import("../package.json", { with: { type: "json" } })).default;

const routes = buildRouteMap({
	routes: {
		config: configRoute,
		backfill: backfillCommand,
		embeddings: embeddingsCommand,
		import: importCommand,
		search: searchCommand,
		explain: explainCommand,
	},
	docs: { brief: description },
});

export const app = buildApplication(routes, {
	name,
	versionInfo: { currentVersion: version },
	scanner: { caseStyle: "allow-kebab-for-camel" },
	localization: {
		loadText: () => ({
			headers: {
				usage: "usage",
				aliases: "aliases",
				commands: "commands",
				flags: "flags",
				arguments: "arguments",
			},
			keywords: { default: "default =", separator: "separator =" },
			briefs: {
				help: "print help information and exit",
				helpAll: "print help information (including hidden commands/flags) and exit",
				version: "print version information and exit",
				argumentEscapeSequence: "all subsequent inputs should be interpreted as arguments",
			},
			currentVersionIsNotLatest: () => "current version is not latest",
			noTextAvailableForLocale: ({ requestedLocale, defaultLocale }) =>
				`no text available for locale ${requestedLocale}, defaulting to ${defaultLocale}`,
			noCommandRegisteredForInput: ({ input, corrections }) =>
				`no command exists for ${input}, did you mean ${corrections}?`,
			exceptionWhileParsingArguments: (exc) =>
				`unable to parse arguments, ${formatException(exc)}`,
			exceptionWhileLoadingCommandFunction: (exc) =>
				`unable to load command function, ${formatException(exc)}`,
			exceptionWhileLoadingCommandContext: (exc) =>
				`unable to load command context, ${formatException(exc)}`,
			exceptionWhileRunningCommand: (exc) => `command failed, ${formatException(exc)}`,
			commandErrorResult: (err) => err.message,
		}),
	},
});
