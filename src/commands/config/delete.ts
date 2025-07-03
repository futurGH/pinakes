import { buildCommand } from "@stricli/core";
import type { AppContext } from "../../context.ts";
import { ConfigSettings } from "../../util/db.ts";

export const configDeleteCommand = buildCommand({
	func: configDeleteCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [
				{
					placeholder: "key",
					brief: "config key to delete",
					parse: (key: string): keyof ConfigSettings => {
						if (!(key in ConfigSettings)) {
							throw new SyntaxError("invalid config key");
						}
						return key as keyof ConfigSettings;
					},
				},
			],
		},
	},
	docs: {
		brief: "delete a config value",
	},
});

async function configDeleteCommandImpl(this: AppContext, _: {}, key: keyof ConfigSettings) {
	await this.db.deleteConfig(key);
	console.log(`${key} deleted`);
}
