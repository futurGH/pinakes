import { ArgumentParseError, buildCommand } from "@stricli/core";
import type { AppContext } from "../../context.ts";
import { ConfigSettings } from "../../util/db.ts";

export const configSetCommand = buildCommand({
	func: configSetCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [
				{
					placeholder: "key",
					brief: "config key to set",
					parse: (key: string): keyof ConfigSettings => {
						if (!(key in ConfigSettings)) {
							throw new ArgumentParseError("key" as never, key, `invalid config key`);
						}
						return key as keyof ConfigSettings;
					},
				},
				{
					placeholder: "value",
					brief: "value to set",
					parse: String,
				},
			],
		},
	},
	docs: {
		brief: "get a config value",
	},
});

async function configSetCommandImpl(
	this: AppContext,
	_: {},
	key: keyof ConfigSettings,
	value: string,
) {
	await this.db.setConfig(key, value);
	console.log(`${key} = ${value}`);
}
