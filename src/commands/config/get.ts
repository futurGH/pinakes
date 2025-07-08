import { buildCommand } from "@stricli/core";
import pc from "picocolors";
import type { AppContext } from "../../context.ts";
import { ConfigSettings } from "../../util/db.ts";

export const configGetCommand = buildCommand({
	func: configGetCommandImpl,
	parameters: {
		positional: {
			kind: "tuple",
			parameters: [{
				placeholder: "key",
				brief: "config key to get value for",
				parse: (key: string): keyof ConfigSettings => {
					if (!(key in ConfigSettings)) {
						throw new SyntaxError("invalid config key");
					}
					return key as keyof ConfigSettings;
				},
			}],
		},
	},
	docs: { brief: "get a config value" },
});

async function configGetCommandImpl(this: AppContext, _: {}, key: keyof ConfigSettings) {
	let value = await this.db.getConfig(key);
	if (!value) {
		value = pc.gray("not set");
		const def = ConfigSettings[key].default;
		if (def) value += ` (default: ${def})`;
	}
	console.log(`${key} = ${value}`);
}
