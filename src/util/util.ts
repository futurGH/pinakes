import {
	AliasNotFoundError,
	ArgumentParseError,
	ArgumentScannerError,
	FlagNotFoundError,
	UnexpectedFlagError,
	UnexpectedPositionalError,
	UnsatisfiedFlagError,
	UnsatisfiedPositionalError,
} from "@stricli/core";

export function toDateOrNull(ts: string | number | undefined | null): Date | null {
	const date = new Date(ts ?? NaN);
	return isNaN(date.getTime()) ? null : date;
}

export function formatException(exc: unknown): string {
	if (exc instanceof AliasNotFoundError) {
		return `no alias found for -${exc.input}`;
	}
	if (exc instanceof FlagNotFoundError) {
		let message = `no flag found for --${exc.input}`;
		if (exc.corrections.length) {
			message += `, did you mean ${
				joinWithGrammar(
					exc.corrections.map((correction) => `--${correction}`),
					{
						conjunction: "or",
						serialComma: true,
					},
				)
			}?`;
		}
	}
	if (exc instanceof ArgumentParseError) {
		return `failed to parse value "${exc.input}" for parameter "${exc.externalFlagNameOrPlaceholder}": ${
			exc.exception instanceof Error ? exc.exception.message : exc.exception
		}`;
	}
	if (exc instanceof UnexpectedFlagError) {
		return `too many arguments for --${exc.externalFlagName}, encountered "${exc.input}" after "${exc.previousInput}"`;
	}
	if (exc instanceof UnsatisfiedFlagError) {
		return `expected input for flag --${exc.externalFlagName}` +
			(exc.nextFlagName ? ` but encountered --${exc.nextFlagName} instead` : "");
	}
	if (exc instanceof UnexpectedPositionalError) {
		return `too many arguments, expected ${exc.expectedCount} but encountered "${exc.input}"`;
	}
	if (exc instanceof UnsatisfiedPositionalError) {
		let message: string;
		if (exc.limit) {
			message = `expected at least ${exc.limit[0]} argument(s) for ${exc.placeholder}`;
			if (exc.limit[1] === 0) {
				message += " but found none";
			} else {
				message += ` but only found ${exc.limit[1]}`;
			}
		} else {
			message = `expected argument for ${exc.placeholder}`;
		}
		return message;
	}
	if (exc instanceof Error && !(exc instanceof ArgumentScannerError)) {
		return exc.stack ?? `${exc}`;
	}
	return `${exc}`;
}

interface ConjuctiveJoin {
	readonly conjunction: string;
	readonly serialComma?: boolean;
}

type JoinGrammar = ConjuctiveJoin;

function joinWithGrammar(parts: readonly string[], grammar: JoinGrammar): string {
	if (parts.length <= 1) {
		return parts[0] ?? "";
	}
	if (parts.length === 2) {
		return parts.join(` ${grammar.conjunction} `);
	}
	let allButLast = parts.slice(0, parts.length - 1).join(", ");
	if (grammar.serialComma) {
		allButLast += ",";
	}
	return [allButLast, grammar.conjunction, parts[parts.length - 1]].join(" ");
}
