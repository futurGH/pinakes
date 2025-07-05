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
import {
	AppBskyEmbedImages,
	AppBskyEmbedRecordWithMedia,
	AppBskyEmbedVideo,
	AppBskyFeedDefs,
	type AppBskyFeedPost,
} from "@atcute/bluesky";
import { is } from "@atcute/lexicons/validations";
import { parseCanonicalResourceUri } from "@atcute/lexicons/syntax";

export function errorToString(error: unknown): string {
	return typeof error === "object" && error !== null && "message" in error
		? `${error.message}`
		: `${error}`;
}

export function toDateOrNull(ts: string | number | undefined | null): Date | null {
	const date = new Date(ts ?? NaN);
	return isNaN(date.getTime()) ? null : date;
}

export function parseAtUri(uri: string) {
	const parsed = parseCanonicalResourceUri(uri);
	if (!parsed.ok) throw new Error(`invalid AT URI: ${uri}`);
	return parsed.value;
}

export function logarithmicScale(
	from: [number, number],
	to: [number, number],
	input: number,
): number {
	if (input <= from[0]) return to[0];
	if (input >= from[1]) return to[1];

	if (from[0] === from[1] || to[0] === to[1]) return to[0];

	const logFromMin = Math.log(from[0]);
	const logFromMax = Math.log(from[1]);
	const logToMin = Math.log(to[0]);
	const logToMax = Math.log(to[1]);
	const logInput = Math.log(input);

	const t = (logInput - logFromMin) / (logFromMax - logFromMin);
	const logResult = logToMin + t * (logToMax - logToMin);

	return Math.exp(logResult);
}

export function extractAltTexts(embed: AppBskyFeedPost.Main["embed"] | undefined) {
	if (!embed) return null;

	let altTexts: string[] = [];
	if (is(AppBskyEmbedImages.mainSchema, embed)) {
		altTexts = embed.images.map((image) => image.alt);
	} else if (is(AppBskyEmbedVideo.mainSchema, embed) && embed.alt) {
		altTexts = [embed.alt];
	} else if (is(AppBskyEmbedRecordWithMedia.mainSchema, embed)) {
		altTexts = extractAltTexts(embed.media) ?? [];
	}
	return altTexts;
}

export function tryExtractRootPostFromThreadView(
	view: AppBskyFeedDefs.ThreadViewPost,
	rootUri: string,
): AppBskyFeedDefs.ThreadViewPost | null {
	let parent = view;
	while (is(AppBskyFeedDefs.threadViewPostSchema, parent.parent)) {
		parent = parent.parent;
	}
	return parent.post.uri === rootUri ? parent : null;
}

export function formatException(exc: unknown): string {
	if (exc instanceof AliasNotFoundError) {
		return `no alias found for -${exc.input}`;
	}
	if (exc instanceof FlagNotFoundError) {
		let message = `no flag found for --${exc.input}`;
		if (exc.corrections.length) {
			message += `, did you mean ${
				joinWithGrammar(exc.corrections.map((correction) => `--${correction}`), "or")
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

function joinWithGrammar(parts: readonly string[], conjunction: string): string {
	if (parts.length <= 1) {
		return parts[0] ?? "";
	}
	if (parts.length === 2) {
		return parts.join(` ${conjunction} `);
	}
	const allButLast = parts.slice(0, parts.length - 1).join(", ") + ",";
	return [allButLast, conjunction, parts[parts.length - 1]].join(" ");
}
