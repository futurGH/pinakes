import {
	Client,
	type FetchHandler,
	ok,
	type SimpleFetchHandlerOptions,
	type XRPCErrorPayload,
} from "@atcute/client";
import { IdResolver } from "@atproto/identity";
import PQueue from "p-queue";
import { setTimeout as sleep } from "node:timers/promises";
import { DidNotFoundError } from "@atproto/identity";

type ExtractSuccessData<T> = T extends { ok: true; data: infer D } ? D : never;

type UnknownClientResponse =
	& {
		status: number;
		headers: Headers;
	}
	& ({
		ok: true;
		data: unknown;
	} | {
		ok: false;
		data: XRPCErrorPayload;
	});

const retryableStatusCodes = new Set([408, 429, 500, 502, 503, 504]);
const maxRetries = 5;

const defaultQueueOptions: ConstructorParameters<typeof PQueue>[0] = {
	concurrency: 10,
	interval: 300 * 1000,
	intervalCap: 3000,
};

export class XRPCManager {
	clients = new Map<string, { client: Client; queue: PQueue }>();
	idResolver = new IdResolver();
	didToServiceCache = new Map<string, string | null>();

	async query<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const { client, queue } = this.getOrCreateClient(service);
		let attempt = 0;
		return (await queue.add(async (): Promise<ExtractSuccessData<T>> => {
			try {
				return await ok(fn(client));
			} catch (error) {
				if (await this.shouldRetry(error, attempt++)) {
					return await this.query(service, fn);
				}
				throw error;
			}
		}))!;
	}

	async queryNoRetry<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const { client, queue } = this.getOrCreateClient(service);
		return (await queue.add(async (): Promise<ExtractSuccessData<T>> => {
			return await ok(fn(client));
		}))!;
	}

	async queryByDid<T extends UnknownClientResponse>(
		did: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const service = await this.getServiceForDid(did);
		if (!service) throw new Error("no service endpoint found for did " + did);
		return await this.query(service, fn);
	}

	async shouldRetry(error: unknown, attempt = 0) {
		if (!error || typeof error !== "object") return false;

		const errorStr = `${error}`.toLowerCase();
		if (errorStr.includes("tcp") || errorStr.includes("network") || errorStr.includes("dns")) {
			return true;
		}

		if (error instanceof TypeError) return false;

		if ("headers" in error && error.headers) {
			let reset;
			if (error.headers instanceof Headers && error.headers.has("rate-limit-reset")) {
				reset = parseInt(error.headers.get("rate-limit-reset")!);
			} else if (typeof error.headers === "object" && "rate-limit-reset" in error.headers) {
				reset = parseInt(`${error.headers["rate-limit-reset"]}`);
			}
			if (reset) {
				await sleep(reset * 1000 - Date.now());
				return true;
			}
		}

		if (attempt >= maxRetries) return false;

		if (
			"status" in error && typeof error.status === "number" &&
			retryableStatusCodes.has(error.status)
		) {
			await sleep(Math.pow(2.9, attempt + 1) * 1000);
			return true;
		}

		return false;
	}

	createClient(service: string, queueOptions?: ConstructorParameters<typeof PQueue>[0]) {
		const data = {
			client: new Client({ handler: cacheFetchHandler({ service }) }),
			queue: new PQueue({ ...defaultQueueOptions, ...queueOptions }),
		};
		this.clients.set(service, data);
		return data;
	}

	getOrCreateClient(service: string, queueOptions?: ConstructorParameters<typeof PQueue>[0]) {
		return this.clients.get(service) ?? this.createClient(service, queueOptions);
	}

	async getServiceForDid(did: string) {
		if (this.didToServiceCache.has(did)) return this.didToServiceCache.get(did)!;

		const { pds } = await this.idResolver.did.resolveAtprotoData(did)
			.catch((e) => {
				if (e instanceof DidNotFoundError) {
					return { pds: null };
				}
				throw e;
			});
		this.didToServiceCache.set(did, pds);
		return pds;
	}
}

const fetchHandlerCache = new Map<string, Promise<Response>>();
export function cacheFetchHandler(
	{ service, fetch: _fetch = fetch }: SimpleFetchHandlerOptions,
): FetchHandler {
	return async (pathname, init) => {
		const cacheKey = `${service}/${pathname}:${init.body}`;
		if (fetchHandlerCache.has(cacheKey)) {
			return await fetchHandlerCache.get(cacheKey)!;
		}

		const url = new URL(pathname, service);
		// @ts-expect-error — RequestInit type mismatch between node and deno
		const promise = _fetch(url, init);
		fetchHandlerCache.set(cacheKey, promise);
		const res = await promise;
		if (retryableStatusCodes.has(res.status)) fetchHandlerCache.delete(cacheKey);
		return res;
	};
}
