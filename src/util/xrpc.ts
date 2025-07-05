import { Client, ok, simpleFetchHandler, type XRPCErrorPayload } from "@atcute/client";
import { IdResolver } from "@atproto/identity";
import { setTimeout as sleep } from "node:timers/promises";
import { DidNotFoundError } from "@atproto/identity";
import { LruCache } from "@std/cache";
import { Agent, setGlobalDispatcher } from "undici";

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

const agent = new Agent({ pipelining: 0 });
setGlobalDispatcher(agent);

const retryableStatusCodes = new Set([408, 429, 503, 504]);
const maxRetries = 5;

export class XRPCManager {
	clients = new Map<string, Client>();
	idResolver = new IdResolver();
	didToServiceCache = new LruCache<string, string | null>(100_000);

	async queryByDid<T extends UnknownClientResponse>(
		did: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const service = await this.getServiceForDid(did);
		if (!service) throw new Error("no service endpoint found for did " + did);
		return await this.query(service, fn);
	}

	async query<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
		attempt = 0,
	): Promise<ExtractSuccessData<T>> {
		try {
			return await this.queryNoRetry(service, fn);
		} catch (error) {
			if (await this.shouldRetry(error, attempt++)) {
				return await this.query(service, fn, attempt);
			}
			throw error;
		}
	}

	async queryNoRetry<T extends UnknownClientResponse>(
		service: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		if (service === "https://atproto.brid.gy") throw new Error("bridgy unsupported");

		const client = this.getOrCreateClient(service);
		return await ok(fn(client));
	}

	createClient(service: string) {
		const client = new Client({ handler: simpleFetchHandler({ service }) });
		this.clients.set(service, client);
		return client;
	}

	getOrCreateClient(service: string) {
		return this.clients.get(service) ?? this.createClient(service);
	}

	private async shouldRetry(error: unknown, attempt = 0) {
		if (!error || typeof error !== "object") return false;

		const errorStr = `${error}`.toLowerCase();
		if (errorStr.includes("tcp") || errorStr.includes("network") || errorStr.includes("dns")) {
			return true;
		}

		// this ought to be true, but we just want to rethrow AbortErrors to be handled
		// by the BackgroundQueue so that the task can be retied later
		if (error instanceof DOMException && error.name === "AbortError") return false;
		if (error instanceof TypeError) return false;

		if ("headers" in error && error.headers) {
			let reset;
			if (error.headers instanceof Headers && error.headers.has("rate-limit-reset")) {
				reset = parseInt(error.headers.get("rate-limit-reset")!);
			} else if (typeof error.headers === "object" && "rate-limit-reset" in error.headers) {
				reset = parseInt(`${error.headers["rate-limit-reset"]}`);
			}
			if (reset) {
				console.warn(`rate limited, retrying in ${reset} seconds`, error);
				await sleep(reset * 1000 - Date.now());
				return true;
			}
		}

		if (attempt >= maxRetries) return false;

		if (
			"status" in error && typeof error.status === "number" &&
			retryableStatusCodes.has(error.status)
		) {
			const delay = Math.pow(3, attempt + 1);
			console.warn(`retrying ${error.status} in ${delay} seconds`, error);
			await sleep(delay * 1000);
			return true;
		}

		return false;
	}

	private async getServiceForDid(did: string) {
		if (this.didToServiceCache.has(did)) return this.didToServiceCache.get(did)!;

		const { pds } = await this.idResolver.did.resolveAtprotoData(did)
			.catch((e) => {
				if (e instanceof DidNotFoundError) {
					return { pds: null };
				}
				throw e;
			});
		const service = pds ? new URL(pds).origin : null;
		this.didToServiceCache.set(did, service);
		return service;
	}
}
