import { Client, type ClientResponse, ok, simpleFetchHandler } from "@atcute/client";
import {
	CompositeDidDocumentResolver,
	PlcDidDocumentResolver,
	WebDidDocumentResolver,
} from "@atcute/identity-resolver";
import PQueue from "p-queue";
import { setTimeout as sleep } from "timers/promises";

type ExtractSuccessData<T> = T extends { ok: true; data: infer D } ? D : never;

const retryableStatusCodes = new Set([408, 429, 500, 502, 503, 504]);
const maxRetries = 5;

export class XRPCManager {
	clients = new Map<string, { client: Client; queue: PQueue }>();
	idResolver = new CompositeDidDocumentResolver({
		methods: {
			plc: new PlcDidDocumentResolver(),
			web: new WebDidDocumentResolver(),
		},
	});

	async query<T extends ClientResponse<unknown, unknown>>(
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
					return this.query(service, fn);
				}
				throw error;
			}
		}))!;
	}

	async queryByDid<T extends ClientResponse<unknown, unknown>>(
		did: string,
		fn: (client: Client) => Promise<T>,
	): Promise<ExtractSuccessData<T>> {
		const service = await this.getServiceForDid(did);
		if (!service) throw new Error("no service endpoint found for did " + did);
		return this.query(service, fn);
	}

	async shouldRetry(error: unknown, attempt = 0) {
		if (!error || typeof error !== "object") return false;
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
			client: new Client({ handler: simpleFetchHandler({ service }) }),
			queue: new PQueue(queueOptions),
		};
		this.clients.set(service, data);
		return data;
	}

	getOrCreateClient(service: string, queueOptions?: ConstructorParameters<typeof PQueue>[0]) {
		return this.clients.get(service) ?? this.createClient(service, queueOptions);
	}

	async getServiceForDid(did: string) {
		const didDoc = await this.idResolver.resolve(did as `did:plc:${string}`);
		const endpoint = didDoc.service?.find((s) => s.id === "#atproto_pds")?.serviceEndpoint;
		if (endpoint && typeof endpoint !== "string") {
			throw new Error("invalid service endpoint in did document");
		}
		return endpoint;
	}
}
