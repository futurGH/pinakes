import { EventEmitter } from "node:events";

export interface QueueOptions {
	hardConcurrency: number; // will never have more than this many tasks running at once
	softConcurrency?: number; // long running tasks "fall off" from this limit after softTimeoutMs but continue to count towards softConcurrency
	softTimeoutMs?: number;
}

// task queue that only stores args rather than promises
// and has a soft concurrency limit that can be exceeded by long-running tasks
export class BackgroundQueue<T extends readonly unknown[]> extends EventEmitter {
	activeTasks = 0; // tasks currently counting against softConcurrency
	get totalRunning() { // all tasks currently running
		return this._running;
	}
	get size() { // waiting jobs not yet started
		return this._queue.length;
	}

	private readonly _queue: T[] = [];
	private _running = 0;

	private readonly _fn: (...args: T) => unknown;
	private readonly _hard: number;
	private readonly _soft?: number;
	private readonly _softTimeout: number;

	constructor(
		fn: (...args: T) => unknown,
		{
			hardConcurrency,
			softConcurrency,
			softTimeoutMs = 10_000,
		}: QueueOptions,
	) {
		super();
		this._fn = fn;
		this._hard = hardConcurrency;
		this._soft = softConcurrency;
		this._softTimeout = softTimeoutMs;
	}

	add(...args: T) {
		this._queue.push(args);
		this.emit("queued");
		this._drain();
	}

	async processAll(): Promise<void> {
		if (this.size === 0 && this._running === 0) return;

		return await new Promise((resolve) => {
			const onDone = () => {
				if (this.size === 0 && this._running === 0) {
					this.off("completed", onDone);
					this.off("error", onDone);
					resolve();
				}
			};
			this.on("completed", onDone);
			this.on("error", onDone);
			// trigger check in case nothing new is emitted
			onDone();
		});
	}

	private _canStart(): boolean {
		if (this._queue.length === 0) return false;
		if (this._running >= this._hard) return false;
		if (this._soft !== undefined && this.activeTasks >= this._soft) return false;
		return true;
	}

	private _drain() {
		while (this._canStart()) {
			const args = this._queue.shift()!;
			this._startTask(args);
		}
	}

	private _startTask(args: T) {
		this._running++;

		let countsTowardsSoft = this._soft !== undefined;
		if (countsTowardsSoft) this.activeTasks++;

		const timeoutId = countsTowardsSoft
			? setTimeout(() => {
				if (countsTowardsSoft) {
					countsTowardsSoft = false;
					this.activeTasks = Math.max(this.activeTasks - 1, 0);
					this._drain();
				}
			}, this._softTimeout)
			: null;

		Promise.resolve()
			.then(() => this._fn(...args))
			.catch((err) => {
				// re-queue AbortErrored tasks, emit everything else
				if (err instanceof DOMException && err.name === "AbortError") {
					this._queue.push(args);
				} else {
					this.emit("error", err);
				}
			})
			.finally(() => {
				this._running--;
				if (timeoutId) clearTimeout(timeoutId);
				if (countsTowardsSoft) {
					// fast tasks that finished before the soft timeout
					this.activeTasks = Math.max(this.activeTasks - 1, 0);
				}
				this.emit("completed");
				this._drain();
			});
	}
}
