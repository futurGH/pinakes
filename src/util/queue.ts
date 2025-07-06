import { EventEmitter } from "node:events";

export interface QueueOptions {
	hardConcurrency: number; // will never have more than this many tasks running at once
	softConcurrency?: number; // long running tasks "fall off" from this limit after softTimeoutMs but continue to count towards softConcurrency
	softTimeoutMs?: number; // ms before long-running tasks stop counting towards softConcurrency
	maxQueueSize?: number; // maximum number of tasks allowed to be waiting in the queue
}

// task queue that:
// - only stores arguments, not entire promises
// - has a soft concurrency limit that can be exceeded by long-running tasks
// - processes tasks sequentially in the background; add() resolves as soon as a task is added to the queue
export class BackgroundQueue<T extends readonly unknown[]> extends EventEmitter {
	private readonly _queue: T[] = [];
	private _active = 0;
	private _running = 0;

	private readonly _fn: (...args: T) => unknown;
	private readonly _hard: number;
	private readonly _soft?: number;
	private readonly _softTimeout: number;
	private readonly _maxQueueSize?: number;
	private promisesWaitingForSpace: Array<() => void> = [];

	get activeTasks() { // tasks currently counting against softConcurrency
		return this._active;
	}
	get runningTasks() { // all tasks currently running
		return this._running;
	}
	get size() { // waiting jobs not yet started
		return this._queue.length;
	}

	constructor(
		fn: (...args: T) => unknown,
		{
			hardConcurrency,
			softConcurrency,
			softTimeoutMs = 10_000,
			maxQueueSize,
		}: QueueOptions,
	) {
		super();
		this._fn = fn;
		this._hard = hardConcurrency;
		this._soft = softConcurrency;
		this._softTimeout = softTimeoutMs;
		this._maxQueueSize = maxQueueSize;
	}

	async add(...args: T): Promise<void> {
		// wait for space if max queue size is reached
		if (this._maxQueueSize && this.size >= this._maxQueueSize) {
			await new Promise<void>((resolve) => {
				this.promisesWaitingForSpace.push(resolve);
			});
		}

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
			onDone(); // trigger check in case nothing new is emitted
		});
	}

	private _canStart(): boolean {
		if (this._queue.length === 0) return false;
		if (this._running >= this._hard) return false;
		if (this._soft !== undefined && this._active >= this._soft) return false;
		return true;
	}

	private _drain() {
		while (this._canStart()) {
			const args = this._queue.shift()!;
			// notify pending add() calls of available space in queue
			this._notifyWaiting();
			this._startTask(args);
		}
	}

	private _notifyWaiting() {
		if (!this._maxQueueSize) return;
		while (this.promisesWaitingForSpace.length && this.size < this._maxQueueSize) {
			this.promisesWaitingForSpace.shift()?.();
		}
	}

	private _startTask(args: T) {
		this._running++;

		let countsTowardsSoft = this._soft !== undefined;
		if (countsTowardsSoft) this._active++;

		const timeoutId = countsTowardsSoft
			? setTimeout(() => {
				if (countsTowardsSoft) {
					countsTowardsSoft = false;
					this._active = Math.max(this._active - 1, 0);
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
					this._notifyWaiting();
				} else {
					this.emit("error", err);
				}
			})
			.finally(() => {
				this._running--;
				if (timeoutId) clearTimeout(timeoutId);
				if (countsTowardsSoft) {
					// fast tasks that finished before the soft timeout
					this._active = Math.max(this._active - 1, 0);
				}
				this.emit("completed");
				this._notifyWaiting();
				this._drain();
			});
	}
}
