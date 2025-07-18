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

	private readonly runningTasks = new Set<T>();
	private readonly _fn: (...args: T) => unknown;
	private readonly _hard: number;
	private readonly _soft?: number;
	private readonly _softTimeout: number;
	private readonly _maxQueueSize?: number;

	private promisesWaitingForSpace: Array<() => void> = [];

	get active() { // tasks currently counting against softConcurrency
		return this._active;
	}
	get running() { // all tasks currently running
		return this.runningTasks.size;
	}
	get size() { // waiting jobs not yet started
		return this._queue.length;
	}

	constructor(
		fn: (...args: T) => unknown,
		{ hardConcurrency, softConcurrency, softTimeoutMs = 10_000, maxQueueSize }: QueueOptions,
	) {
		super();
		this._fn = fn;
		this._hard = hardConcurrency;
		this._soft = softConcurrency;
		this._softTimeout = softTimeoutMs;
		this._maxQueueSize = maxQueueSize;
	}

	async add(...args: T): Promise<void> {
		this.emit("queued");

		// wait for space if max queue size is reached
		await this._waitForSpace();

		this._queue.push(args);
		this._drain();
	}

	async prepend(...args: T): Promise<void> {
		this.emit("queued");

		// wait for space if max queue size is reached
		await this._waitForSpace();

		this._queue.unshift(args);
		this._drain();
	}

	async processAll(): Promise<void> {
		if (this.size === 0 && this.running === 0) return;

		return await new Promise((resolve) => {
			const onDone = () => {
				if (this.size === 0 && this.running === 0) {
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
		if (this.running >= this._hard) return false;
		if (this._soft !== undefined && this.active >= this._soft) return false;
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

	private async _waitForSpace() {
		if (!this._maxQueueSize || this.size < this._maxQueueSize) return;
		await new Promise<void>((resolve) => {
			this.promisesWaitingForSpace.push(resolve);
		});
	}

	private _startTask(args: T) {
		this.runningTasks.add(args);

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

		Promise.resolve().then(() => this._fn(...args)).catch((err) => {
			this.emit("error", err);
		}).finally(() => {
			this.runningTasks.delete(args);

			if (timeoutId) clearTimeout(timeoutId);

			if (countsTowardsSoft) {
				// fast tasks that finished before the soft timeout
				this._active = Math.max(this._active - 1, 0);
			}

			this.emit("completed");
			if (this.size === 0 && this.running === 0) {
				this.emit("drained");
			}

			this._notifyWaiting();
			this._drain();
		});
	}
}
