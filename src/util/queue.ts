import PQueue from "p-queue";

// a simple queue that only runs one function with no need for the output
// and can therefore store only arguments instead of promises
export class BackgroundQueue<T extends readonly unknown[]> {
	private pqueue: PQueue;
	private queue: T[];
	private originalConcurrency: number;
	private activeTasks: number = 0;
	private allTasks: Set<Promise<unknown>> = new Set();

	constructor(
		private fn: (...args: T) => unknown,
		options: ConstructorParameters<typeof PQueue>[0],
	) {
		this.originalConcurrency = options?.concurrency ?? 1;
		this.queue = [];

		if (this.originalConcurrency <= 1) {
			// if queue is sequential, just process in order
			this.pqueue = new PQueue(options);
			this.pqueue.on("completed", () => {
				const args = this.queue.shift();
				if (args) this.run(...args);
			});
		} else {
			// if queue is parallel, pass p-queue a very high concurrency limit and handle concurrency ourselves
			this.pqueue = new PQueue({ ...options, concurrency: 200_000 });
		}
	}

	get size() {
		return this.queue.length + this.pqueue.size;
	}

	add(...args: T) {
		if (this.originalConcurrency <= 1) {
			// sequential processing
			if (this.pqueue.size === 0 && this.pqueue.pending <= this.pqueue.concurrency) {
				this.run(...args);
			} else {
				this.queue.push(args);
			}
		} else {
			// custom concurrency limiting
			if (this.activeTasks < this.originalConcurrency) {
				this.run(...args);
			} else {
				this.queue.push(args);
			}
		}
	}

	async processAll() {
		if (this.originalConcurrency <= 1) {
			while (this.queue.length) {
				this.run(...this.queue.shift()!);
			}
			await this.pqueue.onIdle();
		} else {
			// run all queued tasks
			while (this.queue.length) {
				this.run(...this.queue.shift()!);
			}
			// wait for all tasks, including long-running ones
			await Promise.all(this.allTasks);
		}
	}

	private run(...args: T) {
		if (this.originalConcurrency <= 1) {
			// sequential processing
			void this.pqueue.add(() => this.fn(...args))
				.catch(console.error);
		} else {
			// custom concurrency limiting
			this.activeTasks++;

			let completed = false;
			const taskPromise = this.pqueue.add(() => this.fn(...args))
				.catch(console.error)
				.finally(() => {
					this.allTasks.delete(taskPromise);
					completed = true;
				});

			this.allTasks.add(taskPromise);

			// after 10 seconds, stop counting this task towards concurrency
			setTimeout(() => {
				if (completed) return;
				this.activeTasks--;
				// try to start queued tasks
				if (this.queue.length > 0 && this.activeTasks < this.originalConcurrency) {
					const args = this.queue.shift()!;
					this.run(...args);
				}
			}, 10000);
		}
	}
}
