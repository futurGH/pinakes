import PQueue from "p-queue";

// a simple queue that only runs one function with no need for the output
// and can therefore store only arguments instead of promises
export class BackgroundQueue<T extends readonly unknown[], Fn extends (...args: T) => unknown> {
	private pqueue: PQueue;
	private queue: T[];

	constructor(private fn: Fn, options: ConstructorParameters<typeof PQueue>[0]) {
		this.pqueue = new PQueue(options);
		this.queue = [];

		this.pqueue.on("completed", () => {
			const args = this.queue.shift();
			if (args) this.run(...args);
		});
	}

	add(...args: T) {
		if (this.pqueue.size === 0 && this.pqueue.pending <= this.pqueue.concurrency) {
			this.run(...args);
		} else {
			this.queue.push(args);
		}
	}

	private run(...args: T) {
		void this.pqueue.add(() => this.fn(...args))
			.catch(console.error);
	}
}
