/** Deterministic host scheduler used only by distributed tests and examples of host contracts. */
import type { DistributedClientScheduler, DistributedClock, DistributedScheduler } from "./interfaces.ts";
export class FakeDistributedTime implements DistributedClock, DistributedClientScheduler {
	time = 1000;
	#next = 0;
	#tasks = new Map<string, { at: number; task: () => void | Promise<void> }>();
	now(): number {
		return this.time;
	}
	schedule(delay: number, task: () => void): () => void {
		const id = `timer/${++this.#next}`;
		this.#tasks.set(id, { at: this.time + delay, task });
		return () => this.#tasks.delete(id);
	}
	durable(id: string, task: () => Promise<void>): DistributedScheduler {
		return {
			arm: (at) => {
				const old = this.#tasks.get(id);
				if (!old || old.at > at) this.#tasks.set(id, { at, task });
				return Promise.resolve();
			},
		};
	}
	get pending(): number {
		return this.#tasks.size;
	}
	async flush(): Promise<void> {
		for (let i = 0; i < 150; i++) await Promise.resolve();
	}
	async advance(ms: number): Promise<void> {
		const until = this.time + ms;
		for (let count = 0; count < 10000; count++) {
			const next = [...this.#tasks].filter(([, t]) => t.at <= until).sort((a, b) => a[1].at - b[1].at)[0];
			if (!next) {
				this.time = until;
				await this.flush();
				return;
			}
			this.time = next[1].at;
			this.#tasks.delete(next[0]);
			void Promise.resolve(next[1].task()).catch(() => {});
			await this.flush();
		}
		throw new Error("Fake scheduler did not quiesce");
	}
}
export function distributedDeferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void; reject: (error: unknown) => void } {
	let resolve!: (value: T) => void;
	let reject!: (error: unknown) => void;
	const promise = new Promise<T>((a, b) => {
		resolve = a;
		reject = b;
	});
	return { promise, resolve, reject };
}
