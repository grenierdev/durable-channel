import type { ChannelStore, ChannelTransaction } from "./interfaces.ts";
import { cloneRecord } from "./store.ts";

/** Transactional test/single-process store. Stores have independent serialization. */
export class MemoryChannelStore implements ChannelStore {
	#records = new Map<string, unknown>();
	#lock: Promise<unknown> = Promise.resolve();
	transaction<T>(body: (tx: ChannelTransaction) => T): Promise<T> {
		const run = this.#lock.then(() => {
			const writes = new Map<string, unknown>();
			const deleted = new Set<string>();
			const tx: ChannelTransaction = {
				get: <V>(key: string) =>
					cloneRecord((deleted.has(key) ? undefined : writes.has(key) ? writes.get(key) : this.#records.get(key)) as V | undefined),
				set: (key, value) => {
					writes.set(key, cloneRecord(value));
					deleted.delete(key);
				},
				delete: (key) => {
					writes.delete(key);
					deleted.add(key);
				},
				list: <V>(prefix: string): [string, V][] => {
					const keys = new Set([...this.#records.keys(), ...writes.keys()]);
					return [...keys].filter((key) => key.startsWith(prefix) && !deleted.has(key)).sort().map((key) => [key, tx.get<V>(key)!]);
				},
			};
			const result = body(tx);
			if (
				result !== null && (typeof result === "object" || typeof result === "function") && "then" in result &&
				typeof result.then === "function"
			) throw new TypeError("A channel transaction must be synchronous");
			const copy = cloneRecord(result);
			for (const key of deleted) this.#records.delete(key);
			for (const [key, value] of writes) this.#records.set(key, value);
			return copy;
		});
		this.#lock = run.catch(() => {});
		return run;
	}
}
