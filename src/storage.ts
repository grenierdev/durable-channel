/**
 * Persistence behind a key-value shape a hub can carry anywhere.
 *
 * Keys are `string[]` and values are JSON, which is exactly what a Cloudflare Durable Object's
 * `state.storage` accepts, so the interface maps directly onto it. The hub only ever
 * needs get / set / delete and a prefix scan, so nothing else is declared.
 *
 * This module ships process-local {@link MemoryStorage}. Node.js applications can import
 * `SqliteStorage` separately from `durable-channel/storage/sqlite` for persistent storage.
 */

/** A page of a prefix scan. `cursor` is present only when more entries follow. */
export interface DurableChannelStorageListResult {
	readonly entries: readonly { readonly key: string[]; readonly value: unknown }[];
	readonly cursor?: string;
}

/** Options of a prefix scan. `cursor` continues a previous page and is opaque to callers. */
export interface DurableChannelStorageListOptions {
	readonly prefix: readonly string[];
	/**
	 * The key the scan starts at, inclusive. It has to be under `prefix`, and it is what makes a range
	 * addressable without a cursor: a caller that can name where it wants to start — a log paging back
	 * through its own ids — does not have to walk there from the beginning of the prefix.
	 */
	readonly start?: readonly string[];
	readonly cursor?: string;
	readonly limit?: number;
}

/** The whole persistence surface a hub needs. */
export interface DurableChannelStorage {
	get(key: readonly string[]): Promise<unknown>;
	set(key: readonly string[], value: unknown): Promise<void>;
	delete(key: readonly string[]): Promise<void>;
	list(options: DurableChannelStorageListOptions): Promise<DurableChannelStorageListResult>;
}

/**
 * Process-local storage. Values are structurally cloned on the way in and on the way out, so a caller
 * holding a returned object cannot mutate what the store holds. Entries are kept in segment-wise
 * order, which puts `["a", "b"]` before `["a", "b", "c"]`.
 */
export class MemoryStorage implements DurableChannelStorage {
	#entries = new Map<string, { key: string[]; value: unknown }>();

	get(key: readonly string[]): Promise<unknown> {
		const entry = this.#entries.get(encodeKey(key));
		return Promise.resolve(entry === undefined ? undefined : structuredClone(entry.value));
	}

	set(key: readonly string[], value: unknown): Promise<void> {
		this.#entries.set(encodeKey(key), { key: [...key], value: structuredClone(value) });
		return Promise.resolve();
	}

	delete(key: readonly string[]): Promise<void> {
		this.#entries.delete(encodeKey(key));
		return Promise.resolve();
	}

	list(options: DurableChannelStorageListOptions): Promise<DurableChannelStorageListResult> {
		const after = options.cursor === undefined ? undefined : (JSON.parse(options.cursor) as string[]);
		const matching = [...this.#entries.values()]
			.filter(
				(entry) =>
					hasPrefix(entry.key, options.prefix) &&
					(after === undefined || compareKeys(entry.key, after) > 0) &&
					(options.start === undefined || compareKeys(entry.key, options.start) >= 0),
			)
			.sort((left, right) => compareKeys(left.key, right.key));
		const limit = options.limit ?? matching.length;
		const page = matching.slice(0, limit);
		const last = page.at(-1);
		return Promise.resolve({
			entries: page.map((entry) => ({ key: [...entry.key], value: structuredClone(entry.value) })),
			...(last !== undefined && matching.length > page.length ? { cursor: JSON.stringify(last.key) } : {}),
		});
	}
}

function encodeKey(key: readonly string[]): string {
	return JSON.stringify(key);
}

function hasPrefix(key: readonly string[], prefix: readonly string[]): boolean {
	return key.length >= prefix.length && prefix.every((segment, index) => key[index] === segment);
}

function compareKeys(left: readonly string[], right: readonly string[]): number {
	for (let index = 0; index < Math.min(left.length, right.length); index++) {
		if (left[index] !== right[index]) {
			return left[index] < right[index] ? -1 : 1;
		}
	}
	return left.length - right.length;
}
