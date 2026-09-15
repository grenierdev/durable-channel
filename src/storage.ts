/**
 * Persistence behind a key-value shape a hub can carry anywhere.
 *
 * Keys are `string[]` and values are JSON, which is exactly what a Cloudflare Durable Object's
 * `state.storage` and Deno KV both accept, so the interface maps 1:1 onto either. The hub only ever
 * needs get / set / delete and a prefix scan, so nothing else is declared.
 *
 * Two implementations ship here: {@link MemoryStorage}, which is process-local, and
 * {@link DenoKvStorage}, which is the only Deno-specific code in the library and reaches for the
 * runtime inside {@link DenoKvStorage.open} alone, so importing this module on another runtime never
 * touches `Deno`.
 */
import { DurableChannelError } from "./error.ts";

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
		const after = options.cursor === undefined ? undefined : JSON.parse(options.cursor) as string[];
		const matching = [...this.#entries.values()]
			.filter((entry) =>
				hasPrefix(entry.key, options.prefix) &&
				(after === undefined || compareKeys(entry.key, after) > 0) &&
				(options.start === undefined || compareKeys(entry.key, options.start) >= 0)
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

/** The slice of a Deno KV entry this module reads. `versionstamp` is `null` for a missing key. */
interface KvEntry {
	readonly key: readonly unknown[];
	readonly value: unknown;
	readonly versionstamp: string | null;
}

/** A Deno KV list iterator, whose `cursor` resumes after the entry it last yielded. */
interface KvListIterator extends AsyncIterableIterator<KvEntry> {
	readonly cursor: string;
}

/** The slice of `Deno.Kv` this module uses, declared structurally so no `Deno` type is referenced. */
interface Kv {
	get(key: readonly string[]): Promise<KvEntry>;
	set(key: readonly string[], value: unknown): Promise<unknown>;
	delete(key: readonly string[]): Promise<void>;
	list(
		selector: { prefix: readonly string[]; start?: readonly string[] },
		options?: { cursor?: string; limit?: number },
	): KvListIterator;
	close(): void;
}

/**
 * Deno KV storage. Keys map 1:1 onto KV keys and values go through KV's own serialization, so a
 * reader cannot mutate what the store holds. Dispose it — `using storage = await
 * DenoKvStorage.open(":memory:")` — to close the underlying database.
 */
export class DenoKvStorage implements DurableChannelStorage, Disposable {
	#kv: Kv;

	private constructor(kv: Kv) {
		this.#kv = kv;
	}

	/**
	 * Opens a KV database. `path` is forwarded to `Deno.openKv`, so `":memory:"` gives a throwaway
	 * store and no path gives the default one. This is the only place the library looks at `Deno`; on a
	 * runtime without it, the call fails with the code `STORAGE_UNAVAILABLE`.
	 */
	static async open(path?: string): Promise<DenoKvStorage> {
		const runtime = (globalThis as { Deno?: { openKv?(path?: string): Promise<Kv> } }).Deno;
		if (runtime?.openKv === undefined) {
			throw new DurableChannelError("STORAGE_UNAVAILABLE", "Deno KV is not available in this runtime");
		}
		return new DenoKvStorage(await runtime.openKv(path));
	}

	async get(key: readonly string[]): Promise<unknown> {
		const entry = await this.#kv.get(key);
		return entry.versionstamp === null ? undefined : entry.value;
	}

	async set(key: readonly string[], value: unknown): Promise<void> {
		await this.#kv.set(key, value);
	}

	async delete(key: readonly string[]): Promise<void> {
		await this.#kv.delete(key);
	}

	/**
	 * One page of a prefix scan. A `limit` is asked of KV as `limit + 1` entries, so the cursor is
	 * returned only when an entry really follows the page — the same contract as {@link MemoryStorage}.
	 */
	async list(options: DurableChannelStorageListOptions): Promise<DurableChannelStorageListResult> {
		const { limit } = options;
		const selector = options.start === undefined ? { prefix: options.prefix } : { prefix: options.prefix, start: options.start };
		const iterator = this.#kv.list(selector, {
			...(options.cursor !== undefined ? { cursor: options.cursor } : {}),
			...(limit !== undefined ? { limit: limit + 1 } : {}),
		});
		const entries: { key: string[]; value: unknown }[] = [];
		let cursor: string | undefined;
		for await (const entry of iterator) {
			if (entries.length === limit) {
				return { entries, cursor };
			}
			entries.push({ key: entry.key.map(String), value: entry.value });
			if (entries.length === limit) {
				cursor = iterator.cursor;
			}
		}
		return { entries };
	}

	[Symbol.dispose](): void {
		this.#kv.close();
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
