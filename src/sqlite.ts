/**
 * Persistent JSON storage for Node.js, imported from `durable-channel/storage/sqlite`.
 * The core library and browser client do not import this module.
 */
import { DatabaseSync } from "node:sqlite";
import type { DurableChannelStorage, DurableChannelStorageListOptions, DurableChannelStorageListResult } from "./storage.ts";

/** A SQLite-backed store. Dispose it after closing the hubs that use it. */
export class SqliteStorage implements DurableChannelStorage, Disposable {
	#database: DatabaseSync;

	private constructor(path: string) {
		this.#database = new DatabaseSync(path);
		this.#database.exec(`
			CREATE TABLE IF NOT EXISTS durable_channel_entries (
				sort_key TEXT PRIMARY KEY,
				key TEXT NOT NULL,
				value TEXT NOT NULL
			) WITHOUT ROWID
		`);
	}

	/** Open a file-backed database, or an isolated in-memory store when no path is supplied. */
	static async open(path = ":memory:"): Promise<SqliteStorage> {
		return new SqliteStorage(path);
	}

	async get(key: readonly string[]): Promise<unknown> {
		const entry = this.#database.prepare("SELECT value FROM durable_channel_entries WHERE sort_key = ?").get(sortKey(key));
		return entry === undefined ? undefined : JSON.parse(entry.value as string);
	}

	async set(key: readonly string[], value: unknown): Promise<void> {
		const json = JSON.stringify(value);
		if (json === undefined) throw new TypeError("SQLite storage requires a JSON value");
		this.#database
			.prepare(
				"INSERT INTO durable_channel_entries (sort_key, key, value) VALUES (?, ?, ?) ON CONFLICT (sort_key) DO UPDATE SET value = excluded.value",
			)
			.run(sortKey(key), JSON.stringify(key), json);
	}

	async delete(key: readonly string[]): Promise<void> {
		this.#database.prepare("DELETE FROM durable_channel_entries WHERE sort_key = ?").run(sortKey(key));
	}

	/** Scan a prefix in segment-wise order, returning a cursor only when another entry follows. */
	async list(options: DurableChannelStorageListOptions): Promise<DurableChannelStorageListResult> {
		const prefix = sortKey(options.prefix);
		const after = options.cursor === undefined ? "" : sortKey(JSON.parse(options.cursor) as string[]);
		const start = options.start === undefined ? prefix : sortKey(options.start);
		const limit = options.limit;
		if (limit !== undefined && (!Number.isSafeInteger(limit) || limit < 1)) {
			throw new RangeError("A storage scan limit must be a positive safe integer");
		}
		const rows = this.#database
			.prepare(`
			SELECT key, value FROM durable_channel_entries
			WHERE sort_key >= ? AND sort_key < ? AND sort_key >= ?
			AND (? = 0 OR sort_key > ?)
			ORDER BY sort_key LIMIT ?
		`)
			.all(prefix, `${prefix}~`, start, options.cursor === undefined ? 0 : 1, after, limit === undefined ? -1 : limit + 1);
		const page = limit === undefined ? rows : rows.slice(0, limit);
		const entries = page.map((row) => ({ key: JSON.parse(row.key as string) as string[], value: JSON.parse(row.value as string) }));
		return {
			entries,
			...(rows.length > page.length ? { cursor: JSON.stringify(entries.at(-1)!.key) } : {}),
		};
	}

	[Symbol.dispose](): void {
		if (this.#database.isOpen) this.#database.close();
	}
}

/**
 * Encode UTF-16 code units as fixed-width hex and end each segment with a lower-sorting delimiter.
 * SQLite's binary text order then matches JavaScript's segment-wise string order, including empty
 * segments, control characters, and supplementary Unicode characters.
 */
function sortKey(key: readonly string[]): string {
	return key
		.map((segment) => {
			let encoded = "";
			for (let index = 0; index < segment.length; index++) {
				encoded += segment.charCodeAt(index).toString(16).padStart(4, "0");
			}
			return `${encoded}!`;
		})
		.join("");
}
