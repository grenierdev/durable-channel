import type { ChannelStore, ChannelTransaction } from "./interfaces.ts";
import { cloneRecord } from "./store.ts";
/** Structural subset of DO SQLite. Core has no cloudflare:workers dependency. */
export interface DistributedSqliteStorage {
	readonly sql: { exec(query: string, ...bindings: (string | number | null)[]): Iterable<Record<string, unknown>> };
	transactionSync<T>(body: () => T): T;
	/** Called after transactionSync returns, before acknowledging or publishing a commit. */
	sync(): Promise<void>;
}
export class SqliteChannelStore implements ChannelStore {
	#storage: DistributedSqliteStorage;
	#lock: Promise<unknown> = Promise.resolve();
	constructor(storage: DistributedSqliteStorage) {
		this.#storage = storage;
		Array.from(storage.sql.exec("CREATE TABLE IF NOT EXISTS distributed_records (key TEXT PRIMARY KEY, value TEXT NOT NULL)"));
	}
	transaction<T>(body: (tx: ChannelTransaction) => T): Promise<T> {
		const run = this.#lock.then(async () => {
			const result = this.#storage.transactionSync(() => {
				const tx: ChannelTransaction = {
					get: <V>(key: string): V | undefined => {
						const rows = Array.from(this.#storage.sql.exec("SELECT value FROM distributed_records WHERE key = ?", key));
						return rows.length ? JSON.parse(String(rows[0].value)) : undefined;
					},
					set: (key, value) => {
						Array.from(
							this.#storage.sql.exec(
								"INSERT INTO distributed_records(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
								key,
								JSON.stringify(value),
							),
						);
					},
					delete: (key) => {
						Array.from(this.#storage.sql.exec("DELETE FROM distributed_records WHERE key = ?", key));
					},
					list: <V>(prefix: string): [string, V][] =>
						Array.from(
							this.#storage.sql.exec(
								"SELECT key,value FROM distributed_records WHERE substr(key,1,length(?)) = ? ORDER BY key",
								prefix,
								prefix,
							),
						).map((row) => [String(row.key), JSON.parse(String(row.value))]),
				};
				const result = body(tx);
				if (
					result !== null && (typeof result === "object" || typeof result === "function") && "then" in result &&
					typeof result.then === "function"
				) throw new TypeError("A channel transaction must be synchronous");
				return cloneRecord(result);
			});
			await this.#storage.sync();
			return result;
		});
		this.#lock = run.catch(() => {});
		return run;
	}
}
