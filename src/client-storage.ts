import type { DurableChannelPrincipal } from "./channel.ts";
import type { DurableChannelPendingAction } from "./client.ts";

/** One atomic checkpoint of a client. Optimistic state is rebuilt from confirmed state and pending actions. */
export interface DurableChannelClientCheckpoint {
	readonly version: 1;
	readonly clientId: string;
	readonly principal?: DurableChannelPrincipal;
	readonly nextClientSeq: number;
	readonly lastSeenServerSeq: number;
	readonly channels: readonly {
		readonly uri: string;
		readonly confirmed: unknown;
		readonly appliedSeq: number;
		readonly pending: readonly DurableChannelPendingAction[];
	}[];
}

/** A single client's checkpoint store. Use a separate store key for each independent client. */
export interface DurableChannelClientStorage {
	load(): Promise<unknown>;
	save(checkpoint: DurableChannelClientCheckpoint): Promise<void>;
	clear(): Promise<void>;
}

export interface IndexedDBClientStorageOptions {
	/** Database dedicated to client checkpoints. Default: `durable-channel-clients`. */
	readonly databaseName?: string;
	/** Injectable for tests and hosts with their own IndexedDB implementation. */
	readonly indexedDB?: IDBFactory;
}

/** Browser persistence for a client, stored as one record so states and sequence numbers commit together. */
export class IndexedDBClientStorage implements DurableChannelClientStorage {
	#key: string;
	#databaseName: string;
	#factory: IDBFactory | undefined;

	constructor(key: string, options?: IndexedDBClientStorageOptions) {
		if (key.length === 0) throw new TypeError("A client persistence key must not be empty");
		this.#key = key;
		this.#databaseName = options?.databaseName ?? "durable-channel-clients";
		this.#factory = options?.indexedDB;
	}

	load(): Promise<unknown> {
		return this.#transaction("readonly", (store) => store.get(this.#key));
	}

	async save(checkpoint: DurableChannelClientCheckpoint): Promise<void> {
		await this.#transaction("readwrite", (store) => store.put(checkpoint, this.#key));
	}

	async clear(): Promise<void> {
		await this.#transaction("readwrite", (store) => store.delete(this.#key));
	}

	#open(): Promise<IDBDatabase> {
		return new Promise((resolve, reject) => {
			const factory = this.#factory ?? globalThis.indexedDB;
			if (factory === undefined) {
				reject(new Error("IndexedDB is unavailable in this environment"));
				return;
			}
			const request = factory.open(this.#databaseName, 1);
			let blocked = false;
			request.onupgradeneeded = () => {
				request.result.createObjectStore("clients");
			};
			request.onerror = () => reject(request.error);
			request.onblocked = () => {
				blocked = true;
				reject(new Error("Opening the client IndexedDB database is blocked"));
			};
			request.onsuccess = () => {
				if (blocked) request.result.close();
				else resolve(request.result);
			};
		});
	}

	async #transaction(mode: IDBTransactionMode, operation: (store: IDBObjectStore) => IDBRequest): Promise<unknown> {
		const database = await this.#open();
		try {
			return await new Promise((resolve, reject) => {
				const transaction = database.transaction("clients", mode);
				let request: IDBRequest;
				transaction.oncomplete = () => resolve(request.result);
				transaction.onabort = () => reject(transaction.error ?? new Error("The client persistence transaction was aborted"));
				try {
					request = operation(transaction.objectStore("clients"));
				} catch (error) {
					transaction.abort();
					reject(error);
				}
			});
		} finally {
			database.close();
		}
	}
}
