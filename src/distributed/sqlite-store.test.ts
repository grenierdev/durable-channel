import { describe, it } from "node:test";
import { DatabaseSync } from "node:sqlite";
import { assertEquals, assertRejects } from "@std/assert";
import { type DistributedSqliteStorage, SqliteChannelStore } from "./sqlite-store.ts";
import { distributedDeferred } from "./testing.ts";

function adapter(db: DatabaseSync, sync: () => Promise<void> = () => Promise.resolve()): DistributedSqliteStorage {
	return {
		sql: { exec: (query, ...bindings) => db.prepare(query).all(...bindings) },
		transactionSync: (body) => {
			db.exec("BEGIN");
			try {
				const result = body();
				db.exec("COMMIT");
				return result;
			} catch (error) {
				db.exec("ROLLBACK");
				throw error;
			}
		},
		sync,
	};
}
describe("SQLite transactional store", () => {
	it("provides equivalent rollback, isolated records and synchronous-body guarantees", async () => {
		const db = new DatabaseSync(":memory:");
		try {
			const store = new SqliteChannelStore(adapter(db)), value = { values: [1] };
			await store.transaction((tx) => tx.set("a", value));
			value.values.push(2);
			const result = await store.transaction((tx) => tx.get<typeof value>("a")!);
			result.values.push(3);
			await assertRejects(
				() =>
					store.transaction((tx) => {
						tx.delete("a");
						tx.set("b", true);
						throw new Error("fault");
					}),
				Error,
				"fault",
			);
			assertEquals(await store.transaction((tx) => tx.list("")), [["a", { values: [1] }]]);
			await assertRejects(() =>
				store.transaction((tx) => {
					tx.set("bad", true);
					return { then() {} };
				}), TypeError);
			assertEquals(await store.transaction((tx) => tx.get("bad")), undefined);
			await store.transaction((tx) => tx.set("💫/x", 1));
			assertEquals(await store.transaction((tx) => tx.list("💫/")), [["💫/x", 1]]);
		} finally {
			db.close();
		}
	});
	it("awaits the host durability boundary and makes ambiguous committed rows reloadable", async () => {
		const db = new DatabaseSync(":memory:"), gate = distributedDeferred();
		let entered = false, done = false;
		try {
			const store = new SqliteChannelStore(adapter(db, () => {
				entered = true;
				return gate.promise;
			}));
			const write = store.transaction((tx) => tx.set("receipt", { seq: 1 })).then(() => done = true);
			for (let i = 0; i < 10; i++) await Promise.resolve();
			assertEquals(entered, true);
			assertEquals(done, false);
			gate.resolve();
			await write;
			const failing = new SqliteChannelStore(adapter(db, () => Promise.reject(new Error("ambiguous durability"))));
			await assertRejects(() => failing.transaction((tx) => tx.set("receipt", { seq: 2 })), Error, "ambiguous durability");
			assertEquals(await new SqliteChannelStore(adapter(db)).transaction((tx) => tx.get("receipt")), { seq: 2 });
		} finally {
			db.close();
		}
	});
});
