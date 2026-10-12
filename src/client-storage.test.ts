import { deepStrictEqual, rejects } from "node:assert/strict";
import { IDBFactory } from "fake-indexeddb";
import { describe, it } from "vitest";
import { type DurableChannelClientCheckpoint, IndexedDBClientStorage } from "./client-storage.ts";

const checkpoint: DurableChannelClientCheckpoint = {
	version: 1,
	clientId: "alice",
	nextClientSeq: 1,
	lastSeenServerSeq: 0,
	channels: [{ uri: "counter:/1", confirmed: { count: 3 }, appliedSeq: 0, pending: [] }],
};

describe("IndexedDBClientStorage", () => {
	it("commits checkpoints across adapter instances, clones values, and isolates keys", async () => {
		const indexedDB = new IDBFactory();
		const first = new IndexedDBClientStorage("first", { indexedDB });
		const second = new IndexedDBClientStorage("second", { indexedDB });
		deepStrictEqual(await first.load(), undefined);
		await first.save(checkpoint);
		deepStrictEqual(await second.load(), undefined);
		const reopened = new IndexedDBClientStorage("first", { indexedDB });
		const saved = (await reopened.load()) as DurableChannelClientCheckpoint;
		deepStrictEqual(saved, checkpoint);
		(saved.channels[0].confirmed as { count: number }).count = 9;
		deepStrictEqual(await first.load(), checkpoint);
		await reopened.clear();
		deepStrictEqual(await first.load(), undefined);
	});

	it("rejects uncloneable values without replacing a committed checkpoint", async () => {
		const store = new IndexedDBClientStorage("first", { indexedDB: new IDBFactory() });
		await store.save(checkpoint);
		await rejects(store.save({ ...checkpoint, channels: [{ ...checkpoint.channels[0], confirmed: () => {} }] }));
		deepStrictEqual(await store.load(), checkpoint);
	});

	it("reports unavailable IndexedDB", async () => {
		await rejects(new IndexedDBClientStorage("first").load(), /IndexedDB is unavailable/);
	});
});
