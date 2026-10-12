import { deepStrictEqual, notDeepStrictEqual, ok } from "node:assert/strict";
import { describe, it } from "vitest";
import { SqliteStorage } from "./sqlite.ts";
import { type DurableChannelStorage, MemoryStorage } from "./storage.ts";

/** One store under test, disposed at the end of each `it`. */
type Store = Disposable & { readonly storage: DurableChannelStorage };

/**
 * The assertions every {@link DurableChannelStorage} has to satisfy. Written once and run against
 * each implementation, so a store that passes is interchangeable with the others as far as a hub is
 * concerned. Cursors are opaque here on purpose: only their behaviour is part of the contract.
 */
function storageContract(name: string, open: () => Promise<Store>): void {
	describe(name, () => {
		async function seeded(): Promise<Store> {
			const store = await open();
			for (const id of ["c", "a", "b"]) {
				await store.storage.set(["channel", "doc:/:id", `doc:/${id}`], { title: id });
			}
			await store.storage.set(["hub", "serverSeq"], 3);
			return store;
		}

		it("reads back what it wrote and forgets what it deleted", async () => {
			using store = await seeded();
			deepStrictEqual(await store.storage.get(["hub", "serverSeq"]), 3);
			deepStrictEqual(await store.storage.get(["channel", "doc:/:id", "doc:/a"]), { title: "a" });
			await store.storage.delete(["channel", "doc:/:id", "doc:/a"]);
			deepStrictEqual(await store.storage.get(["channel", "doc:/:id", "doc:/a"]), undefined);
			deepStrictEqual(await store.storage.get(["nothing", "here"]), undefined);
		});

		it("lists a prefix in segment-wise order", async () => {
			using store = await seeded();
			const page = await store.storage.list({ prefix: ["channel", "doc:/:id"] });
			deepStrictEqual(
				page.entries.map((entry) => entry.key.at(-1)),
				["doc:/a", "doc:/b", "doc:/c"],
			);
			deepStrictEqual(
				page.entries.map((entry) => entry.value),
				[{ title: "a" }, { title: "b" }, { title: "c" }],
			);
			deepStrictEqual(page.cursor, undefined);
		});

		it("orders a key before its own extensions", async () => {
			using store = await open();
			await store.storage.set(["a", "b", "c"], 2);
			await store.storage.set(["a", "b"], 1);
			const page = await store.storage.list({ prefix: ["a"] });
			deepStrictEqual(
				page.entries.map((entry) => entry.key),
				[
					["a", "b"],
					["a", "b", "c"],
				],
			);
		});

		it("pages with a limit and a cursor, and hands back no cursor at the end", async () => {
			using store = await seeded();
			const first = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 2 });
			deepStrictEqual(
				first.entries.map((entry) => entry.key.at(-1)),
				["doc:/a", "doc:/b"],
			);
			ok(typeof first.cursor === "string" && first.cursor.length > 0, "a partial page carries a cursor");
			const second = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 2, cursor: first.cursor });
			deepStrictEqual(
				second.entries.map((entry) => entry.key.at(-1)),
				["doc:/c"],
			);
			deepStrictEqual(second.cursor, undefined);
			const exact = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 3 });
			deepStrictEqual(exact.entries.length, 3);
			deepStrictEqual(exact.cursor, undefined);
		});

		it("starts a scan at a key the caller names", async () => {
			using store = await seeded();
			const from = await store.storage.list({ prefix: ["channel", "doc:/:id"], start: ["channel", "doc:/:id", "doc:/b"] });
			deepStrictEqual(
				from.entries.map((entry) => entry.key.at(-1)),
				["doc:/b", "doc:/c"],
				"the start is inclusive",
			);
			deepStrictEqual(from.cursor, undefined);

			const limited = await store.storage.list({
				prefix: ["channel", "doc:/:id"],
				start: ["channel", "doc:/:id", "doc:/b"],
				limit: 1,
			});
			deepStrictEqual(
				limited.entries.map((entry) => entry.key.at(-1)),
				["doc:/b"],
			);
			ok(typeof limited.cursor === "string", "a partial page carries a cursor from a started scan too");
			const rest = await store.storage.list({
				prefix: ["channel", "doc:/:id"],
				start: ["channel", "doc:/:id", "doc:/b"],
				cursor: limited.cursor,
			});
			deepStrictEqual(
				rest.entries.map((entry) => entry.key.at(-1)),
				["doc:/c"],
			);

			const beyond = await store.storage.list({ prefix: ["channel", "doc:/:id"], start: ["channel", "doc:/:id", "doc:/z"] });
			deepStrictEqual(beyond.entries, [], "nothing is at or after the end");
		});

		it("keeps a stored value out of reach of the caller", async () => {
			using store = await open();
			const written = { nested: { count: 0 } };
			await store.storage.set(["k"], written);
			written.nested.count = 99;
			const read = (await store.storage.get(["k"])) as { nested: { count: number } };
			deepStrictEqual(read, { nested: { count: 0 } });
			read.nested.count = 42;
			deepStrictEqual(await store.storage.get(["k"]), { nested: { count: 0 } });
			notDeepStrictEqual(await store.storage.get(["k"]), read);
		});
	});
}

storageContract("MemoryStorage", () => Promise.resolve({ storage: new MemoryStorage(), [Symbol.dispose]() {} }));

storageContract("SqliteStorage", async () => {
	const storage = await SqliteStorage.open(":memory:");
	return { storage, [Symbol.dispose]: () => storage[Symbol.dispose]() };
});
