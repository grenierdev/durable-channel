import { describe, it } from "node:test";
import { assert, assertEquals, assertNotEquals } from "@std/assert";
import { DenoKvStorage, type DurableChannelStorage, MemoryStorage } from "./storage.ts";

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
			assertEquals(await store.storage.get(["hub", "serverSeq"]), 3);
			assertEquals(await store.storage.get(["channel", "doc:/:id", "doc:/a"]), { title: "a" });
			await store.storage.delete(["channel", "doc:/:id", "doc:/a"]);
			assertEquals(await store.storage.get(["channel", "doc:/:id", "doc:/a"]), undefined);
			assertEquals(await store.storage.get(["nothing", "here"]), undefined);
		});

		it("lists a prefix in segment-wise order", async () => {
			using store = await seeded();
			const page = await store.storage.list({ prefix: ["channel", "doc:/:id"] });
			assertEquals(page.entries.map((entry) => entry.key.at(-1)), ["doc:/a", "doc:/b", "doc:/c"]);
			assertEquals(page.entries.map((entry) => entry.value), [{ title: "a" }, { title: "b" }, { title: "c" }]);
			assertEquals(page.cursor, undefined);
		});

		it("orders a key before its own extensions", async () => {
			using store = await open();
			await store.storage.set(["a", "b", "c"], 2);
			await store.storage.set(["a", "b"], 1);
			const page = await store.storage.list({ prefix: ["a"] });
			assertEquals(page.entries.map((entry) => entry.key), [["a", "b"], ["a", "b", "c"]]);
		});

		it("pages with a limit and a cursor, and hands back no cursor at the end", async () => {
			using store = await seeded();
			const first = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 2 });
			assertEquals(first.entries.map((entry) => entry.key.at(-1)), ["doc:/a", "doc:/b"]);
			assert(typeof first.cursor === "string" && first.cursor.length > 0, "a partial page carries a cursor");
			const second = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 2, cursor: first.cursor });
			assertEquals(second.entries.map((entry) => entry.key.at(-1)), ["doc:/c"]);
			assertEquals(second.cursor, undefined);
			const exact = await store.storage.list({ prefix: ["channel", "doc:/:id"], limit: 3 });
			assertEquals(exact.entries.length, 3);
			assertEquals(exact.cursor, undefined);
		});

		it("starts a scan at a key the caller names", async () => {
			using store = await seeded();
			const from = await store.storage.list({ prefix: ["channel", "doc:/:id"], start: ["channel", "doc:/:id", "doc:/b"] });
			assertEquals(from.entries.map((entry) => entry.key.at(-1)), ["doc:/b", "doc:/c"], "the start is inclusive");
			assertEquals(from.cursor, undefined);

			const limited = await store.storage.list({
				prefix: ["channel", "doc:/:id"],
				start: ["channel", "doc:/:id", "doc:/b"],
				limit: 1,
			});
			assertEquals(limited.entries.map((entry) => entry.key.at(-1)), ["doc:/b"]);
			assert(typeof limited.cursor === "string", "a partial page carries a cursor from a started scan too");
			const rest = await store.storage.list({
				prefix: ["channel", "doc:/:id"],
				start: ["channel", "doc:/:id", "doc:/b"],
				cursor: limited.cursor,
			});
			assertEquals(rest.entries.map((entry) => entry.key.at(-1)), ["doc:/c"]);

			const beyond = await store.storage.list({ prefix: ["channel", "doc:/:id"], start: ["channel", "doc:/:id", "doc:/z"] });
			assertEquals(beyond.entries, [], "nothing is at or after the end");
		});

		it("keeps a stored value out of reach of the caller", async () => {
			using store = await open();
			const written = { nested: { count: 0 } };
			await store.storage.set(["k"], written);
			written.nested.count = 99;
			const read = await store.storage.get(["k"]) as { nested: { count: number } };
			assertEquals(read, { nested: { count: 0 } });
			read.nested.count = 42;
			assertEquals(await store.storage.get(["k"]), { nested: { count: 0 } });
			assertNotEquals(await store.storage.get(["k"]), read);
		});
	});
}

storageContract("MemoryStorage", () => Promise.resolve({ storage: new MemoryStorage(), [Symbol.dispose]() {} }));

storageContract("DenoKvStorage", async () => {
	const storage = await DenoKvStorage.open(":memory:");
	return { storage, [Symbol.dispose]: () => storage[Symbol.dispose]() };
});
