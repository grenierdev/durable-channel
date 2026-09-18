import { describe, it } from "node:test";
import { assertEquals, assertRejects } from "@std/assert";
import { MemoryChannelStore } from "./memory-store.ts";
import type { ChannelStore } from "./interfaces.ts";

export function storeContract(name: string, factory: () => ChannelStore): void {
	describe(name, () => {
		it("rolls back all writes/deletes and isolates read/write/result references", async () => {
			const store = factory();
			const input = { nested: [1] };
			await store.transaction((tx) => tx.set("a", input));
			input.nested.push(2);
			const output = await store.transaction((tx) => tx.get<typeof input>("a")!);
			output.nested.push(3);
			await assertRejects(
				() =>
					store.transaction((tx) => {
						tx.delete("a");
						tx.set("b", {});
						throw new Error("fault");
					}),
				Error,
				"fault",
			);
			assertEquals(await store.transaction((tx) => tx.list("")), [["a", { nested: [1] }]]);
			await store.transaction((tx) => {
				const value = tx.get<typeof input>("a")!;
				value.nested.push(4);
			});
			assertEquals(await store.transaction((tx) => tx.get("a")), { nested: [1] });
		});
		it("rejects asynchronous bodies without committing the prefix", async () => {
			const store = factory();
			await assertRejects(() =>
				store.transaction(async (tx) => {
					tx.set("bad", true);
					await Promise.resolve();
				}), TypeError);
			await assertRejects(() =>
				store.transaction((tx) => {
					tx.set("bad", true);
					return { then() {} };
				}), TypeError);
			assertEquals(await store.transaction((tx) => tx.get("bad")), undefined);
		});
	});
}
storeContract("memory transactional store", () => new MemoryChannelStore());
