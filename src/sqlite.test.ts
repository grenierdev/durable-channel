import { deepStrictEqual, rejects } from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "vitest";
import { SqliteStorage } from "./sqlite.ts";
import { MemoryStorage } from "./storage.ts";

describe("SqliteStorage persistence", () => {
	it("reopens a file with overwritten values and deletions intact", async () => {
		const directory = await mkdtemp(join(tmpdir(), "durable-channel-"));
		try {
			const path = join(directory, "storage.sqlite");
			{
				using storage = await SqliteStorage.open(path);
				await storage.set(["kept"], { count: 1 });
				await storage.set(["kept"], { count: 2 });
				await storage.set(["deleted"], true);
				await storage.delete(["deleted"]);
				await storage.set(["null"], null);
			}
			using reopened = await SqliteStorage.open(path);
			deepStrictEqual(await reopened.get(["kept"]), { count: 2 });
			deepStrictEqual(await reopened.get(["deleted"]), undefined);
			deepStrictEqual(await reopened.get(["null"]), null);
			deepStrictEqual(
				(await reopened.list({ prefix: [] })).entries.map((entry) => entry.key),
				[["kept"], ["null"]],
			);
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("matches memory storage ordering and paging for empty, escaped, and Unicode keys", async () => {
		using sqlite = await SqliteStorage.open();
		const memory = new MemoryStorage();
		const keys = [
			[],
			[""],
			["", "child"],
			["a"],
			["a", ""],
			["a", "b"],
			["a!", "b"],
			["a\u0000"],
			['a"'],
			["a\\"],
			["ab"],
			["😀"],
			["\uE000"],
		];
		for (const storage of [sqlite, memory]) {
			for (const key of keys.toReversed()) await storage.set(key, key);
		}
		for (const prefix of [[], [""], ["a"], ["a!"], ["😀"]]) {
			deepStrictEqual(await sqlite.list({ prefix }), await memory.list({ prefix }));
			let cursor: string | undefined;
			do {
				const options = { prefix, limit: 2, cursor };
				const actual = await sqlite.list(options);
				deepStrictEqual(actual, await memory.list(options));
				cursor = actual.cursor;
			} while (cursor !== undefined);
		}
		deepStrictEqual(await sqlite.list({ prefix: [], start: ["a"], limit: 3 }), await memory.list({ prefix: [], start: ["a"], limit: 3 }));
	});

	it("rejects invalid limits and values outside the JSON storage contract", async () => {
		using storage = await SqliteStorage.open();
		await rejects(() => storage.set(["key"], undefined), TypeError);
		for (const limit of [0, -1, 1.5, Infinity]) {
			await rejects(() => storage.list({ prefix: [], limit }), RangeError);
		}
	});
});
