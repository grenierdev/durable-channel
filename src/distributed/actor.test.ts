import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import * as v from "valibot";
import { durableChannel } from "../channel.ts";
import { durableRoutes } from "../routes.ts";
import { RejectAction } from "../error.ts";
import { type DistributedChannelRecord, DurableChannelActor } from "./actor.ts";
import { MemoryChannelStore } from "./memory-store.ts";
import type { ChannelStore, ChannelTransaction } from "./interfaces.ts";
import { createDistributedActionId } from "./store.ts";

export const testDefinition = durableChannel().state(v.object({ n: v.number() }), { n: 0 })
	.action((a) =>
		a.name("add").payload(v.number()).client().reduce((s, n) => {
			if (n === -1) {
				s.n = 1000;
				throw new RejectAction("no");
			}
			if (n === -2) throw new Error("reducer failure");
			return { n: s.n + n };
		})
	)
	.action((a) => a.name("private").payload(v.number()).reduce((s, n) => ({ n: s.n + n }))).build();
export const testRoutes = durableRoutes().route("a://", testDefinition).route("b://", testDefinition)
	.route("family:/:id", testDefinition).route("internal://", testDefinition, { internal: true }).build();
const caller = { clientId: "identity", connectionId: "socket" };
function setup(options: { historyLimit?: number; receiptLimit?: number; receiptBytes?: number; store?: ChannelStore } = {}) {
	let now = 1000;
	const store = options.store ?? new MemoryChannelStore();
	const actor = new DurableChannelActor("a://", testRoutes, {
		env: {},
		...options,
		store,
		clock: { now: () => now },
		retryHorizonMs: 1000,
	});
	return { actor, store, time: (n: number) => now = n };
}
async function request<TEnv>(actor: DurableChannelActor<TEnv>, n = 1, id = createDistributedActionId(1900), generation?: string) {
	return await actor.dispatchFrom(caller, {
		generation: generation ?? (await actor.snapshot())!.cursor.generation,
		actionId: id,
		clientSeq: 1,
		name: "add",
		payload: n,
	});
}
class FaultStore implements ChannelStore {
	base = new MemoryChannelStore();
	fail = false;
	ambiguous = false;
	async transaction<T>(body: (tx: ChannelTransaction) => T): Promise<T> {
		const result = await this.base.transaction((tx) => {
			const out = body(tx);
			if (this.fail) {
				this.fail = false;
				throw new Error("storage failure");
			}
			return out;
		});
		if (this.ambiguous) {
			this.ambiguous = false;
			throw new Error("ambiguous commit");
		}
		return result;
	}
}

describe("distributed actor", () => {
	it("atomically persists state, sequence, rejection, receipts and bounded history across restart", async () => {
		const { actor, store } = setup({ historyLimit: 2 });
		const before = (await actor.snapshot())!;
		const first = await request(actor, 3);
		assert(first.type === "committed");
		const rejected = await request(actor, -1);
		assert(rejected.type === "committed");
		assertEquals(rejected.envelope.rejectionReason, "no");
		assertEquals((await actor.snapshot())!.state, { n: 3 });
		await request(actor, 4);
		const restarted = new DurableChannelActor("a://", testRoutes, { env: {}, store });
		assertEquals((await restarted.snapshot())!.state, { n: 7 });
		assertEquals((await restarted.snapshot())!.cursor.channelSeq, 3);
		assertEquals((await restarted.inspectResume(before.cursor)).type, "snapshot");
		const replay = await restarted.inspectResume({ ...before.cursor, channelSeq: 1 });
		assert(replay.type === "replay");
		assertEquals(replay.actions.map((e) => e.channelSeq), [2, 3]);
		assertEquals((await restarted.inspectResume({ ...before.cursor, channelSeq: 4 })).type, "snapshot");
		assertEquals((await restarted.inspectResume({ generation: "wrong", channelSeq: 3 })).type, "snapshot");
	});
	it("retained retries use identity and ID, reject conflicts, and ignore transient socket identity", async () => {
		const { actor } = setup();
		const id = createDistributedActionId(1900);
		const first = await request(actor, 1, id);
		const generation = (await actor.snapshot())!.cursor.generation;
		assertEquals(
			await actor.dispatchFrom({ ...caller, connectionId: "replacement" }, {
				generation,
				actionId: id,
				clientSeq: 999,
				name: "add",
				payload: 1,
			}),
			first,
		);
		await assertRejects(() => request(actor, 2, id), Error, "ACTION_ID_CONFLICT");
		assertEquals((await actor.snapshot())!.cursor.channelSeq, 1);
	});
	it("expires absent receipts, retains recorded outcomes, and persists a clock floor", async () => {
		const { actor, time } = setup();
		const id = createDistributedActionId(1500);
		const first = await request(actor, 1, id);
		time(1600);
		assertEquals(await request(actor, 1, id), first);
		await actor.collectExpiredReceipts();
		time(1200);
		assertEquals(await request(actor, 1, id), { type: "unknown", actionId: id });
		time(2100);
		const unseen = createDistributedActionId(2000);
		assertEquals(await request(actor, 9, unseen), { type: "unknown", actionId: unseen });
		assertEquals((await actor.snapshot())!.state, { n: 1 });
		await assertRejects(() => request(actor, 1, createDistributedActionId(4000)), Error, "RETRY_DEADLINE_TOO_FAR");
	});
	it("refuses live receipt count and byte exhaustion before mutation", async () => {
		const { actor, time } = setup({ receiptLimit: 1 });
		await request(actor);
		await assertRejects(() => request(actor), Error, "RETRY_CAPACITY");
		assertEquals((await actor.snapshot())!.cursor.channelSeq, 1);
		time(2000);
		await request(actor, 2, createDistributedActionId(2500));
		assertEquals((await actor.snapshot())!.state, { n: 3 });
		const small = setup({ receiptBytes: 1 }).actor;
		await assertRejects(() => request(small), Error, "RETRY_CAPACITY");
		assertEquals((await small.snapshot())!.cursor.channelSeq, 0);
	});
	it("rolls back storage and reducer exceptions and resolves ambiguous commits from receipts", async () => {
		const store = new FaultStore();
		const { actor } = setup({ store });
		await actor.snapshot();
		const generation = (await actor.snapshot())!.cursor.generation;
		store.fail = true;
		await assertRejects(() => request(actor, 1, createDistributedActionId(1900), generation), Error, "storage failure");
		assertEquals((await actor.snapshot())!.state, { n: 0 });
		assertEquals(await store.base.transaction((tx) => tx.list("log/")), []);
		await assertRejects(() => request(actor, -2), Error, "reducer failure");
		store.ambiguous = true;
		const result = await request(actor, 2, createDistributedActionId(1900), generation);
		assert(result.type === "committed");
		assertEquals(result.envelope.channelSeq, 1);
		assertEquals((await actor.snapshot())!.state, { n: 2 });
	});
	it("advances only its own sequence and rejects exhaustion without state changes", async () => {
		const { actor, store } = setup();
		const initial = (await actor.snapshot())!;
		await store.transaction((tx) => tx.set("meta", { ...tx.get<DistributedChannelRecord>("meta")!, channelSeq: Number.MAX_SAFE_INTEGER }));
		await assertRejects(() => actor.dispatch(initial.cursor.generation, "add", 1), Error, "SEQUENCE_EXHAUSTED");
		assertEquals((await actor.snapshot())!.state, { n: 0 });
	});
	it("uses tombstones, explicit recreation and generation fences for delayed work", async () => {
		const { actor } = setup();
		const old = (await actor.snapshot())!;
		await actor.destroy(old.cursor.generation);
		await assertRejects(() => actor.snapshot(), Error, "No channel instance");
		const fresh = await actor.create();
		assert(old.cursor.generation !== fresh.cursor.generation);
		await assertRejects(() => actor.dispatch(old.cursor.generation, "add", 1), Error, "STALE_GENERATION");
		await assertRejects(() => actor.destroy(old.cursor.generation), Error, "STALE_GENERATION");
		assertEquals((await actor.snapshot())!.state, { n: 0 });
		const race = await Promise.allSettled([actor.create(), actor.destroy(fresh.cursor.generation)]);
		assertEquals(race.map((r) => r.status), ["rejected", "fulfilled"]);
	});
	it("does not create absent families/internal instances for invalid client work", async () => {
		const store = new MemoryChannelStore();
		const actor = new DurableChannelActor("family:/a", testRoutes, { env: {}, store });
		await assertRejects(() => request(actor, 1, createDistributedActionId(1900), "unknown"));
		assertEquals(await store.transaction((tx) => tx.list("")), []);
		const internal = new DurableChannelActor("internal://", testRoutes, { env: {}, store });
		await assertRejects(() => request(internal, 1, createDistributedActionId(1900), "unknown"), Error, "No route matches");
		assertEquals(await store.transaction((tx) => tx.list("")), []);
	});
	it("leniently replays invalid and server-only client actions while leaving state unchanged", async () => {
		const { actor } = setup();
		const generation = (await actor.snapshot())!.cursor.generation;
		for (const [name, payload] of [["missing", 1], ["private", 1], ["add", "wrong"]]) {
			const result = await actor.dispatchFrom(caller, {
				generation,
				name: name as string,
				payload,
				actionId: createDistributedActionId(1900),
				clientSeq: 1,
			});
			assert(result.type === "committed" && result.envelope.rejectionReason);
		}
		assertEquals((await actor.snapshot())!.state, { n: 0 });
		assertEquals((await actor.snapshot())!.cursor.channelSeq, 3);
	});
	it("a gated store on A cannot block independently stored B", async () => {
		let release!: () => void;
		const gate = new Promise<void>((r) => release = r);
		const base = new MemoryChannelStore();
		let blocked = false;
		const store: ChannelStore = {
			async transaction(body) {
				if (blocked) await gate;
				return await base.transaction(body);
			},
		};
		const a = new DurableChannelActor("a://", testRoutes, { env: {}, store });
		const b = new DurableChannelActor("b://", testRoutes, { env: {}, store: new MemoryChannelStore() });
		const ag = (await a.snapshot())!.cursor.generation;
		const bg = (await b.snapshot())!.cursor.generation;
		blocked = true;
		let done = false;
		const pending = a.dispatch(ag, "add", 1).then(() => done = true);
		assertEquals((await b.dispatch(bg, "add", 1)).channelSeq, 1);
		assertEquals(done, false);
		release();
		await pending;
	});
});
