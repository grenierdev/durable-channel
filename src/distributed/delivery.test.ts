import { describe, it } from "node:test";
import { assert, assertEquals } from "@std/assert";
import * as v from "valibot";
import { durableChannel } from "../channel.ts";
import { durableRoutes } from "../routes.ts";
import { DurableChannelActor } from "./actor.ts";
import { MemoryChannelStore } from "./memory-store.ts";
import type { DistributedMembership } from "./protocol.ts";
import { distributedDeferred, FakeDistributedTime } from "./testing.ts";

const routes = durableRoutes().route(
	"a://",
	durableChannel().state(v.number(), 0).action((a) => a.name("add").payload(v.number()).reduce((s, n) => s + n)).build(),
).build();
describe("distributed publication scheduling", () => {
	it("rotates bounded concurrency past stalled gateways and quiesces after leases expire", async () => {
		const time = new FakeDistributedTime();
		let delivered = 0;
		const actor: DurableChannelActor<unknown> = new DurableChannelActor<unknown>("a://", routes, {
			env: {},
			store: new MemoryChannelStore(),
			clock: time,
			timeouts: time,
			leaseMs: 100,
			retryMs: 10,
			deliveryTimeoutMs: 20,
			deliveryConcurrency: 1,
			scheduler: time.durable("a", () => actor.alarm()),
			gateways: (id) => ({
				deliver: (m) => {
					if (id !== "z-healthy") return new Promise(() => {});
					delivered++;
					return Promise.resolve({ generation: m.generation, revision: m.revision, channelSeq: 1, clients: 1 });
				},
				notify: () => Promise.resolve(),
			}),
		});
		const generation = (await actor.snapshot())!.cursor.generation;
		for (const gatewayId of ["a-stalled", "b-stalled", "z-healthy"]) await actor.resume({ gatewayId, generation, revision: 1 });
		assertEquals((await actor.dispatch(generation, "add", 1)).channelSeq, 1);
		await time.advance(70);
		assertEquals(delivered, 1);
		await time.advance(200);
		assertEquals(time.pending, 0);
	});
	it("an old ACK cannot clear a newer target; an ACK loss duplicates handoff safely", async () => {
		const time = new FakeDistributedTime(),
			store = new MemoryChannelStore(),
			first = distributedDeferred(),
			release = distributedDeferred();
		let calls = 0;
		const actor: DurableChannelActor<unknown> = new DurableChannelActor<unknown>("a://", routes, {
			env: {},
			store,
			clock: time,
			timeouts: time,
			retryMs: 10,
			scheduler: time.durable("a", () => actor.alarm()),
			gateways: () => ({
				notify: () => Promise.resolve(),
				deliver: async (m) => {
					calls++;
					if (calls === 1) {
						first.resolve();
						await release.promise;
					}
					const seq = m.entry.type === "replay"
						? m.entry.cursor.channelSeq
						: m.entry.type === "snapshot"
						? m.entry.snapshot.cursor.channelSeq
						: 0;
					if (calls === 2) throw new Error("ACK lost after socket handoff");
					return { generation: m.generation, revision: m.revision, channelSeq: seq, clients: 1 };
				},
			}),
		});
		const generation = (await actor.snapshot())!.cursor.generation;
		await actor.resume({ gatewayId: "g", generation, revision: 1 });
		await actor.dispatch(generation, "add", 1);
		await first.promise;
		await actor.dispatch(generation, "add", 1);
		release.resolve();
		await time.flush();
		const mid = (await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!;
		assertEquals([mid.ack, mid.target], [1, 2]);
		await time.advance(20);
		const final = (await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!;
		assertEquals([final.ack, final.target], [2, 2]);
		assert(calls >= 3);
	});
});
