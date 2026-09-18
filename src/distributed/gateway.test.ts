import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import * as v from "valibot";
import { durableChannel } from "../channel.ts";
import { durableRoutes } from "../routes.ts";
import { DurableChannelActor } from "./actor.ts";
import { DurableChannelGateway } from "./gateway.ts";
import { DurableChannelRouter } from "./router.ts";
import { MemoryChannelStore } from "./memory-store.ts";
import type { DistributedActorEndpoint, DistributedGatewayEndpoint, DistributedSession } from "./interfaces.ts";
import type { DistributedMembership, DistributedMessage } from "./protocol.ts";
import { distributedDeferred, FakeDistributedTime } from "./testing.ts";

const definition = durableChannel().state(v.number(), 0).action((a) => a.name("add").payload(v.number()).client().reduce((s, n) => s + n))
	.build();
const stateless = durableChannel().notification((n) => n.name("note").payload(v.string())).build();
const routes = durableRoutes().route("a://", definition).route("b://", definition).route("stateless://", stateless).build();
function fixture(historyLimit = 2) {
	const time = new FakeDistributedTime(),
		owners = new Map<string, DurableChannelActor<unknown>>(),
		endpoints = new Map<string, DistributedActorEndpoint>();
	const stores = new Map<string, MemoryChannelStore>(), gateways = new Map<string, DistributedGatewayEndpoint>();
	const router: DurableChannelRouter<unknown> = new DurableChannelRouter(routes, {
		resolve: (uri) => endpoints.get(uri) ?? owners.get(uri)!,
	});
	const makeActor = (uri: string) => {
		const store = stores.get(uri) ?? new MemoryChannelStore();
		stores.set(uri, store);
		const actor = new DurableChannelActor<unknown>(uri, routes, {
			env: {},
			router,
			store,
			clock: time,
			historyLimit,
			leaseMs: 100,
			retryMs: 10,
			deliveryTimeoutMs: 20,
			timeouts: time,
			gateways: (id) => gateways.get(id)!,
			scheduler: time.durable(`actor/${uri}`, () => owners.get(uri)!.alarm()),
		});
		owners.set(uri, actor);
		return actor;
	};
	for (const uri of ["a://", "b://", "stateless://"]) makeActor(uri);
	const makeGateway = (id: string, store = new MemoryChannelStore()) => {
		const gateway = new DurableChannelGateway({
			id,
			router,
			store,
			clock: time,
			timeouts: time,
			renewMs: 30,
			timeoutMs: 20,
			scheduler: time.durable(`gateway/${id}`, () => (gateways.get(id) as DurableChannelGateway<unknown>).alarm()),
		});
		gateways.set(id, gateway);
		return { gateway, store };
	};
	return { time, router, owners, endpoints, stores, gateways, makeActor, makeGateway };
}
function socket(id: string) {
	const messages: DistributedMessage[] = [];
	const closed: string[] = [];
	const session: DistributedSession = {
		id,
		clientId: "stable-" + id,
		send: (m) => {
			messages.push(m);
		},
		close: (reason) => {
			closed.push(reason);
		},
	};
	return { session, messages, closed };
}
const actions = (messages: DistributedMessage[]) => messages.filter((m) => m.type === "action").map((m) => m.channelSeq);

describe("distributed gateway delivery", () => {
	it("fans out through two gateways and retries a lost final action on an otherwise idle channel", async () => {
		const f = fixture(), g1 = f.makeGateway("g1").gateway, g2 = f.makeGateway("g2").gateway, c1 = socket("c1"), c2 = socket("c2");
		const b1 = await g1.connect(c1.session), b2 = await g2.connect(c2.session);
		await g1.subscribe("c1", b1, "a://");
		await g2.subscribe("c2", b2, "a://");
		let dropped = false;
		f.gateways.set("g2", {
			deliver: (m) => {
				if (!dropped) {
					dropped = true;
					return Promise.reject(new Error("lost"));
				}
				return g2.deliver(m);
			},
			notify: (...args) => g2.notify(...args),
		});
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(actions(c1.messages), [1]);
		assertEquals(actions(c2.messages), []);
		await f.time.advance(10);
		assertEquals(actions(c2.messages), [1]);
		f.gateways.set("g2", g2);
		g1.close();
		g2.close();
	});
	it("buffers each local subscriber through snapshot handoff even with an existing socket", async () => {
		const f = fixture(), g = f.makeGateway("g").gateway, c1 = socket("c1"), c2 = socket("c2");
		const b1 = await g.connect(c1.session), b2 = await g.connect(c2.session);
		await g.subscribe("c1", b1, "a://");
		const actor = f.owners.get("a://")!, cut = distributedDeferred(), release = distributedDeferred();
		f.endpoints.set(
			"a://",
			new Proxy(actor, {
				get(target, key) {
					if (key === "resume") {
						return async (...args: Parameters<typeof actor.resume>) => {
							const result = await actor.resume(...args);
							cut.resolve();
							await release.promise;
							return result;
						};
					}
					const value = Reflect.get(target, key);
					return typeof value === "function" ? value.bind(target) : value;
				},
			}),
		);
		const pending = g.subscribe("c2", b2, "a://");
		await cut.promise;
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(actions(c2.messages), []);
		release.resolve();
		await pending;
		await f.time.advance(10);
		assertEquals(actions(c1.messages), [1]);
		assertEquals(actions(c2.messages), [1]);
		await g.unsubscribe("c1", b1, "a://");
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(actions(c1.messages), [1]);
		assertEquals(actions(c2.messages), [1, 2]);
		g.close();
	});
	it("recovers retained publication after owner restart and falls back to bounded-history snapshots", async () => {
		const f = fixture(1), g = f.makeGateway("g").gateway, c = socket("c");
		const binding = await g.connect(c.session);
		await g.subscribe("c", binding, "a://");
		f.gateways.set("g", { deliver: () => Promise.reject(new Error("offline")), notify: () => Promise.resolve() });
		await f.router.dispatch("a://", "add", 1);
		await f.router.dispatch("a://", "add", 1);
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		f.owners.get("a://")!.close();
		f.makeActor("a://");
		f.gateways.set("g", g);
		await f.time.advance(10);
		assert(c.messages.some((m) => m.type === "recovery" && m.entry.type === "snapshot" && m.entry.snapshot.state === 3));
		assertEquals((await f.stores.get("a://")!.transaction((tx) => tx.list("log/"))).length, 1);
		g.close();
	});
	it("reconstructs live subscription rows, including stateless membership, and removes dead socket rows", async () => {
		const f = fixture(), first = f.makeGateway("g"), live = socket("live"), dead = socket("dead");
		const b = await first.gateway.connect(live.session), d = await first.gateway.connect(dead.session);
		await first.gateway.subscribe("live", b, "a://");
		await first.gateway.subscribe("live", b, "stateless://");
		await first.gateway.subscribe("dead", d, "a://");
		first.gateway.close();
		const fresh = f.makeGateway("g", first.store).gateway;
		await fresh.restore([live.session]);
		await f.router.notify("stateless://", "note", "restored");
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assert(live.messages.some((m) => m.type === "notification" && m.payload === "restored"));
		assertEquals(actions(live.messages), [1]);
		assertEquals(
			(await first.store.transaction((tx) => tx.list<{ connectionId: string }>("subscription/"))).some(([, row]) =>
				row.connectionId === "dead"
			),
			false,
		);
		fresh.close();
	});
	it("fences stale connection cleanup and old membership ACK/removal/renewal", async () => {
		const f = fixture(), g = f.makeGateway("g").gateway, old = socket("c"), replacement = socket("c");
		const b1 = await g.connect(old.session);
		await g.subscribe("c", b1, "a://");
		const actor = f.owners.get("a://")!, store = f.stores.get("a://")!;
		const member = (await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!;
		const b2 = await g.connect(replacement.session);
		await g.subscribe("c", b2, "a://");
		await g.disconnect("c", b1);
		await actor.acknowledge("g", member, {
			revision: member.revision,
			generation: member.generation,
			channelSeq: member.target,
			clients: 0,
		});
		await assertRejects(
			() => actor.remove({ gatewayId: "g", revision: member.revision, generation: member.generation }),
			Error,
			"STALE_MEMBERSHIP",
		);
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(actions(replacement.messages), [1]);
		const current = (await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!;
		await actor.remove({ gatewayId: "g", revision: current.revision + 1, generation: current.generation });
		await assertRejects(
			() => actor.renew({ gatewayId: "g", revision: current.revision, generation: current.generation }),
			Error,
			"STALE_MEMBERSHIP",
		);
		g.close();
	});
	it("a stale expiry candidate cannot delete a same-revision renewed lease", async () => {
		const f = fixture(), g = f.makeGateway("g").gateway, c = socket("c");
		const b = await g.connect(c.session);
		await g.subscribe("c", b, "a://");
		const actor = f.owners.get("a://")!,
			store = f.stores.get("a://")!,
			old = (await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!;
		f.time.time += 50;
		await actor.renew({ gatewayId: "g", generation: old.generation, revision: old.revision });
		f.time.time = old.expires;
		await actor.expireMember(old);
		assertEquals((await store.transaction((tx) => tx.get<DistributedMembership>("member/g")))!.active, true);
		f.time.time += 100;
		await assertRejects(
			() => actor.renew({ gatewayId: "g", generation: old.generation, revision: old.revision }),
			Error,
			"MEMBERSHIP_EXPIRED",
		);
		g.close();
	});
	it("ignores a delayed recovery after unsubscribe/resubscribe and accepts future-cursor snapshot recovery", async () => {
		const f = fixture(), g = f.makeGateway("g").gateway, c = socket("c");
		const b = await g.connect(c.session);
		const actor = f.owners.get("a://")!, captured = distributedDeferred(), release = distributedDeferred();
		let count = 0;
		f.endpoints.set(
			"a://",
			new Proxy(actor, {
				get(target, key) {
					if (key === "resume") {
						return async (...args: Parameters<typeof actor.resume>) => {
							const result = await actor.resume(...args);
							if (++count === 1) {
								captured.resolve();
								await release.promise;
							}
							return result;
						};
					}
					const value = Reflect.get(target, key);
					return typeof value === "function" ? value.bind(target) : value;
				},
			}),
		);
		const old = g.subscribe("c", b, "a://");
		await captured.promise;
		await g.unsubscribe("c", b, "a://");
		await f.router.dispatch("a://", "add", 2);
		const generation = (await actor.snapshot())!.cursor.generation;
		const current = await g.subscribe("c", b, "a://", { generation, channelSeq: 999 });
		assertEquals(current.type, "snapshot");
		release.resolve();
		await old;
		const cuts = c.messages.filter((m) => m.type === "recovery" && m.entry.type === "snapshot").map((m) =>
			m.type === "recovery" && m.entry.type === "snapshot" ? m.entry.snapshot.state : null
		);
		assertEquals(cuts, [2]);
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(actions(c.messages), [2]);
		g.close();
	});
	it("retries a lost resume response without another action and stops idle gateway alarms", async () => {
		const f = fixture(), g = f.makeGateway("g").gateway, c = socket("c");
		const b = await g.connect(c.session);
		const actor = f.owners.get("a://")!;
		let drop = true;
		f.endpoints.set(
			"a://",
			new Proxy(actor, {
				get(target, key) {
					if (key === "resume") {
						return async (...args: Parameters<typeof actor.resume>) => {
							const result = await actor.resume(...args);
							if (drop) {
								drop = false;
								return await new Promise<never>(() => {});
							}
							return result;
						};
					}
					const value = Reflect.get(target, key);
					return typeof value === "function" ? value.bind(target) : value;
				},
			}),
		);
		const pending = g.subscribe("c", b, "a://").catch(() => {});
		await f.time.flush();
		await f.time.advance(40);
		await pending;
		assert(c.messages.some((m) => m.type === "recovery"));
		await g.unsubscribe("c", b, "a://");
		await f.time.advance(200);
		assertEquals(f.time.pending, 0);
		g.close();
	});
	it("fences subscribe intent before asynchronous owner resolution", async () => {
		const f = fixture(), started = distributedDeferred(), release = distributedDeferred();
		let first = true;
		const router = new DurableChannelRouter(routes, {
			resolve: async (uri) => {
				if (first) {
					first = false;
					started.resolve();
					await release.promise;
				}
				return f.owners.get(uri)!;
			},
		});
		const g = new DurableChannelGateway({
			id: "g",
			router,
			store: new MemoryChannelStore(),
			scheduler: f.time.durable("g", () => Promise.resolve()),
			timeouts: f.time,
		});
		f.gateways.set("g", g);
		const c = socket("c"), binding = await g.connect(c.session);
		const subscribe = g.subscribe("c", binding, "a://");
		await started.promise;
		await g.unsubscribe("c", binding, "a://");
		release.resolve();
		assertEquals((await subscribe).type, "missing");
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		assertEquals(c.messages, []);
		g.close();
	});
	it("restores local bindings before bounded independent recovery of a stalled owner", async () => {
		const f = fixture(), first = f.makeGateway("g"), live = socket("live");
		const binding = await first.gateway.connect(live.session);
		await first.gateway.subscribe("live", binding, "a://");
		await first.gateway.subscribe("live", binding, "b://");
		first.gateway.close();
		live.messages.length = 0;
		const actor = f.owners.get("a://")!;
		f.endpoints.set(
			"a://",
			new Proxy(actor, {
				get(target, key) {
					if (key === "snapshot") return () => new Promise<never>(() => {});
					const value = Reflect.get(target, key);
					return typeof value === "function" ? value.bind(target) : value;
				},
			}),
		);
		const fresh = f.makeGateway("g", first.store).gateway;
		await fresh.restore([live.session], { recover: false });
		const recovery = fresh.alarm();
		await f.time.flush();
		assert(live.messages.some((m) => m.type === "recovery" && m.channel === "b://"));
		await f.router.dispatch("b://", "add", 1);
		await f.time.flush();
		assert(live.messages.some((m) => m.type === "action" && m.channel === "b://"));
		await f.time.advance(20);
		await recovery;
		fresh.close();
	});
});
