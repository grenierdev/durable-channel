import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { type DurableChannelTransport, InMemoryTransport } from "../transport.ts";
import { DurableChannelHub } from "../hub.ts";
import { MemoryStorage } from "../storage.ts";
import { createRpc } from "../rpc.ts";
import { DurableChannelDistributedClient } from "./client.ts";
import { attachDistributedTransport, createDistributedLink, createDistributedRpc, distributedMessageToFrame } from "./rpc.ts";
import { distributedFixture, distributedTestRoutes } from "./fixture.ts";
import { distributedDeferred } from "./testing.ts";
import type { DurableChannelGateway } from "./gateway.ts";
import type { DurableChannelRpcFrame } from "../rpc.ts";

function link(
	f: ReturnType<typeof distributedFixture>,
	gateway: DurableChannelGateway<unknown>,
	id: string,
	filter?: (frame: DurableChannelRpcFrame) => boolean,
) {
	const [clientSide, serverSide] = InMemoryTransport.pair();
	const transport: DurableChannelTransport = {
		send: (frame) => clientSide.send(frame),
		close: () => clientSide.close(),
		recv: async () => {
			for (;;) {
				const frame = await clientSide.recv();
				if (!frame || !filter || filter(frame)) return frame;
			}
		},
	};
	const attached = attachDistributedTransport(gateway, serverSide, { connectionId: id, clientId: "identity" });
	return {
		transport,
		attached,
		client: new DurableChannelDistributedClient(distributedTestRoutes, transport, {
			clock: f.time,
			scheduler: f.time,
			requestTimeoutMs: 20,
			recoveryDelayMs: 5,
			retryWindowMs: 1000,
		}),
	};
}
describe("distributed optimistic client", () => {
	it("keeps A:100/B:4 independent and recovers B:5 through another authenticated gateway", async () => {
		const f = distributedFixture(), g1 = f.gateway("g1"), g2 = f.gateway("g2");
		let drop = false;
		const first = link(
			f,
			g1,
			"first",
			(frame) =>
				!(drop && "method" in frame && frame.method === "action" &&
					(frame.params as { channel: string; channelSeq: number }).channel === "b://"),
		);
		await first.client.hello({ subscriptions: ["a://", "b://"] });
		for (let i = 0; i < 100; i++) await f.router.dispatch("a://", "add", 1);
		for (let i = 0; i < 4; i++) await f.router.dispatch("b://", "add", 1);
		await f.time.advance(10);
		assertEquals([first.client.cursors["a://"].channelSeq, first.client.cursors["b://"].channelSeq], [100, 4]);
		drop = true;
		await f.router.dispatch("b://", "add", 1);
		await f.time.flush();
		const second = link(f, g2, "second");
		const result = await first.client.reconnect(second.transport);
		assert(result.channels["b://"].type === "replay");
		assertEquals(result.channels["b://"].actions.map((a) => a.channelSeq), [5]);
		assertEquals(first.client.state("a://"), { count: 100 });
		assertEquals(first.client.state("b://"), { count: 5 });
		await first.client.shutdown();
		await first.attached.detach();
		await second.attached.detach();
		g1.close();
		g2.close();
	});
	it("applies optimistically and reconciles echoed/direct/retried duplicates and rejected actions exactly once", async () => {
		const f = distributedFixture(), g = f.gateway("g"), pair = link(f, g, "s");
		await pair.client.hello({ subscriptions: ["a://"] });
		const action = pair.client.dispatch("a://", "add", 3);
		assertEquals(pair.client.state("a://"), { count: 3 });
		assertEquals(pair.client.confirmedState("a://"), { count: 0 });
		assertEquals((await action.settled).status, "confirmed");
		await f.time.flush();
		assertEquals(pair.client.state("a://"), { count: 3 });
		assertEquals((await action.retry()).type, "committed");
		assertEquals(pair.client.cursors["a://"].channelSeq, 1);
		const rejection = pair.client.dispatch("a://", "add", -1);
		assertEquals(pair.client.state("a://"), { count: 3 });
		assertEquals((await rejection.settled).status, "rejected");
		assertEquals(pair.client.cursors["a://"].channelSeq, 2);
		await pair.client.shutdown();
		await pair.attached.detach();
		g.close();
	});
	it("pauses a gap on one channel, keeps another progressing, and retries lost idle resume responses", async () => {
		const f = distributedFixture(), g = f.gateway("g");
		let drop = true;
		const pair = link(f, g, "s", (frame) => {
			if (
				"method" in frame && frame.method === "action" && (frame.params as { channel: string; channelSeq: number }).channel === "b://" &&
				(frame.params as { channelSeq: number }).channelSeq === 1 && drop
			) {
				drop = false;
				return false;
			}
			return true;
		});
		await pair.client.hello({ subscriptions: ["a://", "b://"] });
		await f.router.dispatch("b://", "add", 1);
		await f.time.flush();
		await f.router.dispatch("b://", "add", 1);
		await f.time.flush();
		assertEquals(pair.client.confirmedState("b://"), { count: 0 });
		await f.router.dispatch("a://", "add", 7);
		await f.time.flush();
		assertEquals(pair.client.state("a://"), { count: 7 });
		await f.time.advance(0);
		assertEquals(pair.client.state("b://"), { count: 2 });
		await pair.client.shutdown();
		await pair.attached.detach();
		g.close();
	});
	it("installs mixed replay/snapshot recovery and fresh generations", async () => {
		const f = distributedFixture(1), g1 = f.gateway("g1"), g2 = f.gateway("g2"), pair = link(f, g1, "s");
		await pair.client.hello({ subscriptions: ["a://", "b://"] });
		await f.router.dispatch("b://", "add", 2);
		await f.time.flush();
		await pair.transport.close();
		await f.time.flush();
		await f.router.dispatch("a://", "add", 1);
		await f.router.dispatch("a://", "add", 1);
		await f.router.dispatch("b://", "add", 1);
		const next = link(f, g2, "s2"), result = await pair.client.reconnect(next.transport);
		assertEquals(result.channels["a://"].type, "snapshot");
		assertEquals(result.channels["b://"].type, "replay");
		assertEquals(pair.client.state("a://"), { count: 2 });
		assertEquals(pair.client.state("b://"), { count: 3 });
		const old = pair.client.cursors["a://"].generation;
		await f.router.destroy("a://");
		await f.router.create("a://", { count: 20 });
		await pair.client.recover("a://");
		assert(pair.client.cursors["a://"].generation !== old);
		assertEquals(pair.client.state("a://"), { count: 20 });
		await pair.client.shutdown();
		await next.attached.detach();
		g1.close();
		g2.close();
	});
	it("rejects the global protocol and closes the provisional global connection before any dispatch/reconnect", async () => {
		const hub = new DurableChannelHub(distributedTestRoutes, { env: {}, storage: new MemoryStorage() }), rpc = createRpc(hub);
		const [clientSide, serverSide] = InMemoryTransport.pair();
		let bound: string | undefined;
		const calls: string[] = [];
		const serving = (async () => {
			for (;;) {
				const frame = await serverSide.recv();
				if (!frame) break;
				if ("method" in frame) calls.push(frame.method);
				const response = await rpc.handle(frame, {
					get clientId() {
						return bound;
					},
					bind: (id) => {
						bound = id;
						hub.connect({ id, send() {} });
					},
				});
				if (response) serverSide.send(response);
			}
			if (bound) {
				hub.disconnect(bound);
				bound = undefined;
			}
		})();
		const client = new DurableChannelDistributedClient(distributedTestRoutes, clientSide, { clientId: "distributed-client" });
		await assertRejects(() => client.hello(), Error, "did not negotiate");
		await serving;
		assertEquals(calls, ["hello"]);
		assertEquals(bound, undefined);
		await client.shutdown();
		await hub.close();
	});
	it("ignores overlapping recoveries resolving backwards and unsubscribe/resubscribe while a cut is pending", async () => {
		const f = distributedFixture(), g = f.gateway("g"), [clientSide, serverSide] = InMemoryTransport.pair(), rpc = createDistributedRpc(g);
		const serverLink = createDistributedLink(g, {
			id: "s",
			clientId: "identity",
			send: (m) => serverSide.send(distributedMessageToFrame(m)),
			close: () => serverSide.close(),
		});
		const client = new DurableChannelDistributedClient(distributedTestRoutes, clientSide, {
			clock: f.time,
			scheduler: f.time,
			requestTimeoutMs: 1000,
		});
		let gate: ReturnType<typeof distributedDeferred<void>> | undefined, seen: ReturnType<typeof distributedDeferred<void>> | undefined;
		const serving = (async () => {
			for (;;) {
				const frame = await serverSide.recv();
				if (!frame) return;
				const held = "method" in frame && frame.method === "subscribe" ? gate : undefined;
				if (held) gate = undefined;
				void rpc.handle(frame, serverLink).then(async (response) => {
					if (held) {
						seen!.resolve();
						await held.promise;
					}
					if (response) serverSide.send(response);
				});
			}
		})();
		await client.hello({ subscriptions: ["a://"] });
		gate = distributedDeferred();
		seen = distributedDeferred();
		const firstGate = gate, old = client.recover("a://");
		await seen.promise;
		await f.router.dispatch("a://", "add", 1);
		await f.time.flush();
		await client.recover("a://");
		firstGate.resolve();
		await old;
		assertEquals(client.state("a://"), { count: 1 });
		assertEquals(client.cursors["a://"].channelSeq, 1);
		gate = distributedDeferred();
		seen = distributedDeferred();
		const secondGate = gate, stale = client.recover("a://");
		await seen.promise;
		await client.unsubscribe("a://");
		await f.router.dispatch("a://", "add", 1);
		await client.subscribe("a://");
		secondGate.resolve();
		await stale;
		assertEquals(client.state("a://"), { count: 2 });
		await client.shutdown();
		await serving;
		await serverLink.detach();
		g.close();
	});
	it("retains a newer unsolicited snapshot while an older recovery response is delayed", async () => {
		const f = distributedFixture(1), g = f.gateway("g"), [left, right] = InMemoryTransport.pair(), rpc = createDistributedRpc(g);
		const server = createDistributedLink(g, {
			id: "s",
			clientId: "identity",
			send: (m) => {
				if (m.type !== "action") right.send(distributedMessageToFrame(m));
			},
			close: () => right.close(),
		});
		const client = new DurableChannelDistributedClient(distributedTestRoutes, left, {
			clock: f.time,
			scheduler: f.time,
			requestTimeoutMs: 1000,
		});
		const captured = distributedDeferred(), release = distributedDeferred();
		let hold = true;
		const serving = (async () => {
			for (;;) {
				const frame = await right.recv();
				if (!frame) return;
				const wait = "method" in frame && frame.method === "subscribe" && hold;
				if (wait) hold = false;
				void rpc.handle(frame, server).then(async (response) => {
					if (wait) {
						captured.resolve();
						await release.promise;
					}
					if (response) right.send(response);
				});
			}
		})();
		await client.hello({ subscriptions: ["a://"] });
		const old = client.recover("a://");
		await captured.promise;
		for (let i = 0; i < 10; i++) await f.router.dispatch("a://", "add", 1);
		await f.time.advance(10);
		right.send(
			distributedMessageToFrame({
				type: "recovery",
				channel: "a://",
				revision: 999,
				entry: { type: "snapshot", snapshot: (await f.router.snapshot("a://"))! },
			}),
		);
		await f.time.flush();
		release.resolve();
		await old;
		assertEquals(client.state("a://"), { count: 10 });
		assertEquals(client.cursors["a://"].channelSeq, 10);
		await client.shutdown();
		await serving;
		await server.detach();
		g.close();
	});
	it("retries a lost resume response on an idle channel and reports bounded-buffer backpressure", async () => {
		const f = distributedFixture(), g = f.gateway("g"), [left, right] = InMemoryTransport.pair(), rpc = createDistributedRpc(g);
		const server = createDistributedLink(g, {
			id: "s",
			clientId: "identity",
			send: (m) => right.send(distributedMessageToFrame(m)),
			close: () => right.close(),
		});
		const client = new DurableChannelDistributedClient(distributedTestRoutes, left, {
			clock: f.time,
			scheduler: f.time,
			requestTimeoutMs: 20,
			recoveryDelayMs: 5,
			bufferLimit: 2,
		});
		let dropped = false, hold = false, calls = 0;
		const serving = (async () => {
			for (;;) {
				const frame = await right.recv();
				if (!frame) return;
				void rpc.handle(frame, server).then((response) => {
					if ("method" in frame && frame.method === "subscribe") {
						calls++;
						if (!dropped) {
							dropped = true;
							return;
						}
						if (hold) return;
					}
					if (response) right.send(response);
				});
			}
		})();
		await client.hello({ subscriptions: ["a://"] });
		const pending = client.recover("a://");
		await f.time.flush();
		await f.time.advance(35);
		await pending;
		assert(calls >= 2);
		assertEquals((await client.dispatch("a://", "add", 1).settled).status, "confirmed");
		hold = true;
		const overflow = client.recover("a://");
		await f.time.flush();
		const generation = client.cursors["a://"].generation;
		for (const channelSeq of [3, 4, 5]) {
			right.send(distributedMessageToFrame({ type: "action", channel: "a://", generation, channelSeq, name: "add", payload: 1 }));
		}
		await f.time.flush();
		await overflow;
		assertEquals(client.connected, false);
		assert(client.connectionError?.message.includes("buffer"));
		assertEquals(client.cursors["a://"].channelSeq, 1);
		await client.shutdown();
		await serving;
		await server.detach();
		g.close();
	});
	it("settles pending snapshot outcomes as unknown, preserves immutable retry IDs across gateway movement", async () => {
		const f = distributedFixture(), g = f.gateway("g"), [left, right] = InMemoryTransport.pair(), rpc = createDistributedRpc(g);
		const server = createDistributedLink(g, {
			id: "s",
			clientId: "identity",
			send: (m) => right.send(distributedMessageToFrame(m)),
			close: () => right.close(),
		});
		const client = new DurableChannelDistributedClient(distributedTestRoutes, left, { clock: f.time, scheduler: f.time });
		let dispatches = 0;
		const serving = (async () => {
			for (;;) {
				const frame = await right.recv();
				if (!frame) return;
				if ("method" in frame && frame.method === "dispatch") {
					dispatches++;
					continue;
				}
				void rpc.handle(frame, server).then((response) => {
					if (response) right.send(response);
				});
			}
		})();
		await client.hello({ subscriptions: ["a://"] });
		const pending = client.dispatch("a://", "add", 3);
		await f.time.flush();
		await f.router.destroy("a://");
		await f.router.create("a://");
		await client.recover("a://");
		assertEquals((await pending.settled).status, "unknown");
		assertEquals(client.state("a://"), { count: 0 });
		assertEquals(dispatches, 1);
		await client.shutdown();
		await serving;
		await server.detach();
		g.close();
		const f2 = distributedFixture(), first = link(f2, f2.gateway("g1"), "first");
		await first.client.hello({ subscriptions: ["a://"] });
		const action = first.client.dispatch("a://", "add", 2);
		assertEquals((await action.settled).status, "confirmed");
		const moved = link(f2, f2.gateway("g2"), "moved");
		await first.client.reconnect(moved.transport);
		const retry = await action.retry();
		assert(retry.type === "committed");
		assertEquals(retry.envelope.actionId, action.actionId);
		assertEquals(retry.envelope.channelSeq, 1);
		assertEquals(first.client.state("a://"), { count: 2 });
		await first.client.shutdown();
		await moved.attached.detach();
	});
	it("ignores old transport frames and replaces hello subscription sets", async () => {
		const f = distributedFixture(), g = f.gateway("g"), [left, right] = InMemoryTransport.pair();
		const old: DurableChannelTransport = { send: (frame) => left.send(frame), recv: () => left.recv(), close() {} };
		const attached = attachDistributedTransport(g, right, { connectionId: "old", clientId: "identity" });
		const client = new DurableChannelDistributedClient(distributedTestRoutes, old, { clock: f.time, scheduler: f.time });
		await client.hello({ subscriptions: ["a://", "b://"] });
		await client.hello({ subscriptions: ["a://"] });
		assertEquals(client.subscriptions, ["a://"]);
		const next = link(f, f.gateway("g2"), "new");
		await client.reconnect(next.transport);
		const cursor = client.cursors["a://"];
		right.send(
			distributedMessageToFrame({
				type: "action",
				channel: "a://",
				...cursor,
				channelSeq: cursor.channelSeq + 1,
				name: "add",
				payload: 999,
			}),
		);
		await f.time.flush();
		assertEquals(client.state("a://"), { count: 0 });
		await client.shutdown();
		left.close();
		await attached.detach();
		await next.attached.detach();
		g.close();
	});
	it("fences stale hello failures and transport replacements that await an old close", async () => {
		const [left, right] = InMemoryTransport.pair(), client = new DurableChannelDistributedClient(distributedTestRoutes, left);
		const old = client.hello().catch((error) => error), newer = client.hello();
		const request1 = await right.recv(), request2 = await right.recv();
		assert(request1 && "id" in request1 && request2 && "id" in request2);
		right.send({
			jsonrpc: "2.0",
			id: request2.id!,
			result: { protocol: "durable-channel/distributed-1", clientId: "identity", channels: {} },
		});
		await newer;
		right.send({ jsonrpc: "2.0", id: request1.id!, error: { code: -32603, message: "old failure" } });
		await old;
		assertEquals(client.connected, true);
		await client.shutdown();
		const f = distributedFixture(), first = link(f, f.gateway("g1"), "first"), gate = distributedDeferred();
		const delayedClose: DurableChannelTransport = {
			send: (frame) => first.transport.send(frame),
			recv: () => first.transport.recv(),
			close: async () => {
				await gate.promise;
				await first.transport.close();
			},
		};
		const moving = new DurableChannelDistributedClient(distributedTestRoutes, delayedClose, { clock: f.time, scheduler: f.time });
		await moving.hello({ subscriptions: ["a://"] });
		const [abandoned, abandonedPeer] = InMemoryTransport.pair();
		const stale = moving.reconnect(abandoned).catch((error) => error);
		await f.time.flush();
		const newest = link(f, f.gateway("g2"), "newest");
		let hellos = 0;
		const observed: DurableChannelTransport = {
			send: (frame) => {
				if ("method" in frame && frame.method === "hello") hellos++;
				return newest.transport.send(frame);
			},
			recv: () => newest.transport.recv(),
			close: () => newest.transport.close(),
		};
		await moving.reconnect(observed);
		gate.resolve();
		await stale;
		assertEquals(hellos, 1);
		assertEquals(moving.connected, true);
		await moving.shutdown();
		abandonedPeer.close();
		await first.attached.detach();
		await newest.attached.detach();
	});
	it("keeps separate clients convergent and refuses authenticated identity changes", async () => {
		const f = distributedFixture(), g = f.gateway("g"), one = link(f, g, "one"), two = link(f, g, "two");
		await one.client.hello({ subscriptions: ["a://"] });
		await two.client.hello({ subscriptions: ["a://"] });
		await one.client.of("a://").dispatch({}, "add", 2).settled;
		await f.time.flush();
		assertEquals(two.client.state("a://"), { count: 2 });
		await two.client.dispatch("a://", "add", 3).settled;
		await f.time.advance(10);
		assertEquals(one.client.state("a://"), { count: 5 });
		const [clientSide, serverSide] = InMemoryTransport.pair();
		const wrong = attachDistributedTransport(g, serverSide, { connectionId: "wrong", clientId: "different" });
		await assertRejects(() => one.client.reconnect(clientSide), Error, "CLIENT_IDENTITY_CHANGED");
		assertEquals(one.client.connected, false);
		const typecheck = () => {
			// @ts-expect-error Distributed handles reject invalid action names.
			one.client.of("a://").dispatch({}, "missing", 1);
			// @ts-expect-error Distributed handles reject invalid action payloads.
			one.client.of("a://").dispatch({}, "add", "wrong");
		};
		void typecheck;
		await one.client.shutdown();
		await two.client.shutdown();
		await wrong.detach();
		g.close();
	});
});
