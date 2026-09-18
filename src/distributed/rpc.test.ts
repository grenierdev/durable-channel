import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import { DurableChannelClient } from "../client.ts";
import { InMemoryTransport } from "../transport.ts";
import { distributedFixture, distributedTestRoutes } from "./fixture.ts";
import { attachDistributedSocket, attachDistributedTransport, createDistributedLink, createDistributedRpc } from "./rpc.ts";
import { DISTRIBUTED_PROTOCOL, type DistributedReconnectResult } from "./protocol.ts";
import { distributedDeferred } from "./testing.ts";
import { createDistributedActionId } from "./store.ts";

describe("distributed RPC", () => {
	it("rejects global and unknown hello versions before binding authenticated identity", async () => {
		const f = distributedFixture(), gateway = f.gateway("g"), rpc = createDistributedRpc(gateway);
		const link = createDistributedLink(gateway, { id: "socket", clientId: "host-identity", send() {}, close() {} });
		for (const params of [{ clientId: "global" }, { protocol: "distributed-0" }]) {
			const result = await rpc.handle({ jsonrpc: "2.0", id: 1, method: "hello", params }, link);
			assert(result && "error" in result);
			assertEquals(link.binding, undefined);
		}
		const [clientSide, serverSide] = InMemoryTransport.pair();
		const attached = attachDistributedTransport(gateway, serverSide, { connectionId: "global", clientId: "host" });
		const global = new DurableChannelClient(distributedTestRoutes, clientSide);
		global.connect();
		await assertRejects(() => global.hello());
		assertEquals(attached.link.binding, undefined);
		await global.shutdown();
		await attached.detach();
		gateway.close();
	});
	it("uses independent cursor maps and host identity for replay and exact retries", async () => {
		const f = distributedFixture();
		for (let i = 0; i < 100; i++) await f.router.dispatch("a://", "add", 1);
		for (let i = 0; i < 4; i++) await f.router.dispatch("b://", "add", 1);
		const a = (await f.router.snapshot("a://"))!, b = (await f.router.snapshot("b://"))!;
		await f.router.dispatch("b://", "add", 1);
		const gateway = f.gateway("g"),
			rpc = createDistributedRpc(gateway),
			link = createDistributedLink(gateway, { id: "s", clientId: "host", send() {}, close() {} });
		const hello = await rpc.handle({
			jsonrpc: "2.0",
			id: 1,
			method: "hello",
			params: {
				protocol: DISTRIBUTED_PROTOCOL,
				clientId: "spoofed",
				subscriptions: ["a://", "b://", "private://"],
				cursors: { "a://": a.cursor, "b://": b.cursor },
			},
		}, link);
		assert(hello && "result" in hello);
		const channels = (hello.result as DistributedReconnectResult).channels;
		assert(channels["b://"].type === "replay");
		assertEquals(channels["b://"].actions.map((e) => e.channelSeq), [5]);
		assertEquals(channels["private://"].type, "missing");
		const params = {
			channel: "b://",
			generation: b.cursor.generation,
			name: "add",
			payload: 1,
			actionId: createDistributedActionId(1900),
			clientSeq: 1,
			clientId: "spoofed",
		};
		const dispatch = await rpc.handle({ jsonrpc: "2.0", id: 2, method: "dispatch", params }, link);
		assert(dispatch && "result" in dispatch);
		assertEquals((dispatch.result as { envelope: { origin: { clientId: string } } }).envelope.origin.clientId, "host");
		await link.detach();
		gateway.close();
	});
	it("validates generations/numbers, hides internal routes and maps unexpected errors safely", async () => {
		const f = distributedFixture(),
			gateway = f.gateway("g"),
			rpc = createDistributedRpc(gateway),
			link = createDistributedLink(gateway, { id: "s", clientId: "host", send() {}, close() {} });
		await rpc.handle({ jsonrpc: "2.0", id: 1, method: "hello", params: { protocol: DISTRIBUTED_PROTOCOL } }, link);
		const generation = (await f.router.snapshot("a://"))!.cursor.generation;
		for (
			const patch of [{ generation: "" }, { clientSeq: -1 }, { clientSeq: Number.MAX_SAFE_INTEGER + 1 }, { channel: "private://" }, {
				generation: "stale",
			}]
		) {
			const reply = await rpc.handle({
				jsonrpc: "2.0",
				id: 2,
				method: "dispatch",
				params: { channel: "a://", generation, name: "add", payload: 1, actionId: createDistributedActionId(1900), clientSeq: 1, ...patch },
			}, link);
			assert(reply && "error" in reply);
		}
		const failure = await rpc.handle({
			jsonrpc: "2.0",
			id: 3,
			method: "exec",
			params: { channel: "a://", generation, name: "fail", params: null },
		}, link);
		assert(failure && "error" in failure);
		assertEquals(failure.error.message, "Internal error");
		assertEquals((await f.router.snapshot("a://"))!.cursor.channelSeq, 0);
		await link.detach();
		gateway.close();
	});
	it("allows hello to retry a transient connection-binding failure", async () => {
		const f = distributedFixture(), gateway = f.gateway("g");
		const original = gateway.connect.bind(gateway);
		let fail = true;
		gateway.connect = (session) => {
			if (fail) {
				fail = false;
				return Promise.reject(new Error("temporary store outage"));
			}
			return original(session);
		};
		const link = createDistributedLink(gateway, { id: "retry", clientId: "host", send() {}, close() {} }),
			rpc = createDistributedRpc(gateway);
		const frame = { jsonrpc: "2.0", id: 1, method: "hello", params: { protocol: DISTRIBUTED_PROTOCOL } };
		const first = await rpc.handle(frame, link);
		assert(first && "error" in first);
		assertEquals(link.binding, undefined);
		const second = await rpc.handle(frame, link);
		assert(second && "result" in second);
		assert(link.binding !== undefined);
		await link.detach();
		gateway.close();
	});
	it("detached socket adapters ignore new frames and delayed old responses", async () => {
		const f = distributedFixture(), gateway = f.gateway("g"), rpc = createDistributedRpc(gateway), release = distributedDeferred();
		const listeners = new Map<string, (event: { data: unknown }) => void>(), sent: string[] = [];
		const original = rpc.handle;
		rpc.handle = async (...args) => {
			const result = await original(...args);
			await release.promise;
			return result;
		};
		const attached = attachDistributedSocket(rpc, gateway, {
			send: (s) => {
				sent.push(s);
			},
			close() {},
			addEventListener: (type, fn) => {
				listeners.set(type, fn);
			},
		}, { connectionId: "s", clientId: "host" });
		const ping = { data: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "ping", params: {} }) };
		listeners.get("message")!(ping);
		await f.time.flush();
		await attached.detach();
		release.resolve();
		await f.time.flush();
		listeners.get("message")!(ping);
		await f.time.flush();
		assertEquals(sent, []);
		gateway.close();
	});
});
