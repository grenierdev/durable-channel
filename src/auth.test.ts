import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import * as v from "valibot";
import {
	attachSocket,
	AuthenticationRequiredError,
	ConnectionNotFoundError,
	createRpc,
	durableChannel,
	DurableChannelClient,
	type DurableChannelConnection,
	DurableChannelHub,
	type DurableChannelMessage,
	type DurableChannelPrincipal,
	type DurableChannelRpcFrame,
	durableRoutes,
	IdentityChangedError,
	InMemoryTransport,
	MemoryStorage,
	NotInitializedError,
	PermissionDeniedError,
	RejectAction,
	RouteNotFoundError,
	RpcError,
} from "./mod.ts";

type Principal = { id: string; role: "admin" | "member"; name: string };
type Env = { seen: string[]; wait(): Promise<void> };
const alice: Principal = { id: "alice", role: "admin", name: "Alice" };
const bob: Principal = { id: "bob", role: "member", name: "Bob" };

const channel = durableChannel().env<Env>().principal<Principal>()
	.state(v.object({ authors: v.array(v.string()) }), { authors: [] })
	.action((a) =>
		a.name("post").payload(v.object({ author: v.string() })).client()
			.reduce((state, payload, meta) => {
				if (!meta.origin?.principalId || payload.author !== meta.origin.principalId) throw new RejectAction("Sign in as the author");
				return { authors: [...state.authors, payload.author] };
			})
			.effect(async (ctx) => {
				await ctx.env.wait();
				ctx.env.seen.push(ctx.principal?.name ?? "anonymous");
				await ctx.exec("private://", "who", {});
			})
	)
	.action((a) =>
		a.name("guest").payload(v.object({})).client().reduce((state, _payload, meta) => ({
			authors: [...state.authors, meta.origin?.principalId ?? (meta.origin ? "guest" : "system")],
		}))
	)
	.command((c) =>
		c.name("who").params(v.object({})).result(v.string()).handler((_params, ctx) => {
			const name: string = ctx.principal?.name ?? "anonymous";
			ctx.env.seen.push(name);
			return name;
		})
	)
	.command((c) =>
		c.name("admin").params(v.object({})).result(v.string()).handler((_params, ctx) => {
			if (!ctx.principal) throw new AuthenticationRequiredError();
			if (ctx.principal.role !== "admin") throw new PermissionDeniedError();
			return ctx.principal.name;
		})
	)
	.command((c) =>
		c.name("nested").params(v.object({})).result(v.string()).handler(async (_params, ctx) => {
			await ctx.env.wait();
			return await ctx.exec("private://", "who", {}) as string;
		})
	)
	.command((c) =>
		c.name("system").params(v.object({})).result(v.null()).handler(async (_params, ctx) => {
			await ctx.dispatch("room://", "guest", {});
			return null;
		})
	)
	.build();

const routes = durableRoutes().principal<Principal>().env<Env>()
	.route("room://", channel).route("private://", channel, { internal: true }).build();

function boot(wait: () => Promise<void> = () => Promise.resolve(), storage = new MemoryStorage()) {
	const env: Env = { seen: [], wait };
	const hub = new DurableChannelHub(routes, { env, storage });
	const rpc = createRpc(hub);
	function serve(principal?: Principal) {
		const [transport, peer] = InMemoryTransport.pair();
		const listeners = new Map<string, ((event: { data: unknown }) => void)[]>();
		const sent: DurableChannelRpcFrame[] = [];
		let muted = false;
		let closed = false;
		const emit = (type: string, data?: unknown) => listeners.get(type)?.forEach((fn) => fn({ data }));
		const socket = {
			send(data: string) {
				if (closed) throw new Error("closed");
				const frame = JSON.parse(data);
				sent.push(frame);
				if (!muted) peer.send(frame);
			},
			close() {
				if (!closed) {
					closed = true;
					peer.close();
					emit("close");
				}
			},
			addEventListener(type: string, fn: (event: { data: unknown }) => void) {
				listeners.set(type, [...(listeners.get(type) ?? []), fn]);
			},
		};
		const session = attachSocket(rpc, hub, socket, principal === undefined ? undefined : { principal });
		const loop = (async () => {
			while (true) {
				const frame = await peer.recv();
				if (frame === null) {
					socket.close();
					return;
				}
				emit("message", JSON.stringify(frame));
			}
		})();
		return {
			transport,
			session,
			sent,
			loop,
			mute() {
				muted = true;
			},
		};
	}
	async function open(id: string, principal?: Principal) {
		const link = serve(principal);
		const client = new DurableChannelClient(routes, link.transport, { clientId: id, requestTimeoutMs: 2000 });
		client.connect();
		await client.hello({ subscriptions: ["room://"] });
		return { client, link };
	}
	return { hub, env, serve, open };
}

async function until(test: () => boolean): Promise<void> {
	const deadline = Date.now() + 2000;
	while (!test()) {
		if (Date.now() > deadline) throw new Error("Condition timed out");
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
}

function gate() {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => release = resolve);
	return { promise, release };
}

describe("optional principals", () => {
	it("allows anonymous callers and lets each command and action decide", async () => {
		const host = boot();
		const { client, link } = await host.open("guest-tab");
		try {
			assertEquals(client.principal, undefined);
			assertEquals(await client.exec("room://", "who", {}), "anonymous");
			const error = await assertRejects(() => client.exec("room://", "admin", {}), RpcError);
			assertEquals(error.code, -32007);
			assertEquals((await client.dispatch("room://", "post", { author: "alice" }).settled).status, "rejected");
			const accepted = await client.dispatch("room://", "guest", {}).settled;
			assert(accepted.status === "confirmed");
			assertEquals(accepted.envelope.origin, { clientId: "guest-tab", clientSeq: 2 });
			await client.exec("room://", "system", {});
			await until(() => client.state("room://").authors.length === 2);
			assertEquals(client.state("room://").authors, ["guest", "system"]);
		} finally {
			await client.shutdown();
			await link.loop;
			await host.hub.close();
		}
	});

	it("shares a principal across tabs and projects only identity onto the wire", async () => {
		const host = boot();
		const first = await host.open("tab-1", alice);
		const second = await host.open("tab-2", alice);
		try {
			assertEquals(first.client.principal, { id: "alice" });
			assertEquals(await first.client.exec("room://", "admin", {}), "Alice");
			const action = first.client.dispatch("room://", "post", { author: "alice" });
			assertEquals(first.client.state("room://").authors, ["alice"]);
			assertEquals((await action.settled).status, "confirmed");
			await until(() => second.client.confirmed("room://").authors.length === 1);
			assertEquals((await second.client.dispatch("room://", "post", { author: "bob" }).settled).status, "rejected");
			assertEquals(second.client.state("room://"), await host.hub.get("room://"));
			const broadcast = second.link.sent.find((frame) => "method" in frame && frame.method === "action");
			assert(broadcast && "params" in broadcast);
			assertEquals((broadcast.params as { origin: unknown }).origin, { clientId: "tab-1", clientSeq: 1, principalId: "alice" });
			assert(!JSON.stringify(second.link.sent).includes('"role"'));
			assert(!JSON.stringify(second.link.sent).includes('"name":"Alice"'));
		} finally {
			await first.client.shutdown();
			await second.client.shutdown();
			await host.hub.close();
		}
	});

	it("keeps principal identity through pending rebases and reconnect replay", async () => {
		const host = boot();
		const { client, link } = await host.open("tab", alice);
		try {
			link.mute();
			const first = client.dispatch("room://", "post", { author: "alice" });
			const second = client.dispatch("room://", "post", { author: "alice" });
			assertEquals(client.state("room://").authors, ["alice", "alice"]);
			await until(() => host.hub.serverSeq === 2);
			link.session.close();
			await until(() => client.connectionState.status === "closed");
			const result = await client.reconnect(host.serve(alice).transport);
			assertEquals(result.principal, { id: "alice" });
			assertEquals((await first.settled).status, "confirmed");
			assertEquals((await second.settled).status, "confirmed");
			assertEquals(client.state("room://"), await host.hub.get("room://"));
		} finally {
			await client.shutdown();
			await host.hub.close();
		}
	});

	it("settles a live echo arriving before the reconnect response", async () => {
		const host = boot();
		const { client, link } = await host.open("tab", alice);
		const [transport, peer] = InMemoryTransport.pair();
		try {
			link.mute();
			const pending = client.dispatch("room://", "post", { author: "alice" });
			await until(() => host.hub.serverSeq === 1);
			link.session.close();
			await until(() => client.connectionState.status === "closed");
			const resumed = client.reconnect(transport);
			const request = await peer.recv();
			assert(request && "id" in request && request.id !== undefined);
			// Model an action broadcast after the server captured its replay result,
			// but before that result reaches the client.
			peer.send({
				jsonrpc: "2.0",
				method: "action",
				params: {
					channel: "room://",
					name: "post",
					payload: { author: "alice" },
					serverSeq: 1,
					origin: { clientId: "tab", clientSeq: 1, principalId: "alice" },
				},
			});
			peer.send({ jsonrpc: "2.0", id: request.id, result: { type: "replay", actions: [], missing: [], principal: { id: "alice" } } });
			await resumed;
			assertEquals((await pending.settled).status, "confirmed");
			assertEquals(client.state("room://").authors, ["alice"]);
		} finally {
			peer.close();
			await client.shutdown();
			await host.hub.close();
		}
	});

	for (const [before, after] of [[alice, bob], [alice, undefined], [undefined, alice]] as const) {
		it(`clears pending work and state when identity changes from ${before?.id} to ${after?.id}`, async () => {
			const host = boot();
			const { client, link } = await host.open("tab", before);
			try {
				link.mute();
				const pending = client.dispatch("room://", "guest", {});
				await until(() => host.hub.serverSeq === 1);
				link.session.close();
				await until(() => client.connectionState.status === "closed");
				await assertRejects(() => client.reconnect(host.serve(after).transport), IdentityChangedError);
				assertEquals((await pending.settled).status, "lost");
				assertThrows(() => client.state("room://"));
				assertEquals(client.principal, undefined);
			} finally {
				await client.shutdown();
				await host.hub.close();
			}
		});
	}

	it("rejects stale, forged and foreign handles and protects active principal ownership", async () => {
		const host = boot();
		let ended = 0;
		const supplied = { ...alice };
		const original = host.hub.connect({
			id: "tab",
			principal: supplied,
			send() {},
			onDisconnect() {
				ended++;
			},
		});
		supplied.id = "changed";
		supplied.name = "Changed";
		assertEquals(original.principal?.name, "Alice");
		assertThrows(() => host.hub.connect({ id: "tab", principal: bob, send() {} }), PermissionDeniedError);
		assertThrows(() => host.hub.connect({ id: "tab", send() {} }), PermissionDeniedError);
		assertEquals(ended, 0);
		const current = host.hub.connect({ id: "tab", principal: alice, send() {} });
		assertEquals(ended, 1);
		const other = boot();
		const foreign = other.hub.connect({ id: "tab", principal: alice, send() {} });
		for (const invalid of [original, foreign, { ...current } as DurableChannelConnection<Principal>]) {
			await assertRejects(() => host.hub.subscribe(invalid, "room://"), ConnectionNotFoundError);
			await assertRejects(() => host.hub.dispatchFrom(invalid, "room://", "guest", {}, 1), ConnectionNotFoundError);
			await assertRejects(() => host.hub.execFrom(invalid, "room://", "who", {}), ConnectionNotFoundError);
			await assertRejects(() => host.hub.reconnect(invalid, 0, ["room://"]), ConnectionNotFoundError);
			assertThrows(() => host.hub.unsubscribe(invalid, "room://"), ConnectionNotFoundError);
			host.hub.disconnect(invalid);
		}
		assertEquals(await host.hub.execFrom(current, "room://", "who", {}), "Alice");
		assertEquals(await host.hub.of("room://").execFrom(current, {}, "who", {}), "Alice");
		await assertRejects(() => host.hub.execFrom(current, "private://", "who", {}), RouteNotFoundError);
		assertEquals(await host.hub.execFrom(current, "room://", "nested", {}), "Alice");
		assertEquals(await host.hub.exec("private://", "who", {}), "anonymous");
		assertThrows(() => host.hub.connect({ id: "empty", principal: { ...alice, id: "" }, send() {} }), PermissionDeniedError);
		host.hub.disconnect(current);
		await assertRejects(() => host.hub.subscribe(current, "room://"), ConnectionNotFoundError);
		await host.hub.close();
		await other.hub.close();
	});

	it("preserves the accepted caller in commands and effects after replacement", async () => {
		const paused = gate();
		const host = boot(() => paused.promise);
		const connection = host.hub.connect({ id: "tab", principal: alice, send() {} });
		const action = host.hub.dispatchFrom(connection, "room://", "post", { author: "alice" }, 1);
		await until(() => host.hub.serverSeq === 1);
		const command = host.hub.execFrom(connection, "room://", "nested", {});
		await new Promise((resolve) => setTimeout(resolve, 0));
		host.hub.connect({ id: "tab", principal: { ...alice, name: "New name" }, send() {} });
		paused.release();
		await action;
		assertEquals(await command, "Alice");
		assertEquals(host.env.seen, ["Alice", "Alice", "Alice"]);
		await host.hub.close();
	});

	it("revalidates handles after hydration and before a queued commit", async () => {
		const hydration = gate();
		const commit = gate();
		const committing = gate();
		let pauseCommit = false;
		class PausedStorage extends MemoryStorage {
			override async get(key: readonly string[]) {
				if (key[0] === "hub") await hydration.promise;
				return await super.get(key);
			}
			override async set(key: readonly string[], value: unknown) {
				if (pauseCommit) {
					committing.release();
					await commit.promise;
				}
				await super.set(key, value);
			}
		}
		const host = boot(undefined, new PausedStorage());
		const stale = host.hub.connect({ id: "tab", principal: alice, send() {} });
		const checks = [
			assertRejects(() => host.hub.subscribe(stale, "room://"), ConnectionNotFoundError),
			assertRejects(() => host.hub.reconnect(stale, 0, ["room://"]), ConnectionNotFoundError),
			assertRejects(() => host.hub.dispatchFrom(stale, "room://", "guest", {}, 1), ConnectionNotFoundError),
			assertRejects(() => host.hub.execFrom(stale, "room://", "who", {}), ConnectionNotFoundError),
		];
		const current = host.hub.connect({ id: "tab", principal: alice, send() {} });
		hydration.release();
		await Promise.all(checks);
		assertEquals(host.hub.serverSeq, 0);
		assertEquals(host.env.seen, []);
		await host.hub.subscribe(current, "room://");
		pauseCommit = true;
		const first = host.hub.dispatch("room://", "guest", {});
		await committing.promise;
		const queued = assertRejects(() => host.hub.dispatchFrom(current, "room://", "guest", {}, 2), ConnectionNotFoundError);
		await new Promise((resolve) => setTimeout(resolve, 0));
		host.hub.disconnect(current);
		commit.release();
		await first;
		await queued;
		assertEquals(host.hub.serverSeq, 1);
		assertEquals((await host.hub.get("room://")).authors, ["system"]);
		await host.hub.close();
	});

	it("returns only public identity on snapshot fallback", async () => {
		const hub = new DurableChannelHub(routes, {
			env: { seen: [], wait: () => Promise.resolve() },
			storage: new MemoryStorage(),
			replayLimit: 1,
		});
		const connection = hub.connect({ id: "tab", principal: alice, send() {} });
		await hub.subscribe(connection, "room://");
		await hub.dispatch("room://", "guest", {});
		await hub.dispatch("room://", "guest", {});
		const result = await hub.reconnect(connection, 0, ["room://"]);
		assertEquals(result.type, "snapshot");
		assertEquals(result.principal, { id: "alice" });
		await hub.close();
	});

	it("closes replaced sockets and keeps the surviving tab usable", async () => {
		const host = boot();
		const first = await host.open("tab", alice);
		const second = await host.open("tab", alice);
		try {
			await until(() => first.client.connectionState.status === "closed");
			first.link.session.detach();
			assertEquals(await second.client.exec("room://", "admin", {}), "Alice");
		} finally {
			await first.client.shutdown();
			await second.client.shutdown();
			await host.hub.close();
		}
	});

	it("ignores client-supplied identity and reports application permission failures", async () => {
		const host = boot();
		const link = host.serve(bob);
		try {
			link.transport.send({ jsonrpc: "2.0", id: 1, method: "hello", params: { clientId: "tab", principal: alice } });
			const hello = await link.transport.recv();
			assert(hello && "result" in hello);
			assertEquals((hello.result as { principal: unknown }).principal, { id: "bob" });
			link.transport.send({ jsonrpc: "2.0", id: 2, method: "exec", params: { channel: "room://", name: "admin", principal: alice } });
			const denied = await link.transport.recv();
			assert(denied && "error" in denied);
			assertEquals(denied.error.code, -32009);
			link.transport.send({
				jsonrpc: "2.0",
				id: 3,
				method: "dispatch",
				params: { channel: "room://", name: "post", payload: { author: "alice" }, clientSeq: 1, principalId: "alice" },
			});
			const refused = await link.transport.recv();
			assert(refused && "result" in refused);
			assertEquals((refused.result as { origin: unknown }).origin, { clientId: "tab", clientSeq: 1, principalId: "bob" });
			assert(typeof (refused.result as { rejectionReason: unknown }).rejectionReason === "string");
		} finally {
			link.session.close();
			await link.loop;
			await host.hub.close();
		}
	});

	it("cleans failed handshakes and blocks dispatch before initialization", async () => {
		const host = boot();
		const link = host.serve();
		const client = new DurableChannelClient(routes, link.transport, { requestTimeoutMs: 2000 });
		client.connect();
		try {
			assertThrows(() => client.dispatch("room://", "guest", {}), NotInitializedError);
			await assertRejects(() => client.hello({ subscriptions: ["room://", "private://"] }), RpcError);
			assertEquals(link.session.link.connection, undefined);
			const sent = link.sent.length;
			await host.hub.dispatch("room://", "guest", {});
			assertEquals(link.sent.length, sent);
			await client.hello({ subscriptions: ["room://"] });
			assertEquals((await client.dispatch("room://", "guest", {}).settled).status, "confirmed");
		} finally {
			await client.shutdown();
			await host.hub.close();
		}
	});

	it("suppresses late responses and queued requests after host closure", async () => {
		const paused = gate();
		const host = boot(() => paused.promise);
		const { client, link } = await host.open("tab", alice);
		const pending = client.exec("room://", "nested", {}).catch(() => undefined);
		const queued = client.exec("room://", "who", {}).catch(() => undefined);
		await new Promise((resolve) => setTimeout(resolve, 0));
		link.session.close(1008, "Signed out");
		const sent = link.sent.length;
		paused.release();
		await pending;
		await queued;
		await new Promise((resolve) => setTimeout(resolve, 0));
		assertEquals(link.sent.length, sent);
		assertEquals(host.env.seen, ["Alice"]);
		await client.shutdown();
		await host.hub.close();
	});
});

/** These fixtures are type checked but never invoked. */
function typeChecks() {
	const host = boot();
	// @ts-expect-error custom principal requires role and name
	host.hub.connect({ id: "tab", principal: { id: "alice" }, send() {} });
	// @ts-expect-error adapter must infer the full principal from the hub
	attachSocket(createRpc(host.hub), host.hub, {} as never, { principal: { id: "alice" } });
	// @ts-expect-error base principals cannot satisfy this channel's requirements
	durableRoutes().env<Env>().route("room://", channel);
	const stateless = durableChannel().principal<Principal>().env<Env>().summary("typed")
		.command((c) =>
			c.name("role").params(v.object({})).result(v.string()).handler((_params, ctx) => {
				const role: "admin" | "member" | undefined = ctx.principal?.role;
				// @ts-expect-error no invented field on the principal
				ctx.principal?.missing;
				ctx.env.seen.push(role ?? "guest");
				return role ?? "guest";
			})
		).build();
	durableRoutes().env<Env>().principal<Principal>().route("relay://", stateless).build();
	const client = new DurableChannelClient(routes, InMemoryTransport.pair()[0]);
	const authors: string[] = client.state("room://").authors;
	void authors;
	// @ts-expect-error clients see only the public ID
	client.principal?.role;
	const plain = durableChannel().state(v.object({ n: v.number() }), { n: 0 }).principal<Principal>().env<Env>()
		.action((a) =>
			a.name("test").payload(v.object({})).reduce((state, _payload, meta) => {
				// @ts-expect-error custom principal data never enters reducers
				meta.origin?.role;
				return state;
			})
		).build();
	durableRoutes().principal<Principal>().env<Env>().route("plain://", plain);
	const base: DurableChannelPrincipal = { id: "base" };
	new DurableChannelHub(durableRoutes().build(), { env: {}, storage: new MemoryStorage() }).connect({
		id: "tab",
		principal: base,
		send(_message: DurableChannelMessage) {},
	});
}
void typeChecks;
