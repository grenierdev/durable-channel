import { deepStrictEqual, ok, rejects, throws } from "node:assert/strict";
import { IDBFactory } from "fake-indexeddb";
import * as v from "valibot";
import { describe, it } from "vitest";
import {
	AuthenticationRequiredError,
	attachSocket,
	ConnectionNotFoundError,
	createRpc,
	DurableChannelClient,
	type DurableChannelConnection,
	DurableChannelHub,
	type DurableChannelMessage,
	type DurableChannelPrincipal,
	type DurableChannelRpcFrame,
	durableChannel,
	durableRoutes,
	IdentityChangedError,
	IndexedDBClientStorage,
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

const channel = durableChannel()
	.env<Env>()
	.principal<Principal>()
	.state(v.object({ authors: v.array(v.string()) }), { authors: [] })
	.action((a) =>
		a
			.name("post")
			.payload(v.object({ author: v.string() }))
			.client()
			.reduce((state, payload, meta) => {
				if (!meta.origin?.principalId || payload.author !== meta.origin.principalId) throw new RejectAction("Sign in as the author");
				return { authors: [...state.authors, payload.author] };
			})
			.effect(async (ctx) => {
				await ctx.env.wait();
				ctx.env.seen.push(ctx.principal?.name ?? "anonymous");
				await ctx.exec("private://", "who", {});
			}),
	)
	.action((a) =>
		a
			.name("guest")
			.payload(v.object({}))
			.client()
			.reduce((state, _payload, meta) => ({
				authors: [...state.authors, meta.origin?.principalId ?? (meta.origin ? "guest" : "system")],
			})),
	)
	.command((c) =>
		c
			.name("who")
			.params(v.object({}))
			.result(v.string())
			.handler((_params, ctx) => {
				const name: string = ctx.principal?.name ?? "anonymous";
				ctx.env.seen.push(name);
				return name;
			}),
	)
	.command((c) =>
		c
			.name("admin")
			.params(v.object({}))
			.result(v.string())
			.handler((_params, ctx) => {
				if (!ctx.principal) throw new AuthenticationRequiredError();
				if (ctx.principal.role !== "admin") throw new PermissionDeniedError();
				return ctx.principal.name;
			}),
	)
	.command((c) =>
		c
			.name("nested")
			.params(v.object({}))
			.result(v.string())
			.handler(async (_params, ctx) => {
				await ctx.env.wait();
				return (await ctx.exec("private://", "who", {})) as string;
			}),
	)
	.command((c) =>
		c
			.name("system")
			.params(v.object({}))
			.result(v.null())
			.handler(async (_params, ctx) => {
				await ctx.dispatch("room://", "guest", {});
				return null;
			}),
	)
	.build();

const routes = durableRoutes()
	.principal<Principal>()
	.env<Env>()
	.route("room://", channel)
	.route("private://", channel, { internal: true })
	.build();

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
		const emit = (type: string, data?: unknown) =>
			listeners.get(type)?.forEach((fn) => {
				fn({ data });
			});
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
	const promise = new Promise<void>((resolve) => (release = resolve));
	return { promise, release };
}

describe("optional principals", () => {
	it("allows anonymous callers and lets each command and action decide", async () => {
		const host = boot();
		const { client, link } = await host.open("guest-tab");
		try {
			deepStrictEqual(client.principal, undefined);
			deepStrictEqual(await client.exec("room://", "who", {}), "anonymous");
			await rejects(
				() => client.exec("room://", "admin", {}),
				(error) => error instanceof RpcError && error.code === -32007,
			);
			deepStrictEqual((await client.dispatch("room://", "post", { author: "alice" }).settled).status, "rejected");
			const accepted = await client.dispatch("room://", "guest", {}).settled;
			ok(accepted.status === "confirmed");
			deepStrictEqual(accepted.envelope.origin, { clientId: "guest-tab", clientSeq: 2 });
			await client.exec("room://", "system", {});
			await until(() => client.state("room://").authors.length === 2);
			deepStrictEqual(client.state("room://").authors, ["guest", "system"]);
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
			deepStrictEqual(first.client.principal, { id: "alice" });
			deepStrictEqual(await first.client.exec("room://", "admin", {}), "Alice");
			const action = first.client.dispatch("room://", "post", { author: "alice" });
			deepStrictEqual(first.client.state("room://").authors, ["alice"]);
			deepStrictEqual((await action.settled).status, "confirmed");
			await until(() => second.client.confirmed("room://").authors.length === 1);
			deepStrictEqual((await second.client.dispatch("room://", "post", { author: "bob" }).settled).status, "rejected");
			deepStrictEqual(second.client.state("room://"), await host.hub.get("room://"));
			const broadcast = second.link.sent.find((frame) => "method" in frame && frame.method === "action");
			ok(broadcast && "params" in broadcast);
			deepStrictEqual((broadcast.params as { origin: unknown }).origin, { clientId: "tab-1", clientSeq: 1, principalId: "alice" });
			ok(!JSON.stringify(second.link.sent).includes('"role"'));
			ok(!JSON.stringify(second.link.sent).includes('"name":"Alice"'));
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
			deepStrictEqual(client.state("room://").authors, ["alice", "alice"]);
			await until(() => host.hub.serverSeq === 2);
			link.session.close();
			await until(() => client.connectionState.status === "closed");
			const result = await client.reconnect(host.serve(alice).transport);
			deepStrictEqual(result.principal, { id: "alice" });
			deepStrictEqual((await first.settled).status, "confirmed");
			deepStrictEqual((await second.settled).status, "confirmed");
			deepStrictEqual(client.state("room://"), await host.hub.get("room://"));
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
			ok(request && "id" in request && request.id !== undefined);
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
			deepStrictEqual((await pending.settled).status, "confirmed");
			deepStrictEqual(client.state("room://").authors, ["alice"]);
		} finally {
			peer.close();
			await client.shutdown();
			await host.hub.close();
		}
	});

	for (const [before, after] of [
		[alice, bob],
		[alice, undefined],
		[undefined, alice],
	] as const) {
		it(`clears pending work and state when identity changes from ${before?.id} to ${after?.id}`, async () => {
			const host = boot();
			const { client, link } = await host.open("tab", before);
			try {
				link.mute();
				const pending = client.dispatch("room://", "guest", {});
				await until(() => host.hub.serverSeq === 1);
				link.session.close();
				await until(() => client.connectionState.status === "closed");
				await rejects(() => client.reconnect(host.serve(after).transport), IdentityChangedError);
				deepStrictEqual((await pending.settled).status, "lost");
				throws(() => client.state("room://"));
				deepStrictEqual(client.principal, undefined);
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
		deepStrictEqual(original.principal?.name, "Alice");
		throws(() => host.hub.connect({ id: "tab", principal: bob, send() {} }), PermissionDeniedError);
		throws(() => host.hub.connect({ id: "tab", send() {} }), PermissionDeniedError);
		deepStrictEqual(ended, 0);
		const current = host.hub.connect({ id: "tab", principal: alice, send() {} });
		deepStrictEqual(ended, 1);
		const other = boot();
		const foreign = other.hub.connect({ id: "tab", principal: alice, send() {} });
		for (const invalid of [original, foreign, { ...current } as DurableChannelConnection<Principal>]) {
			await rejects(() => host.hub.subscribe(invalid, "room://"), ConnectionNotFoundError);
			await rejects(() => host.hub.dispatchFrom(invalid, "room://", "guest", {}, 1), ConnectionNotFoundError);
			await rejects(() => host.hub.execFrom(invalid, "room://", "who", {}), ConnectionNotFoundError);
			await rejects(() => host.hub.reconnect(invalid, 0, ["room://"]), ConnectionNotFoundError);
			throws(() => host.hub.unsubscribe(invalid, "room://"), ConnectionNotFoundError);
			host.hub.disconnect(invalid);
		}
		deepStrictEqual(await host.hub.execFrom(current, "room://", "who", {}), "Alice");
		deepStrictEqual(await host.hub.of("room://").execFrom(current, {}, "who", {}), "Alice");
		await rejects(() => host.hub.execFrom(current, "private://", "who", {}), RouteNotFoundError);
		deepStrictEqual(await host.hub.execFrom(current, "room://", "nested", {}), "Alice");
		deepStrictEqual(await host.hub.exec("private://", "who", {}), "anonymous");
		throws(() => host.hub.connect({ id: "empty", principal: { ...alice, id: "" }, send() {} }), PermissionDeniedError);
		host.hub.disconnect(current);
		await rejects(() => host.hub.subscribe(current, "room://"), ConnectionNotFoundError);
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
		deepStrictEqual(await command, "Alice");
		deepStrictEqual(host.env.seen, ["Alice", "Alice", "Alice"]);
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
			rejects(() => host.hub.subscribe(stale, "room://"), ConnectionNotFoundError),
			rejects(() => host.hub.reconnect(stale, 0, ["room://"]), ConnectionNotFoundError),
			rejects(() => host.hub.dispatchFrom(stale, "room://", "guest", {}, 1), ConnectionNotFoundError),
			rejects(() => host.hub.execFrom(stale, "room://", "who", {}), ConnectionNotFoundError),
		];
		const current = host.hub.connect({ id: "tab", principal: alice, send() {} });
		hydration.release();
		await Promise.all(checks);
		deepStrictEqual(host.hub.serverSeq, 0);
		deepStrictEqual(host.env.seen, []);
		await host.hub.subscribe(current, "room://");
		pauseCommit = true;
		const first = host.hub.dispatch("room://", "guest", {});
		await committing.promise;
		const queued = rejects(() => host.hub.dispatchFrom(current, "room://", "guest", {}, 2), ConnectionNotFoundError);
		await new Promise((resolve) => setTimeout(resolve, 0));
		host.hub.disconnect(current);
		commit.release();
		await first;
		await queued;
		deepStrictEqual(host.hub.serverSeq, 1);
		deepStrictEqual((await host.hub.get("room://")).authors, ["system"]);
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
		deepStrictEqual(result.type, "snapshot");
		deepStrictEqual(result.principal, { id: "alice" });
		await hub.close();
	});

	it("closes replaced sockets and keeps the surviving tab usable", async () => {
		const host = boot();
		const first = await host.open("tab", alice);
		const second = await host.open("tab", alice);
		try {
			await until(() => first.client.connectionState.status === "closed");
			first.link.session.detach();
			deepStrictEqual(await second.client.exec("room://", "admin", {}), "Alice");
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
			ok(hello && "result" in hello);
			deepStrictEqual((hello.result as { principal: unknown }).principal, { id: "bob" });
			link.transport.send({ jsonrpc: "2.0", id: 2, method: "exec", params: { channel: "room://", name: "admin", principal: alice } });
			const denied = await link.transport.recv();
			ok(denied && "error" in denied);
			deepStrictEqual(denied.error.code, -32009);
			link.transport.send({
				jsonrpc: "2.0",
				id: 3,
				method: "dispatch",
				params: { channel: "room://", name: "post", payload: { author: "alice" }, clientSeq: 1, principalId: "alice" },
			});
			const refused = await link.transport.recv();
			ok(refused && "result" in refused);
			deepStrictEqual((refused.result as { origin: unknown }).origin, { clientId: "tab", clientSeq: 1, principalId: "bob" });
			ok(typeof (refused.result as { rejectionReason: unknown }).rejectionReason === "string");
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
			throws(() => client.dispatch("room://", "guest", {}), NotInitializedError);
			await rejects(() => client.hello({ subscriptions: ["room://", "private://"] }), RpcError);
			deepStrictEqual(link.session.link.connection, undefined);
			const sent = link.sent.length;
			await host.hub.dispatch("room://", "guest", {});
			deepStrictEqual(link.sent.length, sent);
			await client.hello({ subscriptions: ["room://"] });
			deepStrictEqual((await client.dispatch("room://", "guest", {}).settled).status, "confirmed");
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
		deepStrictEqual(link.sent.length, sent);
		deepStrictEqual(host.env.seen, ["Alice"]);
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
	const stateless = durableChannel()
		.principal<Principal>()
		.env<Env>()
		.summary("typed")
		.command((c) =>
			c
				.name("role")
				.params(v.object({}))
				.result(v.string())
				.handler((_params, ctx) => {
					const role: "admin" | "member" | undefined = ctx.principal?.role;
					// @ts-expect-error no invented field on the principal
					ctx.principal?.missing;
					ctx.env.seen.push(role ?? "guest");
					return role ?? "guest";
				}),
		)
		.build();
	durableRoutes().env<Env>().principal<Principal>().route("relay://", stateless).build();
	const client = new DurableChannelClient(routes, InMemoryTransport.pair()[0]);
	const authors: string[] = client.state("room://").authors;
	void authors;
	// @ts-expect-error clients see only the public ID
	client.principal?.role;
	const plain = durableChannel()
		.state(v.object({ n: v.number() }), { n: 0 })
		.principal<Principal>()
		.env<Env>()
		.action((a) =>
			a
				.name("test")
				.payload(v.object({}))
				.reduce((state, _payload, meta) => {
					// @ts-expect-error custom principal data never enters reducers
					meta.origin?.role;
					return state;
				}),
		)
		.build();
	durableRoutes().principal<Principal>().env<Env>().route("plain://", plain);
	const base: DurableChannelPrincipal = { id: "base" };
	new DurableChannelHub(durableRoutes().build(), { env: {}, storage: new MemoryStorage() }).connect({
		id: "tab",
		principal: base,
		send(_message: DurableChannelMessage) {},
	});
}
void typeChecks;

describe("persisted client identity", () => {
	it("restores the public principal and clears the checkpoint when the authenticated identity changes", async () => {
		const host = boot();
		const store = new IndexedDBClientStorage("tab", { indexedDB: new IDBFactory() });
		const first = host.serve(alice);
		const client = new DurableChannelClient(routes, first.transport, { storage: store });
		const next = host.serve(bob);
		const restored = new DurableChannelClient(routes, next.transport, { storage: store });
		try {
			client.connect();
			await client.hello({ subscriptions: ["room://"] });
			await client.dispatch("room://", "post", { author: "alice" }).settled;
			await client.shutdown();
			await restored.ready();
			deepStrictEqual(restored.principal, { id: "alice" });
			deepStrictEqual(restored.state("room://"), { authors: ["alice"] });
			restored.connect();
			await rejects(restored.reconnect(), IdentityChangedError);
			throws(() => restored.state("room://"));
			deepStrictEqual(await store.load(), undefined);
			deepStrictEqual(restored.connectionState, { status: "closed", reason: { type: "shutdown" } });
		} finally {
			await client.shutdown();
			await restored.shutdown();
			await Promise.all([first.loop, next.loop]);
			await host.hub.close();
		}
	});
});
