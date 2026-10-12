import { deepStrictEqual, ok, rejects } from "node:assert/strict";
import * as v from "valibot";
import { describe, it } from "vitest";
import {
	ConnectionNotFoundError,
	createRpc,
	type DurableChannelAuthorizationHandler,
	type DurableChannelAuthorizationRequest,
	DurableChannelClient,
	type DurableChannelConnection,
	DurableChannelHub,
	type DurableChannelLink,
	type DurableChannelMessage,
	durableChannel,
	durableRoutes,
	InMemoryTransport,
	MemoryStorage,
	NotClientDispatchableError,
	DurableChannelPermission as Permission,
	PermissionDeniedError,
	RouteNotFoundError,
	RpcError,
	toRpcNotification,
} from "./mod.ts";

type Principal = { id: string; role: "reader" | "writer" };
type Env = { effects: number; commands: number };
type State = { count: number };
type Policy = DurableChannelAuthorizationHandler<Env, State, Principal>;

function boot(policy: Policy, replayLimit = 10) {
	const env: Env = { effects: 0, commands: 0 };
	const definition = durableChannel()
		.env<Env>()
		.principal<Principal>()
		.state(v.object({ count: v.number() }), { count: 0 })
		.client(policy)
		.summary("Authorized counter")
		.action((a) =>
			a
				.name("add")
				.payload(v.object({ by: v.optional(v.number(), 1) }))
				.client()
				.reduce((state, payload) => ({ count: state.count + payload.by }))
				.effect((ctx) => {
					ctx.env.effects++;
				}),
		)
		.action((a) =>
			a
				.name("server")
				.payload(v.object({}))
				.reduce((state) => ({ count: state.count + 1 })),
		)
		.command((c) =>
			c
				.name("read")
				.params(v.object({ label: v.optional(v.string(), "default") }))
				.result(v.number())
				.handler(async (_params, ctx) => {
					ctx.env.commands++;
					return (await ctx.state()).count;
				}),
		)
		.command((c) =>
			c
				.name("nested")
				.params(v.object({}))
				.result(v.number())
				.handler(async (_params, ctx) => (await ctx.exec("private://", "read", {})) as number),
		)
		.notification((n) => n.name("notice").payload(v.string()))
		.build();
	const routes = durableRoutes()
		.env<Env>()
		.principal<Principal>()
		.route("counter://", definition)
		.route("counter:/:id", definition)
		.route("private://", definition, { internal: true })
		.build();
	const hub = new DurableChannelHub(routes, { env, storage: new MemoryStorage(), replayLimit });
	const rpc = createRpc(hub);
	const principal: Principal = { id: "alice", role: "writer" };
	const messages: DurableChannelMessage[] = [];
	const connection = hub.connect({ id: "tab", principal, send: (m) => messages.push(m) });
	function serve(id: string) {
		const [transport, peer] = InMemoryTransport.pair();
		let bound: DurableChannelConnection<Principal> | undefined;
		const link: DurableChannelLink<Principal> = {
			get clientId() {
				return bound?.id;
			},
			get connection() {
				return bound;
			},
			bind(clientId) {
				this.unbind();
				bound = hub.connect({ id: clientId, principal, send: (m) => peer.send(toRpcNotification(m)) });
			},
			unbind() {
				if (bound) hub.disconnect(bound);
				bound = undefined;
			},
		};
		const loop = (async () => {
			try {
				while (true) {
					const frame = await peer.recv();
					if (frame === null) return;
					const response = await rpc.handle(frame, link);
					if (response) peer.send(response);
				}
			} finally {
				link.unbind();
			}
		})();
		const client = new DurableChannelClient(routes, transport, { clientId: id, requestTimeoutMs: 2000 });
		client.connect();
		return { client, loop };
	}
	return { hub, env, connection, messages, serve };
}

function gate() {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => (release = resolve));
	return { promise, release };
}

describe("channel authorization", () => {
	it("preserves policies through stateless and stateful builder branches", async () => {
		const seen: unknown[] = [];
		const base = durableChannel().client(async (ctx) => {
			seen.push(await ctx.state());
			return Permission.None;
		});
		const relay = base
			.env<Env>()
			.principal<Principal>()
			.summary("relay")
			.command((c) =>
				c
					.name("read")
					.params(v.object({}))
					.result(v.null())
					.handler(() => null),
			)
			.notification((n) => n.name("notice").payload(v.string()));
		const stateful = relay.state(v.number(), 0).env<Env>().principal<Principal>().summary("stateful");
		const routes = durableRoutes()
			.env<Env>()
			.principal<Principal>()
			.route("relay://", relay.build())
			.route("state://", stateful.build())
			.route("open://", relay.client().build())
			.build();
		const hub = new DurableChannelHub(routes, { env: { effects: 0, commands: 0 }, storage: new MemoryStorage() });
		const connection = hub.connect({ id: "guest", send() {} });
		try {
			await rejects(() => hub.subscribe(connection, "relay://"), PermissionDeniedError);
			await rejects(() => hub.execFrom(connection, "relay://", "read", {}), PermissionDeniedError);
			await rejects(() => hub.subscribe(connection, "state://"), PermissionDeniedError);
			deepStrictEqual(await hub.subscribe(connection, "open://"), undefined);
			deepStrictEqual(await hub.execFrom(connection, "open://", "read", {}), null);
			deepStrictEqual(await hub.reconnect(connection, 0, ["relay://", "open://"]), { type: "replay", actions: [], missing: ["relay://"] });
			deepStrictEqual(seen, [undefined, undefined, 0, undefined]);
		} finally {
			await hub.close();
		}
	});

	it("rejects hello and subsequent subscriptions through RPC and drops denied re-subscriptions", async () => {
		let allowed = false;
		const host = boot(() => (allowed ? Permission.All : Permission.None));
		const { client, loop } = host.serve("remote");
		try {
			await rejects(
				() => client.hello({ subscriptions: ["counter://"] }),
				(error) => error instanceof RpcError && error.code === -32009,
			);
			await client.hello();
			await rejects(() => client.subscribe("counter://"), RpcError);
			allowed = true;
			await host.hub.subscribe(host.connection, "counter://");
			allowed = false;
			await rejects(() => host.hub.subscribe(host.connection, "counter://"), PermissionDeniedError);
			await host.hub.dispatch("counter://", "add", {});
			deepStrictEqual(host.messages, []);
		} finally {
			await client.shutdown();
			await loop;
			await host.hub.close();
		}
	});

	it("checks independent bits, optional principals, parsed requests and typed server context", async () => {
		const seen: DurableChannelAuthorizationRequest[] = [];
		const host = boot(async (ctx, request) => {
			const state: State = await ctx.state();
			const role: "reader" | "writer" | undefined = ctx.principal?.role;
			deepStrictEqual(ctx.env.effects, state.count);
			deepStrictEqual(ctx.uri, "counter:/one");
			deepStrictEqual(ctx.params, { id: "one" });
			deepStrictEqual(ctx.connectionId, role ? "tab" : "guest");
			seen.push(request);
			return role === "writer" ? Permission.Subscribe | Permission.Dispatch : Permission.Execute;
		});
		try {
			await host.hub.create("counter:/one");
			const guest = host.hub.connect({ id: "guest", send() {} });
			await host.hub.subscribe(host.connection, "counter:/one");
			deepStrictEqual((await host.hub.dispatchFrom(host.connection, "counter:/one", "add", {}, 1))?.rejectionReason, undefined);
			await rejects(() => host.hub.execFrom(host.connection, "counter:/one", "read", {}), PermissionDeniedError);
			await rejects(() => host.hub.subscribe(guest, "counter:/one"), PermissionDeniedError);
			deepStrictEqual(await host.hub.execFrom(guest, "counter:/one", "read", {}), 1);
			deepStrictEqual(
				(await host.hub.dispatchFrom(guest, "counter:/one", "add", {}, 1))?.rejectionReason,
				"PERMISSION_DENIED: Permission denied",
			);
			deepStrictEqual(host.env, { effects: 1, commands: 1 });
			deepStrictEqual(seen, [
				{ type: "subscribe" },
				{ type: "action", name: "add", payload: { by: 1 } },
				{ type: "command", name: "read", params: { label: "default" } },
				{ type: "subscribe" },
				{ type: "command", name: "read", params: { label: "default" } },
				{ type: "action", name: "add", payload: { by: 1 } },
			]);
		} finally {
			await host.hub.close();
		}
	});

	it("keeps server operations trusted and action exposure and internal routes restrictive", async () => {
		let calls = 0;
		const host = boot((_ctx, request) => {
			calls++;
			return request.type === "command" && request.name === "nested" ? Permission.Execute : Permission.None;
		});
		try {
			await host.hub.dispatch("counter://", "add", {});
			deepStrictEqual(await host.hub.exec("counter://", "read", {}), 1);
			deepStrictEqual(calls, 0);
			deepStrictEqual(await host.hub.execFrom(host.connection, "counter://", "nested", {}), 0);
			deepStrictEqual(calls, 1);
			await rejects(() => host.hub.subscribe(host.connection, "private://"), RouteNotFoundError);
			await rejects(() => host.hub.execFrom(host.connection, "private://", "read", {}), RouteNotFoundError);
			await rejects(() => host.hub.dispatchFrom(host.connection, "counter://", "server", {}, 1), NotClientDispatchableError);
			deepStrictEqual(calls, 1);
		} finally {
			await host.hub.close();
		}
	});

	for (const replayLimit of [0, 10]) {
		it(`withholds unauthorized subscriptions from reconnect (${replayLimit ? "replay" : "snapshot"})`, async () => {
			let allowed = true;
			const host = boot(() => (allowed ? Permission.All : Permission.None), replayLimit);
			try {
				await host.hub.subscribe(host.connection, "counter://");
				await host.hub.dispatch("counter://", "add", {});
				await host.hub.dispatch("counter://", "add", {});
				allowed = false;
				const result = await host.hub.reconnect(host.connection, 0, ["counter://"]);
				deepStrictEqual(result.missing, ["counter://"]);
				deepStrictEqual(result.type, replayLimit ? "replay" : "snapshot");
				deepStrictEqual(result.type === "replay" ? result.actions : result.snapshots, []);
				host.messages.length = 0;
				await host.hub.dispatch("counter://", "add", {});
				await host.hub.notify("counter://", "notice", "secret");
				deepStrictEqual(host.messages, []);
			} finally {
				await host.hub.close();
			}
		});
	}

	it("evaluates state-dependent action permissions in commit order", async () => {
		const host = boot(async (ctx) => ((await ctx.state()).count < 1 ? Permission.All : Permission.None));
		try {
			await host.hub.subscribe(host.connection, "counter://");
			const [first, second] = await Promise.all([
				host.hub.dispatchFrom(host.connection, "counter://", "add", {}, 1),
				host.hub.dispatchFrom(host.connection, "counter://", "add", {}, 2),
			]);
			deepStrictEqual(first?.rejectionReason, undefined);
			deepStrictEqual(second?.rejectionReason, "PERMISSION_DENIED: Permission denied");
			deepStrictEqual(await host.hub.get("counter://"), { count: 1 });
			deepStrictEqual(host.env.effects, 1);
		} finally {
			await host.hub.close();
		}
	});

	for (const operation of ["subscribe", "action", "command"] as const) {
		it(`rejects connection replacement during an async ${operation} policy`, async () => {
			const entered = gate();
			const resume = gate();
			const host = boot(async () => {
				entered.release();
				await resume.promise;
				return Permission.All;
			});
			try {
				const pending =
					operation === "subscribe"
						? host.hub.subscribe(host.connection, "counter://")
						: operation === "action"
							? host.hub.dispatchFrom(host.connection, "counter://", "add", {}, 1)
							: host.hub.execFrom(host.connection, "counter://", "read", {});
				const rejected = rejects(() => pending, ConnectionNotFoundError);
				await entered.promise;
				host.hub.connect({ id: "tab", principal: { id: "alice", role: "writer" }, send() {} });
				resume.release();
				await rejected;
				deepStrictEqual(host.env, { effects: 0, commands: 0 });
				deepStrictEqual(host.hub.serverSeq, 0);
			} finally {
				resume.release();
				await host.hub.close();
			}
		});
	}

	it("fails closed on exceptions and invalid masks without exposing policy errors", async () => {
		for (const value of [NaN, Infinity, -1, 0.5, 8, undefined, "7", true, new Error("secret")]) {
			const host = boot(() => {
				if (value instanceof Error) throw value;
				return value as number;
			});
			try {
				await rejects(() => host.hub.subscribe(host.connection, "counter://"), PermissionDeniedError);
				await rejects(() => host.hub.execFrom(host.connection, "counter://", "read", {}), PermissionDeniedError);
				deepStrictEqual(
					(await host.hub.dispatchFrom(host.connection, "counter://", "add", {}, 1))?.rejectionReason,
					"PERMISSION_DENIED: Permission denied",
				);
				deepStrictEqual(host.env, { effects: 0, commands: 0 });
			} finally {
				await host.hub.close();
			}
		}
	});

	it("runs policies only on the server and reconciles optimistic rejection with a pending action", async () => {
		const resume = gate();
		let actionChecks = 0;
		const host = boot(async (ctx, request) => {
			ok(ctx.env !== undefined);
			if (request.type === "command") return Permission.None;
			if (request.type === "action") {
				actionChecks++;
				await resume.promise;
				return (request.payload as { by: number }).by === 5 ? Permission.None : Permission.Dispatch;
			}
			return Permission.Subscribe;
		});
		const { client, loop } = host.serve("optimistic");
		try {
			await client.hello({ subscriptions: ["counter://"] });
			const refused = client.dispatch("counter://", "add", { by: 5 });
			const accepted = client.dispatch("counter://", "add", { by: 2 });
			deepStrictEqual(client.state("counter://"), { count: 7 });
			deepStrictEqual(client.confirmed("counter://"), { count: 0 });
			deepStrictEqual(actionChecks, 0);
			resume.release();
			deepStrictEqual((await refused.settled).status, "rejected");
			deepStrictEqual((await accepted.settled).status, "confirmed");
			deepStrictEqual(client.state("counter://"), { count: 2 });
			deepStrictEqual(client.confirmed("counter://"), await host.hub.get("counter://"));
			deepStrictEqual(client.pending("counter://"), []);
			deepStrictEqual(actionChecks, 2);
			await rejects(
				() => client.exec("counter://", "read", {}),
				(error) => error instanceof RpcError && error.code === -32009,
			);
		} finally {
			resume.release();
			await client.shutdown();
			await loop;
			await host.hub.close();
		}
	});
});
