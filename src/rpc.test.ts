import { describe, it } from "node:test";
import { assert, assertEquals } from "@std/assert";
import * as v from "valibot";
import { durableChannel, type DurableChannelMessage } from "./channel.ts";
import { durableRoutes } from "./routes.ts";
import { type DurableChannelConnection, DurableChannelHub } from "./hub.ts";
import { MemoryStorage } from "./storage.ts";
import { RejectAction } from "./error.ts";
import {
	attachSocket,
	createRpc,
	type DurableChannelLink,
	DurableChannelRpcErrorCodes,
	fromRpcNotification,
	toRpcNotification,
} from "./rpc.ts";

type Env = { now(): string };

const tally = durableChannel()
	.env<Env>()
	.state(v.object({ count: v.number() }), { count: 0 })
	.action((a) =>
		a.name("tally/added")
			.payload(v.object({ by: v.pipe(v.number(), v.integer()) }))
			.client()
			.reduce((state, payload) => {
				if (payload.by <= 0) {
					throw new RejectAction("by must be positive");
				}
				return { count: state.count + payload.by };
			})
	)
	.action((a) => a.name("tally/set").payload(v.object({ to: v.number() })).reduce((_state, payload) => ({ count: payload.to })))
	.command((c) =>
		c.name("open")
			.params(v.object({ id: v.string() }))
			.result(v.object({ uri: v.string(), by: v.string() }))
			.handler(async ({ id }, ctx) => {
				await ctx.create(`note:/${id}`);
				return { uri: `note:/${id}`, by: ctx.connectionId ?? "server" };
			})
	)
	.command((c) =>
		c.name("refuse")
			.params(v.object({}))
			.result(v.null())
			.handler(() => {
				throw new RejectAction("never");
			})
	)
	.notification((n) => n.name("tally/said").payload(v.object({ text: v.string() })))
	.build();

const note = durableChannel()
	.env<Env>()
	.state(v.object({ title: v.string() }), { title: "untitled" })
	.action((a) => a.name("note/titled").payload(v.object({ title: v.string() })).client().reduce((_s, p) => ({ title: p.title })))
	.build();

const feed = durableChannel()
	.env<Env>()
	.command((c) =>
		c.name("say")
			.params(v.object({ line: v.string() }))
			.result(v.null())
			.handler(async ({ line }, ctx) => {
				await ctx.notify(ctx.uri, "feed/line", { line, at: ctx.env.now() });
				return null;
			})
	)
	.notification((n) => n.name("feed/line").payload(v.object({ line: v.string(), at: v.string() })))
	.build();

const routes = durableRoutes()
	.env<Env>()
	.route("tally://", tally)
	.route("note:/:id", note)
	.route("feed:/:id", feed)
	.route("x-secret://", note, { internal: true })
	.build();

/** A hub, its JSON-RPC surface, a link that connects to the hub, and a `call` that drives one frame. */
function boot(options?: { replayLimit?: number }) {
	const hub = new DurableChannelHub(routes, {
		storage: new MemoryStorage(),
		env: { now: () => "2026-09-07T00:00:00.000Z" },
		...(options?.replayLimit !== undefined ? { replayLimit: options.replayLimit } : {}),
	});
	const rpc = createRpc(hub);
	const sent: DurableChannelMessage[] = [];
	const aborter = new AbortController();
	let binding: { clientId: string; connection: DurableChannelConnection } | undefined;
	const link: DurableChannelLink = {
		get clientId(): string | undefined {
			return binding?.clientId;
		},
		bind(clientId: string): void {
			if (binding !== undefined) {
				hub.disconnect(binding.clientId, binding.connection);
			}
			binding = { clientId, connection: hub.connect({ id: clientId, send: (message) => sent.push(message) }) };
		},
	};
	let id = 0;
	return {
		hub,
		rpc,
		link,
		sent,
		/** One request frame. `id` is assigned unless the caller asks for a notification. */
		call(method: string, params: Record<string, unknown> = {}, kind: "request" | "notification" = "request") {
			id += 1;
			const frame = kind === "request" ? { jsonrpc: "2.0", id, method, params } : { jsonrpc: "2.0", method, params };
			return rpc.handle(frame, link, aborter.signal);
		},
		raw(frame: unknown) {
			return rpc.handle(frame, link, aborter.signal);
		},
	};
}

function result(response: unknown): unknown {
	assert(
		response !== undefined && response !== null && typeof response === "object" && "result" in response,
		`expected a result, got ${JSON.stringify(response)}`,
	);
	return (response as { result: unknown }).result;
}

function error(response: unknown): { code: number; message: string; data?: unknown } {
	assert(
		response !== undefined && response !== null && typeof response === "object" && "error" in response,
		`expected an error, got ${JSON.stringify(response)}`,
	);
	return (response as { error: { code: number; message: string; data?: unknown } }).error;
}

/** A socket that records what it was told and lets a test fire its events by hand. */
function fakeSocket() {
	const frames: string[] = [];
	const listeners = new Map<string, ((event: { readonly data: unknown }) => void)[]>();
	let closed: { code?: number } | undefined;
	return {
		frames,
		get closed(): { code?: number } | undefined {
			return closed;
		},
		socket: {
			send(data: string): void {
				frames.push(data);
			},
			close(code?: number): void {
				closed = { code };
			},
			addEventListener(type: string, listener: (event: { readonly data: unknown }) => void): void {
				listeners.set(type, [...(listeners.get(type) ?? []), listener]);
			},
		},
		fire(type: string, data?: unknown): void {
			for (const listener of listeners.get(type) ?? []) {
				listener({ data });
			}
		},
	};
}

/** Resolves once every queued microtask has run, which is what the socket's frame queue needs. */
function flush(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

describe("rpc", () => {
	it("binds the link on hello and answers one snapshot per stateful subscription", async () => {
		const host = boot();
		try {
			await host.hub.create("note:/1", { title: "Draft" });
			const response = await host.call("hello", { clientId: "alice", subscriptions: ["tally://", "note:/1", "feed:/live"] });
			assertEquals(result(response), {
				serverSeq: 0,
				snapshots: [
					{ resource: "tally://", state: { count: 0 }, fromSeq: 0 },
					{ resource: "note:/1", state: { title: "Draft" }, fromSeq: 0 },
				],
			});
			assertEquals(host.link.clientId, "alice");
		} finally {
			await host.hub.close();
		}
	});

	it("answers a ping before hello and refuses every other method until the link is bound", async () => {
		const host = boot();
		try {
			assertEquals(result(await host.call("ping")), null);
			assertEquals(error(await host.call("subscribe", { channel: "tally://" })).code, DurableChannelRpcErrorCodes.NotInitialized);
			assertEquals(error(await host.call("exec", { channel: "tally://", name: "refuse", params: {} })).code, -32003);
		} finally {
			await host.hub.close();
		}
	});

	it("subscribes a channel, and answers no snapshot member for a stateless one", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice" });
			assertEquals(result(await host.call("subscribe", { channel: "tally://" })), {
				snapshot: { resource: "tally://", state: { count: 0 }, fromSeq: 0 },
			});
			assertEquals(result(await host.call("subscribe", { channel: "feed:/live" })), {});
		} finally {
			await host.hub.close();
		}
	});

	it("answers a dispatch with the committed envelope and echoes it on the link", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			const envelope = {
				type: "action" as const,
				channel: "tally://",
				name: "tally/added",
				payload: { by: 2 },
				serverSeq: 1,
				origin: { clientId: "alice", clientSeq: 1 },
			};
			assertEquals(
				result(await host.call("dispatch", { channel: "tally://", clientSeq: 1, name: "tally/added", payload: { by: 2 } })),
				envelope,
			);
			assertEquals(host.sent, [envelope]);
			assertEquals(await host.hub.get("tally://"), { count: 2 });
		} finally {
			await host.hub.close();
		}
	});

	it("answers null for a clientSeq the link has already used", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			await host.call("dispatch", { channel: "tally://", clientSeq: 4, name: "tally/added", payload: { by: 1 } });
			assertEquals(
				result(await host.call("dispatch", { channel: "tally://", clientSeq: 4, name: "tally/added", payload: { by: 1 } })),
				null,
			);
			assertEquals(await host.hub.get("tally://"), { count: 1 });
			assertEquals(host.sent.length, 1);
		} finally {
			await host.hub.close();
		}
	});

	it("dispatches leniently, so a refusal comes back as a rejected envelope every subscriber sees", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			const reasons: (string | undefined)[] = [];
			for (
				const [index, attempt] of [
					{ name: "tally/nope", payload: {} },
					{ name: "tally/set", payload: { to: 3 } },
					{ name: "tally/added", payload: { by: "two" } },
					{ name: "tally/added", payload: { by: -1 } },
				].entries()
			) {
				const envelope = result(await host.call("dispatch", { channel: "tally://", clientSeq: index + 1, ...attempt })) as {
					rejectionReason?: string;
				};
				reasons.push(envelope.rejectionReason);
			}
			assertEquals(reasons.map((reason) => reason?.split(":")[0]), [
				"UNKNOWN_ACTION",
				"NOT_CLIENT_DISPATCHABLE",
				"INVALID_PAYLOAD",
				"by must be positive",
			]);
			assertEquals(await host.hub.get("tally://"), { count: 0 });
			assertEquals(host.sent.length, 4);
		} finally {
			await host.hub.close();
		}
	});

	it("runs a command with the link's client id and maps its refusal onto the rejection code", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice" });
			assertEquals(result(await host.call("exec", { channel: "tally://", name: "open", params: { id: "7" } })), {
				uri: "note:/7",
				by: "alice",
			});
			assertEquals(error(await host.call("exec", { channel: "tally://", name: "refuse", params: {} })), {
				code: DurableChannelRpcErrorCodes.ActionRejected,
				message: "ACTION_REJECTED: never",
				data: { reason: "never" },
			});
		} finally {
			await host.hub.close();
		}
	});

	it("maps the error taxonomy onto the code table", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice" });
			assertEquals(error(await host.call("subscribe", { channel: "nope://" })).code, DurableChannelRpcErrorCodes.ChannelNotFound);
			assertEquals(error(await host.call("subscribe", { channel: "note:/9" })).code, DurableChannelRpcErrorCodes.ChannelNotFound);
			assertEquals(error(await host.call("subscribe", { channel: "x-secret://" })).code, DurableChannelRpcErrorCodes.ChannelNotFound);
			assertEquals(
				error(await host.call("exec", { channel: "tally://", name: "ghost", params: {} })).code,
				DurableChannelRpcErrorCodes.MethodNotFound,
			);
			assertEquals(
				error(await host.call("exec", { channel: "tally://", name: "open", params: {} })).code,
				DurableChannelRpcErrorCodes.InvalidParams,
			);
			await host.call("exec", { channel: "tally://", name: "open", params: { id: "1" } });
			assertEquals(
				error(await host.call("exec", { channel: "tally://", name: "open", params: { id: "1" } })).code,
				DurableChannelRpcErrorCodes.ChannelAlreadyExists,
			);
			assertEquals(error(await host.call("hello", { clientId: "" })).code, DurableChannelRpcErrorCodes.InvalidParams);
			assertEquals(error(await host.call("ghost")).code, DurableChannelRpcErrorCodes.MethodNotFound);
		} finally {
			await host.hub.close();
		}
	});

	it("answers nothing to a notification and reports a frame that is not a request at all", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			assertEquals(await host.call("unsubscribe", { channel: "tally://" }, "notification"), undefined);
			await host.hub.dispatch("tally://", "tally/set", { to: 5 });
			assertEquals(host.sent, []);
			assertEquals(await host.raw({ hello: "there" }), { jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } });
		} finally {
			await host.hub.close();
		}
	});

	it("restores the subscriptions of a reconnect and names what it cannot resume", async () => {
		const host = boot();
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			await host.hub.dispatch("tally://", "tally/set", { to: 4 });
			host.sent.splice(0);
			assertEquals(
				result(
					await host.call("reconnect", { clientId: "alice", lastSeenServerSeq: 1, subscriptions: ["tally://", "note:/9", "feed:/live"] }),
				),
				{ type: "replay", actions: [], missing: ["note:/9"] },
			);
			await host.hub.dispatch("tally://", "tally/set", { to: 6 });
			assertEquals(host.sent.length, 1);
		} finally {
			await host.hub.close();
		}
	});

	it("hands back a snapshot instead of a replay once the ring cannot cover the gap", async () => {
		const host = boot({ replayLimit: 1 });
		try {
			await host.call("hello", { clientId: "alice", subscriptions: ["tally://"] });
			await host.hub.dispatch("tally://", "tally/set", { to: 1 });
			await host.hub.dispatch("tally://", "tally/set", { to: 2 });
			assertEquals(result(await host.call("reconnect", { clientId: "alice", lastSeenServerSeq: 0, subscriptions: ["tally://"] })), {
				type: "snapshot",
				snapshots: [{ resource: "tally://", state: { count: 2 }, fromSeq: 2 }],
				missing: [],
			});
		} finally {
			await host.hub.close();
		}
	});

	it("turns an envelope and a notification into frames, and reads only its own methods back", () => {
		const envelope = {
			type: "action",
			channel: "tally://",
			name: "tally/added",
			payload: { by: 1 },
			serverSeq: 3,
			origin: { clientId: "alice", clientSeq: 1 },
		} as const;
		const frame = toRpcNotification(envelope);
		assertEquals(frame, {
			jsonrpc: "2.0",
			method: "action",
			params: { channel: "tally://", name: "tally/added", payload: { by: 1 }, serverSeq: 3, origin: { clientId: "alice", clientSeq: 1 } },
		});
		assertEquals(fromRpcNotification("action", frame.params), envelope);
		const notification = { type: "notification", channel: "feed:/live", name: "feed/line", payload: { line: "hi" } } as const;
		assertEquals(fromRpcNotification("notification", toRpcNotification(notification).params), notification);
		assertEquals(fromRpcNotification("action", { channel: 12 }), undefined);
		assertEquals(fromRpcNotification("something/else", {}), undefined);
	});

	it("serves a socket in arrival order, refuses an undecodable frame and detaches when it closes", async () => {
		const host = boot();
		const wire = fakeSocket();
		const session = attachSocket(host.rpc, host.hub, wire.socket);
		try {
			wire.fire(
				"message",
				JSON.stringify({ jsonrpc: "2.0", id: 1, method: "hello", params: { clientId: "alice", subscriptions: ["tally://"] } }),
			);
			wire.fire(
				"message",
				JSON.stringify({
					jsonrpc: "2.0",
					id: 2,
					method: "dispatch",
					params: { channel: "tally://", clientSeq: 1, name: "tally/added", payload: { by: 3 } },
				}),
			);
			wire.fire("message", "{ not json");
			await flush();
			assertEquals(session.link.clientId, "alice");
			const decoded = wire.frames.map((frame) => JSON.parse(frame) as { id?: number | null; method?: string; error?: { code: number } });
			assertEquals(decoded.map((frame) => frame.method ?? frame.id), [1, "action", 2, null]);
			assertEquals(decoded[3].error?.code, -32700);
			assertEquals(await host.hub.get("tally://"), { count: 3 });
			wire.fire("close");
			await host.hub.dispatch("tally://", "tally/set", { to: 0 });
			assertEquals(wire.frames.length, 4);
		} finally {
			session.detach();
			await host.hub.close();
		}
	});
});
