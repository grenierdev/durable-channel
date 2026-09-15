/**
 * The wire protocol: a JSON-RPC 2.0 surface over a hub, and the socket glue that serves it.
 *
 * A hub knows nothing about a wire. This module is the one place that does: it declares the seven
 * methods a peer may call (`hello`, `ping`, `reconnect`, `subscribe`, `unsubscribe`, `dispatch`,
 * `exec`), the two notifications a hub pushes back (`action`, `notification`), and the table that maps
 * the library's error codes onto JSON-RPC numbers. The procedures themselves are a **hana** collection,
 * so params and results are valibot-validated and the envelope handling is hana's.
 *
 * One {@link DurableChannelLink} binds one connection — one socket, one in-memory pair, one anything —
 * to one client id. `hello` and `reconnect` bind it; every other method needs it bound already.
 * {@link attachSocket} is the whole server for a socket: it owns a link, answers frames in arrival
 * order, and translates what the hub broadcasts into notifications.
 */
import { Hana, hana, type HanaCollection, JsonRpcError, type JsonRpcRequest, type JsonRpcResponse } from "./hana.ts";
import * as v from "valibot";
import type { DurableChannelEnvelope, DurableChannelMessage, DurableChannelSnapshot } from "./channel.ts";
import { DurableChannelError, NotInitializedError, RejectAction } from "./error.ts";
import type { DurableChannelConnection, DurableChannelHub } from "./hub.ts";
import type { DurableChannelRouteMap } from "./routes.ts";

/**
 * The JSON-RPC codes this protocol answers with. The five reserved JSON-RPC codes keep their meaning;
 * the `-320xx` block carries the channel failures a peer can act on.
 */
export const DurableChannelRpcErrorCodes = {
	/** No route matches the URI, or a route matches but no instance lives there. */
	ChannelNotFound: -32001,
	/** `create` was asked for a URI that already holds an instance. */
	ChannelAlreadyExists: -32002,
	/** The link is bound to no client id: `hello` or `reconnect` has to come first. */
	NotInitialized: -32003,
	/** The link's client id is not connected to the hub any more. */
	ConnectionNotFound: -32004,
	/** A command refused the call by throwing `RejectAction`. `data` carries `{ reason }`. */
	ActionRejected: -32005,
	/** No command of that name on the channel. */
	MethodNotFound: -32601,
	/** Params, an action payload or a notification payload failed its schema. */
	InvalidParams: -32602,
	/** Anything the protocol cannot classify. Never carries a message from the inside. */
	InternalError: -32603,
} as const;

/** One frame of this protocol: a request, a notification (a request with no `id`), or a response. */
export type DurableChannelRpcFrame = JsonRpcRequest | JsonRpcResponse;

/** Wire schema of a snapshot, as `hello`, `subscribe` and `reconnect` return it. */
export const DurableChannelWireSnapshot: v.GenericSchema<DurableChannelSnapshot> = v.object({
	resource: v.string(),
	state: v.unknown(),
	fromSeq: v.number(),
}) as never;

/** Wire schema of a committed action, as `dispatch`, `reconnect` and the `action` notification carry it. */
export const DurableChannelWireEnvelope: v.GenericSchema<DurableChannelEnvelope> = v.object({
	type: v.literal("action"),
	channel: v.string(),
	name: v.string(),
	payload: v.unknown(),
	serverSeq: v.number(),
	origin: v.exactOptional(v.object({ clientId: v.string(), clientSeq: v.number() })),
	rejectionReason: v.exactOptional(v.string()),
}) as never;

const WireNotification = v.object({
	channel: v.string(),
	name: v.string(),
	payload: v.unknown(),
});

const WireReconnectResult = v.variant("type", [
	v.object({ type: v.literal("replay"), actions: v.array(DurableChannelWireEnvelope), missing: v.array(v.string()) }),
	v.object({ type: v.literal("snapshot"), snapshots: v.array(DurableChannelWireSnapshot), missing: v.array(v.string()) }),
]);

/** What `hello` answers: the hub's sequence at handshake time and one snapshot per stateful subscription. */
export interface DurableChannelHelloResult {
	readonly serverSeq: number;
	readonly snapshots: readonly DurableChannelSnapshot[];
}

/** What `subscribe` answers. The member is absent for a stateless channel. */
export interface DurableChannelSubscribeResult {
	readonly snapshot?: DurableChannelSnapshot;
}

/** Binds one connection to one client id. `hello` and `reconnect` call {@link DurableChannelLink.bind}. */
export interface DurableChannelLink {
	/** The client id this link is bound to, or `undefined` before the first `hello` or `reconnect`. */
	readonly clientId: string | undefined;
	/** Binds the link, dropping whatever binding it had. */
	bind(clientId: string): void;
}

/** What every procedure of the collection is given: the hub it serves and the caller's link. */
export interface DurableChannelRpcEnv<TEnv = unknown, TRoutes extends DurableChannelRouteMap = DurableChannelRouteMap> {
	readonly hub: DurableChannelHub<TEnv, TRoutes>;
	readonly link: DurableChannelLink;
}

/** A hub's JSON-RPC surface, as {@link createRpc} builds it. */
export interface DurableChannelRpc<TEnv = unknown, TRoutes extends DurableChannelRouteMap = DurableChannelRouteMap> {
	/** The hana collection, for a host that wants to merge these procedures into a larger one. */
	readonly collection: HanaCollection<DurableChannelRpcEnv<TEnv, TRoutes>>;
	/** The hana runtime over {@link DurableChannelRpc.collection}. */
	readonly hana: Hana<DurableChannelRpcEnv<TEnv, TRoutes>>;
	/**
	 * Handles one decoded frame. Answers a response for a request and `undefined` for a notification,
	 * even when the handler threw.
	 */
	handle(frame: unknown, link: DurableChannelLink, signal?: AbortSignal): Promise<JsonRpcResponse | undefined>;
}

/**
 * Maps a thrown value onto this protocol's error table. A `RejectAction` a command threw becomes
 * `-32005` with `data: { reason }`; a {@link DurableChannelError} keeps its stable code in the message
 * as `` `${code}: ${message}` `` so a peer can read it without matching on prose; anything else becomes
 * `-32603` and leaks nothing.
 */
export function toJsonRpcError(error: unknown): JsonRpcError {
	if (error instanceof JsonRpcError) {
		return error;
	}
	if (error instanceof RejectAction) {
		return new JsonRpcError(DurableChannelRpcErrorCodes.ActionRejected, `ACTION_REJECTED: ${error.reason}`, { reason: error.reason });
	}
	if (error instanceof DurableChannelError) {
		return new JsonRpcError(codeFor(error.code), `${error.code}: ${error.message}`);
	}
	return new JsonRpcError(DurableChannelRpcErrorCodes.InternalError, "Internal error");
}

function codeFor(code: string): number {
	switch (code) {
		case "ROUTE_NOT_FOUND":
		case "CHANNEL_NOT_FOUND":
			return DurableChannelRpcErrorCodes.ChannelNotFound;
		case "CHANNEL_ALREADY_EXISTS":
			return DurableChannelRpcErrorCodes.ChannelAlreadyExists;
		case "NOT_INITIALIZED":
			return DurableChannelRpcErrorCodes.NotInitialized;
		case "CONNECTION_NOT_FOUND":
			return DurableChannelRpcErrorCodes.ConnectionNotFound;
		case "INVALID_PAYLOAD":
			return DurableChannelRpcErrorCodes.InvalidParams;
		case "UNKNOWN_COMMAND":
			return DurableChannelRpcErrorCodes.MethodNotFound;
		default:
			return DurableChannelRpcErrorCodes.InternalError;
	}
}

async function guard<T>(task: () => T | Promise<T>): Promise<T> {
	try {
		return await task();
	} catch (error) {
		throw toJsonRpcError(error);
	}
}

function bound(link: DurableChannelLink): string {
	if (link.clientId === undefined) {
		throw toJsonRpcError(new NotInitializedError());
	}
	return link.clientId;
}

/** Turns a message the hub broadcast into the notification frame a peer expects. */
export function toRpcNotification(message: DurableChannelMessage): JsonRpcRequest {
	if (message.type === "notification") {
		return {
			jsonrpc: "2.0",
			method: "notification",
			params: { channel: message.channel, name: message.name, payload: message.payload },
		};
	}
	const { type: _type, ...params } = message;
	return { jsonrpc: "2.0", method: "action", params };
}

/**
 * Reads a notification frame back into a message. Returns `undefined` for a method this protocol does
 * not define and for params that fail their schema, so a peer speaking something else is ignored rather
 * than trusted.
 */
export function fromRpcNotification(method: string, params: unknown): DurableChannelMessage | undefined {
	if (method === "action") {
		const parsed = v.safeParse(DurableChannelWireEnvelope, { type: "action", ...(params as Record<string, unknown>) });
		return parsed.success ? parsed.output : undefined;
	}
	if (method === "notification") {
		const parsed = v.safeParse(WireNotification, params);
		return parsed.success ? { type: "notification", ...parsed.output } : undefined;
	}
	return undefined;
}

/**
 * Builds the JSON-RPC surface of one hub.
 *
 * `dispatch` is a **request**, not a notification: the answer carries the committed envelope, or `null`
 * when the `clientSeq` was a duplicate the hub ignored. It always dispatches leniently, so an unknown
 * action, a server-only action and a payload that fails its schema all come back as a rejected envelope
 * every subscriber also sees, instead of an error only the caller learns about.
 */
export function createRpc<TEnv, TRoutes extends DurableChannelRouteMap>(
	hub: DurableChannelHub<TEnv, TRoutes>,
): DurableChannelRpc<TEnv, TRoutes> {
	type Env = DurableChannelRpcEnv<TEnv, TRoutes>;
	const collection = hana()
		.env<Env>()
		.def((b) =>
			b.name("hello")
				.summary("Binds the link to a client id and subscribes it")
				.params(v.object({ clientId: v.pipe(v.string(), v.minLength(1)), subscriptions: v.optional(v.array(v.string()), []) }))
				.result(v.object({ serverSeq: v.number(), snapshots: v.array(DurableChannelWireSnapshot) }))
				.handler(async (params, ctx) =>
					await guard(async () => {
						ctx.env.link.bind(params.clientId);
						const snapshots: DurableChannelSnapshot[] = [];
						for (const uri of params.subscriptions) {
							const snapshot = await ctx.env.hub.subscribe(params.clientId, uri);
							if (snapshot !== undefined) {
								snapshots.push(snapshot);
							}
						}
						return { serverSeq: ctx.env.hub.serverSeq, snapshots };
					})
				)
		)
		.def((b) =>
			b.name("ping")
				.summary("Liveness check. Works before hello")
				.params(v.object({}))
				.result(v.null())
				.handler(() => null)
		)
		.def((b) =>
			b.name("reconnect")
				.summary("Rebinds the link and restores its subscriptions")
				.params(v.object({
					clientId: v.pipe(v.string(), v.minLength(1)),
					lastSeenServerSeq: v.number(),
					subscriptions: v.optional(v.array(v.string()), []),
				}))
				.result(WireReconnectResult)
				.handler(async (params, ctx) =>
					await guard(async () => {
						ctx.env.link.bind(params.clientId);
						const result = await ctx.env.hub.reconnect(params.clientId, params.lastSeenServerSeq, params.subscriptions);
						return result.type === "replay"
							? { type: "replay" as const, actions: [...result.actions], missing: [...result.missing] }
							: { type: "snapshot" as const, snapshots: [...result.snapshots], missing: [...result.missing] };
					})
				)
		)
		.def((b) =>
			b.name("subscribe")
				.summary("Subscribes the link to one channel")
				.params(v.object({ channel: v.string() }))
				.result(v.object({ snapshot: v.exactOptional(DurableChannelWireSnapshot) }))
				.handler(async (params, ctx) =>
					await guard(async () => {
						const snapshot = await ctx.env.hub.subscribe(bound(ctx.env.link), params.channel);
						return snapshot === undefined ? {} : { snapshot };
					})
				)
		)
		.def((b) =>
			b.name("unsubscribe")
				.summary("Drops one subscription of the link")
				.params(v.object({ channel: v.string() }))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.hub.unsubscribe(bound(ctx.env.link), params.channel);
					return null;
				})
		)
		.def((b) =>
			b.name("dispatch")
				.summary("Commits an action for the link, leniently")
				.params(v.object({
					channel: v.string(),
					clientSeq: v.number(),
					name: v.string(),
					payload: v.optional(v.unknown(), {}),
				}))
				.result(v.nullable(DurableChannelWireEnvelope))
				.handler(async (params, ctx) =>
					await guard(async () =>
						await ctx.env.hub.dispatchFrom(
							bound(ctx.env.link),
							params.channel,
							params.name,
							params.payload,
							params.clientSeq,
							{ lenient: true },
						) ?? null
					)
				)
		)
		.def((b) =>
			b.name("exec")
				.summary("Runs a command of a channel on behalf of the link")
				.params(v.object({ channel: v.string(), name: v.string(), params: v.optional(v.unknown(), {}) }))
				.result(v.unknown())
				.handler(async (params, ctx, signal) =>
					await guard(async () =>
						await ctx.env.hub.exec(params.channel, params.name, params.params, { connectionId: bound(ctx.env.link), signal })
					)
				)
		)
		.build();
	const runtime = new Hana(collection);
	return {
		collection,
		hana: runtime,
		handle: (frame, link, signal) => runtime.handle(frame, { env: { hub, link } }, signal ?? new AbortController().signal),
	};
}

/** The slice of a socket {@link attachSocket} needs. A `WebSocket` and a Durable Object socket both fit. */
export interface DurableChannelSocket {
	send(data: string): void;
	close(code?: number, reason?: string): void;
	addEventListener(type: string, listener: (event: { readonly data: unknown }) => void): void;
}

/** What {@link attachSocket} hands back. */
export interface DurableChannelSocketSession {
	/** The link this socket is bound to, so a host can read the client id it settled on. */
	readonly link: DurableChannelLink;
	/**
	 * Disconnects the link from the hub and stops answering frames. Called by the socket's own `close`
	 * and `error` events, and safe to call again.
	 */
	detach(): void;
}

const PARSE_ERROR = JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });

async function textOf(data: unknown): Promise<string> {
	if (typeof data === "string") {
		return data;
	}
	if (data instanceof ArrayBuffer) {
		return new TextDecoder().decode(data);
	}
	if (data instanceof Uint8Array) {
		return new TextDecoder().decode(data);
	}
	if (data instanceof Blob) {
		return await data.text();
	}
	throw new TypeError("Unsupported frame payload");
}

/**
 * Serves one socket: one link, one text frame per JSON-RPC message, in both directions.
 *
 * Frames are answered in arrival order — one promise chain, so a slow `exec` cannot let the next frame
 * overtake it — and everything the hub broadcasts to the bound connection leaves as an `action` or
 * `notification` frame. This is the whole server side of the protocol; a Hono route, a `Deno.serve`
 * handler and a Durable Object's `fetch` all need nothing more than to hand their socket over.
 */
export function attachSocket<TEnv, TRoutes extends DurableChannelRouteMap>(
	rpc: DurableChannelRpc<TEnv, TRoutes>,
	hub: DurableChannelHub<TEnv, TRoutes>,
	socket: DurableChannelSocket,
): DurableChannelSocketSession {
	const aborter = new AbortController();
	let binding: { clientId: string; connection: DurableChannelConnection } | undefined;
	let queue: Promise<void> = Promise.resolve();
	let detached = false;
	const release = (): void => {
		if (binding === undefined) {
			return;
		}
		const stale = binding;
		binding = undefined;
		hub.disconnect(stale.clientId, stale.connection);
	};
	const link: DurableChannelLink = {
		get clientId(): string | undefined {
			return binding?.clientId;
		},
		bind(clientId: string): void {
			release();
			const connection = hub.connect({ id: clientId, send: (message) => socket.send(JSON.stringify(toRpcNotification(message))) });
			binding = { clientId, connection };
		},
	};
	const detach = (): void => {
		if (detached) {
			return;
		}
		detached = true;
		aborter.abort();
		release();
	};
	const step = async (data: unknown): Promise<void> => {
		if (detached) {
			return;
		}
		let frame: unknown;
		try {
			frame = JSON.parse(await textOf(data));
		} catch {
			socket.send(PARSE_ERROR);
			return;
		}
		const response = await rpc.handle(frame, link, aborter.signal);
		if (response !== undefined) {
			socket.send(JSON.stringify(response));
		}
	};
	const on = (type: string, listener: (event: { readonly data: unknown }) => void): void => socket.addEventListener(type, listener);
	on("message", (event) => {
		const { data } = event;
		queue = queue.then(async () => {
			try {
				await step(data);
			} catch {
				// A socket that refuses a frame is gone: nothing left to report the failure on.
				detach();
			}
		});
	});
	on("close", detach);
	on("error", detach);
	return { link, detach };
}
