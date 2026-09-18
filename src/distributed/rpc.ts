import * as v from "valibot";
import { Hana, hana, type HanaCollection, JsonRpcError, type JsonRpcRequest, type JsonRpcResponse } from "../hana.ts";
import { DurableChannelError } from "../error.ts";
import { type DurableChannelSocket, toJsonRpcError } from "../rpc.ts";
import type { DurableChannelTransport } from "../transport.ts";
import type { DurableChannelRouteMap } from "../routes.ts";
import type { DurableChannelGateway } from "./gateway.ts";
import type { DistributedSession } from "./interfaces.ts";
import {
	DISTRIBUTED_PROTOCOL,
	DistributedCursorSchema,
	DistributedDispatchResultSchema,
	DistributedEnvelopeSchema,
	DistributedGeneration,
	DistributedHelloResultSchema,
	type DistributedMessage,
	DistributedReconnectSchema,
	DistributedResumeSchema,
	DistributedSequence,
} from "./protocol.ts";
import { distributedError } from "./store.ts";

export interface DistributedRpcLink {
	readonly id: string;
	readonly clientId: string;
	readonly binding: number | undefined;
	bind(): Promise<number>;
	detach(): Promise<void>;
}
export interface DistributedRpcEnv {
	link: DistributedRpcLink;
}
export interface DistributedRpc {
	readonly collection: HanaCollection<DistributedRpcEnv>;
	readonly hana: Hana<DistributedRpcEnv>;
	handle(frame: unknown, link: DistributedRpcLink): Promise<JsonRpcResponse | undefined>;
}
export function createDistributedLink<TEnv, R extends DurableChannelRouteMap>(
	gateway: DurableChannelGateway<TEnv, R>,
	session: DistributedSession,
	restoredBinding?: number,
): DistributedRpcLink {
	let binding = restoredBinding;
	let closed = false;
	let bindingPromise: Promise<number> | undefined;
	return {
		id: session.id,
		clientId: session.clientId,
		get binding() {
			return binding;
		},
		async bind() {
			if (closed) throw distributedError("STALE_CONNECTION");
			if (binding !== undefined) return binding;
			bindingPromise ??= gateway.connect(session);
			const pending = bindingPromise;
			try {
				binding = await pending;
			} catch (error) {
				if (bindingPromise === pending) bindingPromise = undefined;
				throw error;
			}
			if (closed) {
				await gateway.disconnect(session.id, binding);
				throw distributedError("STALE_CONNECTION");
			}
			return binding;
		},
		async detach() {
			closed = true;
			if (binding !== undefined) await gateway.disconnect(session.id, binding);
		},
	};
}
function bound(link: DistributedRpcLink): number {
	if (link.binding === undefined) throw distributedError("NOT_INITIALIZED");
	return link.binding;
}
async function guard<T>(task: () => Promise<T>): Promise<T> {
	try {
		return await task();
	} catch (error) {
		if (error instanceof DurableChannelError) throw new JsonRpcError(-32010, error.code, { code: error.code });
		throw toJsonRpcError(error);
	}
}
/** Identity is host-bound on the link; clientId in hello is ignored by the distributed server. */
export function createDistributedRpc<TEnv, R extends DurableChannelRouteMap>(gateway: DurableChannelGateway<TEnv, R>): DistributedRpc {
	const collection = hana().env<DistributedRpcEnv>()
		.def((b) =>
			b.name("hello").params(
				v.object({
					protocol: v.literal(DISTRIBUTED_PROTOCOL),
					clientId: v.optional(v.string()),
					subscriptions: v.optional(v.array(v.string()), []),
					cursors: v.optional(v.record(v.string(), DistributedCursorSchema), {}),
				}),
			)
				.result(DistributedHelloResultSchema).handler((p, ctx) =>
					guard(async () => {
						const binding = await ctx.env.link.bind();
						return {
							protocol: DISTRIBUTED_PROTOCOL,
							clientId: ctx.env.link.clientId,
							...await gateway.reconnect(ctx.env.link.id, binding, p.subscriptions, p.cursors),
						};
					})
				)
		)
		.def((b) => b.name("ping").params(v.object({})).result(v.null()).handler(() => null))
		.def((b) =>
			b.name("subscribe").params(v.object({ channel: v.string(), cursor: v.optional(DistributedCursorSchema) })).result(
				DistributedResumeSchema,
			)
				.handler((p, ctx) => guard(() => gateway.subscribe(ctx.env.link.id, bound(ctx.env.link), p.channel, p.cursor)))
		)
		.def((b) =>
			b.name("unsubscribe").params(v.object({ channel: v.string() })).result(v.null())
				.handler((p, ctx) =>
					guard(async () => {
						await gateway.unsubscribe(ctx.env.link.id, bound(ctx.env.link), p.channel);
						return null;
					})
				)
		)
		.def((b) =>
			b.name("reconnect").params(
				v.object({ subscriptions: v.array(v.string()), cursors: v.optional(v.record(v.string(), DistributedCursorSchema), {}) }),
			).result(DistributedReconnectSchema)
				.handler((p, ctx) => guard(() => gateway.reconnect(ctx.env.link.id, bound(ctx.env.link), p.subscriptions, p.cursors)))
		)
		.def((b) =>
			b.name("dispatch").params(
				v.object({
					channel: v.string(),
					generation: DistributedGeneration,
					actionId: v.string(),
					clientSeq: DistributedSequence,
					name: v.string(),
					payload: v.unknown(),
				}),
			).result(DistributedDispatchResultSchema)
				.handler((p, ctx) =>
					guard(() => {
						const { channel, ...request } = p;
						return gateway.dispatch(ctx.env.link.id, bound(ctx.env.link), channel, request);
					})
				)
		)
		.def((b) =>
			b.name("exec").params(
				v.object({ channel: v.string(), generation: v.optional(DistributedGeneration), name: v.string(), params: v.unknown() }),
			).result(v.unknown())
				.handler((p, ctx) => guard(() => gateway.exec(ctx.env.link.id, bound(ctx.env.link), p.channel, p.name, p.params, p.generation)))
		)
		.build();
	const runtime = new Hana(collection);
	return { collection, hana: runtime, handle: (frame, link) => runtime.handle(frame, { env: { link } }, new AbortController().signal) };
}
export function distributedMessageToFrame(message: DistributedMessage): JsonRpcRequest {
	const { type, ...params } = message;
	return { jsonrpc: "2.0", method: type, params };
}
export function distributedFrameMessage(method: string, params: unknown): DistributedMessage | undefined {
	if (method === "action") {
		const parsed = v.safeParse(DistributedEnvelopeSchema, { type: "action", ...(params as Record<string, unknown>) });
		return parsed.success ? parsed.output : undefined;
	}
	if (method === "recovery") {
		const parsed = v.safeParse(
			v.object({ channel: v.string(), entry: DistributedResumeSchema, revision: v.optional(DistributedSequence) }),
			params,
		);
		return parsed.success ? { type: "recovery", ...parsed.output } : undefined;
	}
	if (method === "notification") {
		const parsed = v.safeParse(v.object({ channel: v.string(), name: v.string(), payload: v.unknown() }), params);
		return parsed.success ? { type: "notification", ...parsed.output } : undefined;
	}
	return undefined;
}
export interface DistributedSocketSession {
	readonly link: DistributedRpcLink;
	detach(): Promise<void>;
}
/** Socket events run independently after hello, so a slow channel command cannot block another URI. */
export function attachDistributedSocket<TEnv, R extends DurableChannelRouteMap>(
	rpc: DistributedRpc,
	gateway: DurableChannelGateway<TEnv, R>,
	socket: DurableChannelSocket,
	identity: { connectionId: string; clientId: string },
): DistributedSocketSession {
	const link = createDistributedLink(gateway, {
		id: identity.connectionId,
		clientId: identity.clientId,
		send: (message) => socket.send(JSON.stringify(distributedMessageToFrame(message))),
		close: (reason) => socket.close(1012, reason.slice(0, 120)),
	});
	let detached = false;
	const detach = async (): Promise<void> => {
		if (detached) return;
		detached = true;
		await link.detach();
	};
	socket.addEventListener("message", (event) => {
		if (detached) return;
		void (async () => {
			let frame: unknown;
			try {
				frame = JSON.parse(typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data as ArrayBuffer));
			} catch {
				socket.send(JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }));
				return;
			}
			const response = await rpc.handle(frame, link);
			if (response && !detached) socket.send(JSON.stringify(response));
		})().catch(detach);
	});
	socket.addEventListener("close", () => {
		void detach();
	});
	socket.addEventListener("error", () => {
		void detach();
	});
	return { link, detach };
}
/** Framed transport counterpart, useful for workers, message ports and deterministic integration tests. */
export function attachDistributedTransport<TEnv, R extends DurableChannelRouteMap>(
	gateway: DurableChannelGateway<TEnv, R>,
	transport: DurableChannelTransport,
	identity: { connectionId: string; clientId: string },
): DistributedSocketSession {
	const rpc = createDistributedRpc(gateway);
	const link = createDistributedLink(gateway, {
		id: identity.connectionId,
		clientId: identity.clientId,
		send: (message) => transport.send(distributedMessageToFrame(message)),
		close: () => {
			void transport.close();
		},
	});
	void (async () => {
		try {
			for (;;) {
				const frame = await transport.recv();
				if (!frame) break;
				void rpc.handle(frame, link).then(async (response) => {
					if (response) await transport.send(response);
				}).catch(() => link.detach());
			}
		} finally {
			await link.detach();
		}
	})().catch(() => link.detach());
	return {
		link,
		detach: async () => {
			await transport.close();
			await link.detach();
		},
	};
}
