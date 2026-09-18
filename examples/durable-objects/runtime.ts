import {
	type DistributedActorEndpoint,
	type DistributedScheduler,
	DurableChannelError,
	DurableChannelRouter,
	RejectAction,
} from "../../src/mod.ts";
import type { ChannelObject } from "./channel-object.ts";
import type { GatewayObject } from "./gateway-object.ts";
import { routes } from "./routes.ts";
export interface Env {
	CHANNELS: DurableObjectNamespace<ChannelObject>;
	GATEWAYS: DurableObjectNamespace<GatewayObject>;
	LOGICAL_HUB: string;
	AUTH?: Fetcher;
}
export interface OwnerIdentity {
	hub: string;
	uri: string;
}
export interface OwnerCall extends OwnerIdentity {
	method: keyof DistributedActorEndpoint;
	args: unknown[];
}
export type OwnerResult = { ok: true; value: unknown } | { ok: false; code: string; reason?: string };
export function channelName(hub: string, uri: string): string {
	return JSON.stringify([hub, "channel", uri]);
}
export function gatewayName(hub: string, bucket: number): string {
	return JSON.stringify([hub, "gateway", bucket]);
}
export function alarmScheduler(storage: DurableObjectStorage): DistributedScheduler {
	return {
		arm: async (at) => {
			const current = await storage.getAlarm();
			if (current === null || at < current) await storage.setAlarm(at);
			await storage.sync();
		},
	};
}
/** RPC transports carry stable error DTOs; arbitrary internal exception text is never sent. */
export async function safeOwnerCall(task: () => Promise<unknown>): Promise<OwnerResult> {
	try {
		return { ok: true, value: await task() };
	} catch (error) {
		if (error instanceof RejectAction) return { ok: false, code: "ACTION_REJECTED", reason: error.reason };
		if (error instanceof DurableChannelError) return { ok: false, code: error.code };
		console.error("Durable Channel owner call failed", error);
		return { ok: false, code: "INTERNAL_ERROR" };
	}
}
export function ownerEndpoint(env: Env, hub: string, uri: string): DistributedActorEndpoint {
	const call = async (method: keyof DistributedActorEndpoint, args: unknown[]): Promise<unknown> => {
		// Reconstruct the address/stub for each call; no stub enters durable rows or serialized contexts.
		const response = await env.CHANNELS.getByName(channelName(hub, uri)).invoke({ hub, uri, method, args });
		const result = response as OwnerResult;
		if (!result.ok) {
			if (result.code === "ACTION_REJECTED") throw new RejectAction(result.reason ?? "Rejected");
			if (result.code === "INTERNAL_ERROR") throw new Error("Remote operation failed");
			throw new DurableChannelError(result.code, result.code);
		}
		return structuredClone(result.value);
	};
	return {
		snapshot: (g) => call("snapshot", [g]) as ReturnType<DistributedActorEndpoint["snapshot"]>,
		create: (s) => call("create", [s]) as ReturnType<DistributedActorEndpoint["create"]>,
		destroy: (g) => call("destroy", [g]) as Promise<void>,
		dispatch: (g, n, p) => call("dispatch", [g, n, p]) as ReturnType<DistributedActorEndpoint["dispatch"]>,
		dispatchFrom: (c, r) => call("dispatchFrom", [c, r]) as ReturnType<DistributedActorEndpoint["dispatchFrom"]>,
		exec: (n, p, g, c) => call("exec", [n, p, g, c]),
		execFrom: (c, n, p, g) => call("execFrom", [c, n, p, g]),
		notify: (n, p, g) => call("notify", [n, p, g]) as Promise<void>,
		resume: (r) => call("resume", [r]) as ReturnType<DistributedActorEndpoint["resume"]>,
		remove: (r) => call("remove", [r]) as Promise<void>,
		renew: (r) => call("renew", [r]) as ReturnType<DistributedActorEndpoint["renew"]>,
		alarm: () => call("alarm", []) as Promise<void>,
	};
}
export function routerFor(env: Env, hub: string): DurableChannelRouter {
	return new DurableChannelRouter(routes, { resolve: (uri) => ownerEndpoint(env, hub, uri) });
}
