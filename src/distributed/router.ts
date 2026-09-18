import type { InferInput, InferOutput } from "valibot";
import type { DurableChannelInstance } from "../channel.ts";
import { RouteNotFoundError, StatelessChannelError } from "../error.ts";
import { type DurableChannelRouteMap, type DurableChannelRoutes, matchRoute, type PathToParams, resolveUri } from "../routes.ts";
import type { DistributedActorEndpoint, DistributedCaller, DistributedDirectory } from "./interfaces.ts";
import type { DistributedEnvelope, DistributedSnapshot } from "./protocol.ts";
import { distributedError } from "./store.ts";

export type DistributedExecutionEndpoint = Pick<
	DistributedActorEndpoint,
	"snapshot" | "create" | "destroy" | "dispatch" | "dispatchFrom" | "exec" | "execFrom" | "notify"
>;
export interface DistributedRuntimeOperations {
	snapshot(uri: string, generation?: string): Promise<DistributedSnapshot | undefined>;
	get(uri: string, generation?: string): Promise<unknown>;
	has(uri: string, generation?: string): Promise<boolean>;
	create(uri: string, state?: unknown): Promise<DistributedSnapshot>;
	destroy(uri: string, generation?: string): Promise<void>;
	dispatch(uri: string, name: string, payload: unknown, generation?: string): Promise<DistributedEnvelope>;
	exec(uri: string, name: string, params: unknown, generation?: string, caller?: DistributedCaller): Promise<unknown>;
	notify(uri: string, name: string, payload: unknown, generation?: string): Promise<void>;
	list(template: string): AsyncIterable<DurableChannelInstance>;
}
export interface DurableChannelRouterOptions {
	resolve(uri: string): DistributedActorEndpoint | Promise<DistributedActorEndpoint>;
	directory?: DistributedDirectory;
}
export interface DistributedRouteHandle<R extends DurableChannelRouteMap, K extends keyof R & string> {
	uri(params: PathToParams<K>): string;
	get(params: PathToParams<K>, generation?: string): Promise<R[K]["state"]>;
	snapshot(params: PathToParams<K>, generation?: string): Promise<DistributedSnapshot | undefined>;
	create(params: PathToParams<K>, state?: R[K]["state"]): Promise<DistributedSnapshot>;
	destroy(params: PathToParams<K>, generation?: string): Promise<void>;
	dispatch<N extends keyof R[K]["actions"] & string>(
		params: PathToParams<K>,
		name: N,
		payload: InferInput<R[K]["actions"][N]>,
		generation?: string,
	): Promise<DistributedEnvelope>;
	exec<N extends keyof R[K]["commands"] & string>(
		params: PathToParams<K>,
		name: N,
		payload: InferInput<R[K]["commands"][N]["params"]>,
		generation?: string,
	): Promise<InferOutput<R[K]["commands"][N]["result"]>>;
}
/** A routing table plus owner resolution. It holds no authoritative state, locks, cursors or caches. */
export class DurableChannelRouter<TEnv = unknown, R extends DurableChannelRouteMap = DurableChannelRouteMap>
	implements DistributedRuntimeOperations {
	readonly routes: DurableChannelRoutes<TEnv, R>;
	readonly options: DurableChannelRouterOptions;
	constructor(routes: DurableChannelRoutes<TEnv, R>, options: DurableChannelRouterOptions) {
		this.routes = routes;
		this.options = options;
	}
	async endpoint(uri: string, publicOnly = false): Promise<DistributedActorEndpoint> {
		const match = matchRoute(this.routes, uri);
		if (!match || (publicOnly && match.route.internal)) throw new RouteNotFoundError(uri);
		return await this.options.resolve(uri);
	}
	async snapshot(uri: string, generation?: string): Promise<DistributedSnapshot | undefined> {
		return await (await this.endpoint(uri)).snapshot(generation);
	}
	async get(uri: string, generation?: string): Promise<unknown> {
		const snapshot = await this.snapshot(uri, generation);
		if (!snapshot) throw new StatelessChannelError(uri, "get");
		return snapshot.state;
	}
	async has(uri: string, generation?: string): Promise<boolean> {
		try {
			await this.snapshot(uri, generation);
			return true;
		} catch (error) {
			if (error && typeof error === "object" && "code" in error && ["CHANNEL_NOT_FOUND", "ROUTE_NOT_FOUND"].includes(String(error.code))) {
				return false;
			}
			throw error;
		}
	}
	async create(uri: string, state?: unknown): Promise<DistributedSnapshot> {
		return await (await this.endpoint(uri)).create(state);
	}
	async destroy(uri: string, generation?: string): Promise<void> {
		const endpoint = await this.endpoint(uri);
		const cursor = generation ?? (await endpoint.snapshot())?.cursor.generation;
		if (!cursor) throw new StatelessChannelError(uri, "destroy");
		await endpoint.destroy(cursor);
	}
	async dispatch(uri: string, name: string, payload: unknown, generation?: string): Promise<DistributedEnvelope> {
		const endpoint = await this.endpoint(uri);
		const cursor = generation ?? (await endpoint.snapshot())?.cursor.generation;
		if (!cursor) throw new StatelessChannelError(uri, "dispatch");
		return await endpoint.dispatch(cursor, name, payload);
	}
	async exec(uri: string, name: string, params: unknown, generation?: string, caller?: DistributedCaller): Promise<unknown> {
		const endpoint = await this.endpoint(uri);
		return await endpoint.exec(name, params, generation ?? (await endpoint.snapshot())?.cursor.generation, caller);
	}
	async notify(uri: string, name: string, payload: unknown, generation?: string): Promise<void> {
		const endpoint = await this.endpoint(uri);
		await endpoint.notify(name, payload, generation ?? (await endpoint.snapshot())?.cursor.generation);
	}
	async *list(template: string): AsyncGenerator<DurableChannelInstance> {
		if (!this.routes.routes.some((r) => r.template === template)) throw new RouteNotFoundError(template);
		if (!this.options.directory) throw distributedError("UNSUPPORTED_OPERATION", "Distributed list requires a directory provider");
		yield* this.options.directory.list(template);
	}
	of<K extends keyof R & string>(template: K): DistributedRouteHandle<R, K> {
		const uri = (params: PathToParams<K>): string => resolveUri(template, params);
		return {
			uri,
			get: (p, g) => this.get(uri(p), g) as Promise<R[K]["state"]>,
			snapshot: (p, g) => this.snapshot(uri(p), g),
			create: (p, state) => this.create(uri(p), state),
			destroy: (p, g) => this.destroy(uri(p), g),
			dispatch: (p, name, payload, g) => this.dispatch(uri(p), name, payload, g),
			exec: (p, name, payload, g) => this.exec(uri(p), name, payload, g) as never,
		};
	}
}
