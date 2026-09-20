/**
 * The authoritative side of a durable channel: instances, connections, sequencing and persistence.
 *
 * One hub owns a route map, the instances that live under it, every connection, one global
 * `serverSeq`, a bounded ring of committed envelopes for replay, and the storage the state is
 * persisted to. Only web-standard APIs are used, so a hub can also run inside a Cloudflare Durable
 * Object with the object's own `state.storage` behind {@link DurableChannelStorage}.
 *
 * A commit — validate the payload, run the reducer, validate the new state, persist, append to the
 * ring, broadcast — is serialized hub-wide by one promise-chain mutex. Commands run outside that
 * mutex and take it once per `dispatch`, and so does an accepted action's effect, which is why an
 * effect may dispatch again without deadlocking.
 *
 * A command or an effect may also hand the hub work that outlives it with `ctx.background`. Those
 * tasks are tracked per instance URI, aborted when the instance is destroyed, and awaited by
 * {@link DurableChannelHub.close}, so nothing keeps running after a hub is shut down.
 */
import { type GenericSchema, type InferInput, type InferOutput, safeParse } from "valibot";
import { describeRoutes, type DurableChannelDocument, type DurableChannelDocumentOptions } from "./document.ts";
import type {
	DurableChannel,
	DurableChannelActionDefinition,
	DurableChannelActionMap,
	DurableChannelActionOrigin,
	DurableChannelCommandContext,
	DurableChannelCommandMap,
	DurableChannelEnvelope,
	DurableChannelInstance,
	DurableChannelMessage,
	DurableChannelNotificationMap,
	DurableChannelOperations,
	DurableChannelPrincipal,
	DurableChannelReconnectResult,
	DurableChannelSnapshot,
} from "./channel.ts";
import {
	ChannelAlreadyExistsError,
	ChannelNotFoundError,
	ConnectionNotFoundError,
	DurableChannelError,
	InvalidPayloadError,
	InvalidResultError,
	InvalidStateError,
	NotClientDispatchableError,
	PermissionDeniedError,
	RejectAction,
	RouteNotFoundError,
	StatelessChannelError,
	UnknownActionError,
	UnknownCommandError,
	UnknownNotificationError,
} from "./error.ts";
import {
	type DurableChannelRoute,
	type DurableChannelRouteMap,
	type DurableChannelRouteMatch,
	type DurableChannelRoutes,
	isSingleton,
	matchRoute,
	type PathToParams,
	resolveUri,
} from "./routes.ts";
import type { DurableChannelStorage } from "./storage.ts";

const SERVER_SEQ_KEY = ["hub", "serverSeq"];

declare const connectionBrand: unique symbol;

/** A hub-issued capability. Only the hub that issued it may accept it. */
export interface DurableChannelConnection<TPrincipal extends DurableChannelPrincipal = DurableChannelPrincipal> {
	readonly [connectionBrand]: true;
	readonly id: string;
	readonly principal: Readonly<TPrincipal> | undefined;
}

/** Trusted host input. Identity is never read from a protocol frame. */
export interface DurableChannelConnectionOptions<TPrincipal extends DurableChannelPrincipal = DurableChannelPrincipal> {
	readonly id: string;
	readonly principal?: TPrincipal;
	send(message: DurableChannelMessage): void;
	onDisconnect?(): void;
}

/** What a hub needs to run. */
export interface DurableChannelHubOptions<TEnv = unknown> {
	readonly storage: DurableChannelStorage;
	readonly env: TEnv;
	/** How many committed envelopes stay replayable. Default 1024. */
	readonly replayLimit?: number;
}

/** Options of {@link DurableChannelHub.dispatchFrom}. */
export interface DurableChannelDispatchOptions {
	/**
	 * Commit an unacceptable action as a rejected envelope instead of throwing. For transports where a
	 * dispatch has no response channel, this is the only way the peer learns the outcome.
	 */
	readonly lenient?: boolean;
}

/** Options of {@link DurableChannelHub.exec}. */
export interface DurableChannelExecOptions {
	readonly signal?: AbortSignal;
}

interface Caller<TPrincipal extends DurableChannelPrincipal> extends DurableChannelExecOptions {
	readonly connectionId?: string;
	readonly principal?: Readonly<TPrincipal>;
}

/** A typed view of one route. `params` fills the template's `:name` segments. */
export interface DurableChannelHandle<
	TRoutes extends DurableChannelRouteMap,
	TTemplate extends keyof TRoutes & string,
	TPrincipal extends DurableChannelPrincipal = DurableChannelPrincipal,
> {
	uri(params: PathToParams<TTemplate>): string;
	get(params: PathToParams<TTemplate>): Promise<TRoutes[TTemplate]["state"]>;
	has(params: PathToParams<TTemplate>): Promise<boolean>;
	create(params: PathToParams<TTemplate>, state?: TRoutes[TTemplate]["state"]): Promise<void>;
	destroy(params: PathToParams<TTemplate>): Promise<void>;
	subscribe(connection: DurableChannelConnection<TPrincipal>, params: PathToParams<TTemplate>): Promise<DurableChannelSnapshot | undefined>;
	dispatch<TName extends keyof TRoutes[TTemplate]["actions"] & string>(
		params: PathToParams<TTemplate>,
		name: TName,
		payload: InferInput<TRoutes[TTemplate]["actions"][TName]>,
	): Promise<DurableChannelEnvelope>;
	notify<TName extends keyof TRoutes[TTemplate]["notifications"] & string>(
		params: PathToParams<TTemplate>,
		name: TName,
		payload: InferInput<TRoutes[TTemplate]["notifications"][TName]>,
	): Promise<void>;
	exec<TName extends keyof TRoutes[TTemplate]["commands"] & string>(
		params: PathToParams<TTemplate>,
		name: TName,
		commandParams: InferInput<TRoutes[TTemplate]["commands"][TName]["params"]>,
		options?: DurableChannelExecOptions,
	): Promise<InferOutput<TRoutes[TTemplate]["commands"][TName]["result"]>>;
	execFrom<TName extends keyof TRoutes[TTemplate]["commands"] & string>(
		connection: DurableChannelConnection<TPrincipal>,
		params: PathToParams<TTemplate>,
		name: TName,
		commandParams: InferInput<TRoutes[TTemplate]["commands"][TName]["params"]>,
		options?: DurableChannelExecOptions,
	): Promise<InferOutput<TRoutes[TTemplate]["commands"][TName]["result"]>>;
	list(): AsyncIterable<DurableChannelInstance<TRoutes[TTemplate]["state"]>>;
}

interface Link<TPrincipal extends DurableChannelPrincipal> {
	readonly connection: DurableChannelConnection<TPrincipal>;
	readonly send: (message: DurableChannelMessage) => void;
	readonly onDisconnect: (() => void) | undefined;
	readonly subscriptions: Set<string>;
	/** Highest `clientSeq` this link has accepted. Never persisted: a new link restarts at 0. */
	watermark: number;
}

interface Instance<TEnv = unknown, TPrincipal extends DurableChannelPrincipal = DurableChannelPrincipal> {
	readonly uri: string;
	readonly route: DurableChannelRoute<TEnv, TPrincipal>;
	readonly params: Readonly<Record<string, string>>;
	readonly schema: GenericSchema;
	state: unknown;
}

/** Bounded ring of committed envelopes, keyed by `serverSeq`. Empty after a restart. */
class ReplayRing {
	#items: (DurableChannelEnvelope | undefined)[];
	#head = 0;
	#size = 0;

	constructor(limit: number) {
		this.#items = new Array(Math.max(1, limit)).fill(undefined);
	}

	append(envelope: DurableChannelEnvelope): void {
		this.#items[(this.#head + this.#size) % this.#items.length] = envelope;
		if (this.#size === this.#items.length) {
			this.#head = (this.#head + 1) % this.#items.length;
		} else {
			this.#size += 1;
		}
	}

	since(seq: number): DurableChannelEnvelope[] {
		const found: DurableChannelEnvelope[] = [];
		for (let offset = 0; offset < this.#size; offset += 1) {
			const envelope = this.#items[(this.#head + offset) % this.#items.length];
			if (envelope !== undefined && envelope.serverSeq > seq) {
				found.push(envelope);
			}
		}
		return found;
	}

	/** `true` when the ring still holds every sequence after `seq`. */
	covers(seq: number): boolean {
		const oldest = this.#size === 0 ? undefined : this.#items[this.#head];
		return oldest !== undefined && oldest.serverSeq <= seq + 1;
	}
}

function reasonOf(error: DurableChannelError): string {
	return `${error.code}: ${error.message}`;
}

/**
 * A hub over one route map. `TRoutes` comes from the route map, so `dispatch`, `exec`, `notify`, `get`
 * and {@link DurableChannelHub.of} are typed by template and member name.
 */
export class DurableChannelHub<
	TEnv = unknown,
	TRoutes extends DurableChannelRouteMap = DurableChannelRouteMap,
	TPrincipal extends DurableChannelPrincipal = DurableChannelPrincipal,
> {
	#routes: DurableChannelRoutes<TEnv, TRoutes, TPrincipal>;
	#storage: DurableChannelStorage;
	#env: TEnv;
	#replay: ReplayRing;
	#instances = new Map<string, Instance<TEnv, TPrincipal>>();
	#links = new Map<string, Link<TPrincipal>>();
	#aborters = new Map<string, AbortController>();
	#running = new Set<Promise<void>>();
	#serverSeq = 0;
	#lock: Promise<unknown> = Promise.resolve();
	#hydration: Promise<void> | undefined;
	#closed = false;

	constructor(routes: DurableChannelRoutes<TEnv, TRoutes, TPrincipal>, options: DurableChannelHubOptions<TEnv>) {
		this.#routes = routes;
		this.#storage = options.storage;
		this.#env = options.env;
		this.#replay = new ReplayRing(options.replayLimit ?? 1024);
	}

	/** The last committed sequence number. Meaningful once {@link DurableChannelHub.ready} has resolved. */
	get serverSeq(): number {
		return this.#serverSeq;
	}

	/**
	 * Describes every route this hub mounts — state, actions, commands and notifications as JSON Schema —
	 * in the OpenRPC-flavoured shape of {@link describeRoutes}. Pure: it reads the definitions only, so the
	 * document is the same before and after the hub has run.
	 */
	generateSchema(options?: DurableChannelDocumentOptions): DurableChannelDocument {
		return describeRoutes(this.#routes, options);
	}

	/** Loads the persisted `serverSeq`. Idempotent, and awaited by every other method anyway. */
	ready(): Promise<void> {
		this.#hydration ??= this.#hydrate();
		return this.#hydration;
	}

	/**
	 * Registers a connection. Reusing a live client ID replaces its link only for the same principal
	 * (including both anonymous). The returned handle is required for every caller-scoped operation.
	 */
	connect(options: DurableChannelConnectionOptions<TPrincipal>): DurableChannelConnection<TPrincipal> {
		const principal = options.principal === undefined ? undefined : Object.freeze({ ...options.principal });
		if (
			typeof options.id !== "string" || !options.id ||
			(principal !== undefined && (typeof principal.id !== "string" || principal.id.length === 0))
		) {
			throw new PermissionDeniedError();
		}
		const previous = this.#links.get(options.id);
		if (previous !== undefined && previous.connection.principal?.id !== principal?.id) {
			throw new PermissionDeniedError();
		}
		const connection = Object.freeze({ id: options.id, principal }) as DurableChannelConnection<TPrincipal>;
		// Install the replacement before notifying the old transport: its cleanup cannot remove us.
		this.#links.set(connection.id, {
			connection,
			send: options.send,
			onDisconnect: options.onDisconnect,
			subscriptions: new Set(),
			watermark: 0,
		});
		if (previous !== undefined) this.#disconnected(previous);
		return connection;
	}

	/** Drops this exact link; stale cleanup never affects its replacement. */
	disconnect(connection: DurableChannelConnection<TPrincipal>): void {
		const link = this.#links.get(connection.id);
		if (link === undefined || link.connection !== connection) {
			return;
		}
		this.#links.delete(connection.id);
		this.#disconnected(link);
	}

	#disconnected(link: Link<TPrincipal>): void {
		link.subscriptions.clear();
		try {
			link.onDisconnect?.();
		} catch { /* Host cleanup must not break registration or a commit. */ }
	}

	/**
	 * Subscribes a connection. Returns the snapshot, or `undefined` for a stateless channel. An
	 * internal route is refused with {@link RouteNotFoundError}, exactly like an unknown one.
	 */
	async subscribe(connection: DurableChannelConnection<TPrincipal>, uri: string): Promise<DurableChannelSnapshot | undefined> {
		await this.ready();
		const link = this.#link(connection);
		const match = this.#reachable(uri);
		if (match.route.definition.state === undefined) {
			link.subscriptions.add(uri);
			return undefined;
		}
		const instance = await this.#instance(uri, "subscribe");
		this.#link(connection);
		link.subscriptions.add(uri);
		return { resource: uri, state: instance.state, fromSeq: this.#serverSeq };
	}

	/** Drops one subscription. Requires a current connection; an absent subscription is a no-op. */
	unsubscribe(connection: DurableChannelConnection<TPrincipal>, uri: string): void {
		this.#link(connection).subscriptions.delete(uri);
	}

	/**
	 * Restores a link's subscriptions and hands back what it missed. Replay while the ring still covers
	 * everything after `lastSeenServerSeq`, fresh snapshots otherwise. URIs the hub cannot resume come
	 * back in `missing` — an unknown route, an internal route and a destroyed instance alike; a
	 * stateless URI is re-subscribed and listed in neither.
	 */
	async reconnect(
		connection: DurableChannelConnection<TPrincipal>,
		lastSeenServerSeq: number,
		uris: readonly string[],
	): Promise<DurableChannelReconnectResult> {
		await this.ready();
		const link = this.#link(connection);
		link.subscriptions.clear();
		const missing: string[] = [];
		const resumed: Instance<TEnv, TPrincipal>[] = [];
		for (const uri of uris) {
			const match = matchRoute(this.#routes, uri);
			if (match === undefined || match.route.internal) {
				missing.push(uri);
				continue;
			}
			if (match.route.definition.state === undefined) {
				link.subscriptions.add(uri);
				continue;
			}
			try {
				resumed.push(await this.#instance(uri, "subscribe"));
			} catch (error) {
				if (!(error instanceof DurableChannelError)) {
					throw error;
				}
				missing.push(uri);
				continue;
			}
			this.#link(connection);
			link.subscriptions.add(uri);
		}
		this.#link(connection);
		const identity = connection.principal === undefined ? {} : { principal: { id: connection.principal.id } };
		if (lastSeenServerSeq === this.#serverSeq || this.#replay.covers(lastSeenServerSeq)) {
			return {
				...identity,
				type: "replay",
				actions: this.#replay.since(lastSeenServerSeq).filter((envelope) => link.subscriptions.has(envelope.channel)),
				missing,
			};
		}
		return {
			...identity,
			type: "snapshot",
			snapshots: resumed.map((instance) => ({ resource: instance.uri, state: instance.state, fromSeq: this.#serverSeq })),
			missing,
		};
	}

	/** Commits a server-origin action. Throws rather than committing a rejected envelope. */
	dispatch<TTemplate extends keyof TRoutes & string, TName extends keyof TRoutes[TTemplate]["actions"] & string>(
		uri: TTemplate,
		name: TName,
		payload: InferInput<TRoutes[TTemplate]["actions"][TName]>,
	): Promise<DurableChannelEnvelope>;
	dispatch(uri: string, name: string, payload: unknown): Promise<DurableChannelEnvelope>;
	async dispatch(uri: string, name: string, payload: unknown): Promise<DurableChannelEnvelope> {
		await this.ready();
		const instance = await this.#instance(uri, "dispatch");
		const action = this.#action(instance, name);
		if (action === undefined) {
			throw new UnknownActionError(uri, name);
		}
		const parsed = safeParse(action.payload, payload);
		if (!parsed.success) {
			throw new InvalidPayloadError(uri, name);
		}
		return await this.#commit(instance, name, parsed.output, undefined, undefined, action);
	}

	/**
	 * Commits an action a connection dispatched. Returns `undefined` when `clientSeq` is not greater
	 * than the highest this link has accepted, consuming no sequence. With `lenient`, an unknown action,
	 * a server-only action or a payload failing its schema becomes a rejected envelope instead of a
	 * thrown error; a missing channel, an internal route or an unknown connection always throws.
	 */
	async dispatchFrom(
		connection: DurableChannelConnection<TPrincipal>,
		uri: string,
		name: string,
		payload: unknown,
		clientSeq: number,
		options?: DurableChannelDispatchOptions,
	): Promise<DurableChannelEnvelope | undefined> {
		await this.ready();
		const link = this.#link(connection);
		this.#reachable(uri);
		const instance = await this.#instance(uri, "dispatch");
		this.#link(connection);
		if (clientSeq <= link.watermark) {
			return undefined;
		}
		link.watermark = clientSeq;
		const origin: DurableChannelActionOrigin = Object.freeze({
			clientId: connection.id,
			clientSeq,
			...(connection.principal === undefined ? {} : { principalId: connection.principal.id }),
		});
		const refuse = async (error: DurableChannelError): Promise<DurableChannelEnvelope> => {
			if (options?.lenient !== true) {
				throw error;
			}
			return await this.#commit(instance, name, payload, origin, reasonOf(error), undefined, connection);
		};
		const action = this.#action(instance, name);
		if (action === undefined) {
			return await refuse(new UnknownActionError(uri, name));
		}
		if (!action.client) {
			return await refuse(new NotClientDispatchableError(uri, name));
		}
		const parsed = safeParse(action.payload, payload);
		if (!parsed.success) {
			return await refuse(new InvalidPayloadError(uri, name));
		}
		return await this.#commit(instance, name, parsed.output, origin, undefined, action, connection);
	}

	/** Runs a command. Params and result are validated against their schemas. */
	exec<TTemplate extends keyof TRoutes & string, TName extends keyof TRoutes[TTemplate]["commands"] & string>(
		uri: TTemplate,
		name: TName,
		params: InferInput<TRoutes[TTemplate]["commands"][TName]["params"]>,
		options?: DurableChannelExecOptions,
	): Promise<InferOutput<TRoutes[TTemplate]["commands"][TName]["result"]>>;
	exec(uri: string, name: string, params: unknown, options?: DurableChannelExecOptions): Promise<unknown>;
	async exec(uri: string, name: string, params: unknown, options?: DurableChannelExecOptions): Promise<unknown> {
		return await this.#exec(uri, name, params, { ...(options?.signal === undefined ? {} : { signal: options.signal }) });
	}

	/** Runs a remote command using a live connection, hiding internal routes. */
	execFrom<TTemplate extends keyof TRoutes & string, TName extends keyof TRoutes[TTemplate]["commands"] & string>(
		connection: DurableChannelConnection<TPrincipal>,
		uri: TTemplate,
		name: TName,
		params: InferInput<TRoutes[TTemplate]["commands"][TName]["params"]>,
		options?: DurableChannelExecOptions,
	): Promise<InferOutput<TRoutes[TTemplate]["commands"][TName]["result"]>>;
	execFrom(
		connection: DurableChannelConnection<TPrincipal>,
		uri: string,
		name: string,
		params: unknown,
		options?: DurableChannelExecOptions,
	): Promise<unknown>;
	async execFrom(
		connection: DurableChannelConnection<TPrincipal>,
		uri: string,
		name: string,
		params: unknown,
		options?: DurableChannelExecOptions,
	): Promise<unknown> {
		this.#link(connection);
		this.#reachable(uri);
		return await this.#exec(uri, name, params, {
			connectionId: connection.id,
			...(connection.principal === undefined ? {} : { principal: connection.principal }),
			...(options?.signal === undefined ? {} : { signal: options.signal }),
		}, connection);
	}

	async #exec(
		uri: string,
		name: string,
		params: unknown,
		options?: Caller<TPrincipal>,
		connection?: DurableChannelConnection<TPrincipal>,
	): Promise<unknown> {
		await this.ready();
		if (connection !== undefined) this.#link(connection);
		const match = this.#match(uri);
		const definition = match.route.definition;
		const command = Object.hasOwn(definition.commands, name) ? definition.commands[name] : undefined;
		if (command === undefined) {
			throw new UnknownCommandError(uri, name);
		}
		const parsed = safeParse(command.params, params);
		if (!parsed.success) {
			throw new InvalidPayloadError(uri, name);
		}
		if (definition.state !== undefined) {
			await this.#instance(uri, "exec");
		}
		if (connection !== undefined) this.#link(connection);
		const result = await command.handler(parsed.output as never, this.#context(uri, match.params, options));
		const validated = safeParse(command.result, result);
		if (!validated.success) {
			throw new InvalidResultError(uri, name);
		}
		return validated.output;
	}

	/** Sends a notification to the URI's current subscribers. Never stored, never replayed. */
	notify<TTemplate extends keyof TRoutes & string, TName extends keyof TRoutes[TTemplate]["notifications"] & string>(
		uri: TTemplate,
		name: TName,
		payload: InferInput<TRoutes[TTemplate]["notifications"][TName]>,
	): Promise<void>;
	notify(uri: string, name: string, payload: unknown): Promise<void>;
	async notify(uri: string, name: string, payload: unknown): Promise<void> {
		await this.ready();
		const match = this.#match(uri);
		const definition = match.route.definition;
		const notification = Object.hasOwn(definition.notifications, name) ? definition.notifications[name] : undefined;
		if (notification === undefined) {
			throw new UnknownNotificationError(uri, name);
		}
		const parsed = safeParse(notification.payload, payload);
		if (!parsed.success) {
			throw new InvalidPayloadError(uri, name);
		}
		this.#broadcast({ type: "notification", channel: uri, name, payload: parsed.output });
	}

	/** The current state of an instance. */
	get<TTemplate extends keyof TRoutes & string>(uri: TTemplate): Promise<TRoutes[TTemplate]["state"]>;
	get(uri: string): Promise<unknown>;
	async get(uri: string): Promise<unknown> {
		await this.ready();
		return (await this.#instance(uri, "get")).state;
	}

	/** `true` when the route matches and an instance exists. Always `true` for a stateless route. */
	async has(uri: string): Promise<boolean> {
		await this.ready();
		const match = matchRoute(this.#routes, uri);
		if (match === undefined) {
			return false;
		}
		if (match.route.definition.state === undefined || this.#instances.has(uri) || isSingleton(match.route)) {
			return true;
		}
		return await this.#storage.get(["channel", match.route.template, uri]) !== undefined;
	}

	/** Creates an instance of a parameterized route. `state` defaults to the definition's initial state. */
	async create(uri: string, state?: unknown): Promise<void> {
		await this.ready();
		const match = this.#match(uri);
		const definition = match.route.definition;
		if (definition.state === undefined) {
			throw new StatelessChannelError(uri, "create");
		}
		if (await this.has(uri)) {
			throw new ChannelAlreadyExistsError(uri);
		}
		const parsed = safeParse(definition.state, state === undefined ? definition.initialState : state);
		if (!parsed.success) {
			throw new InvalidStateError(uri);
		}
		this.#instances.set(uri, { uri, route: match.route, params: match.params, schema: definition.state, state: parsed.output });
		await this.#storage.set(["channel", match.route.template, uri], parsed.output);
	}

	/** Deletes an instance, its persisted state, its background tasks and every subscription to it. */
	async destroy(uri: string): Promise<void> {
		await this.ready();
		const instance = await this.#instance(uri, "destroy");
		this.#abortBackground(uri);
		await this.#storage.delete(["channel", instance.route.template, uri]);
		this.#instances.delete(uri);
		for (const link of this.#links.values()) {
			link.subscriptions.delete(uri);
		}
	}

	/**
	 * Aborts every background task the hub still tracks and waits for all of them to settle. New tasks
	 * are refused afterwards, so a closed hub cannot start work again; everything else keeps working.
	 */
	async close(): Promise<void> {
		this.#closed = true;
		for (const uri of [...this.#aborters.keys()]) {
			this.#abortBackground(uri);
		}
		while (this.#running.size > 0) {
			await Promise.all([...this.#running]);
		}
	}

	/**
	 * Every instance of a template, as one prefix scan of the storage. Yields nothing for a stateless
	 * route. The scan starts on the first `next()`, so an unknown template throws there, not here.
	 */
	async *list(template: string): AsyncGenerator<DurableChannelInstance> {
		await this.ready();
		const route = this.#routes.routes.find((candidate) => candidate.template === template);
		if (route === undefined) {
			throw new RouteNotFoundError(template);
		}
		if (route.definition.state === undefined) {
			return;
		}
		let cursor: string | undefined;
		do {
			const page = await this.#storage.list({ prefix: ["channel", template], ...(cursor !== undefined ? { cursor } : {}) });
			for (const entry of page.entries) {
				const uri = entry.key[entry.key.length - 1];
				yield { uri, params: matchRoute(this.#routes, uri)?.params ?? {}, state: entry.value };
			}
			cursor = page.cursor;
		} while (cursor !== undefined);
	}

	/** A typed handle on one template, so a family's instances are addressed by parameters. */
	of<TTemplate extends keyof TRoutes & string>(template: TTemplate): DurableChannelHandle<TRoutes, TTemplate, TPrincipal> {
		const uri = (params: PathToParams<TTemplate>): string => resolveUri(template, params as Record<string, string>);
		return {
			uri,
			get: (params) => this.get(uri(params)) as never,
			has: (params) => this.has(uri(params)),
			create: (params, state) => this.create(uri(params), state),
			destroy: (params) => this.destroy(uri(params)),
			subscribe: (connectionId, params) => this.subscribe(connectionId, uri(params)),
			dispatch: (params, name, payload) => this.dispatch(uri(params), name, payload),
			notify: (params, name, payload) => this.notify(uri(params), name, payload),
			exec: (params, name, commandParams, options) => this.exec(uri(params), name, commandParams, options) as never,
			execFrom: (connection, params, name, commandParams, options) =>
				this.execFrom(connection, uri(params), name, commandParams, options) as never,
			list: () => this.list(template) as never,
		};
	}

	async #hydrate(): Promise<void> {
		const stored = await this.#storage.get(SERVER_SEQ_KEY);
		if (typeof stored === "number") {
			this.#serverSeq = stored;
		}
	}

	#link(connection: DurableChannelConnection<TPrincipal>): Link<TPrincipal> {
		const link = this.#links.get(connection?.id);
		if (link === undefined || link.connection !== connection) {
			throw new ConnectionNotFoundError(connection?.id ?? "<invalid>");
		}
		return link;
	}

	#match(uri: string): DurableChannelRouteMatch<TEnv, TPrincipal> {
		const match = matchRoute(this.#routes, uri);
		if (match === undefined) {
			throw new RouteNotFoundError(uri);
		}
		return match;
	}

	/** The match a connection is allowed to see: an internal route is as good as no route at all. */
	#reachable(uri: string): DurableChannelRouteMatch<TEnv, TPrincipal> {
		const match = this.#match(uri);
		if (match.route.internal) {
			throw new RouteNotFoundError(uri);
		}
		return match;
	}

	/** Launches a tracked background task for `uri`, unless the hub is already closed. */
	#background(uri: string, task: (signal: AbortSignal) => void | Promise<void>): void {
		if (this.#closed) {
			return;
		}
		let aborter = this.#aborters.get(uri);
		if (aborter === undefined) {
			aborter = new AbortController();
			this.#aborters.set(uri, aborter);
		}
		const signal = aborter.signal;
		const running = (async () => {
			try {
				await task(signal);
			} catch {
				// A background task has no caller to report to: whatever it threw dies here.
			}
		})();
		this.#running.add(running);
		void running.then(() => {
			this.#running.delete(running);
		});
	}

	#abortBackground(uri: string): void {
		const aborter = this.#aborters.get(uri);
		if (aborter === undefined) {
			return;
		}
		this.#aborters.delete(uri);
		aborter.abort();
	}

	#action(instance: Instance<TEnv, TPrincipal>, name: string): DurableChannelActionDefinition<unknown, TEnv, TPrincipal> | undefined {
		const actions = instance.route.definition.actions;
		return Object.hasOwn(actions, name) ? actions[name] : undefined;
	}

	/** The instance at `uri`, hydrating it from storage and auto-creating a singleton on first touch. */
	async #instance(uri: string, operation: string): Promise<Instance<TEnv, TPrincipal>> {
		const match = this.#match(uri);
		const definition: DurableChannel<
			TEnv,
			unknown,
			DurableChannelActionMap,
			DurableChannelCommandMap,
			DurableChannelNotificationMap,
			TPrincipal
		> = match.route.definition;
		if (definition.state === undefined) {
			throw new StatelessChannelError(uri, operation);
		}
		const cached = this.#instances.get(uri);
		if (cached !== undefined) {
			return cached;
		}
		const key = ["channel", match.route.template, uri];
		const stored = await this.#storage.get(key);
		if (stored === undefined && !isSingleton(match.route)) {
			throw new ChannelNotFoundError(uri);
		}
		const state = stored === undefined ? definition.initialState : stored;
		const instance: Instance<TEnv, TPrincipal> = { uri, route: match.route, params: match.params, schema: definition.state, state };
		this.#instances.set(uri, instance);
		if (stored === undefined) {
			await this.#storage.set(key, state);
		}
		return instance;
	}

	/**
	 * The hub calls every command handler and every action effect is given. `exec` forwards the
	 * caller's connection and abort signal to the nested command, and `background` is bound to `uri`,
	 * the instance the context belongs to.
	 */
	#operations(uri: string, options?: Caller<TPrincipal>): DurableChannelOperations<TEnv> {
		return {
			env: this.#env,
			dispatch: (target: string, name: string, payload: unknown) => this.dispatch(target, name, payload),
			notify: (target: string, name: string, payload: unknown) => this.notify(target, name, payload),
			get: (target: string) => this.get(target),
			has: (target: string) => this.has(target),
			create: (target: string, state?: unknown) => this.create(target, state),
			destroy: (target: string) => this.destroy(target),
			list: (template: string) => this.list(template),
			exec: (target: string, name: string, commandParams: unknown) => this.#exec(target, name, commandParams, options),
			background: (task: (signal: AbortSignal) => void | Promise<void>) => this.#background(uri, task),
			abortBackground: () => this.#abortBackground(uri),
		};
	}

	#context(
		uri: string,
		params: Readonly<Record<string, string>>,
		options?: Caller<TPrincipal>,
	): DurableChannelCommandContext<TEnv, unknown, DurableChannelActionMap, DurableChannelNotificationMap, TPrincipal> {
		return {
			...this.#operations(uri, options),
			uri,
			params,
			connectionId: options?.connectionId,
			principal: options?.principal,
			signal: options?.signal ?? new AbortController().signal,
			state: () => this.get(uri),
		};
	}

	/**
	 * Validate, reduce, validate, persist, append, broadcast — under the hub-wide mutex — then run the
	 * action's effect outside it, so the effect may dispatch again. A reducer that throws
	 * {@link RejectAction}, or a caller that already decided to refuse, still consumes a sequence and
	 * still reaches every subscriber, but leaves the state untouched and runs no effect. An effect that
	 * throws propagates to the dispatcher, by which time the envelope is already committed.
	 */
	async #commit(
		instance: Instance<TEnv, TPrincipal>,
		name: string,
		payload: unknown,
		origin: DurableChannelActionOrigin | undefined,
		rejectionReason: string | undefined,
		action: DurableChannelActionDefinition<unknown, TEnv, TPrincipal> | undefined,
		connection?: DurableChannelConnection<TPrincipal>,
	): Promise<DurableChannelEnvelope> {
		const committed = await this.#withLock(async () => {
			if (connection !== undefined) this.#link(connection);
			let reason = rejectionReason;
			let state = instance.state;
			if (reason === undefined && action !== undefined) {
				try {
					const reduced = action.reduce(instance.state, payload, {
						uri: instance.uri,
						params: instance.params,
						...(origin !== undefined ? { origin } : {}),
					});
					const parsed = safeParse(instance.schema, reduced);
					if (!parsed.success) {
						throw new InvalidStateError(instance.uri);
					}
					state = parsed.output;
				} catch (error) {
					if (!(error instanceof RejectAction)) {
						throw error;
					}
					reason = error.reason;
				}
			}
			const serverSeq = this.#serverSeq + 1;
			const envelope: DurableChannelEnvelope = {
				type: "action",
				channel: instance.uri,
				name,
				payload,
				serverSeq,
				...(origin !== undefined ? { origin } : {}),
				...(reason !== undefined ? { rejectionReason: reason } : {}),
			};
			if (reason === undefined) {
				instance.state = state;
				await this.#storage.set(["channel", instance.route.template, instance.uri], state);
			}
			this.#serverSeq = serverSeq;
			await this.#storage.set(SERVER_SEQ_KEY, serverSeq);
			this.#replay.append(envelope);
			this.#broadcast(envelope);
			return { envelope, state };
		});
		if (committed.envelope.rejectionReason === undefined && action?.effect !== undefined) {
			await action.effect({
				...this.#operations(
					instance.uri,
					connection === undefined ? undefined : {
						connectionId: connection.id,
						...(connection.principal === undefined ? {} : { principal: connection.principal }),
					},
				),
				uri: instance.uri,
				params: instance.params,
				connectionId: origin?.clientId,
				principal: connection?.principal,
				state: committed.state,
				payload,
				envelope: committed.envelope,
			});
		}
		return committed.envelope;
	}

	#broadcast(message: DurableChannelMessage): void {
		for (const link of this.#links.values()) {
			if (!link.subscriptions.has(message.channel)) {
				continue;
			}
			try {
				link.send(message);
			} catch {
				// A dead socket must not break a commit; the peer will resubscribe or reconnect.
				this.disconnect(link.connection);
			}
		}
	}

	#withLock<T>(task: () => Promise<T>): Promise<T> {
		const run = this.#lock.then(task, task);
		this.#lock = run.then(() => undefined, () => undefined);
		return run;
	}
}
