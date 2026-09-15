/**
 * The mirroring half of a durable channel: a client that runs the same reducers the hub runs.
 *
 * The client is handed the **same route map** the hub mounts, so every action it needs is already
 * there, schema and reducer included. That is what a mirror is: for each subscribed channel it keeps
 * the last state the server confirmed, the actions it has dispatched but not seen echoed, and the
 * optimistic state those pending actions produce on top of the confirmed one. A UI renders the
 * optimistic state and gets its own move on screen before the round trip; the confirmed state is the
 * truth, and the server always wins.
 *
 * Everything ambient stays out. Effects and background work are server-side, `env` never reaches a
 * reducer, and the client never retries, backs off or opens a socket by itself: it is given a
 * {@link DurableChannelTransport} and the application decides when to `connect`, when to `reconnect`
 * and when to give up. Only web-standard APIs are used, so the same client runs in a browser, in Deno
 * and inside a worker.
 */
import type { InferInput, InferOutput } from "valibot";
import * as v from "valibot";
import type {
	DurableChannelActionMap,
	DurableChannelCommandMap,
	DurableChannelEnvelope,
	DurableChannelReconnectResult,
	DurableChannelSnapshot,
} from "./channel.ts";
import {
	ChannelNotFoundError,
	ClientClosedError,
	type DurableChannelClientError,
	InvalidPayloadError,
	NotClientDispatchableError,
	RouteNotFoundError,
	RpcError,
	RpcTimeoutError,
	StatelessChannelError,
	TransportError,
	UnknownActionError,
} from "./error.ts";
import {
	type DurableChannelHelloResult,
	type DurableChannelRpcFrame,
	DurableChannelWireEnvelope,
	DurableChannelWireSnapshot,
	fromRpcNotification,
} from "./rpc.ts";
import {
	type DurableChannelRoute,
	type DurableChannelRouteMap,
	type DurableChannelRoutes,
	type DurableChannelRouteTypes,
	type MatchRoute,
	matchRoute,
	type PathToParams,
	resolveUri,
} from "./routes.ts";
import type { DurableChannelTransport } from "./transport.ts";

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_SUBSCRIPTION_BUFFER = 4096;

// ─── Type-level route lookup ─────────────────────────────────────────────────

type Matched<TRoutes extends DurableChannelRouteMap, TUri extends string> = Extract<MatchRoute<TRoutes, TUri>, DurableChannelRouteTypes>;

/** The state type the route of a URI literal declares, or `unknown` when nothing matches it. */
export type DurableChannelClientStateOf<TRoutes extends DurableChannelRouteMap, TUri extends string> = [Matched<TRoutes, TUri>] extends
	[never] ? unknown : Matched<TRoutes, TUri>["state"];

type ActionsOf<TRoutes extends DurableChannelRouteMap, TUri extends string> = [Matched<TRoutes, TUri>] extends [never]
	? DurableChannelActionMap
	: Matched<TRoutes, TUri>["actions"];

type CommandsOf<TRoutes extends DurableChannelRouteMap, TUri extends string> = [Matched<TRoutes, TUri>] extends [never]
	? DurableChannelCommandMap
	: Matched<TRoutes, TUri>["commands"];

// ─── Events and outcomes ─────────────────────────────────────────────────────

/**
 * What a subscription streams. `state` fires after every local change to a channel's optimistic state —
 * a snapshot, a confirmed echo, an optimistic apply and a rollback alike — so a view can redraw from
 * one event type and ignore the rest.
 */
export type DurableChannelClientEvent =
	| { readonly type: "action"; readonly envelope: DurableChannelEnvelope }
	| { readonly type: "notification"; readonly channel: string; readonly name: string; readonly payload: unknown }
	| { readonly type: "state"; readonly channel: string; readonly state: unknown };

/** One event of {@link DurableChannelClient.events}, tagged with the channel it belongs to. */
export interface DurableChannelClientEventRecord {
	readonly channel: string;
	readonly event: DurableChannelClientEvent;
}

/** How a dispatched action ended. */
export type DurableChannelDispatchOutcome =
	| { readonly status: "confirmed"; readonly envelope: DurableChannelEnvelope }
	| { readonly status: "rejected"; readonly envelope: DurableChannelEnvelope; readonly reason: string }
	| { readonly status: "duplicate" }
	| { readonly status: "lost"; readonly error?: DurableChannelClientError };

/** What {@link DurableChannelClient.dispatch} hands back, before the hub has said anything. */
export interface DurableChannelDispatchHandle {
	/** The sequence number this client gave the action. The hub dedupes on it, per link. */
	readonly clientSeq: number;
	/** Resolves once the outcome is known. Never rejects. */
	readonly settled: Promise<DurableChannelDispatchOutcome>;
}

/** One action applied optimistically and still waiting for its echo. */
export interface DurableChannelPendingAction {
	readonly clientSeq: number;
	readonly name: string;
	readonly payload: unknown;
}

/** Where the client's link stands. */
export type DurableChannelConnectionState =
	| { readonly status: "idle" }
	| { readonly status: "connected" }
	| { readonly status: "closing" }
	| { readonly status: "closed"; readonly reason: DurableChannelClosedReason };

/** Why a link ended. */
export type DurableChannelClosedReason =
	| { readonly type: "shutdown" }
	| { readonly type: "transport"; readonly error: TransportError };

/** What {@link DurableChannelClient.subscribe} hands back. */
export interface DurableChannelSubscribeHandle {
	/** The snapshot the hub sent, or `undefined` for a stateless channel. */
	readonly snapshot: DurableChannelSnapshot | undefined;
	readonly subscription: DurableChannelSubscription;
}

/** What a client needs beyond a route map and a transport. */
export interface DurableChannelClientOptions {
	/** The identity the hub dedupes and reconnects by. Defaults to a fresh `crypto.randomUUID()`. */
	readonly clientId?: string;
	/** How long a request waits before {@link RpcTimeoutError}. Default 30 000 ms; `0` disables it. */
	readonly requestTimeoutMs?: number;
	/** How many events one subscription buffers before the oldest are dropped. Default 4096. */
	readonly subscriptionBuffer?: number;
}

// ─── Fan-out ─────────────────────────────────────────────────────────────────

interface Cursor<T> {
	position: number;
	waiter: ((result: IteratorResult<T>) => void) | undefined;
	detached: boolean;
}

/**
 * One publisher, many readers, each with its own position. A reader created after a value was published
 * never sees it, and a reader that lags by more than `limit` values skips the gap rather than holding
 * the buffer open forever.
 */
class BroadcastQueue<T> {
	#buffer: T[] = [];
	#base = 0;
	#cursors = new Set<Cursor<T>>();
	#closed = false;
	#limit: number;

	constructor(limit: number) {
		this.#limit = Math.max(1, Math.floor(limit));
	}

	publish(value: T): void {
		if (this.#closed) {
			return;
		}
		this.#buffer.push(value);
		if (this.#buffer.length > this.#limit) {
			const dropped = this.#buffer.length - this.#limit;
			this.#buffer.splice(0, dropped);
			this.#base += dropped;
			for (const cursor of this.#cursors) {
				cursor.position = Math.max(cursor.position, this.#base);
			}
		}
		const last = this.#base + this.#buffer.length - 1;
		for (const cursor of this.#cursors) {
			const waiter = cursor.waiter;
			if (waiter === undefined || cursor.position > last) {
				continue;
			}
			const item = this.#buffer[cursor.position - this.#base];
			cursor.position += 1;
			cursor.waiter = undefined;
			waiter({ value: item, done: false });
		}
		this.#trim();
	}

	close(): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		for (const cursor of this.#cursors) {
			const waiter = cursor.waiter;
			if (waiter === undefined || cursor.position < this.#base + this.#buffer.length) {
				continue;
			}
			cursor.waiter = undefined;
			waiter({ value: undefined, done: true });
		}
	}

	reader(): AsyncIterableIterator<T> {
		const cursor: Cursor<T> = { position: this.#base + this.#buffer.length, waiter: undefined, detached: this.#closed };
		if (!this.#closed) {
			this.#cursors.add(cursor);
		}
		const iterator: AsyncIterableIterator<T> = {
			[Symbol.asyncIterator]: () => iterator,
			next: () => {
				if (cursor.detached) {
					return Promise.resolve({ value: undefined, done: true } as IteratorResult<T>);
				}
				const index = cursor.position - this.#base;
				if (index >= 0 && index < this.#buffer.length) {
					const item = this.#buffer[index];
					cursor.position += 1;
					this.#trim();
					return Promise.resolve({ value: item, done: false });
				}
				if (this.#closed) {
					return Promise.resolve({ value: undefined, done: true } as IteratorResult<T>);
				}
				return new Promise<IteratorResult<T>>((resolve) => {
					cursor.waiter = resolve;
				});
			},
			return: () => {
				if (!cursor.detached) {
					cursor.detached = true;
					const waiter = cursor.waiter;
					cursor.waiter = undefined;
					this.#cursors.delete(cursor);
					this.#trim();
					// A `for await` that is parked in `next()` has to be let go, or it never ends.
					waiter?.({ value: undefined, done: true } as IteratorResult<T>);
				}
				return Promise.resolve({ value: undefined, done: true } as IteratorResult<T>);
			},
		};
		return iterator;
	}

	#trim(): void {
		if (this.#cursors.size === 0) {
			this.#base += this.#buffer.length;
			this.#buffer = [];
			return;
		}
		let lowest = Number.POSITIVE_INFINITY;
		for (const cursor of this.#cursors) {
			lowest = Math.min(lowest, cursor.position);
		}
		const drop = lowest - this.#base;
		if (drop > 0) {
			this.#buffer.splice(0, drop);
			this.#base += drop;
		}
	}
}

/**
 * One consumer's view of a channel's events. Closing it ends this iterator only: the client stays
 * subscribed until {@link DurableChannelClient.unsubscribe} is called for the URI.
 */
export class DurableChannelSubscription implements AsyncIterableIterator<DurableChannelClientEvent> {
	readonly uri: string;
	#inner: AsyncIterableIterator<DurableChannelClientEvent>;

	constructor(uri: string, inner: AsyncIterableIterator<DurableChannelClientEvent>) {
		this.uri = uri;
		this.#inner = inner;
	}

	next(): Promise<IteratorResult<DurableChannelClientEvent>> {
		return this.#inner.next();
	}

	return(): Promise<IteratorResult<DurableChannelClientEvent>> {
		return this.#inner.return?.() ?? Promise.resolve({ value: undefined, done: true });
	}

	[Symbol.asyncIterator](): this {
		return this;
	}

	/** Ends this iterator. Does not unsubscribe. */
	async close(): Promise<void> {
		await this.return();
	}
}

// ─── Internals ───────────────────────────────────────────────────────────────

interface PendingEntry {
	readonly clientSeq: number;
	readonly name: string;
	readonly payload: unknown;
	settled: boolean;
	settle(outcome: DurableChannelDispatchOutcome): void;
}

interface Mirror<TEnv> {
	readonly uri: string;
	readonly route: DurableChannelRoute<TEnv>;
	readonly params: Readonly<Record<string, string>>;
	readonly stateful: boolean;
	readonly events: BroadcastQueue<DurableChannelClientEvent>;
	confirmed: unknown;
	optimistic: unknown;
	pending: PendingEntry[];
	/** The highest `serverSeq` applied to this channel. A snapshot sets it to its `fromSeq`. */
	appliedSeq: number;
	/**
	 * `false` while a `hello`, `subscribe` or `reconnect` that will restate this channel is in flight.
	 * Envelopes that arrive meanwhile go to `buffered` and are replayed once the answer has landed.
	 */
	primed: boolean;
	buffered: DurableChannelEnvelope[];
}

interface PendingRequest {
	readonly method: string;
	resolve(value: unknown): void;
	reject(error: DurableChannelClientError): void;
	timer: ReturnType<typeof setTimeout> | undefined;
}

const HelloResultSchema = v.object({ serverSeq: v.number(), snapshots: v.array(DurableChannelWireSnapshot) });
const SubscribeResultSchema = v.object({ snapshot: v.optional(DurableChannelWireSnapshot) });
const ReconnectResultSchema = v.variant("type", [
	v.object({ type: v.literal("replay"), actions: v.array(DurableChannelWireEnvelope), missing: v.array(v.string()) }),
	v.object({ type: v.literal("snapshot"), snapshots: v.array(DurableChannelWireSnapshot), missing: v.array(v.string()) }),
]);

function asTransportError(error: unknown): TransportError {
	return error instanceof TransportError ? error : new TransportError("io", "The transport failed", { cause: error });
}

// ─── Client ──────────────────────────────────────────────────────────────────

/**
 * A mirror of the channels a hub serves, typed by the same route map the hub mounts.
 *
 * `connect()` starts the receive loop; `hello()` binds the client id and takes the first snapshots.
 * From there `dispatch` applies an action locally and sends it, `state(uri)` is what to render,
 * `confirmed(uri)` is what the server last agreed to, and `reconnect(transport)` resumes on a fresh
 * transport from the sequence the client last saw.
 */
export class DurableChannelClient<TEnv = unknown, TRoutes extends DurableChannelRouteMap = DurableChannelRouteMap> {
	#routes: DurableChannelRoutes<TEnv, TRoutes>;
	#transport: DurableChannelTransport;
	#clientId: string;
	#requestTimeoutMs: number;
	#subscriptionBuffer: number;
	#mirrors = new Map<string, Mirror<TEnv>>();
	#subscribed = new Set<string>();
	#requests = new Map<number, PendingRequest>();
	#events: BroadcastQueue<DurableChannelClientEventRecord>;
	#states: BroadcastQueue<DurableChannelConnectionState>;
	#state: DurableChannelConnectionState = { status: "idle" };
	#nextRequestId = 1;
	#nextClientSeq = 1;
	#lastSeenServerSeq = 0;
	#loop: Promise<void> | undefined;
	#done = false;

	constructor(
		routes: DurableChannelRoutes<TEnv, TRoutes>,
		transport: DurableChannelTransport,
		options?: DurableChannelClientOptions,
	) {
		this.#routes = routes;
		this.#transport = transport;
		this.#clientId = options?.clientId ?? crypto.randomUUID();
		const timeout = options?.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		this.#requestTimeoutMs = timeout > 0 ? timeout : 0;
		this.#subscriptionBuffer = options?.subscriptionBuffer ?? DEFAULT_SUBSCRIPTION_BUFFER;
		this.#events = new BroadcastQueue(this.#subscriptionBuffer);
		this.#states = new BroadcastQueue(this.#subscriptionBuffer);
	}

	/** The identity the hub knows this client by, across every transport it reconnects on. */
	get clientId(): string {
		return this.#clientId;
	}

	/** Where the link stands. A transport failure leaves it `closed` until a `reconnect`. */
	get connectionState(): DurableChannelConnectionState {
		return this.#state;
	}

	/** The highest `serverSeq` this client has applied, which is what a `reconnect` resumes from. */
	get lastSeenServerSeq(): number {
		return this.#lastSeenServerSeq;
	}

	/** The URIs the client believes it is subscribed to, in subscription order. */
	get subscriptions(): readonly string[] {
		return [...this.#subscribed];
	}

	/** A fresh reader of the link's state transitions. */
	stateChanges(): AsyncIterableIterator<DurableChannelConnectionState> {
		return this.#states.reader();
	}

	/** A fresh reader of every inbound event, whatever channel it belongs to. */
	events(): AsyncIterableIterator<DurableChannelClientEventRecord> {
		return this.#events.reader();
	}

	/** Starts the receive loop over the current transport. Idempotent. */
	connect(): void {
		if (this.#loop !== undefined) {
			return;
		}
		this.#assertOpen();
		this.#setState({ status: "connected" });
		this.#loop = this.#drive(this.#transport);
	}

	/**
	 * Closes the transport, fails every request in flight with {@link ClientClosedError}, settles every
	 * pending action `lost` and ends every subscription and event stream. A shut-down client is done:
	 * `reconnect` will not revive it.
	 */
	async shutdown(): Promise<void> {
		if (this.#done) {
			return;
		}
		this.#setState({ status: "closing" });
		this.#tearDown({ type: "shutdown" });
		try {
			await this.#transport.close();
		} catch {
			// Closing a transport is best effort: it is already going away.
		}
		if (this.#loop !== undefined) {
			try {
				await this.#loop;
			} catch {
				// Whatever the loop saw was already reported through the connection state.
			}
		}
	}

	/**
	 * The handshake: binds the client id on the hub's side and subscribes the URIs given. Every snapshot
	 * that comes back is applied, and a URI the hub refuses fails the whole call — the client would
	 * otherwise carry a subscription set the hub does not share.
	 */
	async hello(args?: { readonly subscriptions?: readonly string[] }): Promise<DurableChannelHelloResult> {
		this.#assertOpen();
		const subscriptions = [...(args?.subscriptions ?? [])];
		const created: string[] = [];
		for (const uri of subscriptions) {
			const existing = this.#mirrors.get(uri);
			if (existing === undefined) {
				this.#mirror(uri).primed = false;
				created.push(uri);
				continue;
			}
			existing.primed = false;
			existing.buffered = [];
		}
		try {
			const result = v.parse(HelloResultSchema, await this.#request("hello", { clientId: this.#clientId, subscriptions }));
			this.#see(result.serverSeq);
			for (const uri of subscriptions) {
				this.#subscribed.add(uri);
			}
			for (const snapshot of result.snapshots) {
				this.#applySnapshot(snapshot);
			}
			for (const uri of subscriptions) {
				this.#primeWithoutSnapshot(uri);
			}
			return result;
		} catch (error) {
			for (const uri of created) {
				this.#forget(uri);
			}
			throw error;
		}
	}

	/** Liveness. Works before `hello`, so it is also how a caller checks a link it did not open. */
	async ping(): Promise<void> {
		await this.#request("ping", {});
	}

	/**
	 * Resumes the client, on `transport` when one is given and on the current one otherwise.
	 *
	 * A `replay` answer is applied envelope by envelope: the confirmed states stand, pending actions
	 * re-reconcile against their own echoes, and whatever is still pending afterwards was never
	 * committed and settles `lost`. A `snapshot` answer replaces the confirmed states outright and every
	 * pending action settles `lost`, because the hub can no longer say what became of it. A URI in
	 * `missing` — an unknown route, an internal one, a destroyed instance — is dropped locally and its
	 * subscription iterators end, which is how a peer learns to forget it.
	 */
	async reconnect(transport?: DurableChannelTransport): Promise<DurableChannelReconnectResult> {
		if (this.#done) {
			throw new ClientClosedError();
		}
		if (transport !== undefined) {
			this.#attach(transport);
		}
		this.#assertOpen();
		for (const mirror of this.#mirrors.values()) {
			mirror.primed = false;
			mirror.buffered = [];
		}
		const subscriptions = [...this.#subscribed];
		const result = v.parse(
			ReconnectResultSchema,
			await this.#request("reconnect", { clientId: this.#clientId, lastSeenServerSeq: this.#lastSeenServerSeq, subscriptions }),
		);
		for (const uri of result.missing) {
			this.#subscribed.delete(uri);
			this.#forget(uri);
		}
		if (result.type === "replay") {
			for (const mirror of this.#mirrors.values()) {
				mirror.primed = true;
			}
			for (const envelope of result.actions) {
				this.#inbound(envelope);
			}
			for (const mirror of this.#mirrors.values()) {
				this.#drain(mirror);
				this.#lose(mirror);
				this.#restate(mirror);
			}
			return result;
		}
		const resumed = new Set(result.snapshots.map((snapshot) => snapshot.resource));
		for (const snapshot of result.snapshots) {
			const mirror = this.#mirrors.get(snapshot.resource);
			if (mirror !== undefined) {
				this.#lose(mirror);
				this.#applySnapshot(snapshot);
			}
		}
		for (const uri of [...this.#mirrors.keys()]) {
			if (resumed.has(uri)) {
				continue;
			}
			const mirror = this.#mirrors.get(uri);
			if (mirror !== undefined && !mirror.stateful) {
				this.#primeWithoutSnapshot(uri);
				continue;
			}
			this.#subscribed.delete(uri);
			this.#forget(uri);
		}
		return result;
	}

	/**
	 * Subscribes to a channel and hands back its snapshot with a fresh iterator. The iterator is
	 * attached before the request goes out, so nothing that arrives during the round trip is missed.
	 */
	async subscribe(uri: string): Promise<DurableChannelSubscribeHandle> {
		this.#assertOpen();
		const known = this.#mirrors.has(uri);
		const mirror = this.#mirrors.get(uri) ?? this.#mirror(uri);
		if (!known) {
			mirror.primed = false;
		}
		const subscription = new DurableChannelSubscription(uri, mirror.events.reader());
		try {
			const result = v.parse(SubscribeResultSchema, await this.#request("subscribe", { channel: uri }));
			this.#subscribed.add(uri);
			if (result.snapshot !== undefined) {
				this.#applySnapshot(result.snapshot);
			} else {
				this.#primeWithoutSnapshot(uri);
			}
			return { snapshot: result.snapshot, subscription };
		} catch (error) {
			await subscription.close();
			if (!known) {
				this.#forget(uri);
			}
			throw error;
		}
	}

	/**
	 * Another iterator over a channel the client is already subscribed to. Throws
	 * {@link ChannelNotFoundError} for a URI it knows nothing about: `subscribe` or `hello` first.
	 */
	attachSubscription(uri: string): DurableChannelSubscription {
		this.#assertOpen();
		const mirror = this.#mirrors.get(uri);
		if (mirror === undefined) {
			throw new ChannelNotFoundError(uri);
		}
		return new DurableChannelSubscription(uri, mirror.events.reader());
	}

	/** Drops a subscription: tells the hub, forgets the mirror, ends the iterators, loses the pending. */
	unsubscribe(uri: string): Promise<void> {
		if (this.#state.status !== "connected") {
			this.#subscribed.delete(uri);
			this.#forget(uri);
			return Promise.resolve();
		}
		this.#subscribed.delete(uri);
		this.#notify("unsubscribe", { channel: uri });
		this.#forget(uri);
		return Promise.resolve();
	}

	/**
	 * Applies an action locally and sends it.
	 *
	 * Everything the route map can decide is decided here, before a frame leaves: an unknown action, an
	 * action the definition did not mark `.client()` and a payload that fails its schema all throw the
	 * same `DurableChannelError` the hub would have raised. A reducer that refuses the action with
	 * `RejectAction` does **not**: the action still goes out, because the server owns that decision, and
	 * the optimistic state simply ignores it.
	 */
	dispatch<TUri extends string, TName extends keyof ActionsOf<TRoutes, TUri> & string>(
		uri: TUri,
		name: TName,
		payload: InferInput<ActionsOf<TRoutes, TUri>[TName]>,
	): DurableChannelDispatchHandle;
	dispatch(uri: string, name: string, payload: unknown): DurableChannelDispatchHandle;
	dispatch(uri: string, name: string, payload: unknown): DurableChannelDispatchHandle {
		this.#assertOpen();
		const match = matchRoute(this.#routes, uri);
		if (match === undefined) {
			throw new RouteNotFoundError(uri);
		}
		const definition = match.route.definition;
		if (definition.state === undefined) {
			throw new StatelessChannelError(uri, "dispatch");
		}
		if (!Object.hasOwn(definition.actions, name)) {
			throw new UnknownActionError(uri, name);
		}
		const action = definition.actions[name];
		if (!action.client) {
			throw new NotClientDispatchableError(uri, name);
		}
		if (!v.safeParse(action.payload, payload).success) {
			throw new InvalidPayloadError(uri, name);
		}
		const clientSeq = this.#nextClientSeq;
		this.#nextClientSeq += 1;
		let resolve: (outcome: DurableChannelDispatchOutcome) => void = () => {};
		const settled = new Promise<DurableChannelDispatchOutcome>((next) => {
			resolve = next;
		});
		const entry: PendingEntry = {
			clientSeq,
			name,
			payload,
			settled: false,
			settle(outcome) {
				if (entry.settled) {
					return;
				}
				entry.settled = true;
				resolve(outcome);
			},
		};
		const mirror = this.#mirrors.get(uri);
		if (mirror !== undefined) {
			mirror.pending.push(entry);
			this.#restate(mirror);
		}
		this.#request("dispatch", { channel: uri, clientSeq, name, payload }).then(
			(result) => this.#answered(uri, entry, result),
			(error: DurableChannelClientError) => {
				if (error instanceof TransportError) {
					// The hub may well have committed it. A reconnect's replay is what settles it.
					return;
				}
				this.#discard(uri, entry, { status: "lost", error });
			},
		);
		return { clientSeq, settled };
	}

	/** Runs a command on the hub. Params and result are validated there, and typed here. */
	exec<TUri extends string, TName extends keyof CommandsOf<TRoutes, TUri> & string>(
		uri: TUri,
		name: TName,
		params: InferInput<CommandsOf<TRoutes, TUri>[TName]["params"]>,
	): Promise<InferOutput<CommandsOf<TRoutes, TUri>[TName]["result"]>>;
	exec(uri: string, name: string, params: unknown): Promise<unknown>;
	async exec(uri: string, name: string, params: unknown): Promise<unknown> {
		return await this.#request("exec", { channel: uri, name, params });
	}

	/** What to render: the confirmed state with every pending action replayed on top of it. */
	state<TUri extends string>(uri: TUri): DurableChannelClientStateOf<TRoutes, TUri>;
	state(uri: string): unknown {
		return this.#stateful(uri).optimistic;
	}

	/** What the hub last agreed to, with no pending action on top. */
	confirmed<TUri extends string>(uri: TUri): DurableChannelClientStateOf<TRoutes, TUri>;
	confirmed(uri: string): unknown {
		return this.#stateful(uri).confirmed;
	}

	/** The actions dispatched on this channel and not yet echoed, oldest first. */
	pending(uri: string): readonly DurableChannelPendingAction[] {
		return this.#stateful(uri).pending.map(({ clientSeq, name, payload }) => ({ clientSeq, name, payload }));
	}

	/** A view of one template, so a family's instances are addressed by their parameters. */
	of<TTemplate extends keyof TRoutes & string>(template: TTemplate): DurableChannelClientHandle<TRoutes, TTemplate> {
		const uri = (params: PathToParams<TTemplate>): string => resolveUri(template, params as Record<string, string>);
		return {
			uri,
			state: (params) => this.state(uri(params)) as never,
			confirmed: (params) => this.confirmed(uri(params)) as never,
			pending: (params) => this.pending(uri(params)),
			subscribe: (params) => this.subscribe(uri(params)),
			unsubscribe: (params) => this.unsubscribe(uri(params)),
			attachSubscription: (params) => this.attachSubscription(uri(params)),
			dispatch: (params, name, payload) => this.dispatch(uri(params), name, payload),
			exec: (params, name, commandParams) => this.exec(uri(params), name, commandParams) as never,
		};
	}

	// ─── State ─────────────────────────────────────────────────────────────────

	#stateful(uri: string): Mirror<TEnv> {
		const mirror = this.#mirrors.get(uri);
		if (mirror === undefined) {
			throw new ChannelNotFoundError(uri);
		}
		if (!mirror.stateful) {
			throw new StatelessChannelError(uri, "state");
		}
		return mirror;
	}

	#mirror(uri: string): Mirror<TEnv> {
		const match = matchRoute(this.#routes, uri);
		if (match === undefined) {
			throw new RouteNotFoundError(uri);
		}
		const definition = match.route.definition;
		const mirror: Mirror<TEnv> = {
			uri,
			route: match.route,
			params: match.params,
			stateful: definition.state !== undefined,
			events: new BroadcastQueue(this.#subscriptionBuffer),
			confirmed: definition.initialState,
			optimistic: definition.initialState,
			pending: [],
			appliedSeq: 0,
			primed: true,
			buffered: [],
		};
		this.#mirrors.set(uri, mirror);
		return mirror;
	}

	#forget(uri: string): void {
		const mirror = this.#mirrors.get(uri);
		if (mirror === undefined) {
			return;
		}
		this.#mirrors.delete(uri);
		this.#lose(mirror);
		mirror.events.close();
	}

	/** Settles every action still pending on a channel as `lost` and drops it from the optimistic state. */
	#lose(mirror: Mirror<TEnv>): void {
		const pending = mirror.pending.splice(0);
		for (const entry of pending) {
			entry.settle({ status: "lost" });
		}
	}

	#see(serverSeq: number): void {
		this.#lastSeenServerSeq = Math.max(this.#lastSeenServerSeq, serverSeq);
	}

	/** Runs one action's reducer, or hands the state back untouched when it cannot. */
	#reduce(mirror: Mirror<TEnv>, state: unknown, name: string, payload: unknown): unknown {
		const actions = mirror.route.definition.actions;
		if (!Object.hasOwn(actions, name)) {
			return state;
		}
		const action = actions[name];
		const parsed = v.safeParse(action.payload, payload);
		if (!parsed.success) {
			return state;
		}
		try {
			return action.reduce(state, parsed.output, { uri: mirror.uri, params: mirror.params });
		} catch {
			// A refusal, or a reducer that disagrees with the server's: the confirmed state is the truth.
			return state;
		}
	}

	/**
	 * Recomputes the optimistic state and announces it, but only when something really moved — unless
	 * `force`, which a snapshot needs because it restates the channel whether the value differs or not.
	 */
	#restate(mirror: Mirror<TEnv>, force = false): void {
		if (!mirror.stateful) {
			return;
		}
		let optimistic = mirror.confirmed;
		for (const entry of mirror.pending) {
			optimistic = this.#reduce(mirror, optimistic, entry.name, entry.payload);
		}
		if (!force && optimistic === mirror.optimistic) {
			return;
		}
		mirror.optimistic = optimistic;
		this.#publish(mirror.uri, { type: "state", channel: mirror.uri, state: optimistic });
	}

	#applySnapshot(snapshot: DurableChannelSnapshot): void {
		const mirror = this.#mirrors.get(snapshot.resource);
		if (mirror === undefined) {
			return;
		}
		const schema = mirror.route.definition.state;
		const parsed = schema === undefined ? undefined : v.safeParse(schema, snapshot.state);
		mirror.confirmed = parsed?.success === true ? parsed.output : snapshot.state;
		mirror.appliedSeq = snapshot.fromSeq;
		mirror.primed = true;
		this.#see(snapshot.fromSeq);
		this.#drain(mirror);
		this.#restate(mirror, true);
	}

	/** Primes a channel that will get no snapshot — a stateless route, or one already up to date. */
	#primeWithoutSnapshot(uri: string): void {
		const mirror = this.#mirrors.get(uri);
		if (mirror === undefined || mirror.primed) {
			return;
		}
		mirror.primed = true;
		this.#drain(mirror);
		this.#restate(mirror);
	}

	#drain(mirror: Mirror<TEnv>): void {
		const buffered = mirror.buffered.splice(0);
		for (const envelope of buffered) {
			this.#reconcile(mirror, envelope);
		}
	}

	// ─── Reconciliation ────────────────────────────────────────────────────────

	#inbound(envelope: DurableChannelEnvelope): void {
		const mirror = this.#mirrors.get(envelope.channel);
		if (mirror === undefined) {
			this.#publish(envelope.channel, { type: "action", envelope });
			return;
		}
		if (!mirror.primed) {
			mirror.buffered.push(envelope);
			return;
		}
		this.#reconcile(mirror, envelope);
	}

	#reconcile(mirror: Mirror<TEnv>, envelope: DurableChannelEnvelope): void {
		const own = envelope.origin?.clientId === this.#clientId ? envelope.origin : undefined;
		if (own !== undefined) {
			const settled: PendingEntry[] = [];
			mirror.pending = mirror.pending.filter((entry) => {
				if (entry.clientSeq > own.clientSeq) {
					return true;
				}
				settled.push(entry);
				return false;
			});
			for (const entry of settled) {
				entry.settle(entry.clientSeq === own.clientSeq ? outcomeOf(envelope) : { status: "lost" });
			}
		}
		if (envelope.serverSeq <= mirror.appliedSeq) {
			this.#restate(mirror);
			return;
		}
		mirror.appliedSeq = envelope.serverSeq;
		this.#see(envelope.serverSeq);
		this.#publish(mirror.uri, { type: "action", envelope });
		if (envelope.rejectionReason === undefined) {
			mirror.confirmed = this.#reduce(mirror, mirror.confirmed, envelope.name, envelope.payload);
		}
		this.#restate(mirror);
	}

	/** Settles a dispatch from the direct answer, which is the only path when the URI is not mirrored. */
	#answered(uri: string, entry: PendingEntry, result: unknown): void {
		if (result === null) {
			this.#discard(uri, entry, { status: "duplicate" });
			return;
		}
		const parsed = v.safeParse(DurableChannelWireEnvelope, result);
		if (!parsed.success) {
			this.#discard(uri, entry, { status: "lost" });
			return;
		}
		this.#inbound(parsed.output);
		if (!entry.settled) {
			this.#discard(uri, entry, outcomeOf(parsed.output));
		}
	}

	#discard(uri: string, entry: PendingEntry, outcome: DurableChannelDispatchOutcome): void {
		const mirror = this.#mirrors.get(uri);
		if (mirror !== undefined) {
			mirror.pending = mirror.pending.filter((candidate) => candidate !== entry);
			entry.settle(outcome);
			this.#restate(mirror);
			return;
		}
		entry.settle(outcome);
	}

	#publish(channel: string, event: DurableChannelClientEvent): void {
		this.#mirrors.get(channel)?.events.publish(event);
		this.#events.publish({ channel, event });
	}

	// ─── Link ──────────────────────────────────────────────────────────────────

	#assertOpen(): void {
		if (this.#done || this.#state.status === "closing") {
			throw new ClientClosedError();
		}
		if (this.#state.status === "closed") {
			throw new ClientClosedError("The transport is closed: reconnect on a new one");
		}
	}

	#setState(next: DurableChannelConnectionState): void {
		this.#state = next;
		this.#states.publish(next);
	}

	#attach(transport: DurableChannelTransport): void {
		this.#failRequests(new TransportError("closed", "The transport was replaced"));
		this.#transport = transport;
		this.#loop = undefined;
		this.#setState({ status: "connected" });
		this.#loop = this.#drive(transport);
	}

	async #drive(transport: DurableChannelTransport): Promise<void> {
		try {
			while (this.#transport === transport && this.#state.status === "connected") {
				const frame = await transport.recv();
				if (frame === null) {
					if (this.#transport === transport) {
						this.#tearDown({ type: "transport", error: new TransportError("closed", "The transport closed") });
					}
					return;
				}
				this.#frame(frame as unknown as Record<string, unknown>);
			}
		} catch (error) {
			if (this.#transport !== transport) {
				return;
			}
			this.#tearDown({ type: "transport", error: asTransportError(error) });
		}
	}

	#frame(frame: Record<string, unknown>): void {
		const id = frame.id;
		if (typeof frame.method === "string") {
			if (typeof id === "number" || typeof id === "string") {
				// A server-initiated request. Nothing here answers one, and leaving it pending on the
				// server would leak: refuse it the way JSON-RPC says to.
				void this.#send({ jsonrpc: "2.0", id, error: { code: -32601, message: "Method not found" } }).catch(() => {});
				return;
			}
			const message = fromRpcNotification(frame.method, frame.params);
			if (message === undefined) {
				return;
			}
			if (message.type === "action") {
				this.#inbound(message);
				return;
			}
			this.#publish(message.channel, {
				type: "notification",
				channel: message.channel,
				name: message.name,
				payload: message.payload,
			});
			return;
		}
		if (typeof id !== "number") {
			return;
		}
		const request = this.#requests.get(id);
		if (request === undefined) {
			return;
		}
		this.#requests.delete(id);
		clearTimeout(request.timer);
		if ("error" in frame) {
			const error = frame.error as { code?: unknown; message?: unknown; data?: unknown };
			request.reject(
				new RpcError(
					typeof error.code === "number" ? error.code : -32603,
					typeof error.message === "string" ? error.message : "Unknown error",
					error.data,
				),
			);
			return;
		}
		request.resolve(frame.result);
	}

	#request(method: string, params: Record<string, unknown>): Promise<unknown> {
		this.#assertOpen();
		const id = this.#nextRequestId;
		this.#nextRequestId += 1;
		return new Promise<unknown>((resolve, reject) => {
			const request: PendingRequest = { method, resolve, reject, timer: undefined };
			this.#requests.set(id, request);
			if (this.#requestTimeoutMs > 0) {
				request.timer = setTimeout(() => {
					if (this.#requests.delete(id)) {
						reject(new RpcTimeoutError(method, this.#requestTimeoutMs));
					}
				}, this.#requestTimeoutMs);
			}
			void this.#send({ jsonrpc: "2.0", id, method, params }).catch((error: DurableChannelClientError) => {
				if (this.#requests.delete(id)) {
					clearTimeout(request.timer);
					reject(error);
				}
			});
		});
	}

	#notify(method: string, params: Record<string, unknown>): void {
		void this.#send({ jsonrpc: "2.0", method, params }).catch(() => {
			// A notification has no answer to fail: the connection state already says what happened.
		});
	}

	async #send(frame: DurableChannelRpcFrame): Promise<void> {
		try {
			await this.#transport.send(frame);
		} catch (error) {
			const failure = asTransportError(error);
			this.#tearDown({ type: "transport", error: failure });
			throw failure;
		}
	}

	#failRequests(error: DurableChannelClientError): void {
		const requests = [...this.#requests.values()];
		this.#requests.clear();
		for (const request of requests) {
			clearTimeout(request.timer);
			request.reject(error);
		}
	}

	/**
	 * A transport death leaves the mirrors, the subscriptions and the pending actions in place, so a
	 * `reconnect` on a fresh transport can resume from them. A shutdown ends everything.
	 */
	#tearDown(reason: DurableChannelClosedReason): void {
		if (this.#done) {
			return;
		}
		this.#setState({ status: "closed", reason });
		if (reason.type === "transport") {
			this.#failRequests(reason.error);
			return;
		}
		this.#done = true;
		this.#failRequests(new ClientClosedError());
		for (const mirror of this.#mirrors.values()) {
			this.#lose(mirror);
			mirror.events.close();
		}
		this.#mirrors.clear();
		this.#subscribed.clear();
		this.#events.close();
		this.#states.close();
	}
}

function outcomeOf(envelope: DurableChannelEnvelope): DurableChannelDispatchOutcome {
	return envelope.rejectionReason === undefined
		? { status: "confirmed", envelope }
		: { status: "rejected", envelope, reason: envelope.rejectionReason };
}

/** A client's view of one template, the counterpart of the hub's own handle. */
export interface DurableChannelClientHandle<TRoutes extends DurableChannelRouteMap, TTemplate extends keyof TRoutes & string> {
	uri(params: PathToParams<TTemplate>): string;
	state(params: PathToParams<TTemplate>): TRoutes[TTemplate]["state"];
	confirmed(params: PathToParams<TTemplate>): TRoutes[TTemplate]["state"];
	pending(params: PathToParams<TTemplate>): readonly DurableChannelPendingAction[];
	subscribe(params: PathToParams<TTemplate>): Promise<DurableChannelSubscribeHandle>;
	unsubscribe(params: PathToParams<TTemplate>): Promise<void>;
	attachSubscription(params: PathToParams<TTemplate>): DurableChannelSubscription;
	dispatch<TName extends keyof TRoutes[TTemplate]["actions"] & string>(
		params: PathToParams<TTemplate>,
		name: TName,
		payload: InferInput<TRoutes[TTemplate]["actions"][TName]>,
	): DurableChannelDispatchHandle;
	exec<TName extends keyof TRoutes[TTemplate]["commands"] & string>(
		params: PathToParams<TTemplate>,
		name: TName,
		commandParams: InferInput<TRoutes[TTemplate]["commands"][TName]["params"]>,
	): Promise<InferOutput<TRoutes[TTemplate]["commands"][TName]["result"]>>;
}
