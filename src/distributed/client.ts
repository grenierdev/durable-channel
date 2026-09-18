import { type InferInput, type InferOutput, safeParse } from "valibot";
import type { DurableChannel, DurableChannelNotification } from "../channel.ts";
import {
	ClientClosedError,
	InvalidPayloadError,
	NotClientDispatchableError,
	RouteNotFoundError,
	RpcError,
	RpcTimeoutError,
	TransportError,
	UnknownActionError,
} from "../error.ts";
import {
	type DurableChannelRouteMap,
	type DurableChannelRoutes,
	type MatchRoute,
	matchRoute,
	type PathToParams,
	resolveUri,
} from "../routes.ts";
import type { DurableChannelTransport } from "../transport.ts";
import type { DistributedClientScheduler, DistributedClock } from "./interfaces.ts";
import { distributedClientScheduler, systemClock } from "./interfaces.ts";
import {
	type ChannelCursor,
	DISTRIBUTED_PROTOCOL,
	type DistributedDispatchRequest,
	type DistributedDispatchResult,
	DistributedDispatchResultSchema,
	type DistributedEnvelope,
	DistributedHelloResultSchema,
	type DistributedMessage,
	type DistributedReconnectResult,
	type DistributedResumeEntry,
	DistributedResumeSchema,
} from "./protocol.ts";
import { distributedFrameMessage } from "./rpc.ts";
import { cloneRecord, createDistributedActionId, distributedError } from "./store.ts";

export type DistributedDispatchOutcome = { status: "confirmed"; envelope: DistributedEnvelope } | {
	status: "rejected";
	envelope: DistributedEnvelope;
	reason: string;
} | { status: "unknown"; actionId: string };
export interface DistributedDispatchHandle {
	readonly actionId: string;
	readonly generation: string;
	readonly clientSeq: number;
	readonly settled: Promise<DistributedDispatchOutcome>;
	/** Explicitly retries this immutable action ID/generation; never extends its deadline. */
	retry(): Promise<DistributedDispatchResult>;
}
export type DistributedClientEvent =
	| { type: "state"; channel: string; state: unknown; cursor?: ChannelCursor }
	| DurableChannelNotification;
export class DistributedSubscription implements AsyncIterableIterator<DistributedClientEvent> {
	#queue: DistributedClientEvent[] = [];
	#waiters: ((result: IteratorResult<DistributedClientEvent>) => void)[] = [];
	#closed = false;
	#overflow: () => void;
	constructor(overflow: () => void) {
		this.#overflow = overflow;
	}
	[Symbol.asyncIterator](): AsyncIterableIterator<DistributedClientEvent> {
		return this;
	}
	next(): Promise<IteratorResult<DistributedClientEvent>> {
		const event = this.#queue.shift();
		if (event) return Promise.resolve({ done: false, value: event });
		if (this.#closed) return Promise.resolve({ done: true, value: undefined });
		return new Promise((resolve) => this.#waiters.push(resolve));
	}
	return(): Promise<IteratorResult<DistributedClientEvent>> {
		this.close();
		return Promise.resolve({ done: true, value: undefined });
	}
	push(event: DistributedClientEvent): void {
		if (this.#closed) return;
		const waiter = this.#waiters.shift();
		if (waiter) waiter({ done: false, value: event });
		else if (this.#queue.length < 4096) this.#queue.push(event);
		else this.#overflow();
	}
	close(): void {
		this.#closed = true;
		this.#queue = [];
		for (const resolve of this.#waiters.splice(0)) resolve({ done: true, value: undefined });
	}
}
interface Pending {
	request: DistributedDispatchRequest;
	settle: (outcome: DistributedDispatchOutcome) => void;
}
interface Mirror<TEnv> {
	uri: string;
	definition: DurableChannel<TEnv>;
	params: Readonly<Record<string, string>>;
	epoch: number;
	base: unknown;
	view: unknown;
	cursor?: ChannelCursor;
	pending: Map<string, Pending>;
	stream: DistributedSubscription;
	recovering: boolean;
	attempting: boolean;
	attempts: number;
	cancel?: () => void;
	buffer: DistributedEnvelope[];
	bytes: number;
	revision: number;
	recoveryFrame?: { entry: DistributedResumeEntry; epoch: number; bytes: number };
}
interface Request {
	resolve: (value: unknown) => void;
	reject: (error: unknown) => void;
	cancel: () => void;
}
export interface DurableChannelDistributedClientOptions {
	/** Hint for connecting to legacy endpoints; distributed identity always comes from authenticated hello. */
	clientId?: string;
	clock?: DistributedClock;
	scheduler?: DistributedClientScheduler;
	requestTimeoutMs?: number;
	retryWindowMs?: number;
	recoveryDelayMs?: number;
	maxRecoveryDelayMs?: number;
	bufferLimit?: number;
	bufferBytes?: number;
}
export type DistributedClientState<R extends DurableChannelRouteMap, U extends string> = [MatchRoute<R, U>] extends [never] ? unknown
	: MatchRoute<R, U>["state"];
export interface DistributedClientRouteHandle<R extends DurableChannelRouteMap, K extends keyof R & string> {
	uri(params: PathToParams<K>): string;
	state(params: PathToParams<K>): R[K]["state"] | undefined;
	subscribe(params: PathToParams<K>): Promise<{ entry: DistributedResumeEntry; subscription: DistributedSubscription }>;
	unsubscribe(params: PathToParams<K>): Promise<void>;
	dispatch<N extends keyof R[K]["actions"] & string>(
		params: PathToParams<K>,
		name: N,
		payload: InferInput<R[K]["actions"][N]>,
	): DistributedDispatchHandle;
	exec<N extends keyof R[K]["commands"] & string>(
		params: PathToParams<K>,
		name: N,
		payload: InferInput<R[K]["commands"][N]["params"]>,
	): Promise<InferOutput<R[K]["commands"][N]["result"]>>;
}
/** Optimistic mirrors with independent channel cursors and recovery epochs. */
export class DurableChannelDistributedClient<TEnv = unknown, R extends DurableChannelRouteMap = DurableChannelRouteMap> {
	#routes: DurableChannelRoutes<TEnv, R>;
	#transport: DurableChannelTransport;
	#options: DurableChannelDistributedClientOptions;
	#mirrors = new Map<string, Mirror<TEnv>>();
	#epochs = new Map<string, number>();
	#requests = new Map<number, Request>();
	#outbound = new Map<string, Promise<unknown>>();
	#requestId = 0;
	#clientSeq = 0;
	#transportEpoch = 0;
	#handshakeEpoch = 0;
	#started = false;
	#verified = false;
	#live = false;
	#closed = false;
	#identity: string | undefined;
	#error: Error | undefined;
	constructor(
		routes: DurableChannelRoutes<TEnv, R>,
		transport: DurableChannelTransport,
		options: DurableChannelDistributedClientOptions = {},
	) {
		this.#routes = routes;
		this.#transport = transport;
		this.#options = options;
	}
	get connectionError(): Error | undefined {
		return this.#error;
	}
	get clientId(): string | undefined {
		return this.#identity;
	}
	get subscriptions(): string[] {
		return [...this.#mirrors.keys()];
	}
	get cursors(): Record<string, ChannelCursor> {
		return Object.fromEntries([...this.#mirrors].filter(([, m]) => m.cursor).map(([uri, m]) => [uri, { ...m.cursor! }]));
	}
	get connected(): boolean {
		return this.#live && this.#verified;
	}
	private get scheduler(): DistributedClientScheduler {
		return this.#options.scheduler ?? distributedClientScheduler;
	}
	private nextEpoch(uri: string): number {
		const next = (this.#epochs.get(uri) ?? 0) + 1;
		this.#epochs.set(uri, next);
		return next;
	}
	private current(m: Mirror<TEnv>, epoch: number): boolean {
		return this.#mirrors.get(m.uri) === m && m.epoch === epoch;
	}
	private mirror(uri: string): Mirror<TEnv> {
		let m = this.#mirrors.get(uri);
		if (m) return m;
		const match = matchRoute(this.#routes, uri);
		if (!match || match.route.internal) throw new RouteNotFoundError(uri);
		m = {
			uri,
			definition: match.route.definition,
			params: match.params,
			epoch: this.nextEpoch(uri),
			base: undefined,
			view: undefined,
			pending: new Map(),
			stream: new DistributedSubscription(() => this.fail(distributedError("DISTRIBUTED_BACKPRESSURE"))),
			recovering: true,
			attempting: false,
			attempts: 0,
			buffer: [],
			bytes: 0,
			revision: 0,
		};
		this.#mirrors.set(uri, m);
		return m;
	}
	connect(): void {
		if (this.#closed) throw new ClientClosedError();
		if (this.#started) return;
		this.#started = true;
		this.#error = undefined;
		this.#live = true;
		void this.drive(this.#transport, this.#transportEpoch);
	}
	private request(method: string, params: Record<string, unknown>): Promise<unknown> {
		if (this.#closed) return Promise.reject(new ClientClosedError());
		this.connect();
		if (!this.#live) return Promise.reject(new TransportError("closed", "Distributed transport is closed"));
		const id = ++this.#requestId;
		return new Promise((resolve, reject) => {
			const cancel = this.scheduler.schedule(this.#options.requestTimeoutMs ?? 5000, () => {
				this.#requests.delete(id);
				reject(new RpcTimeoutError(method, this.#options.requestTimeoutMs ?? 5000));
			});
			this.#requests.set(id, { resolve, reject, cancel });
			try {
				Promise.resolve(this.#transport.send({ jsonrpc: "2.0", id, method, params })).catch((e) => {
					this.#requests.delete(id);
					cancel();
					reject(e);
				});
			} catch (error) {
				this.#requests.delete(id);
				cancel();
				reject(error);
			}
		});
	}
	private async drive(transport: DurableChannelTransport, epoch: number): Promise<void> {
		try {
			for (;;) {
				const frame = await transport.recv();
				if (epoch !== this.#transportEpoch) return;
				if (!frame) throw new TransportError("closed", "Distributed transport closed");
				if ("method" in frame) {
					const message = distributedFrameMessage(frame.method, frame.params);
					if (message) this.receive(message);
				} else if (typeof frame.id === "number") {
					const request = this.#requests.get(frame.id);
					if (!request) continue;
					this.#requests.delete(frame.id);
					request.cancel();
					if ("error" in frame) request.reject(new RpcError(frame.error.code, frame.error.message, frame.error.data));
					else request.resolve(frame.result);
				}
			}
		} catch (error) {
			if (epoch === this.#transportEpoch) this.fail(error);
		}
	}
	private fail(error: unknown): void {
		if (!this.#live && this.#error) return;
		this.#error = error instanceof Error ? error : new Error("Distributed transport failed", { cause: error });
		this.#live = false;
		this.#verified = false;
		for (const request of this.#requests.values()) {
			request.cancel();
			request.reject(error);
		}
		this.#requests.clear();
		for (const m of this.#mirrors.values()) {
			m.cancel?.();
			m.cancel = undefined;
			m.recovering = true;
			this.unknown(m);
		}
		void this.#transport.close();
	}
	private begin(m: Mirror<TEnv>): number {
		m.cancel?.();
		m.cancel = undefined;
		m.epoch = this.nextEpoch(m.uri);
		m.recovering = true;
		m.attempting = false;
		m.attempts = 0;
		m.buffer = [];
		m.bytes = 0;
		m.recoveryFrame = undefined;
		return m.epoch;
	}
	async hello(args: { subscriptions?: readonly string[] } = {}): Promise<DistributedReconnectResult> {
		const handshake = ++this.#handshakeEpoch;
		const requested = args.subscriptions ?? this.subscriptions;
		for (const [uri, m] of this.#mirrors) {
			if (!requested.includes(uri)) {
				this.#mirrors.delete(uri);
				this.nextEpoch(uri);
				m.cancel?.();
				this.unknown(m);
				m.stream.close();
			}
		}
		const selected = requested.map((uri) => this.mirror(uri));
		const epochs = new Map(selected.map((m) => [m, this.begin(m)]));
		const transportEpoch = this.#transportEpoch;
		try {
			const result = safeParse(
				DistributedHelloResultSchema,
				await this.request("hello", {
					protocol: DISTRIBUTED_PROTOCOL,
					clientId: this.#options.clientId ?? "distributed",
					subscriptions: selected.map((m) => m.uri),
					cursors: this.cursors,
				}),
			);
			if (!result.success) throw new TransportError("protocol", "Peer did not negotiate durable-channel/distributed-1");
			if (transportEpoch !== this.#transportEpoch || handshake !== this.#handshakeEpoch) throw distributedError("SUPERSEDED_RECOVERY");
			if (this.#identity && this.#identity !== result.output.clientId) throw distributedError("CLIENT_IDENTITY_CHANGED");
			this.#identity = result.output.clientId;
			this.#error = undefined;
			this.#verified = true;
			for (const [m, epoch] of epochs) this.install(m, epoch, result.output.channels[m.uri] ?? { type: "missing" });
			return { channels: result.output.channels };
		} catch (error) {
			if (transportEpoch === this.#transportEpoch && handshake === this.#handshakeEpoch) this.fail(error);
			throw error;
		}
	}
	async reconnect(transport: DurableChannelTransport): Promise<DistributedReconnectResult> {
		if (this.#closed) throw new ClientClosedError();
		const transportEpoch = ++this.#transportEpoch;
		this.#handshakeEpoch++;
		const old = this.#transport;
		this.#transport = transport;
		this.#started = false;
		this.#verified = false;
		for (const request of this.#requests.values()) {
			request.cancel();
			request.reject(distributedError("TRANSPORT_REPLACED"));
		}
		this.#requests.clear();
		for (const m of this.#mirrors.values()) {
			this.begin(m);
			m.revision = 0;
		}
		await old.close();
		if (transportEpoch !== this.#transportEpoch) throw distributedError("SUPERSEDED_RECOVERY");
		return await this.hello();
	}
	async subscribe(uri: string): Promise<{ entry: DistributedResumeEntry; subscription: DistributedSubscription }> {
		this.assertReady();
		const m = this.mirror(uri), epoch = this.begin(m);
		try {
			const result = safeParse(
				DistributedResumeSchema,
				await this.request("subscribe", { channel: uri, ...(m.cursor ? { cursor: m.cursor } : {}) }),
			);
			if (!result.success) throw distributedError("INVALID_RECOVERY");
			this.install(m, epoch, result.output);
			return { entry: result.output, subscription: m.stream };
		} catch (error) {
			if (this.current(m, epoch)) this.schedule(m);
			throw error;
		}
	}
	async unsubscribe(uri: string): Promise<void> {
		const m = this.#mirrors.get(uri);
		this.nextEpoch(uri);
		this.#mirrors.delete(uri);
		if (m) {
			m.cancel?.();
			this.unknown(m);
			m.stream.close();
		}
		if (this.connected) await this.request("unsubscribe", { channel: uri });
	}
	/** Starts a replacement recovery epoch. Failed attempts retry while the subscription stays active. */
	async recover(uri: string): Promise<void> {
		const m = this.#mirrors.get(uri);
		if (!m) return;
		const epoch = this.begin(m);
		await this.recoverAttempt(m, epoch);
	}
	private schedule(m: Mirror<TEnv>, immediate = false): void {
		if (!this.connected || this.#mirrors.get(m.uri) !== m || m.cancel || m.attempting) return;
		const epoch = m.epoch;
		const delay = immediate
			? 0
			: Math.min(this.#options.maxRecoveryDelayMs ?? 5000, (this.#options.recoveryDelayMs ?? 100) * 2 ** Math.min(m.attempts, 6));
		m.cancel = this.scheduler.schedule(delay, () => {
			m.cancel = undefined;
			void this.recoverAttempt(m, epoch);
		});
	}
	private async recoverAttempt(m: Mirror<TEnv>, epoch: number): Promise<void> {
		if (!this.current(m, epoch) || !this.connected || m.attempting) return;
		m.attempting = true;
		try {
			const entry = safeParse(
				DistributedResumeSchema,
				await this.request("subscribe", { channel: m.uri, ...(m.cursor ? { cursor: m.cursor } : {}) }),
			);
			if (!entry.success) throw distributedError("INVALID_RECOVERY");
			this.install(m, epoch, entry.output);
		} catch {
			if (this.current(m, epoch)) {
				m.recovering = true;
				m.attempts++;
			}
		} finally {
			if (this.current(m, epoch)) {
				m.attempting = false;
				if (m.recovering) this.schedule(m);
			}
		}
	}
	private assertReady(): void {
		if (this.#closed) throw new ClientClosedError();
		if (!this.connected) throw distributedError("NOT_INITIALIZED");
	}
	state<U extends string>(uri: U): DistributedClientState<R, U> | undefined {
		return cloneRecord(this.#mirrors.get(uri)?.view) as DistributedClientState<R, U> | undefined;
	}
	confirmedState(uri: string): unknown {
		return cloneRecord(this.#mirrors.get(uri)?.base);
	}
	dispatch(uri: string, name: string, payload: unknown): DistributedDispatchHandle {
		this.assertReady();
		const m = this.#mirrors.get(uri);
		if (!m?.cursor || m.recovering) throw distributedError("RECOVERY_PENDING");
		const action = m.definition.actions[name];
		if (!action || !Object.hasOwn(m.definition.actions, name)) throw new UnknownActionError(uri, name);
		if (!action.client) throw new NotClientDispatchableError(uri, name);
		const parsed = safeParse(action.payload, payload);
		if (!parsed.success) throw new InvalidPayloadError(uri, name);
		const actionId = createDistributedActionId((this.#options.clock ?? systemClock).now() + (this.#options.retryWindowMs ?? 60_000));
		const request: DistributedDispatchRequest = {
			generation: m.cursor.generation,
			actionId,
			clientSeq: ++this.#clientSeq,
			name,
			payload: cloneRecord(parsed.output),
		};
		let settle!: (result: DistributedDispatchOutcome) => void;
		const settled = new Promise<DistributedDispatchOutcome>((resolve) => settle = resolve);
		m.pending.set(actionId, { request, settle });
		this.rebase(m);
		const epoch = m.epoch;
		const send = async (): Promise<DistributedDispatchResult> => {
			this.assertReady();
			const result = safeParse(DistributedDispatchResultSchema, await this.request("dispatch", { channel: uri, ...request }));
			if (!result.success) throw distributedError("INVALID_DISPATCH_RESULT");
			if (this.current(m, epoch)) {
				if (result.output.type === "committed") this.receive(result.output.envelope);
				else this.settleUnknown(m, actionId);
			}
			return result.output;
		};
		const prior = this.#outbound.get(uri) ?? Promise.resolve();
		const task = prior.catch(() => {}).then(async () => {
			if (!this.current(m, epoch)) return;
			try {
				await send();
			} catch {
				if (this.current(m, epoch)) this.settleUnknown(m, actionId);
			}
		});
		this.#outbound.set(uri, task);
		void task.finally(() => {
			if (this.#outbound.get(uri) === task) this.#outbound.delete(uri);
		});
		return { actionId, generation: request.generation, clientSeq: request.clientSeq, settled, retry: send };
	}
	async exec(uri: string, name: string, params: unknown): Promise<unknown> {
		this.assertReady();
		const m = this.#mirrors.get(uri);
		if (!m || m.recovering) throw distributedError("RECOVERY_PENDING");
		return await this.request("exec", { channel: uri, name, params, ...(m.cursor ? { generation: m.cursor.generation } : {}) });
	}
	private settleUnknown(m: Mirror<TEnv>, id: string): void {
		const pending = m.pending.get(id);
		if (!pending) return;
		m.pending.delete(id);
		pending.settle({ status: "unknown", actionId: id });
		this.rebase(m);
	}
	private unknown(m: Mirror<TEnv>): void {
		for (const id of [...m.pending.keys()]) this.settleUnknown(m, id);
	}
	private settle(m: Mirror<TEnv>, envelope: DistributedEnvelope): void {
		if (!envelope.actionId || envelope.origin?.clientId !== this.#identity) return;
		const pending = m.pending.get(envelope.actionId);
		if (!pending || pending.request.generation !== envelope.generation) return;
		m.pending.delete(envelope.actionId);
		pending.settle(
			envelope.rejectionReason === undefined
				? { status: "confirmed", envelope }
				: { status: "rejected", envelope, reason: envelope.rejectionReason },
		);
	}
	private reduce(m: Mirror<TEnv>, state: unknown, envelope: { name: string; payload: unknown }): unknown {
		const action = m.definition.actions[envelope.name];
		if (!action) throw distributedError("UNKNOWN_DISTRIBUTED_ACTION");
		const payload = safeParse(action.payload, envelope.payload);
		if (!payload.success) throw distributedError("INVALID_DISTRIBUTED_ACTION");
		const reduced = action.reduce(cloneRecord(state), payload.output, { uri: m.uri, params: m.params });
		const parsed = safeParse(m.definition.state!, reduced);
		if (!parsed.success) throw distributedError("INVALID_DISTRIBUTED_STATE");
		return parsed.output;
	}
	private rebase(m: Mirror<TEnv>): void {
		let state = cloneRecord(m.base);
		if (state !== undefined) {
			for (const pending of m.pending.values()) {
				try {
					state = this.reduce(m, state, pending.request);
				} catch { /* Rejected optimistic actions leave the mirror unchanged. */ }
			}
		}
		m.view = state;
		m.stream.push({ type: "state", channel: m.uri, state: cloneRecord(state), ...(m.cursor ? { cursor: { ...m.cursor } } : {}) });
	}
	private buffer(m: Mirror<TEnv>, action: DistributedEnvelope): void {
		const bytes = new TextEncoder().encode(JSON.stringify(action)).byteLength;
		if (
			m.buffer.length >= (this.#options.bufferLimit ?? 256) ||
			m.bytes + bytes + (m.recoveryFrame?.bytes ?? 0) > (this.#options.bufferBytes ?? 1_048_576)
		) {
			this.fail(distributedError("DISTRIBUTED_BACKPRESSURE", "Distributed recovery buffer overflow"));
			return;
		}
		m.buffer.push(action);
		m.bytes += bytes;
	}
	private receive(message: DistributedMessage): void {
		const m = this.#mirrors.get(message.channel);
		if (!m) return;
		if (message.type === "notification") {
			if (!this.#verified) return;
			const schema = m.definition.notifications[message.name]?.payload;
			if (schema) {
				const parsed = safeParse(schema, message.payload);
				if (parsed.success) m.stream.push({ ...message, payload: parsed.output });
			}
			return;
		}
		if (message.type === "recovery") {
			if (message.revision !== undefined && message.revision < m.revision) return;
			m.revision = message.revision ?? m.revision;
			if (!this.#verified || m.recovering) {
				const bytes = new TextEncoder().encode(JSON.stringify(message.entry)).byteLength;
				if (m.bytes + bytes > (this.#options.bufferBytes ?? 1_048_576)) {
					this.fail(distributedError("DISTRIBUTED_BACKPRESSURE", "Distributed recovery buffer overflow"));
					return;
				}
				m.recoveryFrame = { entry: message.entry, epoch: m.epoch, bytes };
				return;
			}
			this.install(m, m.epoch, message.entry, true);
			return;
		}
		if (!this.#verified || m.recovering) {
			this.buffer(m, message);
			return;
		}
		if (m.cursor?.generation === message.generation && message.channelSeq <= m.cursor.channelSeq) {
			this.settle(m, message);
			this.rebase(m);
			return;
		}
		if (!m.cursor || m.cursor.generation !== message.generation || message.channelSeq !== m.cursor.channelSeq + 1) {
			this.begin(m);
			this.buffer(m, message);
			this.schedule(m, true);
			return;
		}
		this.apply(m, message);
	}
	private apply(m: Mirror<TEnv>, action: DistributedEnvelope): void {
		if (action.rejectionReason === undefined) m.base = this.reduce(m, m.base, action);
		m.cursor = { generation: action.generation, channelSeq: action.channelSeq };
		this.settle(m, action);
		this.rebase(m);
	}
	private install(m: Mirror<TEnv>, epoch: number, entry: DistributedResumeEntry, unsolicited = false): void {
		if (!this.current(m, epoch)) return;
		if (!unsolicited && entry.type === "missing" && m.recoveryFrame?.epoch === epoch && m.recoveryFrame.entry.type !== "missing") {
			entry = m.recoveryFrame.entry;
			m.recoveryFrame = undefined;
		}
		if (entry.type === "missing") {
			m.cancel?.();
			this.unknown(m);
			m.stream.close();
			this.#mirrors.delete(m.uri);
			this.nextEpoch(m.uri);
			return;
		}
		if (entry.type === "snapshot") {
			const cursor = entry.snapshot.cursor;
			if (
				m.cursor?.generation === cursor.generation &&
				(cursor.channelSeq < m.cursor.channelSeq || (unsolicited && cursor.channelSeq === m.cursor.channelSeq))
			) return;
			const parsed = m.definition.state && safeParse(m.definition.state, entry.snapshot.state);
			if (!parsed || !parsed.success) throw distributedError("INVALID_DISTRIBUTED_STATE");
			this.unknown(m);
			m.base = parsed.output;
			m.cursor = cursor;
		} else if (entry.type === "replay") {
			if (!m.cursor || m.cursor.generation !== entry.cursor.generation) throw distributedError("INVALID_RECOVERY");
			for (const action of entry.actions) {
				if (action.generation !== m.cursor.generation) throw distributedError("INVALID_RECOVERY");
				if (action.channelSeq <= m.cursor.channelSeq) {
					this.settle(m, action);
					continue;
				}
				if (action.channelSeq !== m.cursor.channelSeq + 1) throw distributedError("INVALID_RECOVERY");
				this.apply(m, action);
			}
			if (m.cursor.channelSeq < entry.cursor.channelSeq) throw distributedError("INVALID_RECOVERY");
			this.unknown(m);
		}
		m.recovering = false;
		m.attempts = 0;
		m.cancel?.();
		m.cancel = undefined;
		const buffered = m.buffer.splice(0);
		m.bytes = 0;
		this.rebase(m);
		const recovery = m.recoveryFrame;
		m.recoveryFrame = undefined;
		if (recovery && recovery.epoch === epoch) this.install(m, epoch, recovery.entry, true);
		if (!this.current(m, epoch)) return;
		for (const action of buffered) {
			if (action.generation !== m.cursor?.generation) continue;
			this.receive(action);
		}
	}
	async shutdown(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		this.fail(new ClientClosedError());
		for (const m of this.#mirrors.values()) m.stream.close();
		await this.#transport.close();
	}
	of<K extends keyof R & string>(template: K): DistributedClientRouteHandle<R, K> {
		const uri = (params: PathToParams<K>) => resolveUri(template, params);
		return {
			uri,
			state: (p) => this.state(uri(p)) as R[K]["state"] | undefined,
			subscribe: (p) => this.subscribe(uri(p)),
			unsubscribe: (p) => this.unsubscribe(uri(p)),
			dispatch: (p, name, payload) => this.dispatch(uri(p), name, payload),
			exec: (p, name, payload) => this.exec(uri(p), name, payload) as never,
		};
	}
}
