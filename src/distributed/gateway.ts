/** A gateway owns sockets and recoverable subscription metadata, never channel state. */
import type { DurableChannelRouteMap } from "../routes.ts";
import type {
	ChannelStore,
	ChannelTransaction,
	DistributedClientScheduler,
	DistributedClock,
	DistributedScheduler,
	DistributedSession,
} from "./interfaces.ts";
import { systemClock } from "./interfaces.ts";
import type {
	ChannelCursor,
	DistributedDelivery,
	DistributedDeliveryAck,
	DistributedDispatchRequest,
	DistributedDispatchResult,
	DistributedMessage,
	DistributedReconnectResult,
	DistributedResumeEntry,
} from "./protocol.ts";
import type { DurableChannelRouter } from "./router.ts";
import { distributedTimeout, resumeCursor } from "./delivery.ts";
import { distributedError } from "./store.ts";
interface Row {
	connectionId: string;
	clientId: string;
	binding: number;
	uri: string;
	epoch: number;
	cursor?: ChannelCursor;
}
interface Local extends Row {
	recovering: boolean;
	buffer: DistributedMessage[];
	bytes: number;
}
interface Member {
	revision: number;
	generation?: string;
	active: boolean;
	pending: boolean;
}
interface Bound {
	session: DistributedSession;
	binding: number;
	subscriptions: Map<string, Local>;
}
export interface DurableChannelGatewayOptions<TEnv, R extends DurableChannelRouteMap> {
	id: string;
	router: DurableChannelRouter<TEnv, R>;
	store: ChannelStore;
	scheduler: DistributedScheduler;
	clock?: DistributedClock;
	renewMs?: number;
	timeoutMs?: number;
	timeouts?: DistributedClientScheduler;
	bufferLimit?: number;
	bufferBytes?: number;
}
export class DurableChannelGateway<TEnv = unknown, R extends DurableChannelRouteMap = DurableChannelRouteMap> {
	readonly options: DurableChannelGatewayOptions<TEnv, R>;
	#sessions = new Map<string, Bound>();
	#lock: Promise<unknown> = Promise.resolve();
	#delivery = new Map<string, Promise<unknown>>();
	constructor(options: DurableChannelGatewayOptions<TEnv, R>) {
		this.options = options;
	}
	get id(): string {
		return this.options.id;
	}
	private now(): number {
		return (this.options.clock ?? systemClock).now();
	}
	private mutation<T>(body: (tx: ChannelTransaction) => T): Promise<T> {
		const run = this.#lock.then(async () => {
			await this.options.scheduler.arm(this.now() + (this.options.renewMs ?? 10_000));
			return await this.options.store.transaction(body);
		});
		this.#lock = run.catch(() => {});
		return run;
	}
	private rowKey(id: string, uri: string): string {
		return `subscription/${JSON.stringify([id, uri])}`;
	}
	private bound(id: string, binding: number): Bound {
		const bound = this.#sessions.get(id);
		if (!bound || bound.binding !== binding) throw distributedError("STALE_CONNECTION");
		return bound;
	}
	private current(local: Local): Bound | undefined {
		const bound = this.#sessions.get(local.connectionId);
		return bound?.binding === local.binding && bound.subscriptions.get(local.uri) === local ? bound : undefined;
	}
	private locals(uri: string): Local[] {
		return [...this.#sessions.values()].flatMap((s) => [...s.subscriptions.values()].filter((l) => l.uri === uri));
	}
	async connect(session: DistributedSession): Promise<number> {
		const old = this.#sessions.get(session.id);
		const binding = await this.mutation((tx) => {
			const next = (tx.get<number>(`binding/${session.id}`) ?? 0) + 1;
			if (!Number.isSafeInteger(next)) throw distributedError("SEQUENCE_EXHAUSTED");
			tx.set(`binding/${session.id}`, next);
			for (const [key, row] of tx.list<Row>("subscription/")) if (row.connectionId === session.id) tx.delete(key);
			return next;
		});
		this.#sessions.set(session.id, { session, binding, subscriptions: new Map() });
		old?.session.close("Replaced connection");
		return binding;
	}
	async disconnect(id: string, binding: number): Promise<void> {
		const bound = this.#sessions.get(id);
		if (!bound || bound.binding !== binding) return;
		this.#sessions.delete(id);
		const uris = [...bound.subscriptions.keys()];
		await this.mutation((tx) => {
			for (const [key, row] of tx.list<Row>("subscription/")) if (row.connectionId === id && row.binding === binding) tx.delete(key);
		});
		await Promise.allSettled(uris.map((uri) => this.recover(uri)));
	}
	async subscribe(id: string, binding: number, uri: string, cursor?: ChannelCursor): Promise<DistributedResumeEntry> {
		const bound = this.bound(id, binding), prior = bound.subscriptions.get(uri);
		const local: Local = {
			connectionId: id,
			clientId: bound.session.clientId,
			binding,
			uri,
			epoch: (prior?.epoch ?? 0) + 1,
			...(cursor ? { cursor } : {}),
			recovering: true,
			buffer: [],
			bytes: 0,
		};
		bound.subscriptions.set(uri, local);
		try {
			await this.options.router.endpoint(uri, true);
		} catch (error) {
			if (this.current(local)) bound.subscriptions.delete(uri);
			throw error;
		}
		if (!this.current(local)) return { type: "missing" };
		await this.mutation((tx) => {
			if (this.current(local)) tx.set(this.rowKey(id, uri), this.persisted(local));
		});
		if (!this.current(local)) return { type: "missing" };
		return await this.recover(uri);
	}
	async unsubscribe(id: string, binding: number, uri: string): Promise<void> {
		const bound = this.bound(id, binding), local = bound.subscriptions.get(uri);
		bound.subscriptions.delete(uri);
		await this.mutation((tx) => {
			const row = tx.get<Row>(this.rowKey(id, uri));
			if (row && row.binding === binding && row.epoch === local?.epoch) tx.delete(this.rowKey(id, uri));
		});
		await this.recover(uri);
	}
	async reconnect(
		id: string,
		binding: number,
		subscriptions: string[],
		cursors: Record<string, ChannelCursor> = {},
	): Promise<DistributedReconnectResult> {
		const bound = this.bound(id, binding);
		await Promise.all(
			[...bound.subscriptions.keys()].filter((uri) => !subscriptions.includes(uri)).map((uri) => this.unsubscribe(id, binding, uri)),
		);
		const entries = await Promise.all([...new Set(subscriptions)].map(async (uri): Promise<[string, DistributedResumeEntry]> => {
			try {
				return [uri, await this.subscribe(id, binding, uri, cursors[uri])];
			} catch (e) {
				if (this.missing(e)) return [uri, { type: "missing" }];
				throw e;
			}
		}));
		return { channels: Object.fromEntries(entries) };
	}
	async dispatch(id: string, binding: number, uri: string, request: DistributedDispatchRequest): Promise<DistributedDispatchResult> {
		const bound = this.bound(id, binding);
		return await (await this.options.router.endpoint(uri, true)).dispatchFrom(
			{ clientId: bound.session.clientId, connectionId: id },
			request,
		);
	}
	async exec(id: string, binding: number, uri: string, name: string, params: unknown, generation?: string): Promise<unknown> {
		const bound = this.bound(id, binding);
		return await (await this.options.router.endpoint(uri, true)).execFrom(
			{ clientId: bound.session.clientId, connectionId: id },
			name,
			params,
			generation,
		);
	}
	private persisted(local: Local): Row {
		const { recovering: _, buffer: _b, bytes: _n, ...row } = local;
		return row;
	}
	private missing(error: unknown): boolean {
		return !!error && typeof error === "object" && "code" in error && ["CHANNEL_NOT_FOUND", "ROUTE_NOT_FOUND"].includes(String(error.code));
	}
	private async send(local: Local, message: DistributedMessage): Promise<boolean> {
		const bound = this.current(local);
		if (!bound) return false;
		try {
			await distributedTimeout(Promise.resolve(bound.session.send(message)), this.options.timeoutMs ?? 5000, this.options.timeouts);
			return !!this.current(local);
		} catch {
			bound.session.close("Distributed socket delivery failed; reconnect for recovery");
			void this.disconnect(local.connectionId, local.binding).catch(() => {});
			return false;
		}
	}
	private buffer(local: Local, message: DistributedMessage): void {
		const bytes = new TextEncoder().encode(JSON.stringify(message)).byteLength;
		if (local.buffer.length >= (this.options.bufferLimit ?? 256) || local.bytes + bytes > (this.options.bufferBytes ?? 1_048_576)) {
			this.current(local)?.session.close("DISTRIBUTED_BACKPRESSURE: reconnect for recovery");
			void this.disconnect(local.connectionId, local.binding).catch(() => {});
			return;
		}
		local.buffer.push(message);
		local.bytes += bytes;
	}
	private async install(
		local: Local,
		entry: DistributedResumeEntry,
		epoch: number,
		revision: number,
		authoritative = false,
	): Promise<void> {
		if (!this.current(local) || local.epoch !== epoch) return;
		const cut = resumeCursor(entry);
		if (!authoritative && cut && local.cursor?.generation === cut.generation && cut.channelSeq < local.cursor.channelSeq) {
			local.recovering = false;
			return;
		}
		if (!await this.send(local, { type: "recovery", channel: local.uri, entry, revision }) || local.epoch !== epoch) return;
		const cursor = resumeCursor(entry);
		if (cursor) local.cursor = cursor;
		local.recovering = false;
		const buffered = local.buffer.splice(0);
		local.bytes = 0;
		for (const message of buffered) {
			if (message.type !== "action") continue;
			if (local.cursor && message.generation === local.cursor.generation && message.channelSeq <= local.cursor.channelSeq) continue;
			if (!local.cursor || message.generation !== local.cursor.generation || message.channelSeq !== local.cursor.channelSeq + 1) {
				local.recovering = true;
				break;
			}
			if (await this.send(local, message) && local.epoch === epoch) {
				local.cursor = { generation: message.generation, channelSeq: message.channelSeq };
			}
		}
		await this.save(local);
	}
	private async save(local: Local): Promise<void> {
		await this.mutation((tx) => {
			if (this.current(local)) tx.set(this.rowKey(local.connectionId, local.uri), this.persisted(local));
		});
	}
	/** Replaces membership using a newer persisted revision and recovers every local subscription separately. */
	async recover(uri: string): Promise<DistributedResumeEntry> {
		const locals = this.locals(uri);
		const epochs = new Map(locals.map((local) => {
			local.recovering = true;
			local.epoch++;
			return [local, local.epoch] as const;
		}));
		const revision = await this.mutation((tx) => {
			const old = tx.get<Member>(`member/${uri}`);
			const revision = (old?.revision ?? 0) + 1;
			if (!Number.isSafeInteger(revision)) throw distributedError("SEQUENCE_EXHAUSTED");
			tx.set(`member/${uri}`, { ...old, revision, active: locals.length > 0, pending: true });
			return revision;
		});
		try {
			const endpoint = await distributedTimeout(
				this.options.router.endpoint(uri, true),
				this.options.timeoutMs ?? 5000,
				this.options.timeouts,
			);
			const snapshot = await distributedTimeout(endpoint.snapshot(), this.options.timeoutMs ?? 5000, this.options.timeouts);
			const generation = snapshot?.cursor.generation;
			let cursor: ChannelCursor | undefined;
			if (generation && locals.length && locals.every((local) => local.cursor?.generation === generation)) {
				cursor = { generation: generation!, channelSeq: Math.min(...locals.map((local) => local.cursor!.channelSeq)) };
			}
			const request = { gatewayId: this.id, revision, ...(generation ? { generation } : {}), ...(cursor ? { cursor } : {}) };
			const entry: DistributedResumeEntry = locals.length
				? await distributedTimeout(endpoint.resume(request), this.options.timeoutMs ?? 5000, this.options.timeouts)
				: (await distributedTimeout(endpoint.remove(request), this.options.timeoutMs ?? 5000, this.options.timeouts), { type: "missing" });
			await this.ordered(uri, async () => {
				const current = await this.options.store.transaction((tx) => tx.get<Member>(`member/${uri}`));
				if (current?.revision !== revision) return;
				for (const local of locals) await this.install(local, entry, epochs.get(local)!, revision, true);
			});
			await this.mutation((tx) => {
				if (tx.get<Member>(`member/${uri}`)?.revision === revision) {
					tx.set(`member/${uri}`, {
						revision,
						generation,
						active: this.locals(uri).length > 0,
						pending: this.locals(uri).some((l) => l.recovering),
					});
				}
			});
			return entry;
		} catch (error) {
			if (this.missing(error)) {
				await this.ordered(uri, async () => {
					for (const local of locals) {
						if (local.epoch !== epochs.get(local)) continue;
						await this.send(local, { type: "recovery", channel: uri, entry: { type: "missing" }, revision });
						if (local.epoch !== epochs.get(local)) continue;
						this.current(local)?.subscriptions.delete(uri);
						await this.mutation((tx) => {
							const row = tx.get<Row>(this.rowKey(local.connectionId, uri));
							if (row?.binding === local.binding && row.epoch === local.epoch) tx.delete(this.rowKey(local.connectionId, uri));
						});
					}
				});
				await this.mutation((tx) => {
					if (tx.get<Member>(`member/${uri}`)?.revision === revision) tx.set(`member/${uri}`, { revision, active: false, pending: false });
				});
				return { type: "missing" };
			}
			throw error;
		}
	}
	private async ordered<T>(uri: string, task: () => Promise<T>): Promise<T> {
		const prior = this.#delivery.get(uri) ?? Promise.resolve();
		const run = prior.catch(() => {}).then(task);
		this.#delivery.set(uri, run);
		try {
			return await run;
		} finally {
			if (this.#delivery.get(uri) === run) this.#delivery.delete(uri);
		}
	}
	async deliver(message: DistributedDelivery): Promise<DistributedDeliveryAck> {
		return await this.ordered(message.channel, async () => {
			const member = await this.options.store.transaction((tx) => tx.get<Member>(`member/${message.channel}`));
			const cursor = resumeCursor(message.entry);
			if (!cursor || cursor.generation !== message.generation) throw distributedError("INVALID_DELIVERY");
			const ack = { revision: message.revision, generation: message.generation, channelSeq: cursor.channelSeq, clients: 0 };
			if (!member || member.revision !== message.revision || (member.generation && member.generation !== message.generation)) return ack;
			const locals = this.locals(message.channel);
			if (locals.some((local) => local.recovering)) {
				for (const local of locals) {
					if (local.recovering && message.entry.type === "replay") {
						for (const action of message.entry.actions) this.buffer(local, action);
					}
				}
				throw distributedError("RECOVERY_PENDING");
			}
			for (const local of locals) {
				const epoch = local.epoch;
				if (message.entry.type === "snapshot") await this.install(local, message.entry, local.epoch, message.revision);
				else if (message.entry.type === "replay") {
					for (const action of message.entry.actions) {
						if (local.cursor?.generation === action.generation && action.channelSeq <= local.cursor.channelSeq) continue;
						if (!local.cursor || local.cursor.generation !== action.generation || action.channelSeq !== local.cursor.channelSeq + 1) {
							local.recovering = true;
							throw distributedError("RECOVERY_PENDING");
						}
						if (await this.send(local, action) && local.epoch === epoch) {
							local.cursor = { generation: action.generation, channelSeq: action.channelSeq };
						}
					}
					await this.save(local);
				}
				if (this.current(local)) {
					if (local.epoch !== epoch || local.recovering) throw distributedError("RECOVERY_PENDING");
					ack.clients++;
				}
			}
			return ack;
		});
	}

	async notify(channel: string, generation: string, name: string, payload: unknown): Promise<void> {
		const member = await this.options.store.transaction((tx) => tx.get<Member>(`member/${channel}`));
		if (member?.generation && member.generation !== generation) return;
		await Promise.all(
			this.locals(channel).filter((local) => !local.recovering).map((local) =>
				this.send(local, { type: "notification", channel, name, payload })
			),
		);
	}
	/** Hosts provide reconstructed live sockets; rows without a live binding are removed. */
	async restore(sessions: DistributedSession[], options: { recover?: boolean } = {}): Promise<void> {
		const rows = await this.mutation((tx) => {
			const live = new Set(sessions.map((session) => session.id));
			for (const [key, row] of tx.list<Row>("subscription/")) if (!live.has(row.connectionId)) tx.delete(key);
			return tx.list<Row>("subscription/").map(([, row]) => row);
		});
		for (const session of sessions) {
			const binding = await this.options.store.transaction((tx) => tx.get<number>(`binding/${session.id}`) ?? 1);
			const bound: Bound = { session, binding, subscriptions: new Map() };
			this.#sessions.set(session.id, bound);
			for (const row of rows) {
				if (row.connectionId === session.id && row.clientId === session.clientId && row.binding === binding) {
					bound.subscriptions.set(row.uri, { ...row, cursor: undefined, recovering: true, buffer: [], bytes: 0 });
				}
			}
		}
		if (options.recover !== false) await this.alarm();
	}
	async alarm(): Promise<void> {
		const read = this.#lock.then(() => this.options.store.transaction((tx) => tx.list<Member>("member/")));
		this.#lock = read.catch(() => {});
		const members = await read;
		await Promise.allSettled(members.filter(([, member]) => member.active || member.pending).map(([key]) => this.recover(key.slice(7))));
	}
	close(): void {
		for (const bound of this.#sessions.values()) bound.session.close("Gateway closed");
		this.#sessions.clear();
	}
}
