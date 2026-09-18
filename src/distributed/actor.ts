/** One runtime-fenced owner per canonical channel URI. There is no cross-actor mutex. */
import { safeParse } from "valibot";
import type { DurableChannelActionDefinition, DurableChannelCommandContext, DurableChannelOperations } from "../channel.ts";
import {
	ChannelAlreadyExistsError,
	ChannelNotFoundError,
	InvalidPayloadError,
	InvalidResultError,
	InvalidStateError,
	NotClientDispatchableError,
	RejectAction,
	RouteNotFoundError,
	StatelessChannelError,
	UnknownActionError,
	UnknownCommandError,
	UnknownNotificationError,
} from "../error.ts";
import { type DurableChannelRouteMatch, type DurableChannelRoutes, isSingleton, matchRoute } from "../routes.ts";
import type {
	ChannelStore,
	ChannelTransaction,
	DistributedCaller,
	DistributedClientScheduler,
	DistributedClock,
	DistributedGatewayResolver,
	DistributedScheduler,
} from "./interfaces.ts";
import { systemClock } from "./interfaces.ts";
import {
	type ChannelCursor,
	type DistributedDispatchRequest,
	type DistributedDispatchResult,
	DistributedDispatchSchema,
	type DistributedEnvelope,
	type DistributedResumeEntry,
	type DistributedSnapshot,
} from "./protocol.ts";
import type { DistributedDeliveryAck, DistributedMemberRequest, DistributedMembership } from "./protocol.ts";
import { distributedTimeout } from "./delivery.ts";
import type { DistributedRuntimeOperations } from "./router.ts";
import { canonicalJson, cloneRecord, distributedActionDeadline, distributedError } from "./store.ts";

export interface DistributedChannelRecord {
	generation: string;
	channelSeq: number;
	state: unknown;
	deleted: boolean;
}
interface Receipt {
	fingerprint: string;
	deadline: number;
	envelope: DistributedEnvelope;
	bytes: number;
}
export interface DurableChannelActorOptions<TEnv> {
	store: ChannelStore;
	router?: DistributedRuntimeOperations;
	publishNotification?: (generation: string, name: string, payload: unknown) => Promise<void>;
	env: TEnv;
	clock?: DistributedClock;
	/** Required for durable membership/publication, added by the host before accepting subscribers. */
	scheduler?: DistributedScheduler;
	gateways?: DistributedGatewayResolver;
	leaseMs?: number;
	retryMs?: number;
	deliveryTimeoutMs?: number;
	deliveryConcurrency?: number;
	timeouts?: DistributedClientScheduler;
	waitUntil?: (task: Promise<unknown>) => void;
	historyLimit?: number;
	retryHorizonMs?: number;
	receiptLimit?: number;
	receiptBytes?: number;
	generation?: () => string;
}

export class DurableChannelActor<TEnv = unknown> {
	readonly uri: string;
	readonly options: DurableChannelActorOptions<TEnv>;
	readonly match: DurableChannelRouteMatch<TEnv>;
	#lock: Promise<unknown> = Promise.resolve();
	#aborters = new Map<string, AbortController>();
	#closed = false;
	#deliveries = new Set<string>();
	constructor(uri: string, routes: DurableChannelRoutes<TEnv>, options: DurableChannelActorOptions<TEnv>) {
		const match = matchRoute(routes, uri);
		if (!match) throw new RouteNotFoundError(uri);
		this.uri = uri;
		this.match = match;
		this.options = options;
		for (
			const n of [
				options.historyLimit ?? 256,
				options.receiptLimit ?? 4096,
				options.receiptBytes ?? 4_194_304,
				options.retryHorizonMs ?? 300_000,
			]
		) {
			if (!Number.isSafeInteger(n) || n < 1) throw distributedError("INVALID_OPTIONS");
		}
	}
	protected lock<T>(task: () => Promise<T>): Promise<T> {
		const run = this.#lock.then(task, task);
		this.#lock = run.catch(() => {});
		return run;
	}
	protected transaction<T>(body: (tx: ChannelTransaction) => T): Promise<T> {
		return this.options.store.transaction(body);
	}
	protected get clock(): DistributedClock {
		return this.options.clock ?? systemClock;
	}
	protected publicRoute(): void {
		if (this.match.route.internal) throw new RouteNotFoundError(this.uri);
	}
	protected record(tx: ChannelTransaction, expected?: string): DistributedChannelRecord {
		let record = tx.get<DistributedChannelRecord>("meta");
		if (!record && (isSingleton(this.match.route) || !this.match.route.definition.state)) {
			record = this.fresh(this.match.route.definition.initialState ?? null);
			tx.set("meta", record);
		}
		if (!record || record.deleted) throw new ChannelNotFoundError(this.uri);
		if (expected !== undefined && record.generation !== expected) throw distributedError("STALE_GENERATION");
		return record;
	}
	protected fresh(state: unknown): DistributedChannelRecord {
		return { generation: this.options.generation?.() ?? crypto.randomUUID(), channelSeq: 0, state, deleted: false };
	}
	protected snapshotOf(record: DistributedChannelRecord): DistributedSnapshot {
		return { resource: this.uri, state: record.state, cursor: this.cursorOf(record) };
	}
	protected cursorOf(record: DistributedChannelRecord): ChannelCursor {
		return { generation: record.generation, channelSeq: record.channelSeq };
	}
	async snapshot(generation?: string): Promise<DistributedSnapshot | undefined> {
		return await this.lock(() =>
			this.transaction((tx) => {
				const record = this.record(tx, generation);
				return this.match.route.definition.state ? this.snapshotOf(record) : undefined;
			})
		);
	}
	async create(state?: unknown): Promise<DistributedSnapshot> {
		const schema = this.match.route.definition.state;
		if (!schema) throw new StatelessChannelError(this.uri, "create");
		const parsed = safeParse(schema, state === undefined ? this.match.route.definition.initialState : state);
		if (!parsed.success) throw new InvalidStateError(this.uri);
		return await this.lock(() =>
			this.transaction((tx) => {
				const current = tx.get<DistributedChannelRecord>("meta");
				if (current && !current.deleted) throw new ChannelAlreadyExistsError(this.uri);
				const record = this.fresh(parsed.output);
				for (const prefix of ["log/", "receipt/", "member/"]) for (const [key] of tx.list(prefix)) tx.delete(key);
				tx.set("meta", record);
				return this.snapshotOf(record);
			})
		);
	}
	async destroy(generation: string): Promise<void> {
		await this.lock(() =>
			this.transaction((tx) => {
				const record = this.record(tx, generation);
				tx.set("meta", { ...record, deleted: true });
				for (const [key] of tx.list("member/")) tx.delete(key);
			})
		);
		this.#aborters.get(generation)?.abort();
		this.#aborters.delete(generation);
	}
	/** Trusted server mutation. The caller binds an observed generation. */
	async dispatch(generation: string, name: string, payload: unknown): Promise<DistributedEnvelope> {
		const result = await this.commit(generation, name, payload);
		if (result.type !== "committed") throw distributedError("UNKNOWN_OUTCOME");
		return result.envelope;
	}
	/** Public mutation; identity is authenticated metadata from the gateway's host. */
	async dispatchFrom(caller: DistributedCaller, request: DistributedDispatchRequest): Promise<DistributedDispatchResult> {
		this.publicRoute();
		const parsed = safeParse(DistributedDispatchSchema, { channel: this.uri, ...request });
		if (!parsed.success || !caller.clientId) throw distributedError("INVALID_REQUEST");
		return await this.commit(request.generation, request.name, request.payload, { caller, request });
	}
	protected async commit(
		generation: string,
		name: string,
		payload: unknown,
		client?: { caller: DistributedCaller; request: DistributedDispatchRequest },
	): Promise<DistributedDispatchResult> {
		if (!this.match.route.definition.state) throw new StatelessChannelError(this.uri, "dispatch");
		let fresh = false;
		let committedState: unknown;
		let parsedPayload: unknown = payload;
		let action: DurableChannelActionDefinition<unknown, TEnv> | undefined;
		const receiptKey = client ? `receipt/${JSON.stringify([generation, client.caller.clientId, client.request.actionId])}` : undefined;
		const fingerprint = canonicalJson({ name, payload });
		const result = await this.lock(async (): Promise<DistributedDispatchResult> => {
			try {
				await this.prearm();
				return await this.transaction((tx) => {
					const record = this.record(tx, generation);
					if (receiptKey && client) {
						const receipt = tx.get<Receipt>(receiptKey);
						if (receipt) {
							if (receipt.fingerprint !== fingerprint) throw distributedError("ACTION_ID_CONFLICT");
							return { type: "committed", envelope: receipt.envelope };
						}
						const now = Math.max(this.clock.now(), tx.get<number>("expirationFloor") ?? 0);
						const deadline = distributedActionDeadline(client.request.actionId);
						if (deadline <= now) return { type: "unknown", actionId: client.request.actionId };
						if (deadline > now + (this.options.retryHorizonMs ?? 300_000)) throw distributedError("RETRY_DEADLINE_TOO_FAR");
						this.collectReceipts(tx, now);
						const receipts = tx.list<Receipt>("receipt/");
						if (receipts.length >= (this.options.receiptLimit ?? 4096)) throw distributedError("RETRY_CAPACITY");
					}
					if (record.channelSeq >= Number.MAX_SAFE_INTEGER) throw distributedError("SEQUENCE_EXHAUSTED");
					const definition = this.match.route.definition;
					action = Object.hasOwn(definition.actions, name) ? definition.actions[name] : undefined;
					let reason: string | undefined;
					let state = record.state;
					try {
						if (!action) throw new UnknownActionError(this.uri, name);
						if (client && !action.client) throw new NotClientDispatchableError(this.uri, name);
						const parsed = safeParse(action.payload, payload);
						if (!parsed.success) throw new InvalidPayloadError(this.uri, name);
						parsedPayload = parsed.output;
					} catch (error) {
						if (!client) throw error;
						reason = error instanceof Error ? error.message : "Rejected";
					}
					if (reason === undefined && action) {
						try {
							const reduced = action.reduce(cloneRecord(state), parsedPayload, { uri: this.uri, params: this.match.params });
							const parsed = safeParse(definition.state!, reduced);
							if (!parsed.success) throw new InvalidStateError(this.uri);
							state = parsed.output;
						} catch (error) {
							if (!(error instanceof RejectAction)) throw error;
							reason = error.reason;
						}
					}
					const envelope: DistributedEnvelope = {
						type: "action",
						channel: this.uri,
						name,
						payload: parsedPayload,
						generation,
						channelSeq: record.channelSeq + 1,
						...(client
							? { actionId: client.request.actionId, origin: { clientId: client.caller.clientId, clientSeq: client.request.clientSeq } }
							: {}),
						...(reason === undefined ? {} : { rejectionReason: reason }),
					};
					if (receiptKey && client) {
						const receipt: Receipt = { fingerprint, deadline: distributedActionDeadline(client.request.actionId), envelope, bytes: 0 };
						receipt.bytes = new TextEncoder().encode(JSON.stringify(receipt)).byteLength;
						if (
							tx.list<Receipt>("receipt/").reduce((sum, [, row]) => sum + row.bytes, receipt.bytes) >
								(this.options.receiptBytes ?? 4_194_304)
						) throw distributedError("RETRY_CAPACITY");
						tx.set(receiptKey, receipt);
					}
					tx.set("meta", { ...record, state, channelSeq: envelope.channelSeq });
					tx.set(`log/${String(envelope.channelSeq).padStart(16, "0")}`, envelope);
					for (const [key, old] of tx.list<DistributedEnvelope>("log/")) {
						if (old.channelSeq <= envelope.channelSeq - (this.options.historyLimit ?? 256)) tx.delete(key);
					}
					for (const [key, member] of tx.list<DistributedMembership>("member/")) {
						if (member.active && member.generation === generation && member.expires > this.clock.now()) {
							tx.set(key, { ...member, target: envelope.channelSeq });
						}
					}
					committedState = state;
					fresh = true;
					return { type: "committed", envelope };
				});
			} catch (error) {
				fresh = false;
				// A host may have committed before reporting failure. A receipt resolves that ambiguity.
				if (receiptKey) {
					const receipt = await this.transaction((tx) => tx.get<Receipt>(receiptKey));
					if (receipt && receipt.fingerprint === fingerprint) return { type: "committed", envelope: receipt.envelope };
				}
				throw error;
			}
		});
		if (fresh && result.type === "committed") {
			this.kick();
			await this.afterCommit(result.envelope, committedState, parsedPayload, action, client?.caller);
		}
		return result;
	}
	protected async afterCommit(
		envelope: DistributedEnvelope,
		state: unknown,
		payload: unknown,
		action?: DurableChannelActionDefinition<unknown, TEnv>,
		caller?: DistributedCaller,
	): Promise<void> {
		if (envelope.rejectionReason !== undefined || !action?.effect) return;
		try {
			await action.effect({
				...this.operations(envelope.generation, caller),
				uri: this.uri,
				params: this.match.params,
				connectionId: caller?.connectionId,
				state,
				payload,
				envelope,
			});
		} catch { /* Application effects are best effort, never replayed by delivery/retry. */ }
	}
	protected async active(generation: string): Promise<void> {
		if (this.#closed) throw distributedError("ACTOR_CLOSED");
		await this.lock(() =>
			this.transaction((tx) => {
				this.record(tx, generation);
			})
		);
	}
	protected operations(generation: string, caller?: DistributedCaller): DurableChannelOperations<TEnv> {
		const route = async (target: string) => {
			await this.active(generation);
			if (!this.options.router) throw distributedError("ROUTER_REQUIRED");
			return { router: this.options.router, expected: target === this.uri ? generation : undefined };
		};
		return {
			env: this.options.env,
			dispatch: async (target: string, name: string, payload: unknown) => {
				const r = await route(target);
				return await r.router.dispatch(target, name, payload, r.expected);
			},
			notify: async (target: string, name: string, payload: unknown) => {
				const r = await route(target);
				await r.router.notify(target, name, payload, r.expected);
			},
			get: async (target) => {
				const r = await route(target);
				return await r.router.get(target, r.expected);
			},
			has: async (target) => {
				const r = await route(target);
				return await r.router.has(target, r.expected);
			},
			create: async (target, state) => {
				const r = await route(target);
				if (target === this.uri) throw new ChannelAlreadyExistsError(this.uri);
				await r.router.create(target, state);
			},
			destroy: async (target) => {
				const r = await route(target);
				await r.router.destroy(target, r.expected);
			},
			exec: async (target, name, params) => {
				const r = await route(target);
				return await r.router.exec(target, name, params, r.expected, caller);
			},
			list: (template) => {
				const active = () => this.active(generation);
				const ownUri = this.uri;
				return (async function* () {
					const r = await route(ownUri);
					for await (const row of r.router.list(template)) {
						await active();
						yield row;
					}
				})();
			},
			background: (task) => {
				void (async () => {
					await this.active(generation);
					await task(this.aborter(generation).signal);
				})().catch(() => {});
			},
			abortBackground: () => {
				this.#aborters.get(generation)?.abort();
				this.#aborters.delete(generation);
			},
		};
	}
	private aborter(generation: string): AbortController {
		let aborter = this.#aborters.get(generation);
		if (!aborter) {
			aborter = new AbortController();
			this.#aborters.set(generation, aborter);
		}
		return aborter;
	}
	async exec(name: string, params: unknown, generation?: string, caller?: DistributedCaller): Promise<unknown> {
		return await this.runCommand(name, params, generation, caller);
	}
	async execFrom(caller: DistributedCaller, name: string, params: unknown, generation?: string): Promise<unknown> {
		this.publicRoute();
		return await this.runCommand(name, params, generation, caller);
	}
	private async runCommand(name: string, params: unknown, generation?: string, caller?: DistributedCaller): Promise<unknown> {
		const def = this.match.route.definition;
		if (def.state && !generation) throw distributedError("GENERATION_REQUIRED");
		const command = Object.hasOwn(def.commands, name) ? def.commands[name] : undefined;
		if (!command) throw new UnknownCommandError(this.uri, name);
		const parsed = safeParse(command.params, params);
		if (!parsed.success) throw new InvalidPayloadError(this.uri, name);
		const record = await this.lock(() => this.transaction((tx) => this.record(tx, generation)));
		const ctx: DurableChannelCommandContext<TEnv> = {
			...this.operations(record.generation, caller),
			uri: this.uri,
			params: this.match.params,
			connectionId: caller?.connectionId,
			signal: this.aborter(record.generation).signal,
			state: async () => {
				await this.active(record.generation);
				return (await this.snapshot(record.generation))?.state;
			},
		};
		const result = safeParse(command.result, await command.handler(parsed.output as never, ctx));
		if (!result.success) throw new InvalidResultError(this.uri, name);
		return result.output;
	}
	async notify(name: string, payload: unknown, generation?: string): Promise<void> {
		const notification = this.match.route.definition.notifications[name];
		if (!notification || !Object.hasOwn(this.match.route.definition.notifications, name)) {
			throw new UnknownNotificationError(this.uri, name);
		}
		const parsed = safeParse(notification.payload, payload);
		if (!parsed.success) throw new InvalidPayloadError(this.uri, name);
		const record = await this.lock(() => this.transaction((tx) => this.record(tx, generation)));
		await this.options.publishNotification?.(record.generation, name, parsed.output);
		const members = await this.transaction((tx) =>
			tx.list<DistributedMembership>("member/").map(([, row]) => row).filter((row) => row.active && row.expires > this.clock.now())
		);
		await Promise.allSettled(members.map(async (member) => {
			if (this.options.gateways) {
				await (await this.options.gateways(member.gatewayId)).notify(this.uri, record.generation, name, parsed.output);
			}
		}));
	}
	protected async prearm(): Promise<void> {
		await this.options.scheduler?.arm(this.clock.now() + (this.options.retryMs ?? 1000));
	}
	private kick(): void {
		if (!this.options.gateways) return;
		const task = this.alarm().catch(() => {});
		this.options.waitUntil?.(task);
	}
	private validateMember(request: DistributedMemberRequest): void {
		if (!request.gatewayId || !Number.isSafeInteger(request.revision) || request.revision < 1) throw distributedError("INVALID_MEMBERSHIP");
		if (this.match.route.definition.state && !request.generation) throw distributedError("GENERATION_REQUIRED");
	}
	async resume(request: DistributedMemberRequest): Promise<DistributedResumeEntry> {
		this.publicRoute();
		this.validateMember(request);
		if (!this.options.scheduler || !this.options.gateways) throw distributedError("DELIVERY_HOST_REQUIRED");
		return await this.lock(async () => {
			await this.prearm();
			return await this.transaction((tx) => {
				const record = this.record(tx, request.generation);
				const key = `member/${request.gatewayId}`;
				const old = tx.get<DistributedMembership & { registration: string }>(key);
				const registration = canonicalJson(request.cursor ?? null);
				if (old && old.revision > request.revision) throw distributedError("STALE_MEMBERSHIP");
				if (old && old.revision === request.revision) {
					if (!old.active || old.registration !== registration) throw distributedError("MEMBERSHIP_CONFLICT");
					if (old.expires <= this.clock.now()) throw distributedError("MEMBERSHIP_EXPIRED");
					return this.recovery(tx, record, request.cursor);
				}
				tx.set(key, {
					gatewayId: request.gatewayId,
					revision: request.revision,
					generation: record.generation,
					active: true,
					expires: this.clock.now() + (this.options.leaseMs ?? 30_000),
					ack: request.cursor?.generation === record.generation ? Math.min(request.cursor.channelSeq, record.channelSeq) : 0,
					target: record.channelSeq,
					registration,
				});
				return this.recovery(tx, record, request.cursor);
			});
		});
	}
	async remove(request: DistributedMemberRequest): Promise<void> {
		this.validateMember(request);
		await this.lock(() =>
			this.transaction((tx) => {
				const record = this.record(tx, request.generation);
				const key = `member/${request.gatewayId}`, old = tx.get<DistributedMembership>(key);
				if (old && old.revision > request.revision) throw distributedError("STALE_MEMBERSHIP");
				if (old && old.revision === request.revision && old.active) throw distributedError("MEMBERSHIP_CONFLICT");
				tx.set(key, {
					gatewayId: request.gatewayId,
					revision: request.revision,
					generation: record.generation,
					active: false,
					expires: 0,
					ack: 0,
					target: 0,
				});
			})
		);
	}
	async renew(request: DistributedMemberRequest): Promise<DistributedResumeEntry> {
		this.publicRoute();
		this.validateMember(request);
		return await this.lock(async () => {
			await this.prearm();
			return await this.transaction((tx) => {
				const record = this.record(tx, request.generation);
				const key = `member/${request.gatewayId}`, old = tx.get<DistributedMembership>(key);
				if (!old || old.revision !== request.revision || !old.active) throw distributedError("STALE_MEMBERSHIP");
				if (old.expires <= this.clock.now()) throw distributedError("MEMBERSHIP_EXPIRED");
				tx.set(key, { ...old, expires: Math.max(old.expires, this.clock.now() + (this.options.leaseMs ?? 30_000)) });
				return this.recovery(tx, record, request.cursor);
			});
		});
	}
	/** Compare the captured revision and expiry inside the deleting transaction. */
	async expireMember(candidate: DistributedMembership): Promise<void> {
		await this.lock(() =>
			this.transaction((tx) => {
				const key = `member/${candidate.gatewayId}`, current = tx.get<DistributedMembership>(key);
				if (
					current && current.generation === candidate.generation && current.revision === candidate.revision &&
					current.expires === candidate.expires && current.expires <= this.clock.now()
				) tx.set(key, { ...current, active: false });
			})
		);
	}
	async acknowledge(gatewayId: string, sent: DistributedMembership, ack: DistributedDeliveryAck): Promise<void> {
		await this.lock(() =>
			this.transaction((tx) => {
				const key = `member/${gatewayId}`, current = tx.get<DistributedMembership>(key);
				if (
					!current || !current.active || current.generation !== sent.generation || current.revision !== sent.revision ||
					ack.generation !== sent.generation || ack.revision !== sent.revision
				) return;
				if (!Number.isSafeInteger(ack.channelSeq) || ack.channelSeq < sent.ack || ack.channelSeq > sent.target) {
					throw distributedError("INVALID_ACK");
				}
				tx.set(key, { ...current, ack: Math.max(current.ack, ack.channelSeq), ...(ack.clients === 0 ? { active: false } : {}) });
			})
		);
	}
	/** Called by a durable host alarm. All network work is outside actor mutation serialization. */
	async alarm(): Promise<void> {
		if (!this.options.gateways || this.#closed) return;
		const pending = await this.lock(async () => {
			const wake = await this.transaction((tx) => {
				const now = this.clock.now();
				const times = tx.list<DistributedMembership>("member/").map(([, row]) => row).filter((row) => row.active && row.expires > now)
					.map((row) => row.target > row.ack ? Math.min(row.expires, now + (this.options.retryMs ?? 1000)) : row.expires);
				for (const [, receipt] of tx.list<Receipt>("receipt/")) if (receipt.deadline > now) times.push(receipt.deadline);
				return times.length ? Math.min(...times) : undefined;
			});
			if (wake !== undefined) await this.options.scheduler?.arm(wake);
			return await this.transaction((tx) => {
				this.collectReceipts(tx, Math.max(this.clock.now(), tx.get<number>("expirationFloor") ?? 0));
				const record = tx.get<DistributedChannelRecord>("meta");
				if (!record || record.deleted) return [];
				const work: { member: DistributedMembership; entry: DistributedResumeEntry }[] = [];
				for (const [key, member] of tx.list<DistributedMembership>("member/")) {
					if (member.active && member.expires <= this.clock.now()) {
						tx.set(key, { ...member, active: false });
						continue;
					}
					if (member.active && member.target > member.ack && !this.#deliveries.has(member.gatewayId)) {
						work.push({ member, entry: this.recovery(tx, record, { generation: member.generation, channelSeq: member.ack }) });
					}
				}
				const last = tx.get<string>("deliveryRotation") ?? "";
				const sorted = [...work.filter((w) => w.member.gatewayId > last), ...work.filter((w) => w.member.gatewayId <= last)];
				const selected = sorted.slice(0, Math.max(0, (this.options.deliveryConcurrency ?? 8) - this.#deliveries.size));
				if (selected.length) tx.set("deliveryRotation", selected[selected.length - 1].member.gatewayId);
				return selected;
			});
		});
		await Promise.allSettled(pending.map(async ({ member, entry }) => {
			if (this.#deliveries.has(member.gatewayId)) return;
			this.#deliveries.add(member.gatewayId);
			try {
				const task = (async () =>
					(await this.options.gateways!(member.gatewayId)).deliver({
						channel: this.uri,
						generation: member.generation,
						revision: member.revision,
						entry,
					}))();
				const ack = await distributedTimeout(task, this.options.deliveryTimeoutMs ?? 5000, this.options.timeouts);
				await this.acknowledge(member.gatewayId, member, ack);
			} finally {
				this.#deliveries.delete(member.gatewayId);
			}
		}));
	}

	/** Only this actor's volatile work is cancelled. Other objects are unaffected. */
	close(): void {
		this.#closed = true;
		for (const aborter of this.#aborters.values()) aborter.abort();
		this.#aborters.clear();
	}

	protected collectReceipts(tx: ChannelTransaction, now: number): void {
		tx.set("expirationFloor", now);
		for (const [key, receipt] of tx.list<Receipt>("receipt/")) if (receipt.deadline <= now) tx.delete(key);
	}
	/** GC is explicit so a host alarm can enforce storage limits even on idle channels. */
	async collectExpiredReceipts(): Promise<void> {
		await this.lock(() =>
			this.transaction((tx) => this.collectReceipts(tx, Math.max(this.clock.now(), tx.get<number>("expirationFloor") ?? 0)))
		);
	}
	protected recovery(tx: ChannelTransaction, record: DistributedChannelRecord, cursor?: ChannelCursor): DistributedResumeEntry {
		if (!this.match.route.definition.state) return { type: "stateless" };
		if (
			cursor && cursor.generation === record.generation && cursor.channelSeq <= record.channelSeq &&
			Number.isSafeInteger(cursor.channelSeq) && cursor.channelSeq >= 0
		) {
			const actions = tx.list<DistributedEnvelope>("log/").map(([, e]) => e).filter((e) => e.channelSeq > cursor.channelSeq);
			if (actions.length === record.channelSeq - cursor.channelSeq) return { type: "replay", actions, cursor: this.cursorOf(record) };
		}
		return { type: "snapshot", snapshot: this.snapshotOf(record) };
	}
	/** Capture a consistent cut without membership (trusted diagnostics/tests). Gateways use resume. */
	async inspectResume(cursor?: ChannelCursor): Promise<DistributedResumeEntry> {
		return await this.lock(() => this.transaction((tx) => this.recovery(tx, this.record(tx), cursor)));
	}
}
