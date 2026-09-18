import type { DurableChannelInstance } from "../channel.ts";
import type {
	ChannelCursor,
	DistributedDelivery,
	DistributedDeliveryAck,
	DistributedDispatchRequest,
	DistributedDispatchResult,
	DistributedEnvelope,
	DistributedMemberRequest,
	DistributedResumeEntry,
	DistributedSnapshot,
} from "./protocol.ts";

/** JSON values only. A body is synchronous and must not perform RPC or other external I/O. */
export interface ChannelTransaction {
	get<T>(key: string): T | undefined;
	set(key: string, value: unknown): void;
	delete(key: string): void;
	list<T>(prefix: string): [string, T][];
}
export interface ChannelStore {
	/** Resolve after durability. On ambiguous failure callers reload receipts before repeating work. */
	transaction<T>(body: (tx: ChannelTransaction) => T): Promise<T>;
}
export interface DistributedClock {
	now(): number;
}
export const systemClock: DistributedClock = { now: () => Date.now() };
/** Host must durably arm before resolving. arm only moves a pending wakeup earlier. */
export interface DistributedScheduler {
	arm(at: number): Promise<void>;
}
/** Serializable caller metadata supplied by an authenticated host, never by dispatch JSON. */
export interface DistributedCaller {
	clientId: string;
	connectionId: string;
}
/** One endpoint owns exactly one URI. Trusted methods must never be exposed as public JSON-RPC methods. */
export interface DistributedActorEndpoint {
	snapshot(generation?: string): Promise<DistributedSnapshot | undefined>;
	create(state?: unknown): Promise<DistributedSnapshot>;
	destroy(generation: string): Promise<void>;
	dispatch(generation: string, name: string, payload: unknown): Promise<DistributedEnvelope>;
	dispatchFrom(caller: DistributedCaller, request: DistributedDispatchRequest): Promise<DistributedDispatchResult>;
	exec(name: string, params: unknown, generation?: string, caller?: DistributedCaller): Promise<unknown>;
	execFrom(caller: DistributedCaller, name: string, params: unknown, generation?: string): Promise<unknown>;
	notify(name: string, payload: unknown, generation?: string): Promise<void>;
	resume(request: DistributedMemberRequest): Promise<DistributedResumeEntry>;
	remove(request: DistributedMemberRequest): Promise<void>;
	renew(request: DistributedMemberRequest): Promise<DistributedResumeEntry>;
	alarm(): Promise<void>;
}
export type DistributedOwnerResolver = (uri: string) => DistributedActorEndpoint | Promise<DistributedActorEndpoint>;
export interface DistributedGatewayEndpoint {
	deliver(message: DistributedDelivery): Promise<DistributedDeliveryAck>;
	notify(channel: string, generation: string, name: string, payload: unknown): Promise<void>;
}
export type DistributedGatewayResolver = (gatewayId: string) => DistributedGatewayEndpoint | Promise<DistributedGatewayEndpoint>;
/** The provider defines its consistency (for example eventual). No built-in distributed scan exists. */
export interface DistributedDirectory {
	readonly consistency: string;
	list(template: string): AsyncIterable<DurableChannelInstance>;
}
export interface DistributedSession {
	readonly id: string;
	readonly clientId: string;
	send(message: import("./protocol.ts").DistributedMessage): void | Promise<void>;
	close(reason: string): void;
}
export interface DistributedClientScheduler {
	schedule(delayMs: number, task: () => void): () => void;
}
export const distributedClientScheduler: DistributedClientScheduler = {
	schedule(delay, task) {
		const timer = setTimeout(task, delay);
		return () => clearTimeout(timer);
	},
};
export interface DistributedSubscriptionRecord {
	connectionId: string;
	clientId: string;
	uri: string;
	epoch: number;
	cursor?: ChannelCursor;
}
