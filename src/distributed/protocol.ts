/** The distributed wire contract. No field implies an order across channels. */
import * as v from "valibot";
import type { DurableChannelDistributedEnvelope, DurableChannelNotification } from "../channel.ts";
export const DISTRIBUTED_PROTOCOL = "durable-channel/distributed-1" as const;
export type DistributedEnvelope = DurableChannelDistributedEnvelope;
export interface ChannelCursor {
	generation: string;
	channelSeq: number;
}
export interface DistributedSnapshot {
	resource: string;
	state: unknown;
	cursor: ChannelCursor;
}
export type DistributedResumeEntry =
	| { type: "replay"; actions: DistributedEnvelope[]; cursor: ChannelCursor }
	| { type: "snapshot"; snapshot: DistributedSnapshot }
	| { type: "stateless" }
	| { type: "missing" };
export interface DistributedReconnectResult {
	channels: Record<string, DistributedResumeEntry>;
}
export type DistributedDispatchResult = { type: "committed"; envelope: DistributedEnvelope } | { type: "unknown"; actionId: string };
export type DistributedMessage = DistributedEnvelope | DurableChannelNotification | {
	type: "recovery";
	channel: string;
	entry: DistributedResumeEntry;
	revision?: number;
};
export interface DistributedDispatchRequest {
	generation: string;
	actionId: string;
	clientSeq: number;
	name: string;
	payload: unknown;
}
export interface DistributedMemberRequest {
	gatewayId: string;
	revision: number;
	generation?: string;
	cursor?: ChannelCursor;
}
export interface DistributedMembership {
	gatewayId: string;
	revision: number;
	generation: string;
	active: boolean;
	expires: number;
	ack: number;
	target: number;
}
export interface DistributedDelivery {
	channel: string;
	generation: string;
	revision: number;
	entry: DistributedResumeEntry;
}
export interface DistributedDeliveryAck {
	revision: number;
	generation: string;
	channelSeq: number;
	clients: number;
}
export const DistributedSequence: v.GenericSchema<number> = v.pipe(v.number(), v.safeInteger(), v.minValue(0));
export const DistributedGeneration: v.GenericSchema<string> = v.pipe(v.string(), v.minLength(1), v.maxLength(128));
export const DistributedCursorSchema: v.GenericSchema<ChannelCursor> = v.strictObject({
	generation: DistributedGeneration,
	channelSeq: DistributedSequence,
});
export const DistributedSnapshotSchema: v.GenericSchema<DistributedSnapshot> = v.strictObject({
	resource: v.string(),
	state: v.unknown(),
	cursor: DistributedCursorSchema,
});
export const DistributedEnvelopeSchema: v.GenericSchema<DistributedEnvelope> = v.strictObject({
	type: v.literal("action"),
	channel: v.string(),
	name: v.string(),
	payload: v.unknown(),
	generation: DistributedGeneration,
	channelSeq: v.pipe(DistributedSequence, v.minValue(1)),
	actionId: v.exactOptional(v.string()),
	origin: v.exactOptional(v.strictObject({ clientId: v.string(), clientSeq: DistributedSequence })),
	rejectionReason: v.exactOptional(v.string()),
});
export const DistributedResumeSchema: v.GenericSchema<DistributedResumeEntry> = v.variant("type", [
	v.strictObject({ type: v.literal("replay"), actions: v.array(DistributedEnvelopeSchema), cursor: DistributedCursorSchema }),
	v.strictObject({ type: v.literal("snapshot"), snapshot: DistributedSnapshotSchema }),
	v.strictObject({ type: v.literal("stateless") }),
	v.strictObject({ type: v.literal("missing") }),
]);
export const DistributedReconnectSchema: v.GenericSchema<DistributedReconnectResult> = v.strictObject({
	channels: v.record(v.string(), DistributedResumeSchema),
});
export const DistributedHelloSchema: v.GenericSchema<
	{ protocol: typeof DISTRIBUTED_PROTOCOL; clientId?: string; subscriptions?: string[]; cursors?: Record<string, ChannelCursor> },
	{ protocol: typeof DISTRIBUTED_PROTOCOL; clientId?: string; subscriptions: string[]; cursors: Record<string, ChannelCursor> }
> = v.strictObject({
	protocol: v.literal(DISTRIBUTED_PROTOCOL),
	clientId: v.optional(v.string()),
	subscriptions: v.optional(v.array(v.string()), []),
	cursors: v.optional(v.record(v.string(), DistributedCursorSchema), {}),
});
export const DistributedHelloResultSchema: v.GenericSchema<
	DistributedReconnectResult & { protocol: typeof DISTRIBUTED_PROTOCOL; clientId: string }
> = v
	.strictObject({
		protocol: v.literal(DISTRIBUTED_PROTOCOL),
		clientId: v.string(),
		channels: v.record(v.string(), DistributedResumeSchema),
	});
export const DistributedDispatchSchema: v.GenericSchema<DistributedDispatchRequest & { channel: string }> = v.strictObject({
	channel: v.string(),
	generation: DistributedGeneration,
	actionId: v.pipe(v.string(), v.minLength(1), v.maxLength(160)),
	clientSeq: DistributedSequence,
	name: v.string(),
	payload: v.unknown(),
});
export const DistributedDispatchResultSchema: v.GenericSchema<DistributedDispatchResult> = v.variant("type", [
	v.strictObject({ type: v.literal("committed"), envelope: DistributedEnvelopeSchema }),
	v.strictObject({ type: v.literal("unknown"), actionId: v.string() }),
]);
