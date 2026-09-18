import { describe, it } from "node:test";
import { assert, assertEquals } from "@std/assert";
import * as v from "valibot";
import type {
	DurableChannelCommit,
	DurableChannelDistributedEnvelope,
	DurableChannelEnvelope,
	DurableChannelOperations,
} from "../channel.ts";
import { DISTRIBUTED_PROTOCOL, DistributedCursorSchema, DistributedEnvelopeSchema, DistributedHelloSchema } from "./protocol.ts";

describe("distributed protocol", () => {
	it("rejects unsafe sequences, malformed generations, global fields and protocol mismatches", () => {
		for (const channelSeq of [-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1]) {
			assertEquals(v.safeParse(DistributedCursorSchema, { generation: "g", channelSeq }).success, false);
		}
		assert(v.safeParse(DistributedCursorSchema, { generation: "g", channelSeq: Number.MAX_SAFE_INTEGER }).success);
		assertEquals(v.safeParse(DistributedCursorSchema, { generation: "", channelSeq: 0 }).success, false);
		const action = { type: "action", channel: "a", name: "x", payload: {}, generation: "g", channelSeq: 1 };
		assert(v.safeParse(DistributedEnvelopeSchema, action).success);
		assertEquals(v.safeParse(DistributedEnvelopeSchema, { ...action, serverSeq: 1 }).success, false);
		assertEquals(v.safeParse(DistributedEnvelopeSchema, { ...action, channelSeq: undefined, serverSeq: 1 }).success, false);
		for (const hello of [{}, { protocol: "global" }]) assertEquals(v.safeParse(DistributedHelloSchema, hello).success, false);
		assertEquals(v.parse(DistributedHelloSchema, { protocol: DISTRIBUTED_PROTOCOL }).cursors, {});
	});
	it("truthfully narrows common commits without inventing sequence fields", () => {
		const common = (commit: DurableChannelCommit): number => "serverSeq" in commit ? commit.serverSeq : commit.channelSeq;
		const global: DurableChannelEnvelope = { type: "action", channel: "a", name: "x", payload: {}, serverSeq: 7 };
		const distributed: DurableChannelDistributedEnvelope = {
			type: "action",
			channel: "a",
			name: "x",
			payload: {},
			generation: "g",
			channelSeq: 2,
		};
		assertEquals([common(global), common(distributed)], [7, 2]);
		const check = (_ops: DurableChannelOperations, commit: DurableChannelCommit) => {
			// @ts-expect-error Shared callbacks must narrow before inspecting a global sequence.
			const seq: number = commit.serverSeq;
			return seq;
		};
		void check;
	});
});
