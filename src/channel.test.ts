import { describe, it } from "node:test";
import { assertEquals, assertThrows } from "@std/assert";
import * as v from "valibot";
import { durableChannel } from "./channel.ts";
import { InvalidDefinitionError, RejectAction } from "./error.ts";

describe("channel", () => {
	const counter = durableChannel()
		.env<{ now(): string }>()
		.state(v.object({ count: v.number() }), { count: 0 })
		.action((a) =>
			a.name("counter/incremented")
				.payload(v.object({ by: v.pipe(v.number(), v.integer()) }))
				.client()
				.reduce((state, payload) => {
					if (payload.by <= 0) {
						throw new RejectAction("by must be positive");
					}
					return { count: state.count + payload.by };
				})
		)
		.action((a) =>
			a.name("counter/reset").payload(v.object({ to: v.optional(v.number(), 0) })).reduce((_state, payload) => ({ count: payload.to }))
		)
		.command((c) =>
			c.name("reset")
				.params(v.object({ to: v.optional(v.number(), 0) }))
				.result(v.object({ count: v.number() }))
				.handler(async ({ to }, ctx) => {
					await ctx.dispatch(ctx.uri, "counter/reset", { to });
					await ctx.notify(ctx.uri, "counter/announced", { text: `reset to ${to}` });
					return { count: to };
				})
		)
		.notification((n) => n.name("counter/announced").payload(v.object({ text: v.string() })))
		.build();

	it("keys every member by name and records the client flag", () => {
		assertEquals(Object.keys(counter.actions), ["counter/incremented", "counter/reset"]);
		assertEquals(Object.keys(counter.commands), ["reset"]);
		assertEquals(Object.keys(counter.notifications), ["counter/announced"]);
		assertEquals(counter.actions["counter/incremented"].client, true);
		assertEquals(counter.actions["counter/reset"].client, false);
	});

	it("parses the initial state so schema defaults are applied once", () => {
		const withDefault = durableChannel().state(v.object({ count: v.optional(v.number(), 7) }), {}).build();
		assertEquals(withDefault.initialState, { count: 7 });
		assertEquals(counter.initialState, { count: 0 });
	});

	it("runs a reducer on the parsed payload", () => {
		const meta = { uri: "counter://", params: {} };
		assertEquals(counter.actions["counter/reset"].reduce({ count: 5 }, { to: 3 }, meta), { count: 3 });
		assertThrows(() => counter.actions["counter/incremented"].reduce({ count: 0 }, { by: -1 }, meta), RejectAction, "by must be positive");
	});

	it("records an effect only where one was declared", () => {
		const audited = durableChannel()
			.env<{ now(): string }>()
			.state(v.object({ count: v.number() }), { count: 0 })
			.action((a) =>
				a.name("counter/reset")
					.payload(v.object({ to: v.number() }))
					.reduce((_state, payload) => ({ count: payload.to }))
					.effect(async (ctx) => {
						await ctx.notify("audit://", "audit/entry", { at: ctx.env.now(), count: ctx.state.count, to: ctx.payload.to });
					})
			)
			.build();
		assertEquals(typeof audited.actions["counter/reset"].effect, "function");
		assertEquals("effect" in counter.actions["counter/reset"], false);
	});

	it("throws when a member is incomplete or declared twice", () => {
		assertThrows(
			() => durableChannel().state(v.object({}), {}).action((a) => a.name("x")).build(),
			InvalidDefinitionError,
			"name, payload and reduce",
		);
		assertThrows(() => durableChannel().command((c) => c.name("x")).build(), InvalidDefinitionError, "name, params, result and handler");
		assertThrows(
			() => durableChannel().notification((n) => n.name("x")).build(),
			InvalidDefinitionError,
			"name and payload must be defined",
		);
		assertThrows(
			() =>
				durableChannel()
					.notification((n) => n.name("dup").payload(v.object({})))
					.notification((n) => n.name("dup").payload(v.object({})))
					.build(),
			InvalidDefinitionError,
			'Duplicate notification "dup"',
		);
	});

	it("is copy-on-write", () => {
		const base = durableChannel().state(v.object({ count: v.number() }), { count: 0 });
		const one = base.action((a) => a.name("a").payload(v.object({})).reduce((state) => state));
		const two = one.action((a) => a.name("b").payload(v.object({})).reduce((state) => state));
		assertEquals(Object.keys(base.build().actions), []);
		assertEquals(Object.keys(one.build().actions), ["a"]);
		assertEquals(Object.keys(two.build().actions), ["a", "b"]);

		const named = durableChannel().notification((n) => n.name("first").payload(v.object({})));
		assertEquals(Object.keys(named.build().notifications), ["first"]);
		assertEquals(Object.keys(named.notification((n) => n.name("second").payload(v.object({}))).build().notifications), ["first", "second"]);
	});

	it("builds a stateless channel with no state, no snapshot and no actions", () => {
		const logs = durableChannel()
			.env<{ now(): string }>()
			.command((c) =>
				c.name("append")
					.params(v.object({ line: v.string() }))
					.result(v.null())
					.handler(async ({ line }, ctx) => {
						await ctx.notify(ctx.uri, "log/line", { line, at: ctx.env.now() });
						return null;
					})
			)
			.notification((n) => n.name("log/line").payload(v.object({ line: v.string(), at: v.string() })))
			.build();
		assertEquals(logs.state, undefined);
		assertEquals(logs.initialState, undefined);
		assertEquals(logs.actions, {});
		assertEquals(Object.keys(logs.commands), ["append"]);

		// @ts-expect-error a stateless builder has no `.action()`: the member appears only after `.state()`.
		type _NoActionBeforeState = ReturnType<typeof durableChannel>["action"];
	});
});
