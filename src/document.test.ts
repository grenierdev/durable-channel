import { describe, it } from "node:test";
import { assertEquals } from "@std/assert";
import * as v from "valibot";
import { durableChannel } from "./channel.ts";
import { durableRoutes } from "./routes.ts";
import { describeRoutes, DURABLE_CHANNEL_DOCUMENT_VERSION } from "./document.ts";

const DRAFT = "http://json-schema.org/draft-07/schema#";

const counter = durableChannel()
	.env<{ now(): string }>()
	.summary("A number everyone can bump")
	.state(v.object({ count: v.number(), updatedAt: v.optional(v.string()) }), { count: 0 })
	.action((a) =>
		a.name("counter/incremented")
			.summary("Adds `by` to the count")
			.payload(v.object({ by: v.pipe(v.number(), v.integer()) }))
			.client()
			.reduce((state, payload) => ({ ...state, count: state.count + payload.by }))
	)
	.action((a) =>
		a.name("counter/reset").payload(v.object({ to: v.optional(v.number(), 0) })).reduce((_state, payload) => ({ count: payload.to }))
	)
	.command((c) =>
		c.name("reset")
			.summary("Resets and announces")
			.params(v.object({ to: v.optional(v.number(), 0), reason: v.string() }))
			.result(v.object({ count: v.number() }))
			.handler(async ({ to }, ctx) => {
				await ctx.dispatch(ctx.uri, "counter/reset", { to });
				return { count: to };
			})
	)
	.command((c) => c.name("pair").params(v.tuple([v.string(), v.number()])).result(v.null()).handler(() => null))
	.command((c) => c.name("raw").params(v.string()).result(v.custom<{ a: number }>(() => true)).handler(() => ({ a: 1 })))
	.notification((n) => n.name("counter/announced").summary("Someone reset the counter").payload(v.object({ text: v.string() })))
	.build();

const log = durableChannel()
	.env<{ now(): string }>()
	.summary("A relay")
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

const index = durableChannel().state(v.object({ hits: v.number() }), { hits: 0 }).build();

const routes = durableRoutes()
	.env<{ now(): string }>()
	.route("counter://", counter)
	.route("log:/:stream", log, { summary: "One log stream per name" })
	.route("x-index://", index, { internal: true })
	.build();

describe("document", () => {
	it("describes every route with the JSON Schema of its state, actions, commands and notifications", () => {
		assertEquals(describeRoutes(routes, { info: { title: "Probe", version: "0.0.1" } }), {
			opendurablechannel: DURABLE_CHANNEL_DOCUMENT_VERSION,
			info: { title: "Probe", version: "0.0.1" },
			channels: [
				{
					template: "counter://",
					summary: "A number everyone can bump",
					params: [],
					singleton: true,
					internal: false,
					state: {
						schema: {
							type: "object",
							properties: { count: { type: "number" }, updatedAt: { type: "string" } },
							required: ["count"],
							$schema: DRAFT,
						},
						initial: { count: 0 },
					},
					actions: [
						{
							name: "counter/incremented",
							summary: "Adds `by` to the count",
							client: true,
							payload: { type: "object", properties: { by: { type: "integer" } }, required: ["by"], $schema: DRAFT },
						},
						{
							name: "counter/reset",
							client: false,
							payload: { type: "object", properties: { to: { type: "number", default: 0 } }, required: [], $schema: DRAFT },
						},
					],
					commands: [
						{
							name: "reset",
							summary: "Resets and announces",
							params: [
								{ name: "to", schema: { type: "number", default: 0, $schema: DRAFT } },
								{ name: "reason", required: true, schema: { type: "string", $schema: DRAFT } },
							],
							result: { type: "object", properties: { count: { type: "number" } }, required: ["count"], $schema: DRAFT },
						},
						{
							name: "pair",
							params: [{ schema: { type: "string", $schema: DRAFT } }, { schema: { type: "number", $schema: DRAFT } }],
							result: { type: "null", $schema: DRAFT },
						},
						{
							name: "raw",
							params: [{ schema: { type: "string", $schema: DRAFT } }],
							// `v.custom` has no JSON Schema equivalent: it is documented as "anything" rather than failing the document.
							result: { $schema: DRAFT },
						},
					],
					notifications: [
						{
							name: "counter/announced",
							summary: "Someone reset the counter",
							payload: { type: "object", properties: { text: { type: "string" } }, required: ["text"], $schema: DRAFT },
						},
					],
				},
				{
					template: "log:/:stream",
					// The route option wins over the definition's own summary.
					summary: "One log stream per name",
					params: [{ name: "stream", schema: { type: "string", minLength: 1, $schema: DRAFT } }],
					singleton: false,
					internal: false,
					actions: [],
					commands: [
						{
							name: "append",
							params: [{ name: "line", required: true, schema: { type: "string", $schema: DRAFT } }],
							result: { type: "null", $schema: DRAFT },
						},
					],
					notifications: [
						{
							name: "log/line",
							payload: {
								type: "object",
								properties: { line: { type: "string" }, at: { type: "string" } },
								required: ["line", "at"],
								$schema: DRAFT,
							},
						},
					],
				},
				{
					template: "x-index://",
					params: [],
					singleton: true,
					internal: true,
					state: {
						schema: { type: "object", properties: { hits: { type: "number" } }, required: ["hits"], $schema: DRAFT },
						initial: { hits: 0 },
					},
					actions: [],
					commands: [],
					notifications: [],
				},
			],
		});
	});

	it("falls back to a default info block", () => {
		assertEquals(describeRoutes(durableRoutes().build()).info, { title: "Durable Channel Hub", version: "1.0.0" });
		assertEquals(describeRoutes(durableRoutes().build()).channels, []);
	});

	it("keeps summaries on the built definitions", () => {
		assertEquals(counter.summary, "A number everyone can bump");
		assertEquals(counter.actions["counter/incremented"].summary, "Adds `by` to the count");
		assertEquals(counter.actions["counter/reset"].summary, undefined);
		assertEquals(counter.commands.reset.summary, "Resets and announces");
		assertEquals(counter.notifications["counter/announced"].summary, "Someone reset the counter");
		assertEquals(log.summary, "A relay");
		assertEquals(index.summary, undefined);
	});
});
