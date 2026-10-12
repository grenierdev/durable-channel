import { deepStrictEqual, throws } from "node:assert/strict";
import * as v from "valibot";
import { describe, it } from "vitest";
import { def, Hana, hana, JsonRpcError, toJsonRpcErrorObject } from "./hana.ts";

describe("./hana.ts", () => {
	const defs = hana()
		.env<{ user: string; admin: boolean }>()
		.def((b) =>
			b
				.name("ping")
				.summary("Ping the server")
				.params(
					v.object({
						message: v.string(),
						blep: v.optional(v.boolean()),
					}),
				)
				.result(v.number())
				.handler((_params, _ctx) => {
					return 42;
				}),
		)
		.def((b) =>
			b
				.name("boom")
				.params(v.object({}))
				.result(v.null())
				.handler(() => {
					throw new JsonRpcError(-32005, "UNSUPPORTED_PROTOCOL_VERSION: none of the offered versions is supported", {
						supportedVersions: ["0.9.0"],
					});
				}),
		)
		.def((b) =>
			b
				.name("foobar")
				.params(v.tuple([v.string(), v.number()]))
				.result(v.null())
				.handler((_params, _ctx) => {
					return null;
				}),
		)
		.build();

	const app = new Hana(defs);

	it("invoke", async () => {
		deepStrictEqual(await app.invoke("ping", { message: "hello" }, { env: { user: "alice", admin: true } }, AbortSignal.timeout(1000)), 42);
	});

	it("carries the error data of a JsonRpcError onto the wire", async () => {
		deepStrictEqual(toJsonRpcErrorObject(new JsonRpcError(-32050, "plain")), { code: -32050, message: "plain" });
		deepStrictEqual(
			await app.handle(
				{ jsonrpc: "2.0", id: 7, method: "boom", params: {} },
				{ env: { user: "alice", admin: true } },
				AbortSignal.timeout(1000),
			),
			{
				jsonrpc: "2.0",
				id: 7,
				error: {
					code: -32005,
					message: "UNSUPPORTED_PROTOCOL_VERSION: none of the offered versions is supported",
					data: { supportedVersions: ["0.9.0"] },
				},
			},
		);
	});

	it("generateOpenRPCSchema", () => {
		deepStrictEqual(app.generateOpenRPCSchema({ info: { title: "Test API", version: "1.0.0" } }), {
			openrpc: "1.2.6",
			info: {
				title: "Test API",
				version: "1.0.0",
			},
			methods: [
				{
					name: "ping",
					summary: "Ping the server",
					params: [
						{
							name: "message",
							required: true,
							schema: {
								type: "string",
								$schema: "http://json-schema.org/draft-07/schema#",
							},
						},
						{
							name: "blep",
							schema: {
								type: "boolean",
								$schema: "http://json-schema.org/draft-07/schema#",
							},
						},
					],
					result: {
						type: "number",
						$schema: "http://json-schema.org/draft-07/schema#",
					},
				},
				{
					name: "boom",
					params: [],
					result: {
						type: "null",
						$schema: "http://json-schema.org/draft-07/schema#",
					},
				},
				{
					name: "foobar",
					params: [
						{
							schema: {
								type: "string",
								$schema: "http://json-schema.org/draft-07/schema#",
							},
						},
						{
							schema: {
								type: "number",
								$schema: "http://json-schema.org/draft-07/schema#",
							},
						},
					],
					result: {
						type: "null",
						$schema: "http://json-schema.org/draft-07/schema#",
					},
				},
			],
		});
	});
});

describe("Builder", () => {
	it("builds a collection keyed by procedure name", () => {
		const app = hana()
			.env<{ user: string }>()
			.def((b) =>
				b
					.name("ping")
					.summary("Ping the server")
					.params(v.tuple([v.string(), v.number()]))
					.result(v.number())
					.handler((_params, _ctx, _as) => {
						return 42;
					}),
			)
			.def((b) =>
				b
					.name("foobar")
					.result(v.void())
					.handler((_params, _ctx, _as) => {
						return;
					}),
			)
			.build();
		deepStrictEqual(Object.keys(app.procedures), ["ping", "foobar"]);
		deepStrictEqual(app.procedures.ping.summary, "Ping the server");
		deepStrictEqual(app.procedures.foobar.params.type, "object");
	});

	it("throws when a procedure is incomplete", () => {
		throws(
			() =>
				hana()
					.def((b) => b.name("x"))
					.build(),
			Error,
			"must be defined",
		);
		throws(() => def().build(), Error, "must be defined");
	});

	it("is copy-on-write", () => {
		const base = def()
			.name("a")
			.result(v.number())
			.handler(() => 1);
		const forked = base.name("b");
		deepStrictEqual(base.build().name, "a");
		deepStrictEqual(forked.build().name, "b");

		const one = hana().def(base);
		const two = one.def(forked);
		deepStrictEqual(Object.keys(one.build().procedures), ["a"]);
		deepStrictEqual(Object.keys(two.build().procedures), ["a", "b"]);
	});
});
