import { deepStrictEqual, throws } from "node:assert/strict";
import * as v from "valibot";
import { describe, it } from "vitest";
import { durableChannel } from "./channel.ts";
import { InvalidDefinitionError } from "./error.ts";
import { durableRoutes, isSingleton, matchRoute, type PathToParams, resolveUri } from "./routes.ts";

describe("routes", () => {
	const counter = durableChannel()
		.state(v.object({ count: v.number() }), { count: 0 })
		.build();
	const doc = durableChannel()
		.state(v.object({ title: v.string() }), { title: "" })
		.build();
	const session = durableChannel()
		.state(v.object({ title: v.string() }), { title: "" })
		.build();
	const pair = durableChannel().state(v.object({}), {}).build();

	const routes = durableRoutes()
		.env<{ now(): string }>()
		.route("counter://", counter)
		.route("doc:/:id", doc)
		.route("agent-session:/:uid", session)
		.route("a/:x/b/:y", pair)
		.build();

	it("matches literal and parameter segments without knowing any scheme", () => {
		deepStrictEqual(matchRoute(routes, "counter://")?.route.template, "counter://");
		deepStrictEqual(matchRoute(routes, "counter://")?.params, {});
		deepStrictEqual(matchRoute(routes, "doc:/42")?.params, { id: "42" });
		deepStrictEqual(matchRoute(routes, "agent-session:/9e1")?.params, { uid: "9e1" });
		deepStrictEqual(matchRoute(routes, "a/1/b/2")?.params, { x: "1", y: "2" });
	});

	it("refuses a URI whose shape does not line up", () => {
		deepStrictEqual(matchRoute(routes, "counter:/"), undefined);
		deepStrictEqual(matchRoute(routes, "doc:/"), undefined);
		deepStrictEqual(matchRoute(routes, "doc:/1/2"), undefined);
		deepStrictEqual(matchRoute(routes, "a/1/b/"), undefined);
		deepStrictEqual(matchRoute(routes, "nope://"), undefined);
	});

	it("marks a route internal only where it was asked for", () => {
		const withPrivate = durableRoutes().route("doc:/:id", doc).route("x-catalog://", counter, { internal: true }).build();
		deepStrictEqual(
			withPrivate.routes.map((route) => [route.template, route.internal]),
			[
				["doc:/:id", false],
				["x-catalog://", true],
			],
		);
		deepStrictEqual(
			routes.routes.every((route) => !route.internal),
			true,
		);
	});

	it("tells a singleton from a family", () => {
		deepStrictEqual(
			routes.routes.filter(isSingleton).map((route) => route.template),
			["counter://"],
		);
	});

	it("resolves a template back into a URI", () => {
		deepStrictEqual(resolveUri("counter://", {}), "counter://");
		deepStrictEqual(resolveUri("doc:/:id", { id: "42" }), "doc:/42");
		deepStrictEqual(resolveUri("a/:x/b/:y", { x: "1", y: "2" }), "a/1/b/2");
		throws(() => resolveUri("doc:/:id", {}), InvalidDefinitionError, 'needs a value for ":id"');
		throws(() => resolveUri("doc:/:id", { id: "" }), InvalidDefinitionError, 'needs a value for ":id"');
	});

	it("types a template's parameters", () => {
		const forDoc: PathToParams<"doc:/:id"> = { id: "1" };
		const forPair: PathToParams<"a/:x/b/:y"> = { x: "1", y: "2" };
		const forSingleton: PathToParams<"counter://"> = {};
		deepStrictEqual([forDoc, forPair, forSingleton], [{ id: "1" }, { x: "1", y: "2" }, {}]);
	});

	it("is copy-on-write and refuses a duplicate template", () => {
		const base = durableRoutes().route("counter://", counter);
		const wider = base.route("doc:/:id", doc);
		deepStrictEqual(
			base.build().routes.map((route) => route.template),
			["counter://"],
		);
		deepStrictEqual(
			wider.build().routes.map((route) => route.template),
			["counter://", "doc:/:id"],
		);
		throws(() => base.route("counter://", counter), InvalidDefinitionError, 'Duplicate route "counter://"');
	});
});
