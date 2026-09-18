import * as v from "valibot";
import { durableChannel, durableRoutes, RejectAction } from "../../src/mod.ts";
export const routes = durableRoutes().route(
	"counter:/:id",
	durableChannel().state(v.object({ count: v.number() }), { count: 0 })
		.action((a) => a.name("add").payload(v.number()).client().reduce((state, by) => ({ count: state.count + by })))
		.command((c) => c.name("read").params(v.null()).result(v.object({ count: v.number() })).handler((_p, ctx) => ctx.state()))
		.command((c) =>
			c.name("reject").params(v.null()).result(v.null()).handler(() => {
				throw new RejectAction("example refusal");
			})
		)
		.command((c) =>
			c.name("explode").params(v.null()).result(v.null()).handler(() => {
				throw new Error("do-not-leak");
			})
		)
		.notification((n) => n.name("note").payload(v.string())).build(),
).build();
