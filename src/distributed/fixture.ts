import * as v from "valibot";
import { durableChannel } from "../channel.ts";
import { durableRoutes } from "../routes.ts";
import { RejectAction } from "../error.ts";
import { DurableChannelActor } from "./actor.ts";
import { DurableChannelGateway } from "./gateway.ts";
import { DurableChannelRouter } from "./router.ts";
import { MemoryChannelStore } from "./memory-store.ts";
import type { DistributedActorEndpoint, DistributedGatewayEndpoint } from "./interfaces.ts";
import { FakeDistributedTime } from "./testing.ts";
export const distributedTestDefinition = durableChannel().state(v.object({ count: v.number() }), { count: 0 })
	.action((a) =>
		a.name("add").payload(v.number()).client().reduce((s, n) => {
			if (n < 0) throw new RejectAction("positive only");
			return { count: s.count + n };
		})
	)
	.command((c) => c.name("read").params(v.null()).result(v.object({ count: v.number() })).handler((_p, ctx) => ctx.state()))
	.command((c) =>
		c.name("fail").params(v.null()).result(v.null()).handler(() => {
			throw new Error("secret credentials");
		})
	)
	.notification((n) => n.name("note").payload(v.string())).build();
export const distributedTestRoutes = durableRoutes().route("a://", distributedTestDefinition).route("b://", distributedTestDefinition)
	.route("private://", distributedTestDefinition, { internal: true }).build();
export function distributedFixture(historyLimit = 256) {
	const time = new FakeDistributedTime(),
		actors = new Map<string, DurableChannelActor<unknown>>(),
		gateways = new Map<string, DistributedGatewayEndpoint>(),
		endpoints = new Map<string, DistributedActorEndpoint>();
	const router: DurableChannelRouter<unknown> = new DurableChannelRouter(distributedTestRoutes, {
		resolve: (uri) => endpoints.get(uri) ?? actors.get(uri)!,
	});
	for (const uri of ["a://", "b://", "private://"]) {
		actors.set(
			uri,
			new DurableChannelActor<unknown>(uri, distributedTestRoutes, {
				env: {},
				store: new MemoryChannelStore(),
				router,
				historyLimit,
				clock: time,
				timeouts: time,
				retryMs: 10,
				leaseMs: 100,
				deliveryTimeoutMs: 20,
				scheduler: time.durable(uri, () => actors.get(uri)!.alarm()),
				gateways: (id) => gateways.get(id)!,
			}),
		);
	}
	const gateway = (id: string) => {
		const g: DurableChannelGateway<unknown> = new DurableChannelGateway({
			id,
			router,
			store: new MemoryChannelStore(),
			clock: time,
			timeouts: time,
			renewMs: 30,
			timeoutMs: 20,
			scheduler: time.durable(id, () => g.alarm()),
		});
		gateways.set(id, g);
		return g;
	};
	return { time, actors, gateways, endpoints, router, gateway };
}
