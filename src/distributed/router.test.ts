import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import * as v from "valibot";
import { durableChannel, type DurableChannelOperations } from "../channel.ts";
import { type DurableChannelRoutes, durableRoutes } from "../routes.ts";
import { DurableChannelHub } from "../hub.ts";
import { MemoryStorage } from "../storage.ts";
import { DurableChannelActor } from "./actor.ts";
import { DurableChannelRouter } from "./router.ts";
import { MemoryChannelStore } from "./memory-store.ts";
import { createDistributedActionId } from "./store.ts";

function deferred() {
	let resolve!: () => void;
	const promise = new Promise<void>((r) => resolve = r);
	return { resolve, promise };
}
function cluster<T>(routes: DurableChannelRoutes<T>, env: T) {
	const actors = new Map<string, DurableChannelActor<T>>();
	const router: DurableChannelRouter<T> = new DurableChannelRouter(routes, {
		resolve: (uri) => {
			let actor = actors.get(uri);
			if (!actor) {
				actor = new DurableChannelActor(uri, routes, { env, store: new MemoryChannelStore(), router });
				actors.set(uri, actor);
			}
			return actor;
		},
	});
	return { router, actors };
}
describe("distributed router and contexts", () => {
	it("runs a shared definition's commands/effects in both runtimes, including self-dispatch", async () => {
		const seen: string[] = [];
		const definition = durableChannel().state(v.number(), 0)
			.action((a) =>
				a.name("add").payload(v.number()).reduce((s, n) => s + n)
					.effect(async (ctx) => {
						seen.push("serverSeq" in ctx.envelope ? "global" : "distributed");
						if (ctx.payload === 1) await ctx.dispatch(ctx.uri, "add", 2);
					})
			)
			.command((c) =>
				c.name("run").params(v.string()).result(v.number()).handler(async (target, ctx) => {
					await ctx.dispatch(target, "add", 1);
					return await ctx.get(target) as number;
				})
			).build();
		const routes = durableRoutes().route("a://", definition).route("b://", definition).build();
		const { router } = cluster(routes, {});
		assertEquals(await router.exec("a://", "run", "b://"), 3);
		const hub = new DurableChannelHub(routes, { storage: new MemoryStorage(), env: {} });
		assertEquals(await hub.exec("a://", "run", "b://"), 3);
		assertEquals(seen, ["distributed", "distributed", "global", "global"]);
		await hub.close();
	});
	it("preserves caller metadata across remote commands and checks public generation/visibility", async () => {
		const definition = durableChannel().state(v.number(), 0)
			.command((c) => c.name("identity").params(v.null()).result(v.string()).handler((_p, ctx) => ctx.connectionId ?? "none"))
			.command((c) =>
				c.name("remote").params(v.null()).result(v.string()).handler((_p, ctx) => ctx.exec("b://", "identity", null) as Promise<string>)
			)
			.build();
		const routes = durableRoutes().route("a://", definition).route("b://", definition).route("hidden://", definition, { internal: true })
			.build();
		const { router } = cluster(routes, {});
		const a = await router.endpoint("a://");
		const generation = (await a.snapshot())!.cursor.generation;
		await assertRejects(() => a.execFrom({ clientId: "stable", connectionId: "socket" }, "remote", null), Error, "GENERATION_REQUIRED");
		assertEquals(await a.execFrom({ clientId: "stable", connectionId: "socket" }, "remote", null, generation), "socket");
		await assertRejects(() => router.endpoint("hidden://", true), Error, "No route matches");
		const hidden = await router.endpoint("hidden://");
		await assertRejects(
			() => hidden.execFrom({ clientId: "stable", connectionId: "socket" }, "identity", null, "fake"),
			Error,
			"No route matches",
		);
	});
	it("fences paused commands, effects and background callbacks after recreation even if they ignore abort", async () => {
		for (const mode of ["command", "effect", "background"] as const) {
			const entered = deferred(), release = deferred(), finished = deferred();
			const failures: string[] = [];
			let signal: AbortSignal | undefined;
			const delayed = async (ctx: DurableChannelOperations) => {
				entered.resolve();
				await release.promise;
				for (
					const task of [
						() => ctx.dispatch("a://", "add", 9),
						() => ctx.destroy("a://"),
						() => ctx.create("family:/fresh"),
						() => ctx.notify("a://", "note", null),
						() => ctx.dispatch("b://", "add", 9),
					]
				) {
					try {
						await task();
						failures.push("unexpected success");
					} catch (e) {
						failures.push(String((e as { code: string }).code));
					}
				}
				finished.resolve();
			};
			const definition = durableChannel().state(v.number(), 0)
				.action((a) =>
					a.name("add").payload(v.number()).client().reduce((s, n) => s + n).effect(async (ctx) => {
						if (mode === "effect") await delayed(ctx);
					})
				)
				.notification((n) => n.name("note").payload(v.null()))
				.command((c) =>
					c.name("run").params(v.null()).result(v.null()).handler(async (_p, ctx) => {
						if (mode === "background") {
							ctx.background((abort) => {
								signal = abort;
								return delayed(ctx);
							});
						} else {
							signal = ctx.signal;
							await delayed(ctx);
						}
						return null;
					})
				).build();
			const routes = durableRoutes().route("a://", definition).route("b://", definition).route("family:/:id", definition).build();
			const { router } = cluster(routes, {});
			const old = (await router.snapshot("a://"))!;
			const work = mode === "effect" ? router.dispatch("a://", "add", 1) : router.exec("a://", "run", null);
			await entered.promise;
			await router.destroy("a://", old.cursor.generation);
			if (mode !== "effect") assertEquals(signal?.aborted, true);
			await router.create("a://");
			release.resolve();
			await finished.promise;
			await work;
			assertEquals(failures, Array(5).fill("STALE_GENERATION"));
			assertEquals(await router.get("a://"), 0);
			assertEquals(await router.get("b://"), 0);
			assertEquals(await router.has("family:/fresh"), false);
		}
	});
	it("does not rerun failed effects on exact retries and keeps their originating commit", async () => {
		let calls = 0;
		const definition = durableChannel().state(v.number(), 0).action((a) =>
			a.name("add").payload(v.number()).client().reduce((s, n) => s + n).effect(async (ctx) => {
				calls++;
				await ctx.dispatch("missing://", "x", null);
			})
		).build();
		const { router } = cluster(durableRoutes().route("a://", definition).build(), {});
		const actor = await router.endpoint("a://");
		const generation = (await actor.snapshot())!.cursor.generation;
		const request = { generation, name: "add", payload: 1, clientSeq: 1, actionId: createDistributedActionId(Date.now() + 1000) };
		const result = await actor.dispatchFrom({ clientId: "c", connectionId: "s" }, request);
		assertEquals(await actor.dispatchFrom({ clientId: "c", connectionId: "s2" }, request), result);
		assertEquals(await router.get("a://"), 1);
		assertEquals(calls, 1);
	});
	it("supports stateless commands and fails unsupported listing explicitly", async () => {
		const definition = durableChannel().command((c) =>
			c.name("ping").params(v.null()).result(v.literal("pong")).handler(() => "pong" as const)
		).build();
		const routes = durableRoutes().route("stateless://", definition).build();
		const { router } = cluster(routes, {});
		assertEquals(await router.exec("stateless://", "ping", null), "pong");
		assertEquals(await router.snapshot("stateless://"), undefined);
		await assertRejects(
			async () => {
				for await (const _ of router.list("stateless://")) { /* no directory */ }
			},
			Error,
			"Distributed list requires",
		);
		const typed = new DurableChannelRouter(routes, { resolve: (uri) => router.endpoint(uri) }).of("stateless://");
		assertEquals(await typed.exec({}, "ping", null), "pong");
		const check = () => {
			// @ts-expect-error Typed handles reject unknown commands.
			void typed.exec({}, "unknown", null);
			// @ts-expect-error Typed handles reject wrong command parameters.
			void typed.exec({}, "ping", 1);
		};
		assert(check);
	});
});
