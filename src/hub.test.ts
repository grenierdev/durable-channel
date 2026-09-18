import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import * as v from "valibot";
import { durableChannel, type DurableChannelMessage } from "./channel.ts";
import { durableRoutes } from "./routes.ts";
import { DurableChannelHub } from "./hub.ts";
import { describeRoutes } from "./document.ts";
import { DenoKvStorage, type DurableChannelStorage, MemoryStorage } from "./storage.ts";
import {
	ChannelAlreadyExistsError,
	ChannelNotFoundError,
	ConnectionNotFoundError,
	InvalidPayloadError,
	NotClientDispatchableError,
	RejectAction,
	RouteNotFoundError,
	StatelessChannelError,
	UnknownActionError,
	UnknownCommandError,
} from "./error.ts";

type Env = { now(): string };

const counter = durableChannel()
	.env<Env>()
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
			.result(v.object({ count: v.number(), at: v.string() }))
			.handler(async ({ to }, ctx) => {
				await ctx.dispatch(ctx.uri, "counter/reset", { to });
				await ctx.notify(ctx.uri, "counter/announced", { text: `reset to ${to}` });
				return { count: to, at: ctx.env.now() };
			})
	)
	.notification((n) => n.name("counter/announced").payload(v.object({ text: v.string() })))
	.build();

const doc = durableChannel()
	.env<Env>()
	.state(v.object({ title: v.string(), revision: v.optional(v.number(), 0) }), { title: "untitled" })
	.action((a) =>
		a.name("doc/renamed")
			.payload(v.object({ title: v.string() }))
			.client()
			.reduce((state, payload) => {
				if (payload.title === "") {
					throw new RejectAction("title must not be blank");
				}
				return { ...state, title: payload.title };
			})
			.effect(async (ctx) => {
				await ctx.notify("catalog://", "catalog/docRenamed", { uri: ctx.uri, title: ctx.state.title, at: ctx.env.now() });
				await ctx.dispatch(ctx.uri, "doc/touched", {});
			})
	)
	.action((a) =>
		a.name("doc/titled").payload(v.object({ title: v.string() })).reduce((state, payload) => ({ ...state, title: payload.title }))
	)
	.action((a) => a.name("doc/touched").payload(v.object({})).reduce((state) => ({ ...state, revision: state.revision + 1 })))
	.command((c) =>
		c.name("stamp")
			.params(v.object({}))
			.result(v.object({ by: v.string(), aborted: v.boolean() }))
			.handler((_params, ctx) => ({ by: ctx.connectionId ?? "server", aborted: ctx.signal.aborted }))
	)
	.build();

/**
 * A private index over the `doc:/:id` family: the recipe a channel follows when it needs a catalogue
 * of another channel's instances. Its command creates an instance of another template and dispatches
 * to it, which is the whole reason a command exists.
 */
const catalog = durableChannel()
	.env<Env>()
	.state(v.object({ docs: v.array(v.string()) }), { docs: [] })
	.action((a) =>
		a.name("catalog/docAdded").payload(v.object({ uri: v.string() })).reduce((state, payload) => ({
			docs: [...state.docs, payload.uri],
		}))
	)
	.command((c) =>
		c.name("createDoc")
			.params(v.object({ id: v.string(), title: v.string() }))
			.result(v.object({ uri: v.string(), by: v.string() }))
			.handler(async ({ id, title }, ctx) => {
				const uri = `doc:/${id}`;
				await ctx.create(uri);
				await ctx.dispatch(uri, "doc/titled", { title });
				await ctx.dispatch(ctx.uri, "catalog/docAdded", { uri });
				await ctx.notify(ctx.uri, "catalog/docRenamed", { uri, title, at: ctx.env.now() });
				return { uri, by: ctx.connectionId ?? "server" };
			})
	)
	.command((c) =>
		c.name("stampDoc")
			.params(v.object({ id: v.string() }))
			.result(v.object({ by: v.string(), aborted: v.boolean() }))
			.handler(async ({ id }, ctx) => await ctx.exec(`doc:/${id}`, "stamp", {}) as { by: string; aborted: boolean })
	)
	.notification((n) => n.name("catalog/docRenamed").payload(v.object({ uri: v.string(), title: v.string(), at: v.string() })))
	.build();

/** A private index: reachable by the hub, its commands and its effects, invisible to every connection. */
const index = durableChannel()
	.env<Env>()
	.state(v.object({ hits: v.number() }), { hits: 0 })
	.action((a) => a.name("index/hit").payload(v.object({})).client().reduce((state) => ({ hits: state.hits + 1 })))
	.command((c) => c.name("count").params(v.object({})).result(v.number()).handler(async (_params, ctx) => (await ctx.state()).hits))
	.build();

const log = durableChannel()
	.env<Env>()
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

const routes = durableRoutes()
	.env<Env>()
	.route("counter://", counter)
	.route("doc:/:id", doc)
	.route("catalog://", catalog)
	.route("log:/:stream", log)
	.route("x-index://", index, { internal: true })
	.build();

function boot(options?: { storage?: DurableChannelStorage; replayLimit?: number }) {
	const storage = options?.storage ?? new MemoryStorage();
	const hub = new DurableChannelHub(routes, {
		storage,
		env: { now: () => "2026-09-07T00:00:00.000Z" },
		...(options?.replayLimit !== undefined ? { replayLimit: options.replayLimit } : {}),
	});
	return { hub, storage };
}

function spy(id: string) {
	const received: DurableChannelMessage[] = [];
	return { connection: { id, send: (message: DurableChannelMessage) => received.push(message) }, received };
}

/** Resolves once every already-queued microtask and the current macrotask have run. */
function flush(): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, 0));
}

/**
 * A hub whose only channel hands a background task to the hub and lets the test step through it one
 * dispatch at a time: the task parks on `env.step()`, which the test resolves when it wants the next
 * iteration. A cooperative task returns as soon as it sees its signal aborted; a blind one dispatches
 * anyway, which is how the test observes that the hub swallows what a task throws.
 */
function worker() {
	const gates: (() => void)[] = [];
	const seen: boolean[] = [];
	type WorkerEnv = { step(): Promise<void> };
	const definition = durableChannel()
		.env<WorkerEnv>()
		.state(v.object({ steps: v.number() }), { steps: 0 })
		.action((a) => a.name("worker/stepped").payload(v.object({})).reduce((state) => ({ steps: state.steps + 1 })))
		.command((c) =>
			c.name("run")
				.params(v.object({ cooperative: v.boolean() }))
				.result(v.object({ first: v.number() }))
				.handler(async ({ cooperative }, ctx) => {
					const first = await ctx.dispatch(ctx.uri, "worker/stepped", {});
					ctx.background(async (signal) => {
						for (let index = 0; index < 3; index += 1) {
							await ctx.env.step();
							seen.push(signal.aborted);
							if (cooperative && signal.aborted) {
								return;
							}
							await ctx.dispatch(ctx.uri, "worker/stepped", {});
						}
					});
					if (!("serverSeq" in first)) throw new Error("Expected a global commit");
					return { first: first.serverSeq };
				})
		)
		.build();
	const hub = new DurableChannelHub(durableRoutes().env<WorkerEnv>().route("worker:/:id", definition).build(), {
		storage: new MemoryStorage(),
		env: { step: () => new Promise<void>((resolve) => gates.push(resolve)) },
	});
	return {
		hub,
		seen,
		async release(): Promise<void> {
			gates.shift()?.();
			await flush();
		},
		releaseAll(): void {
			for (const gate of gates.splice(0)) {
				gate();
			}
		},
	};
}

describe("DurableChannelHub", () => {
	it("auto-creates a singleton on first touch and snapshots it at the current sequence", async () => {
		const { hub, storage } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		assertEquals(await hub.subscribe("alice", "counter://"), { resource: "counter://", state: { count: 0 }, fromSeq: 0 });
		assertEquals(await storage.get(["channel", "counter://", "counter://"]), { count: 0 });
		assertEquals(await hub.has("counter://"), true);
		assertEquals(await hub.has("nope://"), false);
	});

	it("refuses an unknown route, a missing instance and an unknown connection", async () => {
		const { hub } = boot();
		hub.connect(spy("alice").connection);
		await assertRejects(() => hub.subscribe("alice", "nope://"), RouteNotFoundError);
		await assertRejects(() => hub.subscribe("alice", "doc:/1"), ChannelNotFoundError);
		await assertRejects(() => hub.subscribe("ghost", "counter://"), ConnectionNotFoundError);
	});

	it("broadcasts a server dispatch to subscribers only and bumps one global sequence", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		const bob = spy("bob");
		hub.connect(alice.connection);
		hub.connect(bob.connection);
		await hub.subscribe("alice", "counter://");
		const envelope = await hub.dispatch("counter://", "counter/incremented", { by: 2 });
		assertEquals(envelope, { type: "action", channel: "counter://", name: "counter/incremented", payload: { by: 2 }, serverSeq: 1 });
		assertEquals(hub.serverSeq, 1);
		assertEquals(alice.received, [envelope]);
		assertEquals(bob.received, []);
		assertEquals(await hub.get("counter://"), { count: 2 });
		await assertRejects(() => hub.dispatch("counter://", "counter/nope", {}), UnknownActionError);
		await assertRejects(() => hub.dispatch("counter://", "counter/incremented", { by: 1.5 }), InvalidPayloadError);
	});

	it("stamps a client dispatch with its origin and honours the client flag", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.subscribe("alice", "counter://");
		const envelope = await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: 3 }, 1);
		assertEquals(envelope?.origin, { clientId: "alice", clientSeq: 1 });
		await assertRejects(() => hub.dispatchFrom("alice", "counter://", "counter/reset", { to: 0 }, 2), NotClientDispatchableError);
		await assertRejects(() => hub.dispatchFrom("alice", "counter://", "counter/nope", {}, 3), UnknownActionError);
		await assertRejects(() => hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: "x" }, 4), InvalidPayloadError);
		await assertRejects(() => hub.dispatchFrom("ghost", "counter://", "counter/incremented", { by: 1 }, 1), ConnectionNotFoundError);
	});

	it("turns an unacceptable client dispatch into a rejected echo when lenient", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.subscribe("alice", "counter://");
		const lenient = { lenient: true };
		const unknown = await hub.dispatchFrom("alice", "counter://", "counter/nope", {}, 1, lenient);
		const serverOnly = await hub.dispatchFrom("alice", "counter://", "counter/reset", { to: 5 }, 2, lenient);
		const badPayload = await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: "x" }, 3, lenient);
		const refused = await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: -1 }, 4, lenient);
		assert(unknown?.rejectionReason?.startsWith("UNKNOWN_ACTION"));
		assert(serverOnly?.rejectionReason?.startsWith("NOT_CLIENT_DISPATCHABLE"));
		assert(badPayload?.rejectionReason?.startsWith("INVALID_PAYLOAD"));
		assertEquals(refused?.rejectionReason, "by must be positive");
		assertEquals(await hub.get("counter://"), { count: 0 });
		assertEquals(hub.serverSeq, 4);
		assertEquals(alice.received.length, 4);
	});

	it("ignores a clientSeq that is not greater than the link's watermark", async () => {
		const { hub } = boot();
		hub.connect(spy("alice").connection);
		await hub.subscribe("alice", "counter://");
		assert(await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: 1 }, 4) !== undefined);
		assertEquals(await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: 1 }, 4), undefined);
		assertEquals(await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: 1 }, 2), undefined);
		assertEquals(hub.serverSeq, 1);
		assertEquals(await hub.get("counter://"), { count: 1 });
	});

	it("gives a command parsed params, a typed result, and the right to dispatch and notify", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.subscribe("alice", "counter://");
		await hub.dispatch("counter://", "counter/incremented", { by: 9 });
		assertEquals(await hub.exec("counter://", "reset", {}), { count: 0, at: "2026-09-07T00:00:00.000Z" });
		assertEquals(await hub.get("counter://"), { count: 0 });
		assertEquals(alice.received.map((message) => message.type), ["action", "action", "notification"]);
		assertEquals(alice.received[2], {
			type: "notification",
			channel: "counter://",
			name: "counter/announced",
			payload: { text: "reset to 0" },
		});
		await assertRejects(() => hub.exec("counter://", "nope", {}), UnknownCommandError);
		await assertRejects(() => hub.exec("counter://", "reset", { to: "x" }), InvalidPayloadError);
	});

	it("never stores a notification", async () => {
		const { hub, storage } = boot();
		hub.connect(spy("alice").connection);
		await hub.subscribe("alice", "counter://");
		await hub.notify("counter://", "counter/announced", { text: "hi" });
		assertEquals(hub.serverSeq, 0);
		const page = await storage.list({ prefix: ["channel"] });
		assertEquals(page.entries.map((entry) => entry.value), [{ count: 0 }]);
	});

	it("creates and lists the instances of a family with their state, in key order", async () => {
		const { hub } = boot();
		await hub.create("doc:/2", { title: "two" });
		await hub.create("doc:/1");
		await hub.create("doc:/3", { title: "three", revision: 4 });
		await assertRejects(() => hub.create("doc:/1"), ChannelAlreadyExistsError);
		const listed = [];
		for await (const instance of hub.list("doc:/:id")) {
			listed.push(instance);
		}
		assertEquals(listed, [
			{ uri: "doc:/1", params: { id: "1" }, state: { title: "untitled", revision: 0 } },
			{ uri: "doc:/2", params: { id: "2" }, state: { title: "two", revision: 0 } },
			{ uri: "doc:/3", params: { id: "3" }, state: { title: "three", revision: 4 } },
		]);
		const titles = [];
		for await (const instance of hub.of("doc:/:id").list()) {
			titles.push(instance.state.title.toUpperCase());
		}
		assertEquals(titles, ["UNTITLED", "TWO", "THREE"]);
		assertEquals(await hub.of("doc:/:id").get({ id: "2" }), { title: "two", revision: 0 });
		assertEquals(hub.of("doc:/:id").uri({ id: "7" }), "doc:/7");
	});

	it("destroys an instance, its storage key, its subscriptions and its resumability", async () => {
		const { hub, storage } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.create("doc:/1");
		await hub.subscribe("alice", "doc:/1");
		await hub.subscribe("alice", "counter://");
		await hub.destroy("doc:/1");
		assertEquals(await hub.has("doc:/1"), false);
		assertEquals((await storage.list({ prefix: ["channel", "doc:/:id"] })).entries, []);
		assertEquals(await hub.reconnect("alice", hub.serverSeq, ["counter://", "doc:/1"]), {
			type: "replay",
			actions: [],
			missing: ["doc:/1"],
		});
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(alice.received.map((message) => message.channel), ["counter://"]);
		await assertRejects(() => hub.subscribe("alice", "doc:/1"), ChannelNotFoundError);
		await assertRejects(() => hub.dispatch("doc:/1", "doc/renamed", { title: "x" }), ChannelNotFoundError);
		await assertRejects(() => hub.get("doc:/1"), ChannelNotFoundError);
	});

	it("runs an accepted action's effect, and none of a rejected one", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.create("doc:/1");
		await hub.subscribe("alice", "doc:/1");
		await hub.subscribe("alice", "catalog://");
		const renamed = await hub.dispatch("doc:/1", "doc/renamed", { title: "Final" });
		assertEquals(alice.received, [
			renamed,
			{
				type: "notification",
				channel: "catalog://",
				name: "catalog/docRenamed",
				payload: { uri: "doc:/1", title: "Final", at: "2026-09-07T00:00:00.000Z" },
			},
			{ type: "action", channel: "doc:/1", name: "doc/touched", payload: {}, serverSeq: renamed.serverSeq + 1 },
		]);
		assertEquals(await hub.get("doc:/1"), { title: "Final", revision: 1 });
		assertEquals(hub.serverSeq, renamed.serverSeq + 1);

		const refused = await hub.dispatch("doc:/1", "doc/renamed", { title: "" });
		assertEquals(refused.rejectionReason, "title must not be blank");
		assertEquals(alice.received.length, 4);
		assertEquals(alice.received[3], refused);
		assertEquals(await hub.get("doc:/1"), { title: "Final", revision: 1 });
	});

	it("hands an effect the connection that dispatched the action", async () => {
		const { hub } = boot();
		const seen: (string | undefined)[] = [];
		const watcher = durableChannel()
			.env<Env>()
			.state(v.object({ hits: v.number() }), { hits: 0 })
			.action((a) =>
				a.name("watch/hit")
					.payload(v.object({}))
					.client()
					.reduce((state) => ({ hits: state.hits + 1 }))
					.effect((ctx) => {
						seen.push(ctx.connectionId);
					})
			)
			.build();
		const watched = new DurableChannelHub(durableRoutes().env<Env>().route("watch://", watcher).build(), {
			storage: new MemoryStorage(),
			env: { now: () => "2026-09-07T00:00:00.000Z" },
		});
		watched.connect(spy("alice").connection);
		await watched.dispatchFrom("alice", "watch://", "watch/hit", {}, 1);
		await watched.dispatch("watch://", "watch/hit", {});
		assertEquals(seen, ["alice", undefined]);
		assertEquals(await watched.get("watch://"), { hits: 2 });
		assertEquals(hub.serverSeq, 0);
	});

	it("lets a command create an instance of another template and dispatch to it", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.subscribe("alice", "catalog://");
		assertEquals(await hub.exec("catalog://", "createDoc", { id: "7", title: "Seven" }, { connectionId: "alice" }), {
			uri: "doc:/7",
			by: "alice",
		});
		assertEquals(await hub.get("doc:/7"), { title: "Seven", revision: 0 });
		assertEquals(await hub.get("catalog://"), { docs: ["doc:/7"] });
		assertEquals(alice.received.map((message) => [message.type, message.name]), [
			["action", "catalog/docAdded"],
			["notification", "catalog/docRenamed"],
		]);
		assertEquals(await hub.exec("catalog://", "createDoc", { id: "8", title: "Eight" }), { uri: "doc:/8", by: "server" });
		await assertRejects(() => hub.exec("catalog://", "createDoc", { id: "8", title: "Again" }), ChannelAlreadyExistsError);
	});

	it("forwards the caller's connection and abort signal to a nested command", async () => {
		const { hub } = boot();
		await hub.create("doc:/1");
		assertEquals(await hub.exec("catalog://", "stampDoc", { id: "1" }, { connectionId: "alice" }), { by: "alice", aborted: false });
		assertEquals(await hub.exec("catalog://", "stampDoc", { id: "1" }), { by: "server", aborted: false });
		const aborter = new AbortController();
		aborter.abort();
		assertEquals(await hub.exec("catalog://", "stampDoc", { id: "1" }, { connectionId: "bob", signal: aborter.signal }), {
			by: "bob",
			aborted: true,
		});
	});

	it("hides an internal route from every connection but not from the hub", async () => {
		const { hub } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		await assertRejects(() => hub.subscribe("alice", "x-index://"), RouteNotFoundError);
		await assertRejects(() => hub.dispatchFrom("alice", "x-index://", "index/hit", {}, 1, { lenient: true }), RouteNotFoundError);
		await hub.subscribe("alice", "counter://");
		assertEquals(await hub.reconnect("alice", hub.serverSeq, ["counter://", "x-index://"]), {
			type: "replay",
			actions: [],
			missing: ["x-index://"],
		});
		await hub.dispatch("x-index://", "index/hit", {});
		assertEquals(await hub.get("x-index://"), { hits: 1 });
		assertEquals(await hub.has("x-index://"), true);
		assertEquals(await hub.exec("x-index://", "count", {}), 1);
		assertEquals(alice.received, []);
	});

	it("runs background work for an instance and stops it when the instance is destroyed", async () => {
		const { hub, seen, release } = worker();
		await hub.create("worker:/1");
		assertEquals(await hub.exec("worker:/1", "run", { cooperative: false }), { first: 1 });
		assertEquals(hub.serverSeq, 1);
		await release();
		assertEquals(hub.serverSeq, 2);
		assertEquals(await hub.get("worker:/1"), { steps: 2 });

		await hub.destroy("worker:/1");
		await release();
		assertEquals(seen, [false, true]);
		assertEquals(hub.serverSeq, 2);
		await hub.close();
	});

	it("aborts and awaits every background task on close, and starts no new one afterwards", async () => {
		const { hub, seen, release, releaseAll } = worker();
		await hub.create("worker:/1");
		await hub.exec("worker:/1", "run", { cooperative: true });
		await release();
		assertEquals(hub.serverSeq, 2);

		let closed = false;
		const closing = hub.close().then(() => {
			closed = true;
		});
		await flush();
		assertEquals(closed, false);
		releaseAll();
		await closing;
		assertEquals(closed, true);
		assertEquals(seen, [false, true]);
		assertEquals(hub.serverSeq, 2);

		await hub.exec("worker:/1", "run", { cooperative: true });
		assertEquals(hub.serverSeq, 3);
		await release();
		assertEquals(seen, [false, true]);
	});

	it("treats every URI of a stateless route as existing and pushes only notifications", async () => {
		const { hub, storage } = boot();
		const alice = spy("alice");
		hub.connect(alice.connection);
		assertEquals(await hub.subscribe("alice", "log:/build"), undefined);
		assertEquals(await hub.has("log:/build"), true);
		assertEquals(await hub.exec("log:/build", "append", { line: "compiled" }), null);
		assertEquals(alice.received, [{
			type: "notification",
			channel: "log:/build",
			name: "log/line",
			payload: { line: "compiled", at: "2026-09-07T00:00:00.000Z" },
		}]);
		assertEquals((await storage.list({ prefix: ["channel"] })).entries, []);
		await assertRejects(() => hub.get("log:/build"), StatelessChannelError);
		await assertRejects(() => hub.create("log:/build"), StatelessChannelError);
		await assertRejects(() => hub.destroy("log:/build"), StatelessChannelError);
		await assertRejects(() => hub.dispatch("log:/build", "log/nope", {}), StatelessChannelError);
		const listed = [];
		for await (const instance of hub.list("log:/:stream")) {
			listed.push(instance);
		}
		assertEquals(listed, []);
	});

	it("replays, snapshots or reports missing on reconnect", async () => {
		const { hub } = boot({ replayLimit: 2 });
		const alice = spy("alice");
		hub.connect(alice.connection);
		await hub.subscribe("alice", "counter://");
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		const caughtUp = await hub.reconnect("alice", 1, ["counter://", "log:/build", "nope://", "doc:/9"]);
		assertEquals(caughtUp, { type: "replay", actions: [], missing: ["nope://", "doc:/9"] });

		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		const replayed = await hub.reconnect("alice", 1, ["counter://"]);
		assertEquals(replayed.type === "replay" ? replayed.actions.map((envelope) => envelope.serverSeq) : [], [2]);

		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		const snapshotted = await hub.reconnect("alice", 1, ["counter://", "log:/build"]);
		assertEquals(snapshotted, { type: "snapshot", snapshots: [{ resource: "counter://", state: { count: 4 }, fromSeq: 4 }], missing: [] });
	});

	it("replaces a link when the same id connects twice", async () => {
		const { hub } = boot();
		const first = spy("alice");
		hub.connect(first.connection);
		await hub.subscribe("alice", "counter://");
		const second = spy("alice");
		hub.connect(second.connection);
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(first.received, []);
		assertEquals(second.received, []);
		await hub.subscribe("alice", "counter://");
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(first.received, []);
		assertEquals(second.received.length, 1);

		hub.disconnect("alice", first.connection);
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(second.received.length, 2);
		hub.disconnect("alice", second.connection);
		await hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(second.received.length, 2);
	});

	it("keeps state and the sequence on Deno KV across a restart", async () => {
		using storage = await DenoKvStorage.open(":memory:");
		const first = boot({ storage });
		first.hub.connect(spy("alice").connection);
		await first.hub.subscribe("alice", "counter://");
		await first.hub.dispatch("counter://", "counter/incremented", { by: 6 });
		await first.hub.create("doc:/kv", { title: "persisted" });
		assertEquals(await storage.get(["hub", "serverSeq"]), 1);

		const second = boot({ storage });
		await second.hub.ready();
		assertEquals(second.hub.serverSeq, 1);
		assertEquals(await second.hub.get("counter://"), { count: 6 });
		assertEquals(await second.hub.get("doc:/kv"), { title: "persisted", revision: 0 });
		const listed = [];
		for await (const instance of second.hub.list("doc:/:id")) {
			listed.push(instance.uri);
		}
		assertEquals(listed, ["doc:/kv"]);
		await second.hub.dispatch("counter://", "counter/incremented", { by: 1 });
		assertEquals(second.hub.serverSeq, 2);
		assertEquals(await storage.get(["hub", "serverSeq"]), 2);
	});

	it("persists state and the sequence, and hydrates a fresh hub from the same storage", async () => {
		const storage = new MemoryStorage();
		const first = boot({ storage });
		first.hub.connect(spy("alice").connection);
		await first.hub.subscribe("alice", "counter://");
		await first.hub.dispatch("counter://", "counter/incremented", { by: 4 });
		assertEquals(await storage.get(["channel", "counter://", "counter://"]), { count: 4 });
		assertEquals(await storage.get(["hub", "serverSeq"]), 1);

		const second = boot({ storage });
		await second.hub.ready();
		await second.hub.ready();
		assertEquals(second.hub.serverSeq, 1);
		assertEquals(await second.hub.get("counter://"), { count: 4 });
		second.hub.connect(spy("alice").connection);
		assertEquals(await second.hub.reconnect("alice", 0, ["counter://"]), {
			type: "snapshot",
			snapshots: [{ resource: "counter://", state: { count: 4 }, fromSeq: 1 }],
			missing: [],
		});
	});

	it("describes its routes as a document", () => {
		const { hub } = boot();
		const info = { title: "Test hub", version: "0.0.1" };
		const document = hub.generateSchema({ info });
		assertEquals(document, describeRoutes(routes, { info }));
		assertEquals(
			document.channels.map((channel) => [channel.template, channel.singleton, channel.internal, channel.state !== undefined]),
			[
				["counter://", true, false, true],
				["doc:/:id", false, false, true],
				["catalog://", true, false, true],
				["log:/:stream", false, false, false],
				["x-index://", true, true, true],
			],
		);
	});
});
