/**
 * The client mirroring two domains a hub serves: a tic-tac-toe game and a chat room.
 *
 * Nothing here is protocol-specific. The definitions are ordinary durable channels, the server is
 * `createRpc` behind an in-memory transport pair for most scenarios and behind a real WebSocket for
 * one, and the point of every test is that the client lands on the same state the hub holds — before
 * the round trip when it can, and after the server has spoken when the two disagree.
 */
import { describe, it } from "node:test";
import { assert, assertEquals, assertRejects, assertThrows } from "@std/assert";
import * as v from "valibot";
import { Hono } from "hono";
import { durableChannel } from "./channel.ts";
import { durableRoutes } from "./routes.ts";
import { type DurableChannelConnection, DurableChannelHub } from "./hub.ts";
import { MemoryStorage } from "./storage.ts";
import { ChannelNotFoundError, NotClientDispatchableError, RejectAction, RpcError, UnknownActionError } from "./error.ts";
import {
	attachSocket,
	createRpc,
	type DurableChannelLink,
	type DurableChannelRpcFrame,
	type DurableChannelSocketSession,
	toRpcNotification,
} from "./rpc.ts";
import { InMemoryTransport, WebSocketTransport } from "./transport.ts";
import { DurableChannelClient, type DurableChannelClientEvent, type DurableChannelSubscription } from "./client.ts";

type Env = { now(): string };

const Mark = v.picklist(["X", "O"]);
const GameState = v.object({
	board: v.pipe(v.array(v.nullable(Mark)), v.length(9)),
	next: Mark,
	winner: v.optional(v.nullable(v.picklist(["X", "O", "draw"])), null),
});

const LINES = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [0, 3, 6], [1, 4, 7], [2, 5, 8], [0, 4, 8], [2, 4, 6]];

function emptyBoard(): (null | "X" | "O")[] {
	return [null, null, null, null, null, null, null, null, null];
}

function winnerOf(board: readonly (null | "X" | "O")[]): "X" | "O" | "draw" | null {
	for (const [a, b, c] of LINES) {
		if (board[a] !== null && board[a] === board[b] && board[a] === board[c]) {
			return board[a];
		}
	}
	return board.every((cell) => cell !== null) ? "draw" : null;
}

const game = durableChannel()
	.env<Env>()
	.state(GameState, { board: emptyBoard(), next: "X" })
	.action((a) =>
		a.name("game/moved")
			.payload(v.object({ cell: v.pipe(v.number(), v.integer(), v.minValue(0), v.maxValue(8)), player: Mark }))
			.client()
			.reduce((state, payload) => {
				if (state.winner !== null) {
					throw new RejectAction("the game is over");
				}
				if (state.board[payload.cell] !== null) {
					throw new RejectAction(`cell ${payload.cell} is taken`);
				}
				if (state.next !== payload.player) {
					throw new RejectAction(`it is ${state.next}'s turn`);
				}
				const board = [...state.board];
				board[payload.cell] = payload.player;
				return { board, next: payload.player === "X" ? "O" : "X", winner: winnerOf(board) };
			})
	)
	.action((a) => a.name("game/cleared").payload(v.object({})).reduce(() => ({ board: emptyBoard(), next: "X" as const, winner: null })))
	.command((c) =>
		c.name("reset")
			.params(v.object({}))
			.result(v.object({ at: v.string() }))
			.handler(async (_params, ctx) => {
				await ctx.dispatch(ctx.uri, "game/cleared", {});
				await ctx.notify(ctx.uri, "game/chat", { text: "new game" });
				return { at: ctx.env.now() };
			})
	)
	.notification((n) => n.name("game/chat").payload(v.object({ text: v.string() })))
	.build();

const room = durableChannel()
	.env<Env>()
	.state(v.object({ messages: v.array(v.object({ author: v.string(), text: v.string() })) }), { messages: [] })
	.action((a) =>
		a.name("room/said")
			.payload(v.object({ author: v.string(), text: v.pipe(v.string(), v.minLength(1)) }))
			.client()
			.reduce((state, payload) => ({ messages: [...state.messages, payload] }))
	)
	.action((a) => a.name("room/cleared").payload(v.object({})).reduce(() => ({ messages: [] })))
	.command((c) =>
		c.name("clear")
			.params(v.object({}))
			.result(v.number())
			.handler(async (_params, ctx) => {
				const before = (await ctx.state()).messages.length;
				await ctx.dispatch(ctx.uri, "room/cleared", {});
				return before;
			})
	)
	.build();

const routes = durableRoutes().env<Env>().route("game:/:id", game).route("room:/:name", room).build();

// ─── Harness ─────────────────────────────────────────────────────────────────

/** Fails fast with a message instead of hanging, and leaves no timer behind for the op sanitizer. */
function withTimeout<T>(promise: Promise<T>, what: string, ms = 5000): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const guard = new Promise<never>((_resolve, reject) => {
		timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
	});
	return Promise.race([promise, guard]).finally(() => clearTimeout(timer)) as Promise<T>;
}

async function until(what: string, predicate: () => boolean, ms = 5000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!predicate()) {
		if (Date.now() > deadline) {
			throw new Error(`timed out waiting for ${what}`);
		}
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
}

/** The next `state` event of a subscription, skipping the action and notification events before it. */
async function nextState(subscription: DurableChannelSubscription, what: string): Promise<unknown> {
	while (true) {
		const step = await withTimeout(subscription.next(), what);
		assert(step.done !== true, `${what}: the subscription ended`);
		if (step.value.type === "state") {
			return step.value.state;
		}
	}
}

/** Consumes a subscription until it ends, which is what an unsubscribe and a shutdown make it do. */
async function endOf(subscription: DurableChannelSubscription, what: string): Promise<void> {
	while (true) {
		const step = await withTimeout(subscription.next(), what);
		if (step.done === true) {
			return;
		}
	}
}

/** Drains a subscription into an array, so a test can read the order events arrived in. */
function collect(subscription: DurableChannelSubscription) {
	const events: DurableChannelClientEvent[] = [];
	const drained = (async () => {
		for await (const event of subscription) {
			events.push(event);
		}
	})();
	return { events, drained, kinds: () => events.map((event) => event.type) };
}

/** One client-side transport with `rpc` answering on the other half, plus the knobs a drop test needs. */
interface ServedLink {
	readonly transport: InMemoryTransport;
	readonly loop: Promise<void>;
	/** Nothing goes back any more — neither responses nor broadcasts. The hub still commits. */
	mute(): void;
	/** Only the hub's broadcasts stop, so a request still gets its answer. */
	deafen(): void;
	hear(): void;
	close(): void;
}

function boot(options?: { replayLimit?: number }) {
	const hub = new DurableChannelHub(routes, {
		storage: new MemoryStorage(),
		env: { now: () => "2026-09-07T00:00:00.000Z" },
		...(options?.replayLimit !== undefined ? { replayLimit: options.replayLimit } : {}),
	});
	const rpc = createRpc(hub);
	const links: ServedLink[] = [];
	const clients: DurableChannelClient<Env, never>[] = [];

	function serve(): ServedLink {
		const [clientHalf, serverHalf] = InMemoryTransport.pair();
		const aborter = new AbortController();
		let binding: { clientId: string; connection: DurableChannelConnection } | undefined;
		let muted = false;
		let deaf = false;
		const out = (frame: DurableChannelRpcFrame): void => {
			if (muted) {
				return;
			}
			try {
				serverHalf.send(frame);
			} catch {
				// The half is closed: the client will notice on its own side.
			}
		};
		const link: DurableChannelLink = {
			get clientId(): string | undefined {
				return binding?.clientId;
			},
			bind(clientId: string): void {
				if (binding !== undefined) {
					hub.disconnect(binding.clientId, binding.connection);
				}
				const connection = hub.connect({
					id: clientId,
					send: (message) => {
						if (!deaf) {
							out(toRpcNotification(message));
						}
					},
				});
				binding = { clientId, connection };
			},
		};
		const loop = (async () => {
			while (true) {
				const frame = await serverHalf.recv();
				if (frame === null) {
					break;
				}
				const response = await rpc.handle(frame, link, aborter.signal);
				if (response !== undefined) {
					out(response);
				}
			}
			if (binding !== undefined) {
				hub.disconnect(binding.clientId, binding.connection);
				binding = undefined;
			}
		})();
		const served: ServedLink = {
			transport: clientHalf,
			loop,
			mute(): void {
				muted = true;
			},
			deafen(): void {
				deaf = true;
			},
			hear(): void {
				deaf = false;
			},
			close(): void {
				aborter.abort();
				serverHalf.close();
			},
		};
		links.push(served);
		return served;
	}

	async function open(clientId: string, subscriptions?: readonly string[]) {
		const link = serve();
		const client = new DurableChannelClient(routes, link.transport, { clientId, requestTimeoutMs: 5000 });
		clients.push(client as never);
		client.connect();
		if (subscriptions !== undefined) {
			await withTimeout(client.hello({ subscriptions }), `${clientId}'s handshake`);
		}
		return { client, link };
	}

	return {
		hub,
		rpc,
		serve,
		open,
		newGame: (id: string) => hub.of("game:/:id").create({ id }),
		newRoom: (name: string) => hub.of("room:/:name").create({ name }),
		async stop(): Promise<void> {
			for (const client of clients) {
				await withTimeout(client.shutdown(), "a client to shut down");
			}
			for (const link of links) {
				link.close();
				await withTimeout(link.loop, "a server loop to end");
			}
			await withTimeout(hub.close(), "the hub to close");
		},
	};
}

describe("DurableChannelClient", () => {
	it("takes a snapshot on hello and types the state from the route the URI matches", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			await host.newRoom("lobby");
			const { client } = await host.open("alice");
			const hello = await withTimeout(client.hello({ subscriptions: ["game:/1", "room:/lobby"] }), "the handshake");
			assertEquals(hello.serverSeq, 0);
			assertEquals(hello.snapshots.map((snapshot) => snapshot.resource), ["game:/1", "room:/lobby"]);
			assertEquals(client.state("game:/1"), { board: emptyBoard(), next: "X", winner: null });
			assertEquals(client.state("room:/lobby"), { messages: [] });
			assertEquals(client.subscriptions, ["game:/1", "room:/lobby"]);
			assertEquals(client.clientId, "alice");
		} finally {
			await host.stop();
		}
	});

	it("shows a move optimistically before the hub has said anything about it", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			const watcher = collect(client.attachSubscription("game:/1"));
			const handle = client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" });
			assertEquals(handle.clientSeq, 1);
			assertEquals(client.state("game:/1").board[4], "X");
			assertEquals(client.state("game:/1").next, "O");
			assertEquals(client.confirmed("game:/1").board[4], null);
			assertEquals(client.pending("game:/1"), [{ clientSeq: 1, name: "game/moved", payload: { cell: 4, player: "X" } }]);
			const outcome = await withTimeout(handle.settled, "the echo of the move");
			assertEquals(outcome.status, "confirmed");
			assertEquals(client.confirmed("game:/1").board[4], "X");
			assertEquals(client.state("game:/1"), client.confirmed("game:/1"));
			assertEquals(client.pending("game:/1"), []);
			assertEquals(watcher.kinds(), ["state", "action", "state"]);
			assertEquals(await host.hub.get("game:/1"), client.confirmed("game:/1"));
		} finally {
			await host.stop();
		}
	});

	it("converges two clients playing the same game", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const alice = await host.open("alice", ["game:/1"]);
			const bob = await host.open("bob", ["game:/1"]);
			const watching = bob.client.attachSubscription("game:/1");
			await withTimeout(alice.client.dispatch("game:/1", "game/moved", { cell: 0, player: "X" }).settled, "alice's move");
			assertEquals(await nextState(watching, "bob to see alice's move"), alice.client.confirmed("game:/1"));
			await withTimeout(bob.client.dispatch("game:/1", "game/moved", { cell: 4, player: "O" }).settled, "bob's move");
			await until("alice to see bob's move", () => alice.client.confirmed("game:/1").board[4] === "O");
			assertEquals(alice.client.state("game:/1"), bob.client.state("game:/1"));
			assertEquals(await host.hub.get("game:/1"), alice.client.state("game:/1"));
			await watching.close();
		} finally {
			await host.stop();
		}
	});

	it("sends a move its own reducer refuses anyway, and lets the server say so", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			await withTimeout(client.dispatch("game:/1", "game/moved", { cell: 0, player: "X" }).settled, "the first move");
			const before = client.state("game:/1");
			const handle = client.dispatch("game:/1", "game/moved", { cell: 0, player: "O" });
			assertEquals(client.state("game:/1"), before);
			const outcome = await withTimeout(handle.settled, "the refusal");
			assertEquals(outcome.status, "rejected");
			assert(outcome.status === "rejected");
			assertEquals(outcome.reason, "cell 0 is taken");
			assertEquals(client.state("game:/1"), client.confirmed("game:/1"));
			assertEquals(client.pending("game:/1"), []);
			assertEquals(await host.hub.get("game:/1"), client.confirmed("game:/1"));
		} finally {
			await host.stop();
		}
	});

	it("rolls an optimistic move back once the hub refuses it", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const alice = await host.open("alice", ["game:/1"]);
			const bob = await host.open("bob", ["game:/1"]);
			// Bob stops hearing the hub, so his confirmed state still believes the board is empty while
			// Alice takes the middle. His own request still gets its answer.
			bob.link.deafen();
			await withTimeout(alice.client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" }).settled, "alice's move");
			const handle = bob.client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" });
			assertEquals(bob.client.state("game:/1").board[4], "X");
			assertEquals(bob.client.confirmed("game:/1").board[4], null);
			const outcome = await withTimeout(handle.settled, "bob's refusal");
			assert(outcome.status === "rejected");
			assertEquals(outcome.reason, "cell 4 is taken");
			assertEquals(bob.client.state("game:/1").board[4], null);
			assertEquals(bob.client.state("game:/1"), bob.client.confirmed("game:/1"));
			// Bob's confirmed state is behind, and a reconnect cannot tell: he applied a later sequence
			// than the one he missed, so he claims to have seen it. Re-subscribing is what fixes that.
			bob.link.hear();
			await bob.client.unsubscribe("game:/1");
			await withTimeout(bob.client.subscribe("game:/1"), "bob's fresh subscription");
			assertEquals(bob.client.state("game:/1"), await host.hub.get("game:/1"));
		} finally {
			await host.stop();
		}
	});

	it("refuses locally what the route map already says is impossible", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			assertThrows(() => client.dispatch("game:/1", "game/cleared", {}), NotClientDispatchableError);
			assertThrows(() => client.dispatch("game:/1", "game/ghost", {}), UnknownActionError);
			assertThrows(() => client.dispatch("game:/1", "game/moved", { cell: 99, player: "X" }), Error);
			assertEquals(host.hub.serverSeq, 0);
			assertEquals(client.pending("game:/1"), []);
		} finally {
			await host.stop();
		}
	});

	it("reports a clientSeq the hub has already passed as a duplicate", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			await host.hub.dispatchFrom("alice", "game:/1", "game/moved", { cell: 0, player: "X" }, 9);
			await until("the client to see the move", () => client.confirmed("game:/1").board[0] === "X");
			const handle = client.dispatch("game:/1", "game/moved", { cell: 1, player: "O" });
			assertEquals(client.state("game:/1").board[1], "O");
			assertEquals(await withTimeout(handle.settled, "the duplicate verdict"), { status: "duplicate" });
			assertEquals(client.state("game:/1").board[1], null);
			assertEquals(client.pending("game:/1"), []);
		} finally {
			await host.stop();
		}
	});

	it("runs a command and mirrors the server-origin action it dispatched", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			await withTimeout(client.dispatch("game:/1", "game/moved", { cell: 0, player: "X" }).settled, "the first move");
			const watching = client.attachSubscription("game:/1");
			const result = await withTimeout(client.exec("game:/1", "reset", {}), "the reset");
			assertEquals(result, { at: "2026-09-07T00:00:00.000Z" });
			const step = await withTimeout(watching.next(), "the reset action");
			assert(step.done !== true && step.value.type === "action");
			assertEquals(step.value.envelope.name, "game/cleared");
			assertEquals(step.value.envelope.origin, undefined);
			await until("the cleared board", () => client.state("game:/1").board[0] === null);
			await watching.close();
			await assertRejects(() => client.exec("game:/1", "ghost", {}), RpcError);
		} finally {
			await host.stop();
		}
	});

	it("delivers a notification to the subscribers of its channel only", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			await host.newGame("2");
			const alice = await host.open("alice", ["game:/1"]);
			const bob = await host.open("bob", ["game:/2"]);
			const heard = collect(alice.client.attachSubscription("game:/1"));
			const missed = collect(bob.client.attachSubscription("game:/2"));
			await host.hub.notify("game:/1", "game/chat", { text: "good luck" });
			await until("alice to hear the chat", () => heard.events.length === 1);
			assertEquals(heard.events, [{ type: "notification", channel: "game:/1", name: "game/chat", payload: { text: "good luck" } }]);
			assertEquals(missed.events, []);
		} finally {
			await host.stop();
		}
	});

	it("mirrors a second definition on the same hub and forgets it on unsubscribe", async () => {
		const host = boot();
		try {
			await host.newRoom("lobby");
			const { client } = await host.open("alice", ["room:/lobby"]);
			const watching = client.attachSubscription("room:/lobby");
			const said = client.dispatch("room:/lobby", "room/said", { author: "alice", text: "hello" });
			assertEquals(client.state("room:/lobby"), { messages: [{ author: "alice", text: "hello" }] });
			await withTimeout(said.settled, "the message");
			assertEquals(await withTimeout(client.exec("room:/lobby", "clear", {}), "the clear"), 1);
			await until("the cleared room", () => client.state("room:/lobby").messages.length === 0);
			await client.unsubscribe("room:/lobby");
			await endOf(watching, "the end of the subscription");
			assertEquals(client.subscriptions, []);
			assertThrows(() => client.state("room:/lobby"), ChannelNotFoundError);
			await host.hub.dispatch("room:/lobby", "room/said", { author: "bob", text: "anyone?" });
			assertEquals((await host.hub.get("room:/lobby") as { messages: unknown[] }).messages.length, 1);
		} finally {
			await host.stop();
		}
	});

	it("keeps a pending action across a dropped transport and settles it from the replay", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client, link } = await host.open("alice", ["game:/1"]);
			// The hub commits the move, then the link dies before either the echo or the answer gets out.
			link.mute();
			const handle = client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" });
			await until("the hub to commit the move", () => host.hub.serverSeq === 1);
			link.close();
			await until("the client to notice the drop", () => client.connectionState.status === "closed");
			assertEquals(client.pending("game:/1").length, 1);
			await host.hub.dispatch("game:/1", "game/cleared", {});
			const next = host.serve();
			const resumed = await withTimeout(client.reconnect(next.transport), "the reconnect");
			assertEquals(resumed.type, "replay");
			assert(resumed.type === "replay");
			assertEquals(resumed.actions.map((envelope) => envelope.name), ["game/moved", "game/cleared"]);
			assertEquals(await withTimeout(handle.settled, "the replayed outcome"), { status: "confirmed", envelope: resumed.actions[0] });
			assertEquals(client.pending("game:/1"), []);
			assertEquals(client.state("game:/1"), await host.hub.get("game:/1"));
			assertEquals(client.lastSeenServerSeq, 2);
		} finally {
			await host.stop();
		}
	});

	it("loses a pending action when the gap is wider than the replay ring", async () => {
		const host = boot({ replayLimit: 1 });
		try {
			await host.newGame("1");
			const { client, link } = await host.open("alice", ["game:/1"]);
			link.mute();
			const handle = client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" });
			await until("the hub to commit the move", () => host.hub.serverSeq === 1);
			link.close();
			await until("the client to notice the drop", () => client.connectionState.status === "closed");
			await host.hub.dispatch("game:/1", "game/moved", { cell: 0, player: "O" });
			await host.hub.dispatch("game:/1", "game/moved", { cell: 1, player: "X" });
			const next = host.serve();
			const resumed = await withTimeout(client.reconnect(next.transport), "the reconnect");
			assertEquals(resumed.type, "snapshot");
			assertEquals(await withTimeout(handle.settled, "the lost outcome"), { status: "lost" });
			assertEquals(client.pending("game:/1"), []);
			assertEquals(client.state("game:/1"), await host.hub.get("game:/1"));
		} finally {
			await host.stop();
		}
	});

	it("forgets a channel the hub cannot resume, and ends its subscription", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			await host.newRoom("lobby");
			const { client } = await host.open("alice", ["game:/1", "room:/lobby"]);
			const watching = client.attachSubscription("room:/lobby");
			await host.hub.destroy("room:/lobby");
			const resumed = await withTimeout(client.reconnect(), "the reconnect");
			assertEquals(resumed.missing, ["room:/lobby"]);
			assertEquals(client.subscriptions, ["game:/1"]);
			await endOf(watching, "the end of the subscription");
			assertThrows(() => client.state("room:/lobby"), ChannelNotFoundError);
			assertEquals(client.state("game:/1"), await host.hub.get("game:/1"));
		} finally {
			await host.stop();
		}
	});

	it("addresses a family through a typed handle", async () => {
		const host = boot();
		try {
			await host.newGame("7");
			const { client } = await host.open("alice", []);
			const games = client.of("game:/:id");
			assertEquals(games.uri({ id: "7" }), "game:/7");
			const { subscription } = await withTimeout(games.subscribe({ id: "7" }), "the subscription");
			await withTimeout(games.dispatch({ id: "7" }, "game/moved", { cell: 8, player: "X" }).settled, "the move");
			assertEquals(games.state({ id: "7" }).board[8], "X");
			assertEquals(games.confirmed({ id: "7" }), await host.hub.get("game:/7"));
			assertEquals(await withTimeout(games.exec({ id: "7" }, "reset", {}), "the reset"), { at: "2026-09-07T00:00:00.000Z" });
			await subscription.close();
		} finally {
			await host.stop();
		}
	});

	it("fans every event into one stream, tagged with its channel", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client } = await host.open("alice", ["game:/1"]);
			const seen: string[] = [];
			const events = client.events();
			const drained = (async () => {
				for await (const record of events) {
					seen.push(`${record.channel} ${record.event.type}`);
				}
			})();
			await withTimeout(client.dispatch("game:/1", "game/moved", { cell: 0, player: "X" }).settled, "the move");
			await host.hub.notify("game:/1", "game/chat", { text: "hi" });
			await until("every event to arrive", () => seen.length === 4);
			assertEquals(seen, ["game:/1 state", "game:/1 action", "game:/1 state", "game:/1 notification"]);
			await events.return?.();
			await drained;
		} finally {
			await host.stop();
		}
	});

	it("ends everything on shutdown and refuses to be used again", async () => {
		const host = boot();
		try {
			await host.newGame("1");
			const { client, link } = await host.open("alice", ["game:/1"]);
			link.mute();
			const handle = client.dispatch("game:/1", "game/moved", { cell: 0, player: "X" });
			const watching = client.attachSubscription("game:/1");
			await withTimeout(client.shutdown(), "the shutdown");
			assertEquals(await withTimeout(handle.settled, "the lost outcome"), { status: "lost" });
			await endOf(watching, "the end of the subscription");
			assertEquals(client.connectionState, { status: "closed", reason: { type: "shutdown" } });
			assertThrows(() => client.dispatch("game:/1", "game/moved", { cell: 1, player: "O" }), Error);
			await assertRejects(() => client.ping(), Error);
		} finally {
			await host.stop();
		}
	});
});

describe("DurableChannelClient over a WebSocket", () => {
	it("plays the same game over a real socket served by Hono", async () => {
		const hub = new DurableChannelHub(routes, { storage: new MemoryStorage(), env: { now: () => "2026-09-07T00:00:00.000Z" } });
		const rpc = createRpc(hub);
		const sessions = new Set<DurableChannelSocketSession>();
		const sockets = new Set<WebSocket>();
		const app = new Hono();
		app.get("/rpc", (context) => {
			const { socket, response } = Deno.upgradeWebSocket(context.req.raw);
			sockets.add(socket);
			socket.addEventListener("close", () => sockets.delete(socket));
			sessions.add(attachSocket(rpc, hub, socket));
			return response;
		});
		const listener = Deno.serve({ port: 0, hostname: "127.0.0.1", onListen() {} }, app.fetch);
		const { port } = listener.addr as Deno.NetAddr;
		const transport = await withTimeout(WebSocketTransport.connect(`ws://127.0.0.1:${port}/rpc`), "the socket to open");
		const client = new DurableChannelClient(routes, transport, { clientId: "alice", requestTimeoutMs: 5000 });
		try {
			await hub.of("game:/:id").create({ id: "1" });
			client.connect();
			await withTimeout(client.ping(), "a ping before hello");
			await withTimeout(client.hello({ subscriptions: ["game:/1"] }), "the handshake");
			const watching = client.attachSubscription("game:/1");
			const handle = client.dispatch("game:/1", "game/moved", { cell: 4, player: "X" });
			assertEquals(client.state("game:/1").board[4], "X");
			const outcome = await withTimeout(handle.settled, "the echo over the socket");
			assertEquals(outcome.status, "confirmed");
			assertEquals(client.confirmed("game:/1"), await hub.get("game:/1"));
			assertEquals(await withTimeout(client.exec("game:/1", "reset", {}), "the reset"), { at: "2026-09-07T00:00:00.000Z" });
			await until("the cleared board", () => client.state("game:/1").board[4] === null);
			const refused = await withTimeout(client.dispatch("game:/1", "game/moved", { cell: 0, player: "O" }).settled, "the refusal");
			assertEquals(refused.status, "rejected");
			await watching.close();
		} finally {
			await withTimeout(client.shutdown(), "the client to shut down");
			for (const session of sessions) {
				session.detach();
			}
			for (const socket of [...sockets]) {
				socket.close(1000);
			}
			await withTimeout(hub.close(), "the hub to close");
			await withTimeout(listener.shutdown(), "the listener to shut down");
		}
	});
});
