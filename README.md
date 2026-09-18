# Durable Channel

A **durable channel** holds a state at a URI and declares the only ways that state can change. State moves through named **actions** whose
reducers are pure and synchronous; side effects live in named **commands**, or in an action's **effect**, which runs once the action is
committed; ephemeral messages are named **notifications**. The colocated **hub** owns the authoritative instances, connections, one global
sequence number, replay ring and storage. The **distributed** implementation assigns each channel to an independent actor and places
connections on gateways, with one durable sequence per channel.

The point of the split is that the definition is isomorphic: the same value drives the server and the client, which mirrors the state by
running the same reducers on the envelopes the hub broadcasts. The library modules use only web-standard APIs, so a hub also runs inside a
Cloudflare Durable Object with the object's `state.storage` behind `DurableChannelStorage`, and the client runs in a browser. The colocated
`rpc.ts` surface and the explicit `distributed/rpc.ts` surface share transports and definitions while keeping their wire contracts separate.

## Install

`durable-channel` is published to npm:

```sh
deno add npm:durable-channel   # Deno
npm install durable-channel    # Node
```

Import it by name:

```ts
import {
	attachSocket,
	createRpc,
	durableChannel,
	DurableChannelClient,
	DurableChannelHub,
	durableRoutes,
	MemoryStorage,
	WebSocketTransport,
} from "durable-channel";
```

`valibot` is the only runtime dependency. The JSON-RPC envelope layer ships in-tree. Distributed core exports also have a `./distributed`
source package entry; platform imports and celld development tooling stay in the Durable Object example.

## Selecting a runtime

| Requirement                                             | Server                                                                 | Client and wire                                                                      | Ordering                                       |
| ------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------- |
| Colocated host, existing deployments, AHP 0.9.0 adapter | `DurableChannelHub`                                                    | `DurableChannelClient`, `createRpc`, `attachSocket`                                  | Global `serverSeq` and scalar reconnect cursor |
| Channels distributed across owners                      | `DurableChannelActor`, `DurableChannelRouter`, `DurableChannelGateway` | `DurableChannelDistributedClient`, `createDistributedRpc`, `attachDistributedSocket` | `{ generation, channelSeq }` per URI           |

The existing seven-method JSON-RPC surface is not itself AHP. The AHP translation and official-client integration live in
[`src/ahp.test.ts`](src/ahp.test.ts) and continue to use the colocated hub. Distributed channels have their own protocol identifier,
`durable-channel/distributed-1`, and cannot serve an AHP client. There is no distributed global sequencer or consistent multi-channel
snapshot.

The following quick start and the API sections through **Errors** describe the existing colocated implementation. See
[Distributed channels](#distributed-channels) for distributed assembly, recovery and migration, and the executable
[Durable Object example](examples/durable-objects/README.md) for real owner/gateway hosting.

## Quick start

```ts
import * as v from "valibot";
import { durableChannel, DurableChannelHub, durableRoutes, MemoryStorage, RejectAction } from "durable-channel";

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
	.action((a) => a.name("counter/reset").payload(v.object({ to: v.optional(v.number(), 0) })).reduce((_state, { to }) => ({ count: to })))
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

const routes = durableRoutes().env<Env>().route("counter://", counter).build();

const hub = new DurableChannelHub(routes, {
	storage: new MemoryStorage(),
	env: { now: () => new Date().toISOString() },
	replayLimit: 1024,
});

hub.connect({ id: "alice", send: (message) => console.log(message) });
await hub.subscribe("alice", "counter://"); // { resource: "counter://", state: { count: 0 }, fromSeq: 0 }
await hub.dispatchFrom("alice", "counter://", "counter/incremented", { by: 2 }, 1); // echoed to alice with `origin`
await hub.exec("counter://", "reset", {}); // { count: 0, at: "…" }
await hub.get("counter://"); // { count: 0 }
```

`send` is called synchronously, in subscription order, for every message the URI's subscribers should see — the originating connection
included.

## Definitions

`durableChannel()` starts a builder. Every method returns a _new_ builder and leaves the receiver unchanged, so a partially configured
builder can be shared; the type parameters accumulate what was registered, which is what makes `hub.dispatch`, `hub.exec`, `hub.notify` and
`hub.of` typed. `build()` returns a frozen plain object.

| Concept          | Declared with                                                     | Carries                                                          | Runs                                                                     |
| ---------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **state**        | `.state(schema, initialState)`                                    | a valibot schema and its initial value, parsed once at `build()` | never — it is data                                                       |
| **action**       | `.action((a) => a.name().payload().client?().reduce().effect?())` | a payload schema, a `client` flag, a pure reducer, an effect     | on the server at commit time, and on a client mirroring the same channel |
| **command**      | `.command((c) => c.name().params().result().handler())`           | params and result schemas and an async handler                   | on the server only                                                       |
| **notification** | `.notification((n) => n.name().payload())`                        | a payload schema                                                 | never persisted, never replayed                                          |

Every builder — the channel itself, an action, a command, a notification — also takes `.summary(text)`, a one-line description that has no
runtime effect and only feeds the generated document (see _Describing a hub_).

A reducer receives `(state, payload, meta)` where `meta` is `{ uri, params }`. It never sees `env`, a clock or randomness: anything ambient
has to arrive in the payload. That is the whole reason a client can replay the same actions and land on the same state. Reducers must be
synchronous and must not mutate the state they are given.

A command handler receives `(params, ctx)`:

| `ctx` member                              | Meaning                                                               |
| ----------------------------------------- | --------------------------------------------------------------------- |
| `env`                                     | the hub's `env`, as declared with `.env<Env>()`                       |
| `uri`, `params`                           | the instance the command was called on, and its template parameters   |
| `connectionId`                            | the connection that asked, when it came from one                      |
| `signal`                                  | the caller's abort signal; the hub never races the handler against it |
| `state()`                                 | the current state of `uri`                                            |
| `dispatch`, `notify`                      | commit an action or push a notification, on this channel or another   |
| `get`, `has`, `create`, `destroy`, `list` | the instance calls of the hub                                         |
| `exec`                                    | another command, on this channel or another                           |
| `background`                              | hands the hub work that outlives the handler, owned by this instance  |
| `abortBackground`                         | aborts every background task of this instance                         |

`exec` forwards the caller's `connectionId` and `signal` to the nested command, so work done on behalf of a connection stays attributable
however deep the call goes, and one abort reaches the whole chain.

Handlers receive the **parsed output** of the params schema and return the **input** of the result schema; the hub validates both. The same
holds for an action: the reducer receives the parsed payload, and the state it returns is validated against the state schema before being
persisted.

## Effects

An action may also declare an **effect**: server-side work to run once the action is committed. It is the counterpart of the reducer — the
reducer says what the state becomes, the effect says what the rest of the system has to hear about it.

```ts
.action((a) =>
	a.name("doc/renamed")
		.payload(v.object({ title: v.string() }))
		.client()
		.reduce((state, payload) => ({ ...state, title: payload.title }))
		.effect(async (ctx) => {
			await ctx.notify("catalog://", "catalog/docRenamed", { uri: ctx.uri, title: ctx.state.title, at: ctx.env.now() });
		})
)
```

| Rule           | Detail                                                                                                                      |
| -------------- | --------------------------------------------------------------------------------------------------------------------------- |
| When           | after the envelope is committed, persisted and broadcast, and **only** when the action was accepted — a rejection runs none |
| Where          | on the server only. `actions[name].effect` exists on the definition, and a client mirroring the channel ignores it          |
| `ctx`          | the command operations plus `uri`, `params`, `connectionId`, `state` (post-reduce), `payload` (parsed) and `envelope`       |
| Ordering       | `dispatch` and `dispatchFrom` resolve only after the effect settles, so a caller sees its consequences                      |
| Reentrancy     | the effect runs outside the commit mutex, so it may `dispatch` again — that commit takes the next `serverSeq`               |
| Failure        | the error propagates to whoever dispatched, **after** the envelope is already committed and delivered. It is not a rollback |
| `connectionId` | the connection that dispatched the action, or `undefined` for a server-origin one                                           |

A reducer stays pure because of this split: everything ambient — a clock, an id, a cross-channel announcement — belongs to the effect.

## Background work

An effect and a command both settle before their caller's `dispatch` or `exec` resolves, so neither is the place for work that takes a
while: a streaming agent, a long import, a watcher. `ctx.background(task)` hands that work to the hub instead.

```ts
.action((a) =>
	a.name("chat/turnStarted")
		.payload(v.object({ turnId: v.string(), text: v.string() }))
		.client()
		.reduce((state, payload) => ({ ...state, turn: payload.turnId }))
		.effect((ctx) => {
			const { turnId } = ctx.payload;
			ctx.background(async (signal) => {
				for (const chunk of ctx.env.agent.reply(ctx.payload.text)) {
					if (signal.aborted) {
						return;
					}
					await ctx.dispatch(ctx.uri, "chat/delta", { turnId, chunk });
				}
			});
		})
)
```

| Rule       | Detail                                                                                                                       |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Ownership  | a task belongs to the URI of the context that started it, and shares one `AbortSignal` with the other tasks of that URI      |
| Detached   | nothing awaits it: the dispatch that started it resolves as soon as the effect returns                                       |
| Aborted by | `ctx.abortBackground()`, `hub.destroy(uri)` and `hub.close()`. A task must check `signal.aborted` after every `await`        |
| Errors     | swallowed — a background task has no caller to report to. A `dispatch` on a URI that was just destroyed throws and dies here |
| Shutdown   | `await hub.close()` aborts every task, waits for all of them to settle, and refuses new ones                                 |

`hub.close()` is what a test or a process shutdown needs: without it a task can outlive the hub, keep dispatching, and — under Deno's op
sanitizer — fail the test that started it.

## Stateless channels

Leave `.state()` out and the channel is a **relay**: no snapshot, no actions, nothing persisted. Use it for a log, a telemetry stream or an
in-game chat feed — anything where a late subscriber has no interest in history. `.action()` does not exist on the builder until `.state()`
has been called, so the impossible combination cannot be written.

| Operation                                              | On a stateless channel                                              |
| ------------------------------------------------------ | ------------------------------------------------------------------- |
| `subscribe`                                            | succeeds, returns `undefined`                                       |
| `has`                                                  | `true` whenever the route matches                                   |
| `notify`, `exec`                                       | work normally                                                       |
| `get`, `create`, `destroy`, `dispatch`, `dispatchFrom` | throw `StatelessChannelError`                                       |
| `list(template)`                                       | yields nothing                                                      |
| `reconnect`                                            | re-subscribes it, and lists it in neither `snapshots` nor `missing` |

## Routes and instances

`durableRoutes()` maps URI templates to definitions. A template is split on `/`; a literal segment matches itself and a `:name` segment
matches exactly one non-empty segment. No scheme is special-cased — `counter://` is `["counter:", "", ""]` and `doc:/:id` is
`["doc:", ":id"]`.

| Template shape                    | Kind          | How an instance appears                                                                         |
| --------------------------------- | ------------- | ----------------------------------------------------------------------------------------------- |
| no `:name` (`counter://`)         | **singleton** | auto-created from `initialState` on first touch; `create()` reports `ChannelAlreadyExistsError` |
| at least one `:name` (`doc:/:id`) | **family**    | needs `create(uri, state?)`; `state` defaults to `initialState`                                 |

A route may also be declared **internal**, which means no connection may reach it:

```ts
durableRoutes().route("doc:/:id", doc).route("x-catalog://", catalog, { internal: true }).build();
```

| From                                        | An internal route is                                                      |
| ------------------------------------------- | ------------------------------------------------------------------------- |
| `subscribe(id, uri)`, `dispatchFrom(id, …)` | refused with `RouteNotFoundError`, exactly like a template that is absent |
| `reconnect(id, seq, uris)`                  | listed in `missing`, so the peer forgets it                               |
| `dispatch`, `exec`, `notify`, `get`, `list` | a route like any other                                                    |
| a command's or an effect's `ctx`            | a route like any other                                                    |

That is the whole difference: the flag lives on the route, the matcher ignores it, and the hub enforces it.

`destroy(uri)` deletes the persisted state, drops the instance, aborts its background tasks and removes it from every subscription; a later
`subscribe` or `dispatch` throws `ChannelNotFoundError`. It destroys **one** instance: the hub knows of no ownership between channels, so a
channel that owns others destroys them itself, in the command that disposes it —

```ts
.command((c) =>
	c.name("disposeSession").params(v.object({})).result(v.null()).handler(async (_params, ctx) => {
		for (const chat of (await ctx.state()).chats) {
			await ctx.exec(chat.resource, "disposeChat", {});
		}
		await ctx.destroy(ctx.uri);
		return null;
	})
)
```

— which keeps every announcement the children owe their own subscribers where it belongs, in the child's own command.

`hub.of(template)` returns a handle typed by the template's parameters:

```ts
const docs = hub.of("doc:/:id");
docs.uri({ id: "42" }); // "doc:/42"
await docs.create({ id: "42" }, { title: "Draft" });
await docs.dispatch({ id: "42" }, "doc/renamed", { title: "Final" });
await docs.get({ id: "42" });
```

`PathToParams<"doc:/:id">` is `{ id: string }`, `MatchRoute<Routes, "doc:/1">` is the route types that URI resolves to at the type level
(which is what types a client's `state("doc:/1")`), and `matchRoute(routes, uri)` / `resolveUri(template, params)` are exported for code
that needs the same matching outside a hub. `list(template)` yields `{ uri, params, state }` per instance in the storage's key order, and
`hub.of(template).list()` types `state` from the route.

A `reconnect` that names a destroyed instance reports it in `missing`, which is how a peer learns to forget it:

```ts
await hub.destroy("doc:/1");
await hub.reconnect("alice", hub.serverSeq, ["counter://", "doc:/1"]); // { type: "replay", actions: [], missing: ["doc:/1"] }
```

### Recipe: a private catalogue channel

A family has no index: `list(template)` is a storage scan, and nothing in an instance's state can hold a clock reading, because reducers are
pure. When a channel needs an index — a "most recently modified first" list, a created-at stamp, a count, the parent a child belongs to —
give it a **singleton channel of its own**, mount it `{ internal: true }`, and feed it from the effects of the actions that change the
indexed data:

```ts
const catalog = durableChannel()
	.env<Env>()
	.state(v.object({ docs: v.record(v.string(), v.object({ createdAt: v.string(), modifiedAt: v.string() })) }), { docs: {} })
	.action((a) => a.name("catalog/touched").payload(v.object({ doc: v.string(), at: v.string() })).reduce(/* … */))
	.command((c) => c.name("createDoc") /* … creates a doc:/:id instance, then dispatches into itself … */)
	.build();
```

The index stays a durable channel — persisted, sequenced, replayable — without polluting the indexed channel's state with fields its own
protocol does not define.

## Hub API

Every method awaits `ready()` first, so `serverSeq` and the persisted state are loaded before anything else happens.

| Member                                                          | Returns                                  | Notes                                                                                    |
| --------------------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------------- |
| `new DurableChannelHub(routes, { storage, env, replayLimit? })` | the hub                                  | `replayLimit` defaults to 1024                                                           |
| `ready()`                                                       | `Promise<void>`                          | loads the persisted `serverSeq`; idempotent                                              |
| `serverSeq`                                                     | `number`                                 | the last committed sequence                                                              |
| `connect({ id, send })`                                         | the connection                           | a second `connect` for a live id replaces the previous link and clears its subscriptions |
| `disconnect(id, connection?)`                                   | `void`                                   | with `connection`, only when it is still the current one for that id                     |
| `subscribe(id, uri)`                                            | `Promise<Snapshot \| undefined>`         | `undefined` for a stateless channel; `RouteNotFoundError` for an internal one            |
| `unsubscribe(id, uri)`                                          | `void`                                   | silent when the connection or the subscription is already gone                           |
| `reconnect(id, lastSeenServerSeq, uris)`                        | `Promise<replay \| snapshot>`            | restores the subscriptions itself; both variants carry `missing`                         |
| `dispatch(uri, name, payload)`                                  | `Promise<Envelope>`                      | server-origin; throws rather than committing a rejected envelope                         |
| `dispatchFrom(id, uri, name, payload, clientSeq, { lenient? })` | `Promise<Envelope \| undefined>`         | `undefined` when `clientSeq` is a duplicate                                              |
| `exec(uri, name, params, { connectionId?, signal? })`           | `Promise<result>`                        | validates params and result                                                              |
| `notify(uri, name, payload)`                                    | `Promise<void>`                          | current subscribers only                                                                 |
| `get(uri)` / `has(uri)`                                         | `Promise<state>` / `Promise<boolean>`    | `get` throws `ChannelNotFoundError`; `has` answers `false` instead                       |
| `create(uri, state?)` / `destroy(uri)`                          | `Promise<void>`                          | see _Routes and instances_                                                               |
| `close()`                                                       | `Promise<void>`                          | aborts every background task, awaits them, and refuses new ones                          |
| `list(template)`                                                | `AsyncGenerator<{ uri, params, state }>` | one prefix scan of the storage, paged                                                    |
| `of(template)`                                                  | a typed handle                           | see above                                                                                |
| `generateSchema({ info? })`                                     | `DurableChannelDocument`                 | every route as JSON Schema, see _Describing a hub_                                       |

## Describing a hub

`hub.generateSchema({ info? })` returns a document describing every route the hub mounts, the way `Hana.generateOpenRPCSchema` describes a
JSON-RPC collection. The unit is the channel template, because that is what a peer subscribes to, and each entry carries a JSON Schema for
the state, every action, every command and every notification. `describeRoutes(routes, options)` is the same function applied to a route map
without a hub, so a client build can produce the document too.

| Member                             | Meaning                                                                                                                                                            |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `durableChannel`                   | the document format version, `"0.1.0"`                                                                                                                             |
| `info`                             | `{ title, version, description? }`; defaults to `{ title: "Durable Channel Hub", version: "1.0.0" }`                                                               |
| `channels[]`                       | one entry per route, in mount order                                                                                                                                |
| `channels[].template`, `params[]`  | the template and one `{ name, schema }` per `:param` (a non-empty string)                                                                                          |
| `channels[].summary`               | the route option's `summary`, else the definition's `.summary()`                                                                                                   |
| `channels[].singleton`, `internal` | no `:param` means singleton; an internal route is hidden from connections                                                                                          |
| `channels[].state`                 | `{ schema, initial }`; absent for a stateless channel                                                                                                              |
| `channels[].actions[]`             | `{ name, summary?, client, payload }`                                                                                                                              |
| `channels[].commands[]`            | `{ name, summary?, params[], result }`; object params become named entries with `required`, a tuple becomes positional entries, anything else one positional entry |
| `channels[].notifications[]`       | `{ name, summary?, payload }`                                                                                                                                      |

Action payloads and command params are converted in valibot's input mode (what a peer sends); state, results and notification payloads in
output mode (what a peer receives). A schema with no JSON Schema equivalent, such as `v.custom()`, becomes `{}` rather than failing the
whole document. For the quick-start counter, with a `.summary()` on the definition:

```json
{
	"durableChannel": "0.1.0",
	"info": { "title": "Counter", "version": "1.0.0" },
	"channels": [
		{
			"template": "counter://",
			"summary": "A number everyone can bump",
			"params": [],
			"singleton": true,
			"internal": false,
			"state": {
				"schema": {
					"type": "object",
					"properties": { "count": { "type": "number" } },
					"required": ["count"],
					"$schema": "http://json-schema.org/draft-07/schema#"
				},
				"initial": { "count": 0 }
			},
			"actions": [
				{
					"name": "counter/incremented",
					"client": true,
					"payload": {
						"type": "object",
						"properties": { "by": { "type": "integer" } },
						"required": ["by"],
						"$schema": "http://json-schema.org/draft-07/schema#"
					}
				}
			],
			"commands": [
				{
					"name": "reset",
					"params": [{ "name": "to", "schema": { "type": "number", "default": 0, "$schema": "http://json-schema.org/draft-07/schema#" } }],
					"result": {
						"type": "object",
						"properties": { "count": { "type": "number" } },
						"required": ["count"],
						"$schema": "http://json-schema.org/draft-07/schema#"
					}
				}
			],
			"notifications": [
				{
					"name": "counter/announced",
					"payload": {
						"type": "object",
						"properties": { "text": { "type": "string" } },
						"required": ["text"],
						"$schema": "http://json-schema.org/draft-07/schema#"
					}
				}
			]
		}
	]
}
```

## Messages

```ts
type DurableChannelEnvelope = {
	type: "action";
	channel: string;
	name: string;
	payload: unknown;
	serverSeq: number;
	origin?: { clientId: string; clientSeq: number };
	rejectionReason?: string;
};
type DurableChannelNotification = { type: "notification"; channel: string; name: string; payload: unknown };
type DurableChannelSnapshot = { resource: string; state: unknown; fromSeq: number };
type DurableChannelReconnectResult =
	| { type: "replay"; actions: DurableChannelEnvelope[]; missing: string[] }
	| { type: "snapshot"; snapshots: DurableChannelSnapshot[]; missing: string[] };
```

`origin` is present only for an action a connection dispatched, and `rejectionReason` only for a refused one; both keys are omitted
otherwise. `payload` is the parsed payload for an accepted action and the raw one for a rejected action, because a rejected payload never
passed its schema.

## Sequencing, replay and reconnect

One `serverSeq` counter belongs to the hub, not to a channel. Every committed envelope increments it, **rejected ones included**, and it is
persisted at `["hub", "serverSeq"]`. A snapshot carries `fromSeq = serverSeq` at the moment it was taken, so every later envelope for that
channel has `serverSeq > fromSeq`.

A commit is: validate the payload → run the reducer → validate the new state → persist the state and `serverSeq` → append to the replay ring
→ broadcast. One promise-chain mutex serializes commits hub-wide; commands run outside it and take it once per `dispatch`.

The replay ring keeps the last `replayLimit` envelopes. `reconnect(id, lastSeenServerSeq, uris)` re-subscribes every resumable URI and then:

| Situation                                                           | Result                                                                                                    |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `lastSeenServerSeq === serverSeq`, or the ring still covers the gap | `replay`, with the envelopes after `lastSeenServerSeq` whose channel is among the surviving subscriptions |
| the gap is wider than the ring                                      | `snapshot`, with one snapshot per surviving stateful URI                                                  |
| unknown route, or a missing instance                                | the URI goes into `missing`                                                                               |

The ring is empty after a restart, so only a fully caught-up client gets a replay then.

Duplicate detection is **per link**: each connection remembers the highest `clientSeq` it accepted, `connect()` starts it at 0, and it is
never persisted. A `clientSeq` that is not greater than the watermark makes `dispatchFrom` return `undefined` without consuming a sequence.

## Rejection and lenient dispatch

A reducer refuses an action by throwing `RejectAction(reason)`. The hub commits the envelope with `rejectionReason` set to that reason,
leaves the state untouched, consumes a sequence, and still delivers it to every subscriber. `RejectAction` is deliberately not a
`DurableChannelError`: refusing an action is normal channel behaviour.

`dispatchFrom(..., { lenient: true })` extends the same treatment to inputs the hub can classify itself, for transports where a dispatch has
no response channel:

| Condition                                           | Strict                       | Lenient                                                   |
| --------------------------------------------------- | ---------------------------- | --------------------------------------------------------- |
| unknown action name                                 | `UnknownActionError`         | rejected envelope, `rejectionReason: "UNKNOWN_ACTION: …"` |
| server-only action from a connection                | `NotClientDispatchableError` | rejected envelope, `"NOT_CLIENT_DISPATCHABLE: …"`         |
| payload fails its schema                            | `InvalidPayloadError`        | rejected envelope, `"INVALID_PAYLOAD: …"`                 |
| unknown route, missing instance, unknown connection | throws                       | throws — there is nothing to echo on                      |

## Client

`DurableChannelClient` is the other half of the isomorphism. Hand it the **same route map** the hub mounts and a transport, and it mirrors
every channel it subscribes to by running the very same reducers — no separate schema, no separate model, nothing to keep in step. The
snippets below assume a map that mounts a tic-tac-toe `game:/:id` and a chat `room:/:name`, which is the pair `client.test.ts` plays with.

```ts
import { DurableChannelClient, WebSocketTransport } from "durable-channel";

const transport = await WebSocketTransport.connect("ws://127.0.0.1:8000/rpc");
const client = new DurableChannelClient(routes, transport, { clientId: "alice" });
client.connect();
await client.hello({ subscriptions: ["game:/42"] });

const move = client.dispatch("game:/42", "game/moved", { cell: 4, player: "X" });
client.state("game:/42").board[4]; // "X" — already, before a frame has come back
client.confirmed("game:/42").board[4]; // null — the hub has not spoken yet
const outcome = await move.settled; // { status: "confirmed", envelope: … }

const { subscription } = await client.subscribe("room:/lobby");
for await (const event of subscription) {
	if (event.type === "state") {
		render(event.state);
	}
}
```

| Member                                                    | Returns                                      | Notes                                                                           |
| --------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------- |
| `new DurableChannelClient(routes, transport, options?)`   | the client                                   | `clientId` defaults to `crypto.randomUUID()`, `requestTimeoutMs` to 30 000      |
| `connect()`                                               | `void`                                       | starts the receive loop over the transport; idempotent                          |
| `hello({ subscriptions? })`                               | `Promise<{ serverSeq, snapshots }>`          | binds the client id, subscribes, applies the snapshots                          |
| `reconnect(transport?)`                                   | `Promise<replay \| snapshot>`                | resumes from `lastSeenServerSeq`, on a fresh transport when one is given        |
| `subscribe(uri)`                                          | `Promise<{ snapshot, subscription }>`        | the iterator is attached before the request leaves, so nothing is missed        |
| `attachSubscription(uri)` / `unsubscribe(uri)`            | a subscription / `Promise<void>`             | another consumer for a URI already subscribed / tells the hub and forgets it    |
| `dispatch(uri, name, payload)`                            | `{ clientSeq, settled }`                     | applies the action locally, then sends it. `settled` never rejects              |
| `exec(uri, name, params)`                                 | `Promise<result>`                            | typed from the route's command                                                  |
| `state(uri)` / `confirmed(uri)` / `pending(uri)`          | the optimistic state / the confirmed one / … | typed from the route the URI literal matches; `unknown` for a runtime string    |
| `events()` / `stateChanges()`                             | `AsyncIterableIterator<…>`                   | every inbound event as `{ channel, event }` / every connection-state transition |
| `connectionState` / `lastSeenServerSeq` / `subscriptions` | the link's state / a number / the URIs       | what a caller needs to decide when to reconnect                                 |
| `ping()` / `shutdown()`                                   | `Promise<void>`                              | `ping` works before `hello`; `shutdown` is final                                |
| `of(template)`                                            | a typed handle                               | `.state({ id })`, `.dispatch({ id }, …)`, `.exec`, `.subscribe`, like the hub's |

### Reconciliation

Per subscribed channel the client keeps three things: `confirmed`, the last state the hub agreed to; `pending`, the actions it has
dispatched and not seen echoed; and `optimistic`, the result of replaying `pending` on top of `confirmed`. A `dispatch` runs the action's
reducer against the optimistic state straight away, so the caller's own move is on screen before the frame has left. When the echo comes
back and its `origin` is this client's, the matching pending action leaves the queue and — unless the hub refused it — is applied to
`confirmed`; an envelope from anyone else is applied to `confirmed` and the pending actions rebase on top of it. The optimistic state is
recomputed after every one of those steps and a `state` event is emitted, which is the only event a view has to watch. The server always
wins: a reducer that refuses an action locally does not stop it being sent, and a rejected echo leaves `confirmed` exactly where it was,
which is what rolls the optimistic effect back.

Everything the route map can decide is decided before a frame leaves — an unknown action, an action the definition did not mark `.client()`
and a payload that fails its schema all throw the `DurableChannelError` the hub would have raised. Effects, background work and `env` are
server-side and the client ignores them, which is exactly why a reducer has to be pure: it is the one piece of a definition that runs twice.

| `ClientEvent`                                      | Fires when                                                                         |
| -------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `{ type: "action", envelope }`                     | an envelope for the channel arrives, rejected ones included                        |
| `{ type: "notification", channel, name, payload }` | the hub pushed a notification                                                      |
| `{ type: "state", channel, state }`                | the channel's optimistic state changed: snapshot, echo, optimistic apply, rollback |

| `DispatchOutcome`                          | Meaning                                                                                    |
| ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `{ status: "confirmed", envelope }`        | the hub committed it and the state moved                                                   |
| `{ status: "rejected", envelope, reason }` | the hub committed a rejected envelope; the optimistic effect is gone                       |
| `{ status: "duplicate" }`                  | the `clientSeq` was not greater than the link's watermark, so the hub ignored it           |
| `{ status: "lost", error? }`               | it never became an envelope: an `RpcError`, a timeout, a shutdown, or a snapshot reconnect |

### Reconnect

The client never retries and never backs off. It reports a dead transport through `connectionState` and `stateChanges()`, and the
application decides what to do — usually open a new transport and call `reconnect(next)`.

| The hub answers | The client does                                                                                                         |
| --------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `replay`        | applies the envelopes in order, so a pending action settles from its own echo; whatever is still pending settles `lost` |
| `snapshot`      | replaces the confirmed states and settles every pending action `lost`, because the hub can no longer say what happened  |
| `missing`       | drops the URI, ends its subscription iterators and settles its pending actions `lost`                                   |

A transport death is not the end of the client: the mirrors, the subscriptions and the pending actions all survive it, which is what lets a
`replay` settle an action whose answer never made it back. `shutdown()` is the end — it settles everything `lost` and closes every stream.

## Wire protocol

`createRpc(hub)` builds the hub's JSON-RPC 2.0 surface: `{ collection, hana, handle(frame, link, signal?) }`, where `collection` is a hana
collection a host may fold into a larger one and `handle` answers one decoded frame. One `DurableChannelLink` binds one connection to one
client id; `hello` and `reconnect` call `link.bind(clientId)`, and every other method refuses until they have.

| Method        | Kind         | Params                                           | Result                                                        |
| ------------- | ------------ | ------------------------------------------------ | ------------------------------------------------------------- |
| `hello`       | request      | `{ clientId, subscriptions? }`                   | `{ serverSeq, snapshots }` — binds the link and subscribes it |
| `ping`        | request      | `{}`                                             | `null`. Works before `hello`                                  |
| `reconnect`   | request      | `{ clientId, lastSeenServerSeq, subscriptions }` | a reconnect result, envelopes as-is                           |
| `subscribe`   | request      | `{ channel }`                                    | `{ snapshot? }` — no member for a stateless channel           |
| `unsubscribe` | notification | `{ channel }`                                    | —                                                             |
| `dispatch`    | request      | `{ channel, clientSeq, name, payload }`          | the envelope, or `null` when the `clientSeq` was a duplicate  |
| `exec`        | request      | `{ channel, name, params }`                      | the command's result                                          |

`dispatch` is a request, not a notification, so a caller settles on the direct answer as well as on the echo — both paths lead to the same
state, because an envelope already applied is recognised by its `serverSeq`. It always dispatches leniently: an unknown action, a
server-only action and a payload that fails its schema come back as a rejected envelope every subscriber also sees.

The hub pushes two notifications back: `action`, whose params are an envelope without its `type`, and `notification`, whose params are
`{ channel, name, payload }`. `toRpcNotification(message)` and `fromRpcNotification(method, params)` are that translation, in both
directions, and `fromRpcNotification` answers `undefined` for anything this protocol does not define.

| `DurableChannelRpcErrorCodes` | Code     | Raised for                                                          |
| ----------------------------- | -------- | ------------------------------------------------------------------- |
| `ChannelNotFound`             | `-32001` | `ROUTE_NOT_FOUND`, `CHANNEL_NOT_FOUND` — an internal route included |
| `ChannelAlreadyExists`        | `-32002` | `CHANNEL_ALREADY_EXISTS`                                            |
| `NotInitialized`              | `-32003` | the link is bound to no client id yet                               |
| `ConnectionNotFound`          | `-32004` | `CONNECTION_NOT_FOUND`                                              |
| `ActionRejected`              | `-32005` | a command threw `RejectAction`; `data` carries `{ reason }`         |
| `MethodNotFound`              | `-32601` | an unknown method, and `UNKNOWN_COMMAND`                            |
| `InvalidParams`               | `-32602` | params or a payload that fails its schema                           |
| `InternalError`               | `-32603` | everything else, with no message from the inside                    |

A `DurableChannelError` keeps its stable code in the message as `` `${code}: ${message}` ``, so a peer can read what happened without
matching on prose. `toJsonRpcError(error)` is the mapping, exported for a host that answers its own methods alongside these.

## Transports

A `DurableChannelTransport` is a framed, ordered, bidirectional stream of **decoded** JSON-RPC messages — three calls, so the client itself
does no I/O at all.

```ts
interface DurableChannelTransport {
	send(frame: DurableChannelRpcFrame): void | Promise<void>;
	recv(): Promise<DurableChannelRpcFrame | null>;
	close(): void | Promise<void>;
}
```

`recv()` resolving to `null` is the clean end of the stream; anything else abnormal is a `TransportError` thrown from `recv()`, so a
consumer can tell a goodbye from a drop.

| Implementation                          | For                                                                                                      |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `InMemoryTransport.pair()`              | tests, and a client and a hub in one runtime. Frames go through JSON, so nothing a socket drops survives |
| `WebSocketTransport.connect(url)`       | the global `WebSocket`, in a browser, in Deno and in Node. One JSON text frame per message               |
| `WebSocketTransport.fromSocket(socket)` | a socket whose handshake you ran yourself                                                                |

Writing your own is those three methods and nothing else: deliver frames in order, resolve `recv()` with `null` on a clean close, throw a
`TransportError` on anything worse.

## Serving over WebSocket

`attachSocket(rpc, hub, socket)` is the whole server side for one socket: it owns a link, answers frames in arrival order — one promise
chain, so a slow `exec` cannot let the next frame overtake it — and turns everything the hub broadcasts into a notification frame. The
socket is typed as `{ send, close, addEventListener }`, so the global `WebSocket`, a Deno upgraded socket and a Durable Object's server-side
socket all fit.

```ts
import { Hono } from "hono";
import { attachSocket, createRpc, DurableChannelHub, MemoryStorage } from "durable-channel";

const hub = new DurableChannelHub(routes, { storage: new MemoryStorage(), env });
const rpc = createRpc(hub);

const app = new Hono();
app.get("/rpc", (context) => {
	const { socket, response } = Deno.upgradeWebSocket(context.req.raw);
	attachSocket(rpc, hub, socket);
	return response;
});

Deno.serve({ port: 8000 }, app.fetch);
```

It hands back `{ link, detach() }`. `detach()` disconnects the link from the hub and stops answering; the socket's own `close` and `error`
events call it already, so a host only needs it to tear a session down early.

## Storage

```ts
interface DurableChannelStorage {
	get(key: readonly string[]): Promise<unknown>;
	set(key: readonly string[], value: unknown): Promise<void>;
	delete(key: readonly string[]): Promise<void>;
	list(
		options: { prefix: readonly string[]; cursor?: string; limit?: number },
	): Promise<{ entries: { key: string[]; value: unknown }[]; cursor?: string }>;
}
```

| Key                          | Value                      |
| ---------------------------- | -------------------------- |
| `["hub", "serverSeq"]`       | the global sequence number |
| `["channel", template, uri]` | one instance's state       |

The template sits in the key so `list(template)` is a single prefix scan.

| Implementation  | Opened with                       | Notes                                                                                             |
| --------------- | --------------------------------- | ------------------------------------------------------------------------------------------------- |
| `MemoryStorage` | `new MemoryStorage()`             | process-local, values structurally cloned in and out                                              |
| `DenoKvStorage` | `await DenoKvStorage.open(path?)` | wraps `Deno.openKv(path)`; `":memory:"` gives a throwaway store; dispose it to close the database |

```ts
using storage = await DenoKvStorage.open(":memory:");
const hub = new DurableChannelHub(routes, { storage, env });
```

Both satisfy the same contract, which `storage.test.ts` asserts once and runs against each: keys sort segment-wise so `["a", "b"]` comes
before `["a", "b", "c"]`, a page carries a `cursor` only when an entry really follows it, cursors are opaque, and a value that was written
cannot be reached again through the object that wrote it. `DenoKvStorage` is the only Deno-specific code in the library and it reaches for
the runtime inside `open` alone, so importing `storage.ts` elsewhere never touches `Deno`; on a runtime without it, `open` fails with a
`DurableChannelError` whose code is `STORAGE_UNAVAILABLE`.

## Errors

Every failure is a `DurableChannelError` carrying a stable string `code`, so a transport can map codes onto its own error table without
matching on messages.

| Class                        | `code`                    | Raised when                                                                       |
| ---------------------------- | ------------------------- | --------------------------------------------------------------------------------- |
| `RouteNotFoundError`         | `ROUTE_NOT_FOUND`         | no template matches the URI                                                       |
| `ChannelNotFoundError`       | `CHANNEL_NOT_FOUND`       | the route matches but no instance exists                                          |
| `ChannelAlreadyExistsError`  | `CHANNEL_ALREADY_EXISTS`  | `create()` on a URI that already holds an instance                                |
| `UnknownActionError`         | `UNKNOWN_ACTION`          | no action of that name on the channel                                             |
| `UnknownCommandError`        | `UNKNOWN_COMMAND`         | no command of that name                                                           |
| `UnknownNotificationError`   | `UNKNOWN_NOTIFICATION`    | no notification of that name                                                      |
| `InvalidPayloadError`        | `INVALID_PAYLOAD`         | an action payload, command params or notification payload fails its schema        |
| `InvalidStateError`          | `INVALID_STATE`           | an initial state, a `create()` state or a reducer's output fails the state schema |
| `InvalidResultError`         | `INVALID_RESULT`          | a command handler's return value fails its result schema                          |
| `NotClientDispatchableError` | `NOT_CLIENT_DISPATCHABLE` | a connection dispatched a server-only action                                      |
| `ConnectionNotFoundError`    | `CONNECTION_NOT_FOUND`    | no connection under that id                                                       |
| `InvalidDefinitionError`     | `INVALID_DEFINITION`      | an incomplete member, a duplicate name, a template missing a parameter value      |
| `StatelessChannelError`      | `STATELESS_CHANNEL`       | a state operation on a channel declared without `.state()`                        |
| `NotInitializedError`        | `NOT_INITIALIZED`         | a wire call that needs a client id arrived on a link nothing has bound            |
| `RejectAction`               | —                         | a reducer refused the action; **not** a `DurableChannelError`                     |

`DurableChannelError` is also thrown directly, with the code `STORAGE_UNAVAILABLE`, when `DenoKvStorage.open` runs on a runtime that has no
`Deno.openKv`. An action effect's failure is not translated at all: whatever it throws reaches whoever dispatched the action.

A client raises those same classes for anything it can decide from the route map, and its own family for everything that is about the link
rather than about a channel:

| Class                       | Carries               | Raised when                                                                    |
| --------------------------- | --------------------- | ------------------------------------------------------------------------------ |
| `DurableChannelClientError` | —                     | the base class of the four below                                               |
| `RpcError`                  | `code`, `data`        | the peer answered a request with a JSON-RPC error object                       |
| `RpcTimeoutError`           | `method`, `timeoutMs` | the request was abandoned locally; no server error happened                    |
| `TransportError`            | `kind`                | `"closed"`, `"io"` or `"protocol"` — the stream ended, threw, or made no sense |
| `ClientClosedError`         | —                     | the client was shut down, before the call or while it was in flight            |

## Colocated limitations

- A client drives **one** transport at a time, and one hub. There is no multi-hub client, no retry or back-off policy, and no persisted
  client state: a fresh client starts from a `hello`, and only the same client instance can `reconnect` from what it has already seen.
  Several tabs of the same application are several clients, each with its own `clientId`; sharing one link between them is out of scope.
- A client's `lastSeenServerSeq` is one number over every channel, as the hub's `serverSeq` is. A client that somehow misses an envelope but
  applies a later one claims to have seen both, and only a fresh `subscribe` will restate that channel.
- A reducer that disagrees with the server's — a different build, a non-deterministic body — is not detected. The client applies what it can
  and keeps the confirmed state it was given; nothing compares the two.
- An effect is not transactional. It runs after the commit, so a failing effect leaves a committed envelope behind and the error only
  reaches whoever dispatched.
- `send` is fire-and-forget. The hub calls it synchronously and swallows a throwing `send`, dropping that connection, so a dead socket
  cannot break a commit. There is no backpressure, no delivery acknowledgement and no outbound queue.
- `list(template)` is a prefix scan with no ordering guarantee beyond the storage's key order, and no filtering.
- A `serverSeq` is consumed by every commit, including a rejection, and the hub never reuses or compacts sequence numbers.
- A snapshot is the whole state. `subscribe` takes no projection, so a peer that wants a window of a large state has to ask for it with a
  command of the channel's own.
- Background tasks live in memory. A hub restart forgets them: whatever they were doing has to be restarted by whoever notices, from the
  persisted state.
- Nothing is logged. Errors that cannot reach a caller are dropped, on the client as well: a malformed inbound frame is a `TransportError`
  rather than a warning, and an undecodable notification is simply ignored.
- The client answers a server-initiated request with `-32601`. There is no way to install a handler for one: the protocol has no method a
  hub calls on a peer.

## Distributed channels

Definitions, reducers, schemas, routes and transport types are shared. Each canonical channel URI resolves to one authoritative endpoint;
that host must guarantee one active owner, including across failover. An actor's process mutex only orders that actor's calls and does not
provide distributed fencing. Independent actors use independent stores and queues, so a stalled mutation on one channel cannot stop another
channel. One hot channel still executes its mutations serially.

The router contains no authoritative state, sequence or registry scan. Its `resolve(uri)` returns an actor endpoint. Gateways resolve
through the router, hold local sockets and persist subscription/binding metadata. Channel membership rows contain stable gateway IDs; host
adapters reconstruct RPC stubs from those IDs. The complete namespace plus canonical URI must identify an owner to avoid tenant collisions.
Host authentication binds a stable client identity on every gateway, independently of transient connection IDs.

The following assembly function makes the host dependencies explicit. The actor's store belongs to this URI alone; the gateway receives its
own independent store and scheduler. A host must install `actor.alarm()` and `gateway.alarm()` as their scheduled callbacks.

```ts
import {
	type ChannelStore,
	type DistributedGatewayResolver,
	type DistributedOwnerResolver,
	type DistributedScheduler,
	DurableChannelActor,
	DurableChannelGateway,
	DurableChannelRouter,
} from "durable-channel";

// Uses `routes` and Env from the quick start above.
function distributedHost(
	resolve: DistributedOwnerResolver,
	gateways: DistributedGatewayResolver,
	channelStore: ChannelStore,
	gatewayStore: ChannelStore,
	channelAlarm: DistributedScheduler,
	gatewayAlarm: DistributedScheduler,
) {
	const router = new DurableChannelRouter(routes, { resolve });
	const actor = new DurableChannelActor("counter://", routes, {
		env: { now: () => new Date().toISOString() },
		router,
		store: channelStore,
		scheduler: channelAlarm,
		gateways,
	});
	const gateway = new DurableChannelGateway({
		id: "gateway-0",
		router,
		store: gatewayStore,
		scheduler: gatewayAlarm,
	});
	return { actor, router, gateway };
}
```

`MemoryChannelStore` clones JSON values and rolls back thrown transactions, but is process memory. `SqliteChannelStore` accepts structural
SQL storage with `transactionSync()` and `sync()`, without importing `cloudflare:workers`. Transaction callbacks must be synchronous and
contain no remote I/O; adapters reject promises/thenables. The host durability boundary completes before successful commit acknowledgement
or publication. The SQLite example uses separate records for metadata/state, replay, receipts, membership and subscription rows.

`DistributedScheduler.arm(at)` must durably establish a wakeup before it resolves, only moving an existing wakeup earlier. Actor/gateway
ordering pre-arms work before committing it; host alarms call back after reconstruction. Process timers alone do not meet this contract on
an evictable runtime. Clock and transient timeout/recovery schedulers are injectable for deterministic tests. The example shows actual
SQLite transactions, durable alarms, serializable RPC errors and hibernatable WebSocket attachments.

For a browser using that host, connect with the shared transport and explicitly select the distributed client:

```ts
import { DurableChannelDistributedClient, WebSocketTransport } from "durable-channel";

const transport = await WebSocketTransport.connect("wss://your-distributed-host/connect");
const distributedClient = new DurableChannelDistributedClient(routes, transport);
await distributedClient.hello({ subscriptions: ["counter://"] });
const action = distributedClient.dispatch("counter://", "counter/incremented", { by: 2 });
const outcome = await action.settled; // confirmed, rejected, or unknown
console.log(distributedClient.state("counter://"), distributedClient.cursors);
```

The example Worker defines `counter:/:id` with an `add` action instead; its client imports
[`examples/durable-objects/routes.ts`](examples/durable-objects/routes.ts) and subscribes to known, host-created URIs. The router and
distributed client both expose typed `of(template)` handles. `createDistributedRpc(gateway)` handles framed JSON-RPC;
`attachDistributedSocket(rpc, gateway, socket,
{ connectionId, clientId })` binds a normal socket, and `attachDistributedTransport` accepts
an existing framed transport. Hibernating DOs use `createDistributedLink` with the restored binding and their own socket callbacks. Gateway
shutdown closes only its local sessions.

### Distributed recovery and lifecycle

A distributed snapshot is `{ resource, state, cursor: { generation, channelSeq } }`. An action envelope uses the shared action fields plus
`generation` and `channelSeq`, and client actions include `actionId`; it has no `serverSeq`. Sequences are nonnegative safe integers, with
the first committed action at 1. Rejected client actions retain unchanged state but still advance the channel sequence and replay a
rejection. Sequence exhaustion rejects before mutation.

Creation returns a fresh opaque generation. Destroy persists a tombstone, removes membership and aborts tracked local work. Recreating the
URI requires explicit creation and generates a different incarnation; even a singleton is not lazily revived from a tombstone. Stateful
public dispatch/commands carry the generation obtained by subscription. Direct endpoint destroy also requires it. Trusted router calls may
resolve the current generation at invocation time, or accept an explicit previously observed generation. Delayed old-generation requests
fail with `STALE_GENERATION`.

Reconnect uses subscriptions and a cursor map. For example, A's cursor of 100 cannot suppress B's action 5:

```json
{
	"subscriptions": ["counter:/a", "counter:/b"],
	"cursors": {
		"counter:/a": { "generation": "generation-a", "channelSeq": 100 },
		"counter:/b": { "generation": "generation-b", "channelSeq": 4 }
	}
}
```

The result contains exactly one entry per requested URI under `channels`. Each entry is `replay` with actions and a cursor, `snapshot` with
a full distributed snapshot, `stateless`, or `missing`. Entries can mix: A can receive a snapshot after history truncation while B replays
action 5. Missing/wrong-generation/future cursors require snapshots; only a complete retained interval produces replay. Absent, tombstoned,
internal or inaccessible resources return `missing`. Stateless membership resumes without a state cursor or replay history. Snapshot cuts
and membership handoff are consistent per channel, with no consistency promise across the result map.

The gateway registers membership before exposing each snapshot cut, buffers each local subscription during recovery and hands messages to
sockets in order. Acknowledgement means handoff to live sockets or their closure for recovery; it does not mean the application has applied
the message. Socket failure closes the session so reconnection uses the client's actual applied cursor. Persisted per-gateway publication
targets, bounded replay and alarms recover the last lost action even without a subsequent action. Truncated history falls back to snapshots.
Increasing membership revisions and generations fence delayed removal, lease cleanup and ACKs; expired leases need a higher revision to
rejoin. One stalled gateway cannot hold a channel commit or healthy gateway delivery.

Distributed mirrors ignore duplicate reductions but use matching IDs to settle actions. A gap pauses only that channel and schedules
recovery with bounded backoff even on an idle connection. Per-subscription epochs fence old replies, frames, unsubscribe/resubscribe and
transport replacement. `hello({ subscriptions })` replaces membership with the supplied set; omitting the set retains current subscriptions.
`reconnect(newTransport)` can move to another gateway with the same authenticated identity. A fresh client without mirror state requests
snapshots; browser persistence is not included.

A snapshot cannot prove whether an outstanding optimistic action committed: such actions settle `unknown`. Replay settles matching recorded
outcomes and marks unresolved actions unknown. The client never automatically creates a new action ID to repeat unknown work. Applications
must reconcile unknown outcomes. `connectionError` exposes protocol/backpressure/transport failure; recovery overflow closes the transport
while preserving actual applied cursors for the next reconnect. Client and gateway action buffers default to 256 entries and 1 MiB per
subscription, with client recovery snapshots counted against that byte budget. State sizes must fit the configured client recovery budget.
Distributed hello validates both request and response protocol identifiers; a distributed client closes a provisional global connection
after detecting its incompatible response.

### Retry receipts, effects and limits

`createDistributedActionId(deadline)` formats an action ID as `<Unix-milliseconds-deadline>.<random-nonce>`. The client defaults to a
60-second retry window. Keep the original ID, name, payload and generation when explicitly calling `action.retry()` after reconnect;
changing the deadline creates a new action, never a retry. Identity is supplied by the host. Receipts are scoped by channel generation and
stable client identity, and an ID reused with different name/payload fails with `ACTION_ID_CONFLICT`.

The actor atomically persists state, sequence, log, receipt and publication work. An exact retained retry returns the recorded envelope
without another reduction, sequence or effect. A retained receipt remains authoritative when returned after its deadline; after collection,
an expired ID returns `unknown` without executing the reducer, even if its original request was sent earlier. Admission uses the maximum of
owner time and a persisted expiration floor, preventing a backwards clock adjustment from reviving collected IDs. Clock skew can shorten the
useful window or make a deadline exceed the permitted horizon; callers should allow a margin and reconcile expiry rather than extend IDs
automatically.

Default actor limits are a 5-minute maximum retry horizon, 4096 receipts, 4 MiB of receipts and 256 replay actions. Live receipts are never
evicted merely to satisfy a count limit. Expired receipts may be collected; exhausted capacity returns retryable `RETRY_CAPACITY` before
mutation. The caller may retry the same ID within its original window after capacity clears. These bounds provide a finite retry contract,
not indefinite exactly-once processing. Default delivery settings are 8 concurrent gateway attempts, a 1-second retry delay, 5-second
attempt timeout and 30-second membership leases; gateway recovery/renewal defaults to 10 seconds. Hosts can configure these limits.

Commands, action effects and `background` run outside commit serialization; self-dispatch does not hold the originating transaction. They
are best-effort application work and arbitrary closures are not durable workflows. A failed distributed effect leaves the original commit
intact and is not replayed by a retry or delivery. After ambiguous storage success, a receipt can confirm the commit without re-running an
effect; an effect might never have started. Destroy aborts tracked work and generation checks reject later context operations even if a
callback ignores cancellation. An already-issued remote/external operation cannot be undone. Context operations on the originating URI
retain its generation; cross-channel operations check the origin is active, then resolve the target's current incarnation. Remote
cancellation is not implemented, and abort signals/functions never cross RPC. Cross-channel effects and commands are not atomic.

Distributed `list(template)` requires an injected `DistributedDirectory` whose `consistency` states its contract. Without one it throws
`UNSUPPORTED_OPERATION`; there is no automatic global catalog or distributed prefix scan. Public routes/actions and schemas are checked at
the gateway/owner boundary, and internal endpoints are trusted host capabilities. Authentication and application authorization remain host
supplied. Local celld tests establish single-node development behavior only; they do not establish production throughput or fleet failover.

### Migrating definitions and storage

Common command/effect contexts now expose `DurableChannelCommit`, the union of the existing `DurableChannelEnvelope` and
`DurableChannelDistributedEnvelope`. This is a source-level change for callbacks inspecting ordering fields; narrow explicitly:

```ts
const committed = await ctx.dispatch(ctx.uri, "counter/incremented", { by: 1 });
if ("serverSeq" in committed) console.log(committed.serverSeq);
else console.log(committed.generation, committed.channelSeq);
```

Callbacks that ignore sequence fields work with both runtimes. Concrete global hub APIs still return `DurableChannelEnvelope`; concrete
distributed APIs return distributed envelopes. The old global envelope, snapshot, storage and reconnect wire shapes are unchanged, and the
AHP fixture uses those shapes unchanged.

Distributed actor/gateway stores use separate namespaces and the new transactional `ChannelStore` contract. They do not read or
automatically convert existing `DurableChannelStorage` records. Moving an existing deployment requires an explicit offline export of
application state, import into fresh distributed generations, and fresh client initialization. An automatic migration tool is deferred.
Select and expose a separate distributed endpoint when moving clients; do not add a distributed flag to an existing AHP endpoint.
