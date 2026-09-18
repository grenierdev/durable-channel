# Distributed channels in Durable Objects

This example uses one SQLite Durable Object per channel and four gateway buckets. The Worker selects a bucket from the authenticated client
identity unless an explicit bucket is supplied. Channel object names encode the full logical hub namespace and canonical URI; gateway names
encode the namespace and bucket. There is no global state object, sequence allocator or directory.

With Deno 2.9.6, celld 0.5.0 and esbuild available, run the example tasks from this directory:

```sh
cd examples/durable-objects
deno install --frozen
deno task distributed:do:check
deno task dev
```

`dev` runs `celld dev` on `http://127.0.0.1:9876` and watches the project. In another terminal, run:

```sh
cd examples/durable-objects
deno task distributed:do:test
```

The check typechecks the harness and Worker, then runs `celld deploy --dry-run` against the checked-in Wrangler configuration. The test uses
real HTTP and WebSocket connections to the already-running celld server. Set `CELLD_TEST_URL` to use a different listener and
`CELLD_TEST_RUN_ID` to make channel names reproducible. Every run otherwise creates unique channel URIs and destroys them during cleanup;
celld retains its local state under `.celld/dev` across restarts.

The harness exercises two gateway objects and two channel objects through celld's Durable Object RPC, WebSockets, SQLite storage and alarms.
It verifies fan-out, independent sequences, a lost final publication recovered by an alarm, moving a client between gateways, retained
action receipts, stale-generation fencing and state reconstruction on a fresh connection. The public celld listener does not expose the
operator eviction API, so this harness does not force hibernation, process restart, ownership movement or multi-node failover.

## Host wiring

`ChannelObject` persists its namespace/URI before any actor operation can establish an alarm. Its actor stores state, sequence, generation,
replay, receipts and gateway membership in SQLite. `GatewayObject` stores connection/subscription metadata in its own SQLite database;
WebSocket attachments contain stable connection and authenticated client IDs plus the binding revision. Reconstruction supplies live
hibernating sockets to `gateway.restore(..., { recover: false })`, then starts bounded per-channel recovery outside the constructor's
`blockConcurrencyWhile` gate. One unreachable owner cannot hold the entire gateway constructor open.

`SqliteChannelStore` uses synchronous SQL transactions and awaits `storage.sync()` before returning. Alarm scheduling reads/sets the
persistent alarm and awaits the same boundary. Actor/gateway mutation ordering pre-arms required wakeups before committing pending work, so
eviction between commit and publication cannot strand the last action. Transient timeout timers bound RPC attempts; the durable alarm
provides recovery after eviction. RPC errors cross as controlled DTOs and are revived at the trusted endpoint adapter. RPC results are
copied as plain data. No stub, callback, abort signal or socket is stored in a channel record.

The example retains eight actions per channel, retries pending delivery after 100 ms, bounds a delivery attempt to 5 seconds, renews gateway
membership every 10 seconds, and leases memberships for 30 seconds. Core receipt and buffer limits still apply. These are illustrative
settings, not capacity recommendations.

## Example controls and Cloudflare prerequisites

The checked-in `wrangler.jsonc` declares SQLite migrations and bindings for both classes. The integration Worker exposes `/test` controls
and accepts a `client` query parameter on `/connect`; these make the celld harness self-contained and are not an authentication design.
Before deploying this example, remove those controls, supply a unique `LOGICAL_HUB`, and require a host-authenticated `AUTH` service
binding. Its `fetch(request)` must return a successful JSON response with a nonempty, stable `clientId`, or reject authentication. The
Worker forwards that identity to gateways; dispatch JSON must not choose it in a production host. Browser clients connect to `/connect`
using `WebSocketTransport`, then select `DurableChannelDistributedClient` and perform the distributed hello/subscription handshake.

Provision known family URIs such as `counter:/a` through trusted host code using `ownerEndpoint(...).create()` before subscribing. Do not
expose the trusted endpoint methods to untrusted request JSON. Public distributed requests cannot access internal routes. The example needs
no channel directory; `router.list()` requires an application-supplied directory provider.

Production authentication, origin policy, deployment credentials and application authorization are host responsibilities; this repository
does not provision them.

## celld validation boundary

The integration task targets celld 0.5.0's persistent local development server. `celld dev` uses a local SQLite object store, so a
successful run establishes API compatibility and local durability behavior rather than replicated fleet durability. Live ownership movement
and failover require multiple celld nodes plus shared object storage and are outside this task. In a fleet, `storage.sync()` must represent
the configured durability boundary; local development storage alone does not imply replication.

Relevant host contracts are documented in the official
[SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/),
[WebSocket hibernation guide](https://developers.cloudflare.com/durable-objects/best-practices/websockets/),
[RPC lifecycle guide](https://developers.cloudflare.com/workers/runtime-apis/rpc/lifecycle/) and
[celld compatibility documentation](https://celld.dev/docs/cloudflare-compat/). Runtime behavior claimed above is exercised on celld 0.5.0
in local development mode; current celld documentation is not evidence of multi-node fleet behavior.
