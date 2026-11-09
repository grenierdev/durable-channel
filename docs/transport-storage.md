---
title: Transport and storage
description: Host the JSON-RPC link over WebSocket and choose a persistence adapter for the hub.
section: Operate
order: 9
visual: transport
---

The core does not know about a wire. A transport moves decoded JSON-RPC frames in order, while storage persists hub sequence and channel state.

## The transport contract

Implement three methods:

```ts
interface DurableChannelTransport {
  send(frame: DurableChannelRpcFrame): void | Promise<void>;
  recv(): Promise<DurableChannelRpcFrame | null>;
  close(): void | Promise<void>;
}
```

`recv()` returns `null` for a clean end and throws `TransportError` for an abnormal one. `InMemoryTransport.pair()` is useful in tests. `WebSocketTransport` accepts the global WebSocket API or a socket whose handshake you control.

## Serve one socket

`createRpc(hub)` builds the JSON-RPC surface. `attachSocket(rpc, hub, socket, { principal? })` owns one ordered session, binds its client id, replies to requests, and turns hub messages into notifications. Authenticate the request and validate its allowed `Origin` before upgrading; identity is trusted host input, never a field accepted from a protocol frame.

```ts
const hub = new DurableChannelHub(routes, { storage, env });
const rpc = createRpc(hub);

app.get("/rpc", async (context) => {
  const request = context.req.raw;
  if (!isAllowedOrigin(request)) return context.text("Forbidden", 403);
  const principal = await readSession(request);
  const { socket, response } = Deno.upgradeWebSocket(request);
  const session = attachSocket(rpc, hub, socket, principal ? { principal } : undefined);
  sessions.add(session);
  return response;
});
```

Keep the returned session in a host-owned registry. On logout, expiry, or revocation, call `session.close(1008, "Session ended")`. `detach()` only unbinds and stops processing; `close()` also closes the socket.

## Choose storage

`MemoryStorage` keeps structurally cloned values in one process. `DenoKvStorage` persists them through `Deno.openKv` and is the only Deno-specific code in the library.

```ts
using storage = await DenoKvStorage.open(":memory:");
const hub = new DurableChannelHub(routes, { storage, env });
```

Storage keeps the global counter at `["hub", "serverSeq"]` and an instance at `["channel", template, uri]`. The template makes `list(template)` one prefix scan.

## Shut down cleanly

Call `await hub.close()` during tests and process shutdown. It aborts background tasks, waits for them to settle, and refuses new work.

Continue with [Schema document](/docs/schema-document) to describe the mounted protocol for tooling.
