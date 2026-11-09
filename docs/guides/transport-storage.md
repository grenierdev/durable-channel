---
title: Transport and storage
description: Host the JSON-RPC link over WebSocket and choose a persistence adapter for the hub.
order: 3
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

Install the Node.js adapters with `npm install @hono/node-server hono ws`, plus `npm install -D @types/ws` for TypeScript.

```ts
import { serve, upgradeWebSocket } from "@hono/node-server";
import { Hono } from "hono";
import { WebSocketServer, type WebSocket } from "ws";
import { attachSocket, createRpc, DurableChannelHub } from "durable-channel";

const hub = new DurableChannelHub(routes, { storage, env });
const rpc = createRpc(hub);
const app = new Hono();

app.get("/rpc", async (context, next) => {
  const request = context.req.raw;
  if (!isAllowedOrigin(request)) return context.text("Forbidden", 403);
  const principal = await readSession(request);
  return upgradeWebSocket(() => ({
    onOpen(_event, socket) {
      const session = attachSocket(rpc, hub, socket.raw as WebSocket, principal ? { principal } : undefined);
      sessions.add(session);
    },
  }))(context, next);
});

serve({ fetch: app.fetch, port: 8000, websocket: { server: new WebSocketServer({ noServer: true }) } });
```

Keep the returned session in a host-owned registry. On logout, expiry, or revocation, call `session.close(1008, "Session ended")`. `detach()` only unbinds and stops processing; `close()` also closes the socket.

## Choose storage

`MemoryStorage` keeps structurally cloned values in one process. `SqliteStorage` persists them as JSON through Node.js SQLite. Import it from its own entry point, so the core library never loads `node:sqlite`.

```ts
import { SqliteStorage } from "durable-channel/storage/sqlite";

using storage = await SqliteStorage.open("./channels.sqlite");
const hub = new DurableChannelHub(routes, { storage, env });
```

A file path persists across restarts. Without a path, `open()` gives a throwaway `":memory:"` store. Close the hub before you dispose its storage.

Storage keeps the global counter at `["hub", "serverSeq"]` and an instance at `["channel", template, uri]`. The template makes `list(template)` one prefix scan.

## Shut down cleanly

Call `await hub.close()` during tests and process shutdown. It aborts background tasks, waits for them to settle, and refuses new work.

Continue with [Schema document](/docs/reference/schema-document) to describe the mounted protocol for tooling.
