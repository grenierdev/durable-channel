---
title: Authentication and authorization
description: Attach trusted identity to connections and authorize subscriptions, actions, and commands at the channel boundary.
section: Build
order: 6
visual: auth
---

Durable Channel carries authenticated identity; it does not authenticate credentials. Your host verifies a request, constructs a principal, and attaches it to the connection. A channel’s server-side client policy can then authorize subscriptions, action dispatches, and command execution. Principals remain optional, so policies must decide what anonymous callers may do.

## Declare the principal type

Declare the application’s principal before registering handlers, then use the same type on the route builder. `.principal<T>()` is a type declaration, not runtime validation. The library checks for a non-empty `id` and shallow-freezes the object; your middleware must validate every custom field.

```ts
import { AuthenticationRequiredError, PermissionDeniedError } from "durable-channel";

type Principal = {
  id: string;
  role: "admin" | "member";
  name: string;
};

const account = durableChannel()
  .env<Env>()
  .principal<Principal>()
  .command((command) =>
    command
      .name("adminName")
      .params(v.object({}))
      .result(v.string())
      .handler((_params, ctx) => {
        if (!ctx.principal) throw new AuthenticationRequiredError();
        if (ctx.principal.role !== "admin") throw new PermissionDeniedError();
        return ctx.principal.name;
      }),
  )
  .build();

const routes = durableRoutes()
  .env<Env>()
  .principal<Principal>()
  .route("account://", account)
  .build();
```

## Attach identity at the host boundary

Authenticate the HTTP request and check its allowed `Origin` before a browser WebSocket upgrade. Pass only the verified principal to `attachSocket`; never accept identity from a client frame.

```ts
async function handleUpgrade(request: Request): Promise<Response> {
  if (!isAllowedOrigin(request)) return new Response("Forbidden", { status: 403 });

  const principal: Principal | undefined = await readSession(request);
  const { socket, response } = Deno.upgradeWebSocket(request);
  const session = attachSocket(
    rpc,
    hub,
    socket,
    principal ? { principal } : undefined,
  );

  sessions.add(session);
  return response;
}
```

Without a socket, pass `principal` to `hub.connect(...)`. Keep the returned connection handle and use it with `subscribe`, `dispatchFrom`, `reconnect`, `unsubscribe`, `disconnect`, and `execFrom`.

## Choose the right authorization layer

Only the principal ID crosses the wire. The browser sees `{ id }` through `client.principal`, and client action envelopes may include `origin.principalId`. Custom fields such as roles and names stay on the server.

| Decision | Enforce it with |
| --- | --- |
| Payload author must equal the authenticated user | `meta.origin?.principalId` in the reducer |
| Connection may subscribe, dispatch, or execute commands | Channel builder’s `.client(handler)` policy |
| Action may be dispatched by any client at all | Action builder’s `.client()` marker |
| A command has a domain-specific precondition | Command handler and a typed error |
| Credentials are valid and not revoked | Host authentication and session lifecycle |

Reducers see only replayable identity data. Channel policies, commands, and effects receive the full principal on the server. Nested `ctx.exec` preserves it; `hub.exec(...)` is trusted server work without a caller, while `hub.execFrom(connection, ...)` represents remote work and hides internal routes.

## Authorize client operations

Call `.client(handler)` on the channel builder to declare a server-only policy. Return a permission bit field by combining `Subscribe`, `Dispatch`, and `Execute`, or return `None` or `All`.

```ts
import {
  durableChannel,
  DurableChannelPermission as Permission,
} from "durable-channel";
import * as v from "valibot";

type Principal = { id: string; role: "reader" | "editor" | "admin" };

const counter = durableChannel()
  .principal<Principal>()
  .state(v.object({ count: v.number() }), { count: 0 })
  .client((ctx, request) => {
    if (ctx.principal?.role === "admin") return Permission.All;
    if (request.type === "command" && request.name === "reset") {
      return Permission.None;
    }
    if (ctx.principal?.role === "editor") {
      return Permission.Subscribe | Permission.Dispatch | Permission.Execute;
    }
    return Permission.Subscribe;
  })
  .action((action) =>
    action
      .name("increment")
      .payload(v.object({ by: v.number() }))
      .client()
      .reduce((state, { by }) => ({ count: state.count + by })),
  )
  .build();
```

| Permission | Controls |
| --- | --- |
| `Subscribe` | Initial and later subscriptions, reconnect replay, and snapshots |
| `Dispatch` | Client-originated actions already exposed with the action builder’s `.client()` |
| `Execute` | Client-originated commands through `execFrom` or the RPC surface |

The policy context supplies `env`, `uri`, route `params`, `connectionId`, the optional typed `principal`, and `state()`. Declare `.state()` before `.client(handler)` to give `state()` its concrete type. The request is one of `{ type: "subscribe" }`, `{ type: "action", name, payload }`, or `{ type: "command", name, params }`. Payloads and params have passed their schemas, but their request fields remain typed as `unknown`.

Policies may be asynchronous, but they should only read state and decide permissions. Action authorization runs inside the commit lock, so a state-dependent check sees the same state the reducer would receive. Keep that path short, and never dispatch back into the hub from it.

## Understand denial and trust

A policy that throws or returns anything other than an integer permission field from `0` through `7` fails closed without exposing its internal error.

| Denied operation | Result |
| --- | --- |
| Subscription | `PermissionDeniedError`; a denied re-subscription also removes the existing subscription |
| Reconnect | The URI appears in `missing` and receives no replay or snapshot |
| Action | A rejected envelope consumes sequence, leaves state unchanged, and runs no effect |
| Command | `PermissionDeniedError` before the handler runs |

Clients never run the policy. They still apply allowed client actions optimistically, then reconcile when the hub broadcasts an accepted or rejected envelope.

Without a channel policy, all three permissions are allowed. Server `dispatch`, server `exec`, and nested work from an accepted handler are trusted and bypass the policy. Individual actions still require their action-level `.client()` marker, and internal routes remain inaccessible to remote clients.

## Handle replacement and revocation

Give each tab a distinct `clientId`; multiple tabs may share the same principal ID. Reusing a live client ID replaces its connection only when both principal IDs match, including when both are anonymous. A different principal receives `PermissionDeniedError` instead of displacing the active connection.

A reconnect must keep the same principal ID. If it changes because of sign-in, sign-out, or account switching, the client throws `IdentityChangedError` and closes. Create a fresh client for the new identity.

The host owns credential expiry and revocation. A granted `Subscribe` permission admits continued delivery until unsubscribe or disconnect; changing the policy does not eject an active subscriber. Keep socket sessions in a registry and call `session.close(1008, "Session ended")` when access ends. Work already admitted is not rolled back, and background work remains owned by its channel instance.

Continue with [Routes and instances](/docs/routes-instances) to separate public and internal channel surfaces.
