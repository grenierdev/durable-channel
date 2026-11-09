---
title: Introduction
description: Understand the client–hub model and choose the shortest path to your first synchronized feature.
order: 0
visual: sync
---

Durable Channel keeps realtime state at a URI and gives that state a small, explicit protocol. You define the state shape and every allowed change once. The hub owns the authoritative instance; clients mirror it by running the same pure reducers.

## The model in one minute

A **channel definition** combines a Valibot state schema, an initial value, named actions, commands, and notifications. Mount definitions on URI templates to make a route map, then give that map to both the hub and the client.

The hub orders accepted and rejected actions under one global `serverSeq`. A connected client applies its own action optimistically, then reconciles when the authoritative envelope returns. Other clients apply that same envelope to the same reducer.

```text
client dispatch → optimistic state → hub validates → reducer → persist
                                                       ↓
other clients ← same reducer ← committed envelope ← broadcast
```

## Pick a path

- **New to the library?** Build the counter in the [tutorial](/docs/tutorial/first-channel), then read [Core concepts](/docs/concepts/primitives).
- **Designing your protocol?** Start with [Actions and validation](/docs/concepts/actions), then separate server work in [Commands and effects](/docs/concepts/commands-effects).
- **Adding identity or access rules?** Read [Authentication and authorization](/docs/guides/authentication) to attach principals and gate client operations.
- **Shipping a connection?** Read [Clients and reconnect](/docs/concepts/clients-reconnect), followed by [Transport and storage](/docs/guides/transport-storage).
- **Generating tooling?** Go to [Schema document](/docs/reference/schema-document) for the route description format.

## What Durable Channel owns

Durable Channel owns state validation, ordered commits, subscriptions, optimistic reconciliation, replay, snapshots, and enforcement of declared channel permissions. It carries a host-authenticated, typed principal through connections and handlers, but your application still defines the authorization policy and owns credential verification, revocation, reconnect timing, backoff, deployment, and the transport host.

That boundary is deliberate. The core uses web-standard APIs, while the JSON-RPC and WebSocket layer lives separately in `rpc.ts` and `transport.ts`.

## A useful vocabulary

| Term | Meaning |
| --- | --- |
| Channel | A definition plus one stateful or stateless protocol |
| Route | A URI template mounted to a channel definition |
| Instance | The state stored at one concrete URI |
| Action | A named, validated state transition |
| Command | Typed server work with a result |
| Effect | Server work triggered after an action commits |
| Notification | A live, ephemeral message that is never replayed |
| Envelope | The ordered record broadcast for an action |
| Principal | Optional host-authenticated identity attached to a connection |
| Client policy | Server-side permission check for subscriptions, actions, and commands |

## Before you build

`durable-channel` is published to npm. Install it with `npm install durable-channel`; the examples in these guides import that package with Valibot.
