---
title: Clients and reconnect
description: Understand optimistic state, reconciliation, replay, snapshots, and the application’s retry boundary.
section: Operate
order: 8
visual: replay
---

A client holds confirmed state, pending local actions, and an optimistic view for every subscription. It owns one transport at a time and reports connection state; the application decides when and how to reconnect.

## Optimistic state

`client.dispatch` runs the reducer immediately against the optimistic state and returns `{ clientSeq, settled }`. The interface can read the new value before the frame reaches the hub.

When the authoritative echo returns, the client removes the matching pending action and applies the envelope to confirmed state. Envelopes from other clients update confirmed state first, then pending actions are replayed over it.

## Reconnect with the same client

After a transport dies, keep the client instance. Its subscriptions, mirrors, pending actions, and `lastSeenServerSeq` survive. Open a new transport and call `client.reconnect(next)`.

```ts
const next = await WebSocketTransport.connect(url);
const result = await client.reconnect(next);
```

The library does not retry or back off automatically. That policy belongs to the application.

A successful `hello` or reconnect exposes only `{ id }` as `client.principal`; custom fields such as roles stay on the server. If a reconnect changes the principal ID—including a sign-in or sign-out—the client throws `IdentityChangedError`, closes, clears its mirrors, and settles pending actions as lost. Create a fresh client when the account changes.

## Replay or snapshot

If the replay ring still covers the gap, the hub returns later envelopes for the surviving subscriptions. If the gap is wider than the ring, it returns fresh snapshots instead.

| Hub answer | Client response |
| --- | --- |
| Replay | Apply envelopes in order and settle matching pending actions |
| Snapshot | Replace confirmed state and settle pending actions as lost |
| Missing URI | Drop the subscription and end its iterator; this also covers a denied `Subscribe` permission |

Use a distinct `clientId` per tab. Several tabs may share one principal ID, but they must remain separate clients with separate reconnect state.

The ring is empty after a hub restart. Only a fully caught-up client can receive an empty replay then; other clients need snapshots.

Reconnect rechecks the channel’s `Subscribe` permission for every requested URI. Denied channels appear in `missing` and receive neither replayed envelopes nor snapshots. A successful subscription admits continued delivery until unsubscribe or disconnect, so close affected sessions when revoking access.

## Close means final

`shutdown()` is different from a dropped transport. It settles pending work as lost, closes streams, and makes the client unusable. Use it when the application is truly done with the client.

Continue with [Transport and storage](/docs/transport-storage) to connect the protocol to your runtime.
