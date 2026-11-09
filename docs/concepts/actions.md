---
title: Actions and validation
description: Follow an action from optimistic client state through validation, persistence, broadcast, and reconciliation.
order: 2
visual: commit
---

An action gives one state change a stable name, a payload schema, and a reducer. That is enough for the type system, the client, and the hub to agree on what the change means.

## The commit sequence

The hub performs one ordered path:

1. Validate and parse the payload.
2. For a client dispatch, authorize `Dispatch` against current state.
3. Run the reducer against current state.
4. Validate the returned state.
5. Persist state and the next `serverSeq`.
6. Append the envelope to the replay ring.
7. Broadcast the envelope to subscribers.

A single promise-chain mutex serializes commits across the entire hub. Commands run outside it and acquire it only when they dispatch.

## Client-safe actions

Calling `.client()` on an action exposes it to `dispatchFrom` and to typed client dispatch. An unmarked action remains server-only. A client attempting it receives `NotClientDispatchableError` before an invalid frame leaves the process. This exposure is separate from the channel builder’s `.client(handler)` policy, which can deny `Dispatch` for a particular connection or request.

```ts
const updateTitle = (action: DurableChannelActionBuilder<Document>) =>
  action
    .name("document/titleChanged")
    .payload(v.object({ title: v.string() }))
    .client()
    .reduce((state, { title }) => ({ ...state, title }));
```

The reducer’s third argument carries `meta.origin` for client dispatches: `{ clientId, clientSeq, principalId? }`. It is absent for server dispatches, including actions emitted by commands. Because the origin is part of the envelope and replay, a reducer can enforce deterministic ownership without gaining access to server-only services.

```ts
.reduce((state, message, meta) => {
  if (message.author !== meta.origin?.principalId) {
    throw new RejectAction("author must match the authenticated principal");
  }
  return { ...state, messages: [...state.messages, message] };
})
```

Use this for identity equality that every client can replay. Keep role and permission checks in the server-side channel policy or command handlers, where the full principal remains private.

## Reject without losing order

Throw `RejectAction(reason)` inside a reducer to refuse a validly shaped payload. The hub consumes a sequence number and broadcasts an envelope with `rejectionReason`, while leaving state unchanged. A channel policy that denies `Dispatch` produces the same ordered kind of rejection before the reducer runs, and no effect runs.

That envelope gives every observer the same account of what happened. The originating client removes the pending optimistic action and recomputes its view from confirmed state.

## Lenient dispatch

`dispatchFrom(..., { lenient: true })` converts unknown actions, server-only actions, and invalid payloads into rejected envelopes instead of throwing. The JSON-RPC surface always uses lenient dispatch so all subscribers see the same refusal.

Continue with [Commands and effects](/docs/concepts/commands-effects) to place server work around the ordered reducer path.
