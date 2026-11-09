---
title: Build your first channel
description: Define a counter, mount it at a URI, start a hub, and commit the first action.
order: 1
visual: definition
---

This guide builds a stateful singleton at `counter://`. Its single client-safe action adds a positive integer to the shared count.

## Import the building blocks

Install the published package, then import it by name:

```sh
npm install durable-channel
```

```ts
import * as v from "valibot";
import {
  durableChannel,
  DurableChannelHub,
  durableRoutes,
  MemoryStorage,
  RejectAction,
} from "durable-channel";
```

## Define state and one action

The reducer is synchronous and pure. Marking the action with `.client()` lets connected clients dispatch it and apply it optimistically.

```ts
const counter = durableChannel()
  .state(v.object({ count: v.number() }), { count: 0 })
  .action((action) =>
    action
      .name("counter/incremented")
      .payload(v.object({ by: v.pipe(v.number(), v.integer()) }))
      .client()
      .reduce((state, { by }) => {
        if (by <= 0) throw new RejectAction("by must be positive");
        return { count: state.count + by };
      }),
  )
  .build();
```

## Mount the route and create the hub

A template with no named parameters is a singleton. Its initial state is created on first touch.

```ts
const routes = durableRoutes()
  .route("counter://", counter)
  .build();

const hub = new DurableChannelHub(routes, {
  storage: new MemoryStorage(),
  env: {},
  replayLimit: 1024,
});
```

## Connect, subscribe, and dispatch

The connection receives every message for its subscriptions, including the envelope originating from itself.

```ts
const connection = hub.connect({ id: "alice-tab", send: console.log });

await hub.subscribe(connection, "counter://");
await hub.dispatchFrom(
  connection,
  "counter://",
  "counter/incremented",
  { by: 2 },
  1,
);

await hub.get("counter://"); // { count: 2 }
```

## Check the contract

Payloads are parsed before the reducer runs, and the reducer result is validated against the state schema before persistence. A `RejectAction` still produces an ordered envelope, but leaves state unchanged and carries a `rejectionReason`.

Continue with [Core concepts](/docs/concepts/primitives) to see how state, actions, commands, and notifications divide responsibility.
