---
title: Core concepts
description: Learn the four primitives and why pure reducers let the same definition run on both sides.
order: 1
visual: definition
---

The definition is the center of Durable Channel. It is immutable while you build it: every builder call returns a new value, and the resulting types accumulate the members and optional client policy you registered.

## State

Calling `.state(schema, initialState)` turns a relay into a stateful channel. The schema validates initial values, created instances, and every reducer result. Leave state out when late subscribers do not need history, as with telemetry or a live chat feed.

## Actions

An action is a named state transition with a payload schema and reducer. The reducer receives `(state, payload, meta)`, where `meta` includes the URI, route parameters, and an optional action origin. Use the action builder’s `.client()` only for actions a connection may originate; a channel policy may further restrict which connections can dispatch them.

Actions are the only primitive that enters the hub-wide ordered sequence. Every committed envelope receives a `serverSeq`, including a rejected action.

## Commands

A command runs on the server, validates its parameters and result, and may read or change channels through its context. Its context also carries the optional authenticated principal for remote work. Use commands for work that needs a return value or access to environment services, and authorize remote execution with the channel policy or the handler’s business rules.

Commands do not become envelopes by themselves. When a command calls `ctx.dispatch`, that action becomes the ordered change.

## Effects and notifications

An action effect runs after its envelope has been persisted and broadcast. It can dispatch again, notify subscribers, call another command, or start background work. If it fails, the original action remains committed.

A notification is ephemeral. Current subscribers receive it; storage and replay do not retain it.

## Why reducers stay pure

The client and hub both run action reducers. A clock, random identifier, database call, network request, or cross-channel operation would make those two executions disagree. Keep ambient work in commands and effects, then feed deterministic data into actions.

```ts
.action((action) =>
  action
    .name("document/renamed")
    .payload(v.object({ title: v.string() }))
    .client()
    .reduce((state, { title }) => ({ ...state, title }))
)
```

Continue with [Actions and validation](/docs/concepts/actions) for commit rules and rejection behavior.
