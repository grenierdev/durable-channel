---
title: Schema document
description: Generate a machine-readable description of every mounted channel, action, command, and notification.
order: 1
visual: schema
---

`hub.generateSchema()` returns a `DurableChannelDocument`. Use `describeRoutes(routes)` when you have a route map without a hub. The document is designed for generators, inspectors, and protocol tooling.

## Generate the document

```ts
const document = hub.generateSchema({
  info: {
    title: "Collaboration hub",
    version: "1.0.0",
    description: "Shared document state",
  },
});
```

The format version is currently `0.1.0`. Each channel entry records its template, named parameters, summary, singleton and internal flags, and the schemas for its state and protocol members.

## Schema direction matters

Action payloads and command parameters describe what a peer sends, so they use Valibot input types. State, command results, and notification payloads describe what a peer receives, so they use output types.

A schema without a JSON Schema equivalent becomes `{}` rather than failing the entire document.

## Document summaries

Give a definition a `.summary()` or pass a route summary when mounting it. The route summary takes precedence and becomes the channel entry’s summary.

```ts
const counter = durableChannel()
  .summary("A number every connected client can increment")
  .state(v.object({ count: v.number() }), { count: 0 })
  .build();
```

## Key exports

| Export | Purpose |
| --- | --- |
| `durable_channel_DOCUMENT_VERSION` | Current document format version |
| `describeRoutes(routes, options)` | Describe a route map without a hub |
| `hub.generateSchema(options)` | Describe the hub’s mounted routes |
| `DurableChannelDocument` | Type of the generated document |

Return to the [documentation overview](/docs) to choose another guide, or open the source repository for the full API and tests.
