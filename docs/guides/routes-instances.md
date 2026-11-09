---
title: Routes and instances
description: Mount definitions on URI templates, create families, protect internal routes, and use typed handles.
order: 2
visual: routes
---

`durableRoutes()` maps URI templates to channel definitions. A literal segment matches itself; `:name` matches one non-empty segment. No scheme is special-cased.

## Singleton or family

A template without a named parameter, such as `counter://`, is a singleton. The hub creates it from the initial state on first touch.

A template such as `doc:/:id` is a family. Create each concrete URI before subscribing or dispatching:

```ts
const routes = durableRoutes()
  .route("counter://", counter)
  .route("doc:/:id", document)
  .build();

await hub.create("doc:/42", { title: "Draft" });
```

## Use a typed route handle

`hub.of(template)` binds the template once and types parameter objects, state, actions, and commands from it.

```ts
const docs = hub.of("doc:/:id");

docs.uri({ id: "42" });
await docs.create({ id: "42" }, { title: "Draft" });
await docs.dispatch({ id: "42" }, "doc/renamed", { title: "Final" });
await docs.get({ id: "42" });
```

## Keep internal routes private

Mount a catalogue or coordination channel with `{ internal: true }` when only server code should reach it. Connection-bound `subscribe`, `dispatchFrom`, and `execFrom` calls see `RouteNotFoundError`; trusted hub methods, commands, and effects still use the route normally.

```ts
durableRoutes()
  .route("doc:/:id", document)
  .route("x-catalog://", catalogue, { internal: true })
  .build();
```

## Destroy deliberately

`destroy(uri)` removes persisted state, aborts background work, drops subscriptions, and removes the instance. The hub does not infer ownership between channels. A parent that owns children should destroy them explicitly in its own command.

Continue with [Transport and storage](/docs/guides/transport-storage) to connect the protocol to your runtime.
