---
title: Commands, effects, and background work
description: Put server-only work in the right lifecycle stage without making reducers impure.
section: Build
order: 5
visual: effects
---

Commands and effects both receive typed operations, but they enter the lifecycle from different places. A caller invokes a command directly; an effect follows an accepted action.

## Commands return results

A command declares a name, parameter schema, result schema, and async handler. The handler receives parsed parameters and returns input accepted by the result schema.

```ts
.command((command) =>
  command
    .name("reset")
    .params(v.object({ to: v.optional(v.number(), 0) }))
    .result(v.object({ count: v.number(), at: v.string() }))
    .handler(async ({ to }, ctx) => {
      await ctx.dispatch(ctx.uri, "counter/reset", { to });
      return { count: to, at: ctx.env.now() };
    }),
)
```

The context can read state, dispatch, notify, execute other commands, create or destroy instances, list a route family, and manage background work. It also exposes `connectionId` and the optional typed `principal` supplied by the host.

For a remote command, the channel policy receives parsed params and must grant `Execute` before the handler runs. A denial throws `PermissionDeniedError`. Server `hub.exec(...)` and nested `ctx.exec(...)` calls are trusted and bypass the policy; command handlers may still enforce their own business rules.

`ctx.exec` preserves the accepted caller’s connection, principal, and abort signal through nested commands. By contrast, `hub.exec(...)` is trusted server work with no caller identity; use `hub.execFrom(connection, ...)` for a remote command.

## Effects follow a commit

An effect runs only after the action is accepted, persisted, and broadcast. It sees the post-reducer state, parsed payload, committed envelope, URI, connection id, and optional principal.

Because the effect runs outside the commit mutex, it may dispatch again. That new action takes the next `serverSeq`. If the effect throws, its caller sees the error after the first action has already committed.

## Background work outlives the caller

Use `ctx.background(task)` for a stream, long import, watcher, or agent turn that should not delay the command or effect. Every task belongs to the URI that started it and shares an abort signal with that instance.

Check `signal.aborted` after every `await`. Tasks are aborted by `ctx.abortBackground()`, `hub.destroy(uri)`, and `hub.close()`.

## Choose the primitive

| Need | Primitive |
| --- | --- |
| Deterministic state change | Action reducer |
| Request with a typed result | Command |
| Work caused by a committed action | Effect |
| Live event with no history | Notification |
| Work that should not delay the caller | Background task |

Continue with [Authentication and authorization](/docs/authentication) to attach trusted identity and enforce client permissions.
