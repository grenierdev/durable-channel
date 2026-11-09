# Durable Channel

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

TanStack Start in SSG mode, with Tailwind CSS, explicitly selected by the user. Deployment target is unspecified. Browser interactions must work from the statically generated page.

## Users

Developers building applications with shared state that updates in realtime.

## Product Purpose

Help developers build and synchronize state. The user confirmed that the landing page should help developers understand the model and begin with an interactive example and quick start.

## Positioning

A channel holds state at a URI and declares named actions with pure, synchronous reducers. The same definition drives an authoritative server hub and clients that mirror state by applying those reducers.

## Operating Context

TypeScript definitions and Valibot schemas. The core uses web-standard APIs. Transport concerns are separate; the repository includes a JSON-RPC surface and WebSocket transport.

## Capabilities and Constraints

- Clients apply optimistic updates and reconcile against authoritative envelopes.
- Reconnection replays retained envelopes or returns snapshots when the replay ring does not cover the gap.
- Applications choose reconnection and backoff policy; the library does not retry automatically.
- Hosts may attach an authenticated, typed principal to a connection; commands and effects receive the full principal while action origins expose only its ID.
- Authentication is optional. Applications declare channel authorization policies for client subscriptions, dispatches, and commands, and still own credential verification and revocation.
- Commands and post-commit effects run on the server. Notifications are ephemeral.
- Storage is an interface, with memory and SQLite implementations. Memory storage is process-local.
- The package is published to npm for Node.js consumers.

## Evidence on Hand

Source: `../../durable-channel/README.md` and `../../durable-channel/src/`. Repository remote: https://github.com/grenierdev/durable-channel. No testimonials, performance benchmarks, customer logos, or commercial claims are supplied.

## Product Principles

- Explain state ownership and synchronization through working behavior.
- Use examples consistent with the actual library API.
- Make the path from demonstration to documentation clear.
- Distinguish illustrative network behavior from measured performance.
