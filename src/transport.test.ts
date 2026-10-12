import { deepStrictEqual, ok, rejects, throws } from "node:assert/strict";
import { upgradeWebSocket } from "@hono/node-server";
import { Hono } from "hono";
import { describe, it } from "vitest";
import { serveApp } from "../test/server.ts";
import { TransportError } from "./error.ts";
import { InMemoryTransport, WebSocketTransport } from "./transport.ts";

/**
 * A server that answers every request frame with its own params. `/garbage` sends a frame that is not
 * JSON at all, and `abort()` tears the whole listener down so a client sees a drop rather than a
 * goodbye — closing a socket with any code, 1012 included, still completes the handshake.
 */
async function serve() {
	const app = new Hono();
	for (const path of ["/rpc", "/garbage"]) {
		app.get(
			path,
			upgradeWebSocket(() => ({
				onOpen(_event, socket) {
					if (path === "/garbage") socket.send("{ not json");
				},
				onMessage(event, socket) {
					const frame = JSON.parse(String(event.data)) as { id?: number; params?: unknown };
					socket.send(JSON.stringify({ jsonrpc: "2.0", id: frame.id ?? null, result: frame.params }));
				},
			})),
		);
	}
	const listener = await serveApp(app);
	return { ...listener, abort: listener.stop };
}

/** Fails fast with a message instead of hanging, and leaves no timer behind. */
function withTimeout<T>(promise: Promise<T>, what: string, ms = 5000): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const guard = new Promise<never>((_resolve, reject) => {
		timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
	});
	return Promise.race([promise, guard]).finally(() => clearTimeout(timer)) as Promise<T>;
}

describe("InMemoryTransport", () => {
	it("delivers what one half sends to the other, in order", async () => {
		const [client, server] = InMemoryTransport.pair();
		try {
			client.send({ jsonrpc: "2.0", id: 1, method: "one", params: {} });
			client.send({ jsonrpc: "2.0", method: "two", params: {} });
			deepStrictEqual(await server.recv(), { jsonrpc: "2.0", id: 1, method: "one", params: {} });
			deepStrictEqual(await server.recv(), { jsonrpc: "2.0", method: "two", params: {} });
			server.send({ jsonrpc: "2.0", id: 1, result: "back" });
			deepStrictEqual(await client.recv(), { jsonrpc: "2.0", id: 1, result: "back" });
		} finally {
			client.close();
		}
	});

	it("hands the frame over as JSON would, so nothing a socket drops survives", async () => {
		const [client, server] = InMemoryTransport.pair();
		try {
			const payload = { kept: 1, dropped: undefined };
			client.send({ jsonrpc: "2.0", id: 1, method: "one", params: payload });
			const received = (await server.recv()) as { params: Record<string, unknown> };
			deepStrictEqual(Object.keys(received.params), ["kept"]);
			ok(received.params !== payload);
		} finally {
			client.close();
		}
	});

	it("wakes a parked receiver", async () => {
		const [client, server] = InMemoryTransport.pair();
		try {
			const parked = server.recv();
			client.send({ jsonrpc: "2.0", method: "late", params: {} });
			deepStrictEqual(await withTimeout(parked, "the parked frame"), { jsonrpc: "2.0", method: "late", params: {} });
		} finally {
			client.close();
		}
	});

	it("ends both halves with null when either one closes, and refuses to send afterwards", async () => {
		const [client, server] = InMemoryTransport.pair();
		const parked = client.recv();
		server.close();
		deepStrictEqual(await withTimeout(parked, "the clean end of the client half"), null);
		deepStrictEqual(await client.recv(), null);
		deepStrictEqual(await server.recv(), null);
		throws(() => client.send({ jsonrpc: "2.0", method: "gone", params: {} }), TransportError);
		client.close();
	});
});

describe("WebSocketTransport", () => {
	it("opens, exchanges frames and ends with null once it closes cleanly", async () => {
		const host = await serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/rpc")), "the socket to open");
			transport.send({ jsonrpc: "2.0", id: 1, method: "echo", params: { hello: "there" } });
			deepStrictEqual(await withTimeout(transport.recv(), "the echo"), { jsonrpc: "2.0", id: 1, result: { hello: "there" } });
			deepStrictEqual(Boolean(transport.lastClose), false);
			const parked = transport.recv();
			transport.close();
			deepStrictEqual(await withTimeout(parked, "the clean end of the socket"), null);
			deepStrictEqual(transport.lastClose?.wasClean, true);
			throws(() => transport.send({ jsonrpc: "2.0", method: "gone", params: {} }), TransportError);
		} finally {
			await host.stop();
		}
	});

	it("reports a connection nobody closed properly as a transport error", async () => {
		const host = await serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/rpc")), "the socket to open");
			const dropped = transport.recv();
			await host.abort();
			await rejects(
				() => withTimeout(dropped, "the drop"),
				(error) => error instanceof TransportError && (error.kind === "io" || error.kind === "closed"),
			);
			deepStrictEqual(transport.lastClose?.wasClean ?? false, false);
			transport.close();
		} finally {
			await host.stop();
		}
	});

	it("reports a frame it cannot decode as a protocol error", async () => {
		const host = await serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/garbage")), "the socket to open");
			await rejects(
				() => withTimeout(transport.recv(), "the undecodable frame"),
				(error) => error instanceof TransportError && error.kind === "protocol",
			);
			transport.close();
		} finally {
			await host.stop();
		}
	});

	it("refuses to open when the route is not a socket", async () => {
		const host = await serve();
		try {
			await rejects(
				() => withTimeout(WebSocketTransport.connect(host.url("/nope")), "the refusal"),
				(error) => error instanceof TransportError && (error.kind === "io" || error.kind === "closed"),
			);
		} finally {
			await host.stop();
		}
	});
});
