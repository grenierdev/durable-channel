import { describe, it } from "node:test";
import { assert, assertEquals, assertInstanceOf, assertRejects, assertThrows } from "@std/assert";
import { TransportError } from "./error.ts";
import { InMemoryTransport, WebSocketTransport } from "./transport.ts";

/**
 * A server that answers every request frame with its own params. `/garbage` sends a frame that is not
 * JSON at all, and `abort()` tears the whole listener down so a client sees a drop rather than a
 * goodbye — closing a socket with any code, 1012 included, still completes the handshake.
 */
function serve() {
	const sockets = new Set<WebSocket>();
	const aborter = new AbortController();
	const listener = Deno.serve({ port: 0, hostname: "127.0.0.1", signal: aborter.signal, onListen() {} }, (request) => {
		const path = new URL(request.url).pathname;
		if (path !== "/rpc" && path !== "/garbage") {
			return new Response("not here", { status: 404 });
		}
		const { socket, response } = Deno.upgradeWebSocket(request);
		sockets.add(socket);
		socket.addEventListener("open", () => {
			if (path === "/garbage") {
				socket.send("{ not json");
			}
		});
		socket.addEventListener("message", (event) => {
			const frame = JSON.parse(String(event.data)) as { id?: number; params?: unknown };
			socket.send(JSON.stringify({ jsonrpc: "2.0", id: frame.id ?? null, result: frame.params }));
		});
		socket.addEventListener("close", () => sockets.delete(socket));
		socket.addEventListener("error", () => sockets.delete(socket));
		return response;
	});
	const { port } = listener.addr as Deno.NetAddr;
	return {
		url: (path: string) => `ws://127.0.0.1:${port}${path}`,
		/** Tears the whole server down without a closing handshake, which is what a drop looks like. */
		async abort(): Promise<void> {
			aborter.abort();
			await listener.finished;
		},
		async stop(): Promise<void> {
			for (const socket of [...sockets]) {
				socket.close(1000);
			}
			aborter.abort();
			await listener.finished;
		},
	};
}

/** Fails fast with a message instead of hanging, and leaves no timer behind for the op sanitizer. */
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
			assertEquals(await server.recv(), { jsonrpc: "2.0", id: 1, method: "one", params: {} });
			assertEquals(await server.recv(), { jsonrpc: "2.0", method: "two", params: {} });
			server.send({ jsonrpc: "2.0", id: 1, result: "back" });
			assertEquals(await client.recv(), { jsonrpc: "2.0", id: 1, result: "back" });
		} finally {
			client.close();
		}
	});

	it("hands the frame over as JSON would, so nothing a socket drops survives", async () => {
		const [client, server] = InMemoryTransport.pair();
		try {
			const payload = { kept: 1, dropped: undefined };
			client.send({ jsonrpc: "2.0", id: 1, method: "one", params: payload });
			const received = await server.recv() as { params: Record<string, unknown> };
			assertEquals(Object.keys(received.params), ["kept"]);
			assert(received.params !== payload);
		} finally {
			client.close();
		}
	});

	it("wakes a parked receiver", async () => {
		const [client, server] = InMemoryTransport.pair();
		try {
			const parked = server.recv();
			client.send({ jsonrpc: "2.0", method: "late", params: {} });
			assertEquals(await withTimeout(parked, "the parked frame"), { jsonrpc: "2.0", method: "late", params: {} });
		} finally {
			client.close();
		}
	});

	it("ends both halves with null when either one closes, and refuses to send afterwards", async () => {
		const [client, server] = InMemoryTransport.pair();
		const parked = client.recv();
		server.close();
		assertEquals(await withTimeout(parked, "the clean end of the client half"), null);
		assertEquals(await client.recv(), null);
		assertEquals(await server.recv(), null);
		assertInstanceOf(assertThrows(() => client.send({ jsonrpc: "2.0", method: "gone", params: {} })), TransportError);
		client.close();
	});
});

describe("WebSocketTransport", () => {
	it("opens, exchanges frames and ends with null once it closes cleanly", async () => {
		const host = serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/rpc")), "the socket to open");
			transport.send({ jsonrpc: "2.0", id: 1, method: "echo", params: { hello: "there" } });
			assertEquals(await withTimeout(transport.recv(), "the echo"), { jsonrpc: "2.0", id: 1, result: { hello: "there" } });
			assertEquals(transport.lastClose, undefined);
			const parked = transport.recv();
			transport.close();
			assertEquals(await withTimeout(parked, "the clean end of the socket"), null);
			assertEquals(transport.lastClose?.wasClean, true);
			assertThrows(() => transport.send({ jsonrpc: "2.0", method: "gone", params: {} }), TransportError);
		} finally {
			await host.stop();
		}
	});

	it("reports a connection nobody closed properly as a transport error", async () => {
		const host = serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/rpc")), "the socket to open");
			const dropped = transport.recv();
			await host.abort();
			const failure = await assertRejects(() => withTimeout(dropped, "the drop"), TransportError);
			// A torn-down connection surfaces as the socket's `error` event or as an unclean `close`,
			// depending on which the runtime notices first. Either way it is not a clean end of stream.
			assert(failure.kind === "io" || failure.kind === "closed", `unexpected kind ${failure.kind}`);
			assertEquals(transport.lastClose?.wasClean ?? false, false);
			transport.close();
		} finally {
			await host.stop();
		}
	});

	it("reports a frame it cannot decode as a protocol error", async () => {
		const host = serve();
		try {
			const transport = await withTimeout(WebSocketTransport.connect(host.url("/garbage")), "the socket to open");
			const failure = await assertRejects(() => withTimeout(transport.recv(), "the undecodable frame"), TransportError);
			assertEquals(failure.kind, "protocol");
			transport.close();
		} finally {
			await host.stop();
		}
	});

	it("refuses to open when the route is not a socket", async () => {
		const host = serve();
		try {
			const failure = await assertRejects(() => withTimeout(WebSocketTransport.connect(host.url("/nope")), "the refusal"), TransportError);
			assert(failure.kind === "io" || failure.kind === "closed");
		} finally {
			await host.stop();
		}
	});
});
