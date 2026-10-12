import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { type ServerType, serve } from "@hono/node-server";
import type { Hono } from "hono";
import { WebSocketServer } from "ws";

/** A real Node HTTP/WebSocket listener, with explicit teardown for upgraded connections. */
export async function serveApp(app: Hono) {
	const websocket = new WebSocketServer({ noServer: true });
	const listener = serve({ fetch: app.fetch, port: 0, hostname: "127.0.0.1", websocket: { server: websocket } });
	await once(listener, "listening");
	const { port } = listener.address() as AddressInfo;
	let stopped = false;
	return {
		url: (path: string) => `ws://127.0.0.1:${port}${path}`,
		async stop(): Promise<void> {
			if (stopped) return;
			stopped = true;
			for (const socket of websocket.clients) socket.terminate();
			websocket.close();
			await close(listener);
		},
	};
}

function close(listener: ServerType): Promise<void> {
	return new Promise((resolve, reject) => listener.close((error?: Error) => (error ? reject(error) : resolve())));
}
