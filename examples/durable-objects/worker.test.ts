/// <reference lib="deno.ns" />
/** Integration against a temporary `celld dev --clean` instance. */
import { describe, it, type TestContext, type TestContextWaitForOptions } from "node:test";
import { assert, assertEquals, assertRejects } from "@std/assert";
import {
	type DistributedSnapshot,
	DurableChannelDistributedClient,
	type DurableChannelRpcFrame,
	type DurableChannelTransport,
} from "../../src/mod.ts";
import { routes } from "./routes.ts";

const baseUrl = new URL(Deno.env.get("CELLD_TEST_URL") ?? "http://127.0.0.1:9876");
const runId = (Deno.env.get("CELLD_TEST_RUN_ID") ?? crypto.randomUUID()).replaceAll(/[^a-zA-Z0-9-]/g, "-");
const channelA = `counter:/celld-${runId}-a`, channelB = `counter:/celld-${runId}-b`;
const clients: DurableChannelDistributedClient[] = [];
const sockets: WebSocket[] = [];
let celld: Deno.ChildProcess | undefined;
let celldStatus: Promise<Deno.CommandStatus> | undefined;

function endpoint(path: string): URL {
	return new URL(path, baseUrl);
}

function ensureWaitFor(context: TestContext): void {
	if (typeof context.waitFor === "function") return;
	context.waitFor = async <T>(condition: () => T, options: TestContextWaitForOptions = {}): Promise<Awaited<T>> => {
		const end = Date.now() + (options.timeout ?? 1000);
		let lastError: unknown;
		do {
			try {
				return await condition();
			} catch (error) {
				lastError = error;
			}
			await new Promise((resolve) => setTimeout(resolve, options.interval ?? 50));
		} while (Date.now() <= end);
		throw lastError;
	};
}

function transport(socket: WebSocket): DurableChannelTransport {
	const queue: (DurableChannelRpcFrame | null)[] = [], waiters: ((frame: DurableChannelRpcFrame | null) => void)[] = [];
	let closed = false;
	const deliver = (frame: DurableChannelRpcFrame | null): void => {
		const waiting = waiters.shift();
		if (waiting) waiting(frame);
		else queue.push(frame);
	};
	socket.addEventListener("message", (event) => deliver(JSON.parse(String(event.data))));
	const ended = (): void => {
		if (closed) return;
		closed = true;
		while (waiters.length) waiters.shift()!(null);
	};
	socket.addEventListener("close", ended);
	socket.addEventListener("error", ended);
	return {
		send: (frame) => socket.send(JSON.stringify(frame)),
		recv: () =>
			queue.length ? Promise.resolve(queue.shift()!) : closed ? Promise.resolve(null) : new Promise((resolve) => waiters.push(resolve)),
		close: () => {
			if (socket.readyState < WebSocket.CLOSING) socket.close(1000, "Local test finished");
			ended();
		},
	};
}

async function open(bucket: number | undefined, identity: string): Promise<DurableChannelTransport> {
	const url = endpoint("/connect");
	url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
	if (bucket !== undefined) url.searchParams.set("gateway", String(bucket));
	url.searchParams.set("client", identity);
	const socket = new WebSocket(url);
	await new Promise<void>((resolve, reject) => {
		const timeout = setTimeout(() => reject(new Error(`Timed out connecting to ${url}`)), 5000);
		socket.addEventListener("open", () => {
			clearTimeout(timeout);
			resolve();
		}, { once: true });
		socket.addEventListener("error", () => {
			clearTimeout(timeout);
			reject(new Error(`WebSocket connection failed: ${url}`));
		}, { once: true });
	});
	sockets.push(socket);
	return transport(socket);
}

async function createClient(bucket: number | undefined, identity: string): Promise<DurableChannelDistributedClient> {
	const client = new DurableChannelDistributedClient(routes, await open(bucket, identity), {
		retryWindowMs: 240_000,
		requestTimeoutMs: 5000,
	});
	clients.push(client);
	await client.hello({ subscriptions: [channelA, channelB] });
	return client;
}

interface ControlResult {
	ok: boolean;
	value?: unknown;
	code?: string;
}

async function controlRaw(body: Record<string, unknown>): Promise<ControlResult> {
	const response = await fetch(endpoint("/test"), {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
	});
	const text = await response.text();
	try {
		return JSON.parse(text) as ControlResult;
	} catch {
		throw new Error(`Invalid test-control response from ${endpoint("/test")}: ${response.status} ${text}`);
	}
}

async function control<T = unknown>(body: Record<string, unknown>): Promise<T> {
	const result = await controlRaw(body);
	assert(result.ok, result.code);
	return result.value as T;
}

async function startCelld(): Promise<void> {
	assertEquals(baseUrl.protocol, "http:", "CELLD_TEST_URL must use http");
	assertEquals(baseUrl.pathname, "/", "CELLD_TEST_URL must not include a path");
	const port = baseUrl.port || "80";
	celld = new Deno.Command("celld", {
		args: ["dev", ".", "--host", baseUrl.hostname, "--port", port, "--clean", "--no-watch"],
		cwd: new URL(".", import.meta.url),
		stdout: "inherit",
		stderr: "inherit",
	}).spawn();
	celldStatus = celld.status;

	const ready = (async () => {
		const end = Date.now() + 15_000;
		while (Date.now() <= end) {
			try {
				const response = await fetch(endpoint("/.well-known/celld/health"));
				if (response.status === 200) return;
			} catch {
				// The listener is not ready yet.
			}
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
		throw new Error(`Timed out waiting for celld at ${baseUrl}`);
	})();
	const exited = celldStatus.then((status) => {
		throw new Error(`celld exited before becoming ready (code ${status.code})`);
	});
	await Promise.race([ready, exited]);
}

async function stopCelld(): Promise<void> {
	if (!celld) return;
	try {
		celld.kill("SIGTERM");
	} catch (error) {
		if (!(error instanceof TypeError && error.message === "Child process has already terminated")) throw error;
	}
	await celldStatus;
	celld = undefined;
	celldStatus = undefined;
}

async function preflight(): Promise<void> {
	const health = await fetch(endpoint("/.well-known/celld/health"));
	assertEquals(health.status, 200, `celld is not healthy at ${baseUrl}`);
	assertEquals(await health.json(), { ok: true });
	await control({ op: "identities", uri: channelA, bucket: 0 });
}

async function destroyIfPresent(uri: string): Promise<void> {
	try {
		const snapshot = await control<DistributedSnapshot | undefined>({ op: "snapshot", uri });
		if (snapshot) await control({ op: "destroy", uri, generation: snapshot.cursor.generation });
	} catch {
		// Best-effort cleanup must not replace the original test result.
	}
}

describe("distributed Durable Objects", () => {
	it("runs the celld integration scenarios", async (context) => {
		ensureWaitFor(context);
		try {
			await startCelld();
			await preflight();
			const a = await control<DistributedSnapshot>({ op: "create", uri: channelA }),
				b = await control<DistributedSnapshot>({ op: "create", uri: channelB });
			assert(a.cursor.generation !== b.cursor.generation);
			const names0 = await control<{ channel: string; gateway: string }>({ op: "identities", uri: channelA, bucket: 0 });
			const names1 = await control<{ channel: string; gateway: string }>({ op: "identities", uri: channelB, bucket: 1 });
			assert(names0.gateway !== names1.gateway && names0.channel !== names1.channel, "expected distinct gateway and channel identities");

			const first = await createClient(0, "alice"), second = await createClient(1, "bob");
			const action = first.dispatch(channelA, "add", 1);
			assertEquals((await action.settled).status, "confirmed");
			await context.waitFor(() => assertEquals((second.state(channelA) as { count: number }).count, 1), {
				interval: 10,
				timeout: 8000,
			});
			await second.dispatch(channelB, "add", 3).settled;
			await first.dispatch(channelA, "add", 1).settled;
			await context.waitFor(
				() => assertEquals([first.cursors[channelB].channelSeq, second.cursors[channelA].channelSeq], [1, 2]),
				{ interval: 10, timeout: 8000 },
			);
			assertEquals([first.cursors[channelA].channelSeq, first.cursors[channelB].channelSeq], [2, 1]);
			await assertRejects(() => first.exec(channelA, "reject", null), Error, "example refusal");
			await assertRejects(() => first.exec(channelA, "explode", null), Error, "Internal error");
			const defaultClient = await createClient(undefined, "carol");
			assertEquals(defaultClient.state(channelA), { count: 2 });
			await defaultClient.shutdown();
			assertEquals([first.state(channelA), second.state(channelA)], [{ count: 2 }, { count: 2 }]);

			await control({ op: "failDelivery", bucket: 0 });
			await control({ op: "dispatch", uri: channelA, by: 4 });
			await context.waitFor(
				() =>
					assertEquals(
						[(first.state(channelA) as { count: number }).count, (second.state(channelA) as { count: number }).count],
						[6, 6],
					),
				{ interval: 10, timeout: 3000 },
			);
			assertEquals([first.state(channelA), second.state(channelA)], [{ count: 6 }, { count: 6 }]);

			const replay = await first.reconnect(await open(1, "alice"));
			const retried = await action.retry();
			assertEquals(
				[replay.channels[channelA].type, retried.type, retried.type === "committed" ? retried.envelope.channelSeq : undefined],
				["replay", "committed", 1],
			);

			const generation = first.cursors[channelA].generation;
			await control({ op: "destroy", uri: channelA, generation });
			await control({ op: "create", uri: channelA });
			const stale = await controlRaw({ op: "dispatch", uri: channelA, generation, by: 100 });
			assertEquals(stale, { ok: false, code: "STALE_GENERATION" });
			await first.recover(channelA);
			assertEquals(first.state(channelA), { count: 0 });
			assert(first.cursors[channelA].generation !== generation);
			assertEquals(first.cursors[channelA].channelSeq, 0);

			const savedCursor = first.cursors[channelB];
			await first.shutdown();
			await second.shutdown();
			const restarted = await createClient(0, "alice");
			assertEquals(restarted.state(channelB), { count: 3 });
			assertEquals(restarted.cursors[channelB], savedCursor);
			assertEquals(restarted.state(channelA), { count: 0 });
		} finally {
			for (const client of clients) await client.shutdown().catch(() => {});
			for (const socket of sockets) {
				try {
					socket.close(1000, "Cleanup");
				} catch { /* Already closed. */ }
			}
			await destroyIfPresent(channelA);
			await destroyIfPresent(channelB);
			await stopCelld();
		}
	});
});
