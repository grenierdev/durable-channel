/**
 * How a client's frames get to a server and back.
 *
 * A {@link DurableChannelTransport} is a framed, ordered, bidirectional stream of JSON-RPC messages.
 * It is the only thing a {@link DurableChannelClient} needs, which is what keeps the client itself free
 * of any I/O: a WebSocket, a pair of in-memory halves, a message port, a worker channel or a pipe all
 * fit behind three calls. Frames are **decoded** messages, not text — the transport owns the encoding,
 * so a peer that already has objects (a `MessagePort`, a same-process pair) never pays for JSON.
 *
 * `recv()` resolving to `null` is the clean end of the stream; anything abnormal is a
 * {@link TransportError} thrown from `recv()`, so a consumer can tell a graceful goodbye from a drop.
 */
import { TransportError } from "./error.ts";
import type { DurableChannelRpcFrame } from "./rpc.ts";

/** A framed, ordered, bidirectional stream of JSON-RPC messages. */
export interface DurableChannelTransport {
	/** Sends one frame. Throws or rejects with a {@link TransportError} when the stream is unusable. */
	send(frame: DurableChannelRpcFrame): void | Promise<void>;
	/**
	 * The next inbound frame, or `null` once the peer has closed cleanly. Throws a
	 * {@link TransportError} on an abnormal close, an I/O failure or an undecodable frame.
	 */
	recv(): Promise<DurableChannelRpcFrame | null>;
	/** Closes the stream. Idempotent, and never throws. */
	close(): void | Promise<void>;
}

interface Waiter {
	resolve(frame: DurableChannelRpcFrame | null): void;
	reject(error: TransportError): void;
}

/**
 * A pair of halves wired to each other in one process. Built for tests and for a client and a hub that
 * live in the same runtime — a worker, a Durable Object serving its own page — with no socket between
 * them.
 *
 * Frames go through `JSON.parse(JSON.stringify(frame))`, so a value a real socket would drop (an
 * `undefined` member, a `Map`, a function) is dropped here too and a test cannot pass by accident.
 * Closing either half ends `recv()` with `null` on **both**: there is one stream, not two.
 */
export class InMemoryTransport implements DurableChannelTransport {
	#inbox: (DurableChannelRpcFrame | null)[] = [];
	#waiters: Waiter[] = [];
	#closed = false;
	#peer: InMemoryTransport | undefined;

	private constructor() {}

	/** A connected pair. Whatever one half sends is what the other half receives. */
	static pair(): [InMemoryTransport, InMemoryTransport] {
		const left = new InMemoryTransport();
		const right = new InMemoryTransport();
		left.#peer = right;
		right.#peer = left;
		return [left, right];
	}

	send(frame: DurableChannelRpcFrame): void {
		if (this.#closed) {
			throw new TransportError("closed", "The transport is closed");
		}
		const peer = this.#peer;
		if (peer !== undefined) {
			peer.#deliver(JSON.parse(JSON.stringify(frame)) as DurableChannelRpcFrame);
		}
	}

	recv(): Promise<DurableChannelRpcFrame | null> {
		if (this.#inbox.length > 0) {
			return Promise.resolve(this.#inbox.shift() ?? null);
		}
		if (this.#closed) {
			return Promise.resolve(null);
		}
		return new Promise((resolve, reject) => {
			this.#waiters.push({ resolve, reject });
		});
	}

	close(): void {
		if (this.#closed) {
			return;
		}
		this.#closed = true;
		this.#deliver(null);
		this.#peer?.close();
	}

	#deliver(frame: DurableChannelRpcFrame | null): void {
		const waiter = this.#waiters.shift();
		if (waiter !== undefined) {
			waiter.resolve(frame);
			return;
		}
		this.#inbox.push(frame);
	}
}

/** Options of {@link WebSocketTransport.connect}. */
export interface WebSocketTransportOptions {
	/** Subprotocols to negotiate. In a browser this and the query string are the only extension points. */
	readonly protocols?: string | readonly string[];
}

/** What the socket's `close` event carried, once it has fired. */
export interface WebSocketCloseInfo {
	readonly code: number;
	readonly reason: string;
	readonly wasClean: boolean;
}

/**
 * A transport over the global `WebSocket`, so it runs in a browser, in Deno and in Node without a
 * dependency. Messages are one JSON text frame each; a binary frame is decoded as UTF-8 text.
 *
 * A clean close ends `recv()` with `null`. An unclean close, a socket error and an undecodable frame
 * all surface as a {@link TransportError} from the next `recv()` — an undecodable frame included,
 * because a peer speaking this protocol never sends one and there is no logger to leave a breadcrumb in.
 */
export class WebSocketTransport implements DurableChannelTransport {
	#socket: WebSocket;
	#inbox: (DurableChannelRpcFrame | null)[] = [];
	#waiters: Waiter[] = [];
	#close: WebSocketCloseInfo | undefined;
	#error: TransportError | undefined;
	#closed = false;

	private constructor(socket: WebSocket) {
		this.#socket = socket;
		socket.binaryType = "arraybuffer";
		socket.addEventListener("message", (event) => {
			try {
				this.#deliver(decode(event.data));
			} catch (error) {
				this.#fail(error instanceof TransportError ? error : new TransportError("protocol", "Undecodable frame", { cause: error }));
			}
		});
		socket.addEventListener("error", () => {
			this.#fail(new TransportError("io", "The socket reported an error"));
		});
		socket.addEventListener("close", (event) => {
			this.#close = { code: event.code, reason: event.reason, wasClean: event.wasClean };
			this.#closed = true;
			if (event.wasClean) {
				const waiters = this.#waiters.splice(0);
				for (const waiter of waiters) {
					waiter.resolve(null);
				}
				return;
			}
			this.#fail(new TransportError("closed", `The socket closed abnormally (code ${event.code})`));
		});
	}

	/**
	 * Opens a socket and resolves once it is open. Rejects with a {@link TransportError} when the socket
	 * errors or closes before that.
	 */
	static connect(url: string | URL, options?: WebSocketTransportOptions): Promise<WebSocketTransport> {
		return new Promise((resolve, reject) => {
			const Socket = (globalThis as { WebSocket?: typeof WebSocket }).WebSocket;
			if (Socket === undefined) {
				reject(new TransportError("io", "This runtime has no global WebSocket"));
				return;
			}
			let socket: WebSocket;
			try {
				socket = options?.protocols === undefined ? new Socket(url) : new Socket(url, [...options.protocols]);
			} catch (error) {
				reject(new TransportError("io", "The socket could not be constructed", { cause: error }));
				return;
			}
			const settle = (outcome: () => void): void => {
				socket.removeEventListener("open", opened);
				socket.removeEventListener("error", failed);
				socket.removeEventListener("close", ended);
				outcome();
			};
			const opened = (): void => settle(() => resolve(new WebSocketTransport(socket)));
			const failed = (): void => settle(() => reject(new TransportError("io", "The socket failed to open")));
			const ended = (event: CloseEvent): void =>
				settle(() => reject(new TransportError("closed", `The socket closed before it opened (code ${event.code})`)));
			socket.addEventListener("open", opened);
			socket.addEventListener("error", failed);
			socket.addEventListener("close", ended);
		});
	}

	/** Wraps a socket that is already open, for a host that ran the handshake itself. */
	static fromSocket(socket: WebSocket): WebSocketTransport {
		if (socket.readyState !== socket.OPEN) {
			throw new TransportError("io", "The socket is not open");
		}
		return new WebSocketTransport(socket);
	}

	/** What the `close` event carried, or `undefined` while the socket is still open. */
	get lastClose(): WebSocketCloseInfo | undefined {
		return this.#close;
	}

	/** Bytes the socket has queued but not yet written. There is no backpressure beyond reading this. */
	get bufferedAmount(): number {
		return this.#socket.bufferedAmount;
	}

	send(frame: DurableChannelRpcFrame): void {
		if (this.#error !== undefined) {
			throw this.#error;
		}
		if (this.#closed) {
			throw new TransportError("closed", "The transport is closed");
		}
		try {
			this.#socket.send(JSON.stringify(frame));
		} catch (error) {
			throw new TransportError("io", "The socket refused the frame", { cause: error });
		}
	}

	recv(): Promise<DurableChannelRpcFrame | null> {
		if (this.#inbox.length > 0) {
			return Promise.resolve(this.#inbox.shift() ?? null);
		}
		if (this.#error !== undefined) {
			return Promise.reject(this.#error);
		}
		if (this.#closed) {
			return Promise.resolve(null);
		}
		return new Promise((resolve, reject) => {
			this.#waiters.push({ resolve, reject });
		});
	}

	close(): void {
		if (this.#socket.readyState === this.#socket.OPEN || this.#socket.readyState === this.#socket.CONNECTING) {
			try {
				this.#socket.close();
			} catch {
				// Closing a socket is best effort: it is already going away.
			}
		}
	}

	#deliver(frame: DurableChannelRpcFrame): void {
		const waiter = this.#waiters.shift();
		if (waiter !== undefined) {
			waiter.resolve(frame);
			return;
		}
		this.#inbox.push(frame);
	}

	#fail(error: TransportError): void {
		this.#error ??= error;
		const waiters = this.#waiters.splice(0);
		for (const waiter of waiters) {
			waiter.reject(this.#error);
		}
	}
}

function decode(data: unknown): DurableChannelRpcFrame {
	if (typeof data === "string") {
		return JSON.parse(data) as DurableChannelRpcFrame;
	}
	if (data instanceof ArrayBuffer) {
		return JSON.parse(new TextDecoder().decode(data)) as DurableChannelRpcFrame;
	}
	if (data instanceof Uint8Array) {
		return JSON.parse(new TextDecoder().decode(data)) as DurableChannelRpcFrame;
	}
	throw new TransportError("protocol", "The socket delivered a payload that is neither text nor bytes");
}
