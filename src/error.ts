/**
 * The error taxonomy of a durable channel.
 *
 * Every failure the library raises is a {@link DurableChannelError} carrying a stable string `code`,
 * so a transport can map codes onto its own error table without matching on messages or classes. The
 * one exception is {@link RejectAction}: a reducer throwing it is a domain decision, not a library
 * failure, and the hub turns it into a rejected envelope instead of propagating it.
 */

/** Base class of every error the library raises. `code` is stable and safe to match on. */
export class DurableChannelError extends Error {
	readonly code: string;

	constructor(code: string, message: string) {
		super(message);
		this.name = new.target.name;
		this.code = code;
	}
}

/** No route template matches the URI. */
export class RouteNotFoundError extends DurableChannelError {
	constructor(uri: string) {
		super("ROUTE_NOT_FOUND", `No route matches "${uri}"`);
	}
}

/** The route matches but no instance exists at that URI. */
export class ChannelNotFoundError extends DurableChannelError {
	constructor(uri: string) {
		super("CHANNEL_NOT_FOUND", `No channel instance at "${uri}"`);
	}
}

/** `create()` was called for a URI that already holds an instance. */
export class ChannelAlreadyExistsError extends DurableChannelError {
	constructor(uri: string) {
		super("CHANNEL_ALREADY_EXISTS", `A channel instance already exists at "${uri}"`);
	}
}

/** The channel declares no action under that name. */
export class UnknownActionError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("UNKNOWN_ACTION", `Channel "${uri}" declares no action "${name}"`);
	}
}

/** The channel declares no command under that name. */
export class UnknownCommandError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("UNKNOWN_COMMAND", `Channel "${uri}" declares no command "${name}"`);
	}
}

/** The channel declares no notification under that name. */
export class UnknownNotificationError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("UNKNOWN_NOTIFICATION", `Channel "${uri}" declares no notification "${name}"`);
	}
}

/** An action payload, a command's params or a notification payload failed its schema. */
export class InvalidPayloadError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("INVALID_PAYLOAD", `Payload of "${name}" on "${uri}" failed its schema`);
	}
}

/** A reducer, or an explicit `create()`, produced a state that fails the channel's state schema. */
export class InvalidStateError extends DurableChannelError {
	constructor(uri: string) {
		super("INVALID_STATE", `State of "${uri}" failed its schema`);
	}
}

/** A command handler returned a value that fails its result schema. */
export class InvalidResultError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("INVALID_RESULT", `Result of command "${name}" on "${uri}" failed its schema`);
	}
}

/** A connection dispatched an action that is declared server-only. */
export class NotClientDispatchableError extends DurableChannelError {
	constructor(uri: string, name: string) {
		super("NOT_CLIENT_DISPATCHABLE", `Action "${name}" on "${uri}" is server-only`);
	}
}

/** No connection is registered under that id. */
export class ConnectionNotFoundError extends DurableChannelError {
	constructor(connectionId: string) {
		super("CONNECTION_NOT_FOUND", `No connection "${connectionId}"`);
	}
}

/** A builder was asked for a definition that is incomplete or contradictory. */
export class InvalidDefinitionError extends DurableChannelError {
	constructor(message: string) {
		super("INVALID_DEFINITION", message);
	}
}

/** A state operation was attempted on a channel declared without `.state()`. */
export class StatelessChannelError extends DurableChannelError {
	constructor(uri: string, operation: string) {
		super("STATELESS_CHANNEL", `Channel "${uri}" is stateless: ${operation} is not available`);
	}
}

/** A wire call that needs a client id arrived on a link that has not been bound yet. */
export class NotInitializedError extends DurableChannelError {
	constructor() {
		super("NOT_INITIALIZED", "This link is bound to no client id: send hello or reconnect first");
	}
}

/**
 * Thrown by a reducer to refuse an action. The hub commits the envelope with `rejectionReason` set to
 * the reason, leaves the state untouched, and every subscriber still sees it. Deliberately not a
 * {@link DurableChannelError}: refusing an action is normal channel behaviour.
 */
export class RejectAction extends Error {
	readonly reason: string;

	constructor(reason: string) {
		super(reason);
		this.name = "RejectAction";
		this.reason = reason;
	}
}

/**
 * The client's own error family, kept apart from {@link DurableChannelError} because none of it is a
 * channel decision: a request timed out, the peer answered with an error code, the transport died, or
 * the client was shut down under a caller's feet. A client still raises `DurableChannelError`
 * subclasses for anything it can decide from the route map alone — an unknown action, a server-only
 * action, a payload that fails its schema — because those are the same refusals the hub would make.
 */
export class DurableChannelClientError extends Error {
	constructor(message: string, options?: { cause?: unknown }) {
		super(message, options);
		this.name = new.target.name;
	}
}

/** The peer answered a request with a JSON-RPC error object. */
export class RpcError extends DurableChannelClientError {
	readonly code: number;
	readonly data: unknown;

	constructor(code: number, message: string, data?: unknown) {
		super(`RPC error ${code}: ${message}`);
		this.code = code;
		this.data = data;
	}
}

/** A request was abandoned locally before the peer answered. No server error happened. */
export class RpcTimeoutError extends DurableChannelClientError {
	readonly method: string;
	readonly timeoutMs: number;

	constructor(method: string, timeoutMs: number) {
		super(`Request "${method}" timed out after ${timeoutMs}ms`);
		this.method = method;
		this.timeoutMs = timeoutMs;
	}
}

/** What kind of transport failure a {@link TransportError} reports. */
export type TransportErrorKind = "closed" | "io" | "protocol";

/** The transport under a client failed: it closed, an I/O call threw, or a frame was undecodable. */
export class TransportError extends DurableChannelClientError {
	readonly kind: TransportErrorKind;

	constructor(kind: TransportErrorKind, message: string, options?: { cause?: unknown }) {
		super(message, options);
		this.kind = kind;
	}
}

/** The client was shut down, either before the call or while it was in flight. */
export class ClientClosedError extends DurableChannelClientError {
	constructor(message = "The client is shut down") {
		super(message);
	}
}
