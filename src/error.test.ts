import { describe, it } from "node:test";
import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import {
	ChannelAlreadyExistsError,
	ChannelNotFoundError,
	ConnectionNotFoundError,
	DurableChannelError,
	InvalidDefinitionError,
	InvalidPayloadError,
	InvalidResultError,
	InvalidStateError,
	NotClientDispatchableError,
	RejectAction,
	RouteNotFoundError,
	StatelessChannelError,
	UnknownActionError,
	UnknownCommandError,
	UnknownNotificationError,
} from "./error.ts";

describe("error", () => {
	const instances: [DurableChannelError, string][] = [
		[new RouteNotFoundError("nope://"), "ROUTE_NOT_FOUND"],
		[new ChannelNotFoundError("doc:/1"), "CHANNEL_NOT_FOUND"],
		[new ChannelAlreadyExistsError("doc:/1"), "CHANNEL_ALREADY_EXISTS"],
		[new UnknownActionError("doc:/1", "x"), "UNKNOWN_ACTION"],
		[new UnknownCommandError("doc:/1", "x"), "UNKNOWN_COMMAND"],
		[new UnknownNotificationError("doc:/1", "x"), "UNKNOWN_NOTIFICATION"],
		[new InvalidPayloadError("doc:/1", "x"), "INVALID_PAYLOAD"],
		[new InvalidStateError("doc:/1"), "INVALID_STATE"],
		[new InvalidResultError("doc:/1", "x"), "INVALID_RESULT"],
		[new NotClientDispatchableError("doc:/1", "x"), "NOT_CLIENT_DISPATCHABLE"],
		[new ConnectionNotFoundError("c1"), "CONNECTION_NOT_FOUND"],
		[new InvalidDefinitionError("nope"), "INVALID_DEFINITION"],
		[new StatelessChannelError("log:/a", "get"), "STATELESS_CHANNEL"],
	];

	it("gives every class a stable code and its own name", () => {
		assertEquals(instances.map(([error]) => error.code), instances.map(([, code]) => code));
		assertEquals(new ChannelNotFoundError("doc:/1").name, "ChannelNotFoundError");
	});

	it("makes every failure catchable as a DurableChannelError", () => {
		for (const [error] of instances) {
			assertInstanceOf(error, DurableChannelError);
			assertInstanceOf(error, Error);
		}
	});

	it("keeps RejectAction outside the taxonomy", () => {
		const rejection = new RejectAction("by must be positive");
		assert(!(rejection instanceof DurableChannelError), "a rejection is a domain signal, not a library failure");
		assertEquals(rejection.reason, "by must be positive");
		assertEquals(rejection.message, "by must be positive");
	});
});
