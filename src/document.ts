/**
 * A machine-readable description of a route map: every channel a hub mounts, with a JSON Schema for its
 * state, its actions, its commands and its notifications.
 *
 * The shape borrows from OpenRPC and OpenAPI — an `info` block, one entry per addressable thing, named
 * params with `required`, one JSON Schema per member — but the unit is a channel template rather than a
 * method or a path, because a template is what a peer subscribes to. Schemas are converted with
 * `@valibot/to-json-schema`: what a peer sends (an action payload, command params) in input mode, what a
 * peer receives (state, a command result, a notification payload) in output mode. A schema with no JSON
 * Schema equivalent — `v.custom`, for one — becomes `{}`, so a definition carrying opaque parts still
 * documents cleanly.
 */
import { type GenericSchema, minLength, pipe, string } from "valibot";
import { type JsonSchema, toJsonSchema } from "@valibot/to-json-schema";
import { type DurableChannelRoute, type DurableChannelRouteMap, type DurableChannelRoutes, isSingleton } from "./routes.ts";

/** The version of the document format itself, carried as the top-level `durableChannel` member. */
export const DURABLE_CHANNEL_DOCUMENT_VERSION = "0.1.0";

/** The `info` block: who the hub is. Same fields as OpenRPC's and OpenAPI's, minus the optional extras. */
export interface DurableChannelDocumentInfo {
	readonly title: string;
	readonly version: string;
	readonly description?: string;
}

export interface DurableChannelDocumentOptions {
	/** Defaults to `{ title: "Durable Channel Hub", version: "1.0.0" }`. */
	readonly info?: DurableChannelDocumentInfo;
}

/** One `:name` segment of a template. Always a non-empty string. */
export interface DurableChannelDocumentTemplateParam {
	readonly name: string;
	readonly schema: JsonSchema;
}

/**
 * One command parameter, OpenRPC style: an object params schema yields one named entry per property,
 * marked `required` unless the property schema is optional, exact-optional or nullish; a tuple yields
 * positional entries without a name; any other schema yields a single positional entry.
 */
export interface DurableChannelDocumentParam {
	readonly name?: string;
	readonly required?: true;
	readonly schema: JsonSchema;
}

export interface DurableChannelDocumentState {
	readonly schema: JsonSchema;
	/** The definition's parsed initial state: what a singleton starts with, and what `create()` uses by default. */
	readonly initial: unknown;
}

export interface DurableChannelDocumentAction {
	readonly name: string;
	readonly summary?: string;
	/** `true` when a connection may dispatch it; server-only otherwise. */
	readonly client: boolean;
	readonly payload: JsonSchema;
}

export interface DurableChannelDocumentCommand {
	readonly name: string;
	readonly summary?: string;
	readonly params: readonly DurableChannelDocumentParam[];
	readonly result: JsonSchema;
}

export interface DurableChannelDocumentNotification {
	readonly name: string;
	readonly summary?: string;
	readonly payload: JsonSchema;
}

/** One route: where it lives, what it is, and everything a peer can send to it or receive from it. */
export interface DurableChannelDocumentChannel {
	readonly template: string;
	/** The route's summary, else the definition's. */
	readonly summary?: string;
	readonly params: readonly DurableChannelDocumentTemplateParam[];
	/** `true` when the template has no `:param`: one instance, auto-created. */
	readonly singleton: boolean;
	/** `true` when connections cannot subscribe to it; the hub, commands and effects still can reach it. */
	readonly internal: boolean;
	/** Absent for a stateless channel. */
	readonly state?: DurableChannelDocumentState;
	readonly actions: readonly DurableChannelDocumentAction[];
	readonly commands: readonly DurableChannelDocumentCommand[];
	readonly notifications: readonly DurableChannelDocumentNotification[];
}

export interface DurableChannelDocument {
	/** The document format version, see {@link DURABLE_CHANNEL_DOCUMENT_VERSION}. */
	readonly opendurablechannel: string;
	readonly info: DurableChannelDocumentInfo;
	/** One entry per route, in mount order. */
	readonly channels: readonly DurableChannelDocumentChannel[];
}

const DEFAULT_INFO: DurableChannelDocumentInfo = { title: "Durable Channel Hub", version: "1.0.0" };

const TEMPLATE_PARAM: JsonSchema = toJsonSchema(pipe(string(), minLength(1)));

const OBJECT_TYPES: readonly string[] = ["object", "loose_object", "strict_object"];

function convert(schema: GenericSchema, typeMode: "input" | "output"): JsonSchema {
	return toJsonSchema(schema, { typeMode, errorMode: "ignore" });
}

function isRequired(schema: GenericSchema): boolean {
	return !["optional", "exact_optional", "nullish"].includes(schema.type);
}

function summarized(summary: string | undefined): { summary: string } | Record<never, never> {
	return summary !== undefined ? { summary } : {};
}

function describeParams(schema: GenericSchema): DurableChannelDocumentParam[] {
	const shape = schema as GenericSchema & { readonly entries?: Record<string, GenericSchema>; readonly items?: readonly GenericSchema[] };
	if (OBJECT_TYPES.includes(schema.type) && shape.entries !== undefined) {
		return Object.entries(shape.entries).map(([name, entry]) => ({
			name,
			...(isRequired(entry) ? { required: true as const } : {}),
			schema: convert(entry, "input"),
		}));
	}
	if (schema.type === "tuple" && shape.items !== undefined) {
		return shape.items.map((item) => ({ schema: convert(item, "input") }));
	}
	return [{ schema: convert(schema, "input") }];
}

function describeChannel<TEnv>(route: DurableChannelRoute<TEnv>): DurableChannelDocumentChannel {
	const { definition } = route;
	return {
		template: route.template,
		...summarized(route.summary ?? definition.summary),
		params: route.params.map((name) => ({ name, schema: TEMPLATE_PARAM })),
		singleton: isSingleton(route),
		internal: route.internal,
		...(definition.state !== undefined ? { state: { schema: convert(definition.state, "output"), initial: definition.initialState } } : {}),
		actions: Object.values(definition.actions).map((action) => ({
			name: action.name,
			...summarized(action.summary),
			client: action.client,
			payload: convert(action.payload, "input"),
		})),
		commands: Object.values(definition.commands).map((command) => ({
			name: command.name,
			...summarized(command.summary),
			params: describeParams(command.params),
			result: convert(command.result, "output"),
		})),
		notifications: Object.values(definition.notifications).map((notification) => ({
			name: notification.name,
			...summarized(notification.summary),
			payload: convert(notification.payload, "output"),
		})),
	};
}

/**
 * Describes a route map. Pure: it reads definitions only, so the result is the same before and after a
 * hub has run. `DurableChannelHub.generateSchema` is this function applied to the hub's routes.
 */
export function describeRoutes<TEnv>(
	routes: DurableChannelRoutes<TEnv, DurableChannelRouteMap>,
	options: DurableChannelDocumentOptions = {},
): DurableChannelDocument {
	return {
		opendurablechannel: DURABLE_CHANNEL_DOCUMENT_VERSION,
		info: options.info ?? DEFAULT_INFO,
		channels: routes.routes.map((route) => describeChannel(route)),
	};
}
