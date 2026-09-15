import {
	type ErrorMessage,
	type GenericSchema,
	type InferInput,
	type InferOutput,
	object,
	type ObjectEntries,
	type ObjectIssue,
	type ObjectSchema,
	safeParse,
	type TupleIssue,
	type TupleItems,
	type TupleSchema,
} from "valibot";
import type { OpenAPIV3_1 } from "openapi-types";
import { toJsonSchema } from "@valibot/to-json-schema";

/** Schemas accepted for procedure params: an object (by-name) or a tuple (by-position). */
export type HanaParamsSchema =
	| ObjectSchema<ObjectEntries, ErrorMessage<ObjectIssue> | undefined>
	| TupleSchema<TupleItems, ErrorMessage<TupleIssue> | undefined>;

/** Params schema used when `.params()` is never called: `v.object({})`. */
export type HanaDefaultParams = ObjectSchema<Record<never, never>, undefined>;

/** Type-level map of procedure name to its params/result schemas. Phantom on `HanaCollection`. */
export type HanaProcedureMap = Record<string, { params: HanaParamsSchema; result: GenericSchema }>;

export interface HanaCollection<TEnv = unknown, TProcedures extends HanaProcedureMap = HanaProcedureMap> {
	readonly procedures: Record<string, HanaProcedure<TEnv>>;
}

export interface HanaProcedure<TEnv = unknown> {
	readonly name: string;
	readonly params: HanaParamsSchema;
	readonly result: GenericSchema;
	readonly handler: (params: unknown, ctx: HanaContext<TEnv>, as: AbortSignal) => unknown | Promise<unknown>;
	readonly summary: string | undefined;
}

export interface HanaContext<TEnv = unknown> {
	env: TEnv;
}

export class HanaCollectionBuilder<
	TEnv = unknown,
	TProcedures extends HanaProcedureMap = Record<never, never>,
> {
	#procedures: Set<HanaProcedureBuilder>;

	constructor(procedures: Set<HanaProcedureBuilder> = new Set()) {
		this.#procedures = procedures;
	}

	build(): HanaCollection<TEnv, TProcedures> {
		return {
			procedures: Object.fromEntries(
				Array.from(this.#procedures).map((p) => {
					const built = p.build();
					return [built.name, built];
				}),
			),
		};
	}

	env<TNewEnv>(): HanaCollectionBuilder<TNewEnv, TProcedures> {
		return new HanaCollectionBuilder<TNewEnv, TProcedures>(this.#procedures);
	}

	def<
		TProcEnv extends TEnv = TEnv,
		TName extends string = string,
		TParams extends HanaParamsSchema = HanaDefaultParams,
		TResult extends GenericSchema = GenericSchema,
	>(
		builder:
			| HanaProcedureBuilder<TProcEnv, TName, TParams, TResult>
			| ((builder: HanaProcedureBuilder<TEnv>) => HanaProcedureBuilder<TProcEnv, TName, TParams, TResult>),
	): HanaCollectionBuilder<
		TEnv,
		& TProcedures
		& {
			[T in TName]: { params: TParams; result: TResult };
		}
	> {
		const result = builder instanceof HanaProcedureBuilder
			? builder
			: builder(new HanaProcedureBuilder<TEnv>(undefined, object({}), object({})));
		return new HanaCollectionBuilder(
			new Set([
				...this.#procedures,
				result as never,
			]),
		);
	}
}

export class HanaProcedureBuilder<
	TEnv = unknown,
	TName extends string = string,
	TParams extends HanaParamsSchema = HanaDefaultParams,
	TResult extends GenericSchema = GenericSchema,
> {
	#name: string | undefined;
	#params: HanaParamsSchema | undefined;
	#result: GenericSchema | undefined;
	#handler: HanaProcedure["handler"] | undefined;
	#summary: string | undefined;

	constructor(
		name?: string,
		params?: HanaParamsSchema,
		result?: GenericSchema,
		handler?: HanaProcedure["handler"] | undefined,
		summary?: string,
	) {
		this.#name = name;
		this.#params = params;
		this.#result = result;
		this.#handler = handler;
		this.#summary = summary;
	}

	build(): HanaProcedure<TEnv> {
		if (!this.#name || !this.#params || !this.#result || !this.#handler) {
			throw new Error("Cannot build HanaProcedure: name, params, result, and handler must be defined");
		}
		return {
			name: this.#name,
			summary: this.#summary,
			params: this.#params,
			result: this.#result,
			handler: this.#handler,
		};
	}

	env<TNewEnv>(): HanaProcedureBuilder<TNewEnv, TName, TParams, TResult> {
		return new HanaProcedureBuilder<TNewEnv, TName, TParams, TResult>(
			this.#name,
			this.#params,
			this.#result,
			this.#handler,
			this.#summary,
		);
	}

	name<TNewName extends string>(name: TNewName): HanaProcedureBuilder<TEnv, TNewName, TParams, TResult> {
		return new HanaProcedureBuilder<TEnv, TNewName, TParams, TResult>(name, this.#params, this.#result, this.#handler, this.#summary);
	}

	summary(summary: string): HanaProcedureBuilder<TEnv, TName, TParams, TResult> {
		return new HanaProcedureBuilder<TEnv, TName, TParams, TResult>(this.#name, this.#params, this.#result, this.#handler, summary);
	}

	params<TNewParams extends HanaParamsSchema>(
		params: TNewParams,
	): HanaProcedureBuilder<TEnv, TName, TNewParams, TResult> {
		return new HanaProcedureBuilder<TEnv, TName, TNewParams, TResult>(this.#name, params, this.#result, this.#handler, this.#summary);
	}

	result<TNewResult extends GenericSchema>(result: TNewResult): HanaProcedureBuilder<TEnv, TName, TParams, TNewResult> {
		return new HanaProcedureBuilder<TEnv, TName, TParams, TNewResult>(this.#name, this.#params, result, this.#handler, this.#summary);
	}

	/**
	 * `params` is the parsed output of the params schema (transforms and defaults applied).
	 * The return value is validated against the result schema, so it is typed as that schema's input.
	 */
	handler(
		handler: (
			params: InferOutput<TParams>,
			ctx: HanaContext<TEnv>,
			as: AbortSignal,
		) => InferInput<TResult> | Promise<InferInput<TResult>>,
	): HanaProcedureBuilder<TEnv, TName, TParams, TResult> {
		return new HanaProcedureBuilder<TEnv, TName, TParams, TResult>(this.#name, this.#params, this.#result, handler as never, this.#summary);
	}
}

export function hana<
	TEnv = unknown,
	TProcedures extends HanaProcedureMap = Record<never, never>,
>(
	builder: (hana: HanaCollectionBuilder<TEnv>) => HanaCollectionBuilder<TEnv, TProcedures> = (b) => b,
): HanaCollectionBuilder<TEnv, TProcedures> {
	return builder(new HanaCollectionBuilder<TEnv>());
}

export function def<
	TEnv = unknown,
	TName extends string = string,
	TParams extends HanaParamsSchema = HanaDefaultParams,
	TResult extends GenericSchema = GenericSchema,
>(
	builder: (def: HanaProcedureBuilder<TEnv>) => HanaProcedureBuilder<TEnv, TName, TParams, TResult> = (b) => b as never,
): HanaProcedureBuilder<TEnv, TName, TParams, TResult> {
	return builder(new HanaProcedureBuilder<TEnv>(undefined, object({}), object({})));
}

export class Hana<TEnv = unknown, TProcedures extends HanaProcedureMap = HanaProcedureMap> {
	#collection: HanaCollection<TEnv, TProcedures>;

	constructor(collection: HanaCollection<TEnv, TProcedures>) {
		this.#collection = collection;
	}

	/**
	 * Call a procedure directly, bypassing the JSON-RPC envelope. Typed from the collection.
	 * Throws `JsonRpcError` subclasses (method not found, invalid params, internal error) or whatever the handler threw.
	 */
	invoke<TName extends keyof TProcedures & string>(
		name: TName,
		params: InferInput<TProcedures[TName]["params"]>,
		ctx: HanaContext<TEnv>,
		as: AbortSignal,
	): Promise<InferOutput<TProcedures[TName]["result"]>> {
		return this.#invoke(name, params, ctx, as) as Promise<InferOutput<TProcedures[TName]["result"]>>;
	}

	async #invoke(name: string, params: unknown, ctx: HanaContext<TEnv>, as: AbortSignal): Promise<unknown> {
		if (!Object.hasOwn(this.#collection.procedures, name)) {
			throw new JsonRpcMethodNotFoundError();
		}
		const procedure = this.#collection.procedures[name];
		const input = params === undefined ? (procedure.params.type === "tuple" ? [] : {}) : params;
		const paramParsed = safeParse(procedure.params, input);
		if (!paramParsed.success) {
			throw new JsonRpcInvalidParamsError();
		}
		const result = await procedure.handler(paramParsed.output, ctx, as);
		const resultParsed = safeParse(procedure.result, result);
		if (!resultParsed.success) {
			throw new JsonRpcInternalError();
		}
		return resultParsed.output;
	}

	/**
	 * Handle one decoded JSON-RPC request object.
	 * Returns a response for calls (success or error) and `undefined` for notifications (no `id`), even when the handler throws.
	 * A value that does not match the request schema is answered with `id: null` and -32600.
	 */
	async handle(request: unknown, ctx: HanaContext<TEnv>, as: AbortSignal): Promise<JsonRpcResponse | undefined> {
		const requestParsed = safeParse(JsonRpcRequest, request);
		if (!requestParsed.success) {
			return { jsonrpc: "2.0", id: null, error: toJsonRpcErrorObject(new JsonRpcInvalidRequestError()) };
		}
		const { method, params } = requestParsed.output;
		const isNotification = !("id" in requestParsed.output);
		const id = requestParsed.output.id ?? null;
		try {
			const result = await this.#invoke(method, params, ctx, as);
			return isNotification ? undefined : { jsonrpc: "2.0", id, result };
		} catch (error) {
			return isNotification ? undefined : { jsonrpc: "2.0", id, error: toJsonRpcErrorObject(error) };
		}
	}

	/**
	 * HTTP adapter. Calls answer 200 with a JSON-RPC body (success, error, or -32700 when the body is not JSON).
	 * Notifications answer 204 with an empty body. `request.signal` is forwarded to the handler.
	 */
	async fetch(httpRequest: Request, ctx: HanaContext<TEnv>): Promise<Response> {
		let body: unknown;
		try {
			body = await httpRequest.json();
		} catch {
			const response: JsonRpcResponse = { jsonrpc: "2.0", id: null, error: toJsonRpcErrorObject(new JsonRpcParseError()) };
			return Response.json(response);
		}
		const response = await this.handle(body, ctx, httpRequest.signal);
		if (response === undefined) {
			return new Response(null, { status: 204 });
		}
		return Response.json(response);
	}

	generateOpenRPCSchema(options: { info?: OpenAPIV3_1.InfoObject | undefined }): unknown {
		return {
			openrpc: "1.2.6",
			info: options.info ?? {
				title: "Hana API",
				version: "1.0.0",
			},
			methods: Object.values(this.#collection.procedures).map((procedure) => ({
				name: procedure.name,
				...(procedure.summary ? { summary: procedure.summary } : {}),
				params: procedure.params.type === "object"
					? Object.entries(procedure.params.entries).map(([name, schema]) => ({
						name,
						...(isRequiredParam(schema) ? { required: true } : {}),
						schema: toJsonSchema(schema, { typeMode: "input" }),
					}))
					: procedure.params.items.map((schema) => ({
						schema: toJsonSchema(schema, { typeMode: "input" }),
					})),
				result: toJsonSchema(procedure.result, { typeMode: "input" }),
			})),
		};
	}
}

/**
 * Maps a thrown value to a JSON-RPC error object. Anything that is not a `JsonRpcError` becomes -32603 without leaking its message.
 * A `JsonRpcError` carrying `data` emits it; the member is omitted when the error has none.
 */
export function toJsonRpcErrorObject(error: unknown): JsonRpcResponseError["error"] {
	if (!(error instanceof JsonRpcError)) {
		return { code: -32603, message: "Internal error" };
	}
	return { code: error.code, message: error.message, ...(error.data !== undefined ? { data: error.data } : {}) };
}

function isRequiredParam(schema: GenericSchema): boolean {
	return !["optional", "exact_optional", "nullish"].includes(schema.type);
}

export class JsonRpcError extends Error {
	code: number;
	/** Structured detail carried on the wire as `error.data`. Some protocols mandate it for specific codes. */
	data?: unknown;
	constructor(code: number, message: string, data?: unknown) {
		super(message);
		this.code = code;
		if (data !== undefined) {
			this.data = data;
		}
	}
}

export class JsonRpcParseError extends JsonRpcError {
	constructor() {
		super(-32700, "Parse error");
	}
}

export class JsonRpcInvalidRequestError extends JsonRpcError {
	constructor() {
		super(-32600, "Invalid Request");
	}
}

export class JsonRpcMethodNotFoundError extends JsonRpcError {
	constructor() {
		super(-32601, "Method not found");
	}
}

export class JsonRpcInvalidParamsError extends JsonRpcError {
	constructor() {
		super(-32602, "Invalid params");
	}
}

export class JsonRpcInternalError extends JsonRpcError {
	constructor() {
		super(-32603, "Internal error");
	}
}

import * as v from "valibot";

/** A JSON-RPC 2.0 request identifier. `null` is legal on the wire but discouraged by the spec. */
export type JsonRpcId = string | number | null;

export interface JsonRpcRequest {
	jsonrpc: "2.0";
	id?: JsonRpcId;
	method: string;
	params?: Record<string, unknown> | unknown[];
}

export const JsonRpcRequest: v.GenericSchema<JsonRpcRequest> = v.pipe(
	v.object({
		jsonrpc: v.literal("2.0"),
		id: v.optional(v.nullable(v.union([v.string(), v.number()]))),
		method: v.string(),
		// `array` must come before `record`: valibot's `record` accepts arrays and rewrites
		// `["a", 1]` into `{ "0": "a", "1": 1 }`, which would break by-position calls.
		params: v.optional(v.union([
			v.array(v.unknown()),
			v.record(v.string(), v.unknown()),
		])),
	}),
	v.title("JsonRpcRequest"),
	v.description("A JSON-RPC request object"),
) as never;

export interface JsonRpcResponseError {
	jsonrpc: "2.0";
	id: JsonRpcId;
	error: {
		code: number;
		message: string;
		data?: unknown;
	};
}

export const JsonRpcResponseError: v.GenericSchema<JsonRpcResponseError> = v.pipe(
	v.object({
		jsonrpc: v.literal("2.0"),
		id: v.nullable(v.union([v.string(), v.number()])),
		error: v.object({
			code: v.number(),
			message: v.string(),
			data: v.optional(v.unknown()),
		}),
	}),
	v.title("JsonRpcResponseError"),
	v.description("A JSON-RPC response error object"),
) as never;

export interface JsonRpcResponseResult {
	jsonrpc: "2.0";
	id: JsonRpcId;
	result: unknown;
}

export const JsonRpcResponseResult: v.GenericSchema<JsonRpcResponseResult> = v.pipe(
	v.object({
		jsonrpc: v.literal("2.0"),
		id: v.nullable(v.union([v.string(), v.number()])),
		result: v.unknown(),
	}),
	v.title("JsonRpcResponseResult"),
	v.description("A JSON-RPC response success object"),
) as never;

export type JsonRpcResponse = JsonRpcResponseError | JsonRpcResponseResult;

export const JsonRpcResponse: v.GenericSchema<JsonRpcResponse> = v.union([
	JsonRpcResponseError,
	JsonRpcResponseResult,
]) as never;
