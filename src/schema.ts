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
