/**
 * The isomorphic half of a durable channel: what a channel *is*, with no I/O anywhere.
 *
 * A channel definition holds a state schema with its initial value, named actions whose pure reducers
 * are the only way the state changes, named commands whose handlers may do side effects, and named
 * notifications the channel may push to its subscribers. A definition without `.state()` is
 * **stateless**: it has no snapshot and no actions, only commands and notifications.
 *
 * Everything is built through copy-on-write builders so valibot's inferred types accumulate into the
 * definition's type parameters; nothing is mutated and a partially configured builder can be shared.
 * The same definition value is handed to a hub on the server and to a client, which is why no module
 * in this file knows about connections, storage or transports.
 */
import { type GenericSchema, type InferInput, type InferOutput, safeParse } from "valibot";
import { InvalidDefinitionError, InvalidStateError } from "./error.ts";

// ─── Messages ────────────────────────────────────────────────────────────────

/** The connection that dispatched an action, and its own sequence number for it. */
export interface DurableChannelActionOrigin {
	readonly clientId: string;
	readonly clientSeq: number;
}

/** A committed action. Carries `rejectionReason` when the hub refused it and left the state untouched. */
export interface DurableChannelEnvelope {
	readonly type: "action";
	readonly channel: string;
	readonly name: string;
	readonly payload: unknown;
	readonly serverSeq: number;
	readonly origin?: DurableChannelActionOrigin;
	readonly rejectionReason?: string;
}

/** A channel notification. Never persisted, never replayed. */
export interface DurableChannelNotification {
	readonly type: "notification";
	readonly channel: string;
	readonly name: string;
	readonly payload: unknown;
}

/** Everything a hub pushes to a connection. */
export type DurableChannelMessage = DurableChannelEnvelope | DurableChannelNotification;

/** A channel's state as of `fromSeq`. Every later envelope for that channel has `serverSeq > fromSeq`. */
export interface DurableChannelSnapshot {
	readonly resource: string;
	readonly state: unknown;
	readonly fromSeq: number;
}

/** What a reconnect could restore. `missing` lists the URIs the hub cannot resume. */
export type DurableChannelReconnectResult =
	| { readonly type: "replay"; readonly actions: readonly DurableChannelEnvelope[]; readonly missing: readonly string[] }
	| { readonly type: "snapshot"; readonly snapshots: readonly DurableChannelSnapshot[]; readonly missing: readonly string[] };

/** One live instance of a route. */
export interface DurableChannelInstance<TState = unknown> {
	readonly uri: string;
	readonly params: Readonly<Record<string, string>>;
	readonly state: TState;
}

// ─── Type-level maps ─────────────────────────────────────────────────────────

/** Action name to payload schema. Phantom on a built definition. */
export type DurableChannelActionMap = Record<string, GenericSchema>;

/** Command name to params and result schemas. Phantom on a built definition. */
export type DurableChannelCommandMap = Record<string, { params: GenericSchema; result: GenericSchema }>;

/** Notification name to payload schema. Phantom on a built definition. */
export type DurableChannelNotificationMap = Record<string, GenericSchema>;

// ─── Definitions ─────────────────────────────────────────────────────────────

/** What a reducer knows about where it runs. Deliberately no `env`, no clock and no randomness. */
export interface DurableChannelActionMeta {
	readonly uri: string;
	readonly params: Readonly<Record<string, string>>;
}

/** A named mutation: a payload schema, a pure synchronous reducer, and an optional server-side effect. */
export interface DurableChannelActionDefinition<TState = unknown, TEnv = unknown> {
	readonly name: string;
	/** A one-line description, for the generated document. */
	readonly summary?: string;
	readonly payload: GenericSchema;
	/** `true` when a connection may dispatch it; server-only otherwise. */
	readonly client: boolean;
	readonly reduce: (state: TState, payload: unknown, meta: DurableChannelActionMeta) => TState;
	/**
	 * Server-side work to run once the action is committed and broadcast. Never part of the isomorphic
	 * surface: a client running the same definition ignores it.
	 */
	readonly effect?: (ctx: DurableChannelActionEffectContext<TEnv, TState>) => void | Promise<void>;
}

/** A named procedure. Its handler receives the parsed params and may do side effects. */
export interface DurableChannelCommandDefinition<TEnv = unknown> {
	readonly name: string;
	/** A one-line description, for the generated document. */
	readonly summary?: string;
	readonly params: GenericSchema;
	readonly result: GenericSchema;
	readonly handler: (params: never, ctx: DurableChannelCommandContext<TEnv>) => unknown;
}

/** A named message the channel may push to its subscribers. */
export interface DurableChannelNotificationDefinition {
	readonly name: string;
	/** A one-line description, for the generated document. */
	readonly summary?: string;
	readonly payload: GenericSchema;
}

/**
 * The hub calls a command handler and an action effect share. `dispatch`, `notify` and the instance
 * calls may target any channel; they are typed for the channel's own members and untyped for others.
 */
export interface DurableChannelOperations<
	TEnv = unknown,
	TActions extends DurableChannelActionMap = DurableChannelActionMap,
	TNotifications extends DurableChannelNotificationMap = DurableChannelNotificationMap,
> {
	readonly env: TEnv;
	dispatch<TName extends keyof TActions & string>(
		uri: string,
		name: TName,
		payload: InferInput<TActions[TName]>,
	): Promise<DurableChannelEnvelope>;
	dispatch(uri: string, name: string, payload: unknown): Promise<DurableChannelEnvelope>;
	notify<TName extends keyof TNotifications & string>(uri: string, name: TName, payload: InferInput<TNotifications[TName]>): Promise<void>;
	notify(uri: string, name: string, payload: unknown): Promise<void>;
	get(uri: string): Promise<unknown>;
	has(uri: string): Promise<boolean>;
	create(uri: string, state?: unknown): Promise<void>;
	destroy(uri: string): Promise<void>;
	list(template: string): AsyncIterable<DurableChannelInstance>;
	/** Runs another command. The caller's `connectionId` and abort signal are forwarded to it. */
	exec(uri: string, name: string, params: unknown): Promise<unknown>;
	/**
	 * Registers server-side work the hub tracks for the instance this context belongs to. The task is
	 * detached — nothing awaits it, and an error it throws is swallowed because there is no caller to
	 * report to — and its `signal` is aborted by {@link DurableChannelOperations.abortBackground}, by a
	 * `destroy` of that URI, and by the hub closing.
	 */
	background(task: (signal: AbortSignal) => void | Promise<void>): void;
	/** Aborts every background task registered for this instance. The next one starts on a fresh signal. */
	abortBackground(): void;
}

/** What a command handler is given: the operations, the instance it was called on, and the caller. */
export interface DurableChannelCommandContext<
	TEnv = unknown,
	TState = unknown,
	TActions extends DurableChannelActionMap = DurableChannelActionMap,
	TNotifications extends DurableChannelNotificationMap = DurableChannelNotificationMap,
> extends DurableChannelOperations<TEnv, TActions, TNotifications> {
	readonly uri: string;
	readonly params: Readonly<Record<string, string>>;
	/** The connection that asked for the command, when it came from one. */
	readonly connectionId: string | undefined;
	readonly signal: AbortSignal;
	state(): Promise<TState>;
}

/**
 * What an action effect is given: the operations, the envelope that was committed, the state its
 * reducer produced, and the parsed payload. `dispatch` and `notify` are untyped here, because an
 * effect usually announces the change on another channel.
 */
export interface DurableChannelActionEffectContext<TEnv = unknown, TState = unknown, TPayload = unknown>
	extends DurableChannelOperations<TEnv> {
	readonly uri: string;
	readonly params: Readonly<Record<string, string>>;
	/** The connection whose dispatch produced the envelope, when it came from one. */
	readonly connectionId: string | undefined;
	/** The state the reducer produced. Already validated, persisted and broadcast. */
	readonly state: TState;
	readonly payload: TPayload;
	readonly envelope: DurableChannelEnvelope;
}

/** The builder's accumulated types, carried on a definition so a route map can read them back. */
export interface DurableChannelTypes<
	TState = unknown,
	TActions extends DurableChannelActionMap = DurableChannelActionMap,
	TCommands extends DurableChannelCommandMap = DurableChannelCommandMap,
	TNotifications extends DurableChannelNotificationMap = DurableChannelNotificationMap,
> {
	readonly state: TState;
	readonly actions: TActions;
	readonly commands: TCommands;
	readonly notifications: TNotifications;
}

/** A built channel definition: a frozen, plain object a hub or a client can run. */
export interface DurableChannel<
	TEnv = unknown,
	TState = unknown,
	TActions extends DurableChannelActionMap = DurableChannelActionMap,
	TCommands extends DurableChannelCommandMap = DurableChannelCommandMap,
	TNotifications extends DurableChannelNotificationMap = DurableChannelNotificationMap,
> {
	/** A one-line description, for the generated document. */
	readonly summary?: string;
	/** The state schema, or `undefined` for a stateless channel. */
	readonly state: GenericSchema | undefined;
	/** The parsed initial state, or `undefined` for a stateless channel. */
	readonly initialState: unknown;
	readonly actions: Readonly<Record<string, DurableChannelActionDefinition<TState, TEnv>>>;
	readonly commands: Readonly<Record<string, DurableChannelCommandDefinition<TEnv>>>;
	readonly notifications: Readonly<Record<string, DurableChannelNotificationDefinition>>;
	/** Phantom carrier for the type parameters above. Always `undefined` at runtime. */
	readonly types: DurableChannelTypes<TState, TActions, TCommands, TNotifications> | undefined;
}

// ─── Member builders ─────────────────────────────────────────────────────────

/** Builds one action. `TEnv` is carried for the effect only: a reducer never sees `env`. */
export class DurableChannelActionBuilder<
	TEnv = unknown,
	TState = unknown,
	TName extends string = string,
	TPayload extends GenericSchema = GenericSchema,
> {
	#name: string | undefined;
	#payload: GenericSchema | undefined;
	#client: boolean;
	#reduce: DurableChannelActionDefinition<TState, TEnv>["reduce"] | undefined;
	#effect: DurableChannelActionDefinition<TState, TEnv>["effect"];
	#summary: string | undefined;

	constructor(
		name?: string,
		payload?: GenericSchema,
		client = false,
		reduce?: DurableChannelActionDefinition<TState, TEnv>["reduce"],
		effect?: DurableChannelActionDefinition<TState, TEnv>["effect"],
		summary?: string,
	) {
		this.#name = name;
		this.#payload = payload;
		this.#client = client;
		this.#reduce = reduce;
		this.#effect = effect;
		this.#summary = summary;
	}

	build(): DurableChannelActionDefinition<TState, TEnv> {
		if (this.#name === undefined || this.#payload === undefined || this.#reduce === undefined) {
			throw new InvalidDefinitionError("Cannot build an action: name, payload and reduce must be defined");
		}
		return {
			name: this.#name,
			...(this.#summary !== undefined ? { summary: this.#summary } : {}),
			payload: this.#payload,
			client: this.#client,
			reduce: this.#reduce,
			...(this.#effect !== undefined ? { effect: this.#effect } : {}),
		};
	}

	name<TNewName extends string>(name: TNewName): DurableChannelActionBuilder<TEnv, TState, TNewName, TPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TNewName, TPayload>(
			name,
			this.#payload,
			this.#client,
			this.#reduce,
			this.#effect,
			this.#summary,
		);
	}

	/** A one-line description, carried into the generated document. */
	summary(summary: string): DurableChannelActionBuilder<TEnv, TState, TName, TPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TName, TPayload>(
			this.#name,
			this.#payload,
			this.#client,
			this.#reduce,
			this.#effect,
			summary,
		);
	}

	payload<TNewPayload extends GenericSchema>(payload: TNewPayload): DurableChannelActionBuilder<TEnv, TState, TName, TNewPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TName, TNewPayload>(
			this.#name,
			payload,
			this.#client,
			this.#reduce as never,
			this.#effect as never,
			this.#summary,
		);
	}

	/** Marks the action client-dispatchable. Actions are server-only by default. */
	client(): DurableChannelActionBuilder<TEnv, TState, TName, TPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TName, TPayload>(
			this.#name,
			this.#payload,
			true,
			this.#reduce,
			this.#effect,
			this.#summary,
		);
	}

	/**
	 * The reducer. It receives the parsed payload and must be pure and synchronous: the same reducer
	 * runs on the client in order to mirror the state.
	 */
	reduce(
		reduce: (state: TState, payload: InferOutput<TPayload>, meta: DurableChannelActionMeta) => TState,
	): DurableChannelActionBuilder<TEnv, TState, TName, TPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TName, TPayload>(
			this.#name,
			this.#payload,
			this.#client,
			reduce as never,
			this.#effect,
			this.#summary,
		);
	}

	/**
	 * Server-side work to run after the action is committed and broadcast, and only when it was
	 * accepted. It may dispatch, notify and touch other instances; a client ignores it entirely.
	 */
	effect(
		effect: (ctx: DurableChannelActionEffectContext<TEnv, TState, InferOutput<TPayload>>) => void | Promise<void>,
	): DurableChannelActionBuilder<TEnv, TState, TName, TPayload> {
		return new DurableChannelActionBuilder<TEnv, TState, TName, TPayload>(
			this.#name,
			this.#payload,
			this.#client,
			this.#reduce,
			effect as never,
			this.#summary,
		);
	}
}

/** Builds one command. */
export class DurableChannelCommandBuilder<
	TEnv = unknown,
	TState = unknown,
	TActions extends DurableChannelActionMap = DurableChannelActionMap,
	TNotifications extends DurableChannelNotificationMap = DurableChannelNotificationMap,
	TName extends string = string,
	TParams extends GenericSchema = GenericSchema,
	TResult extends GenericSchema = GenericSchema,
> {
	#name: string | undefined;
	#params: GenericSchema | undefined;
	#result: GenericSchema | undefined;
	#handler: DurableChannelCommandDefinition<TEnv>["handler"] | undefined;
	#summary: string | undefined;

	constructor(
		name?: string,
		params?: GenericSchema,
		result?: GenericSchema,
		handler?: DurableChannelCommandDefinition<TEnv>["handler"],
		summary?: string,
	) {
		this.#name = name;
		this.#params = params;
		this.#result = result;
		this.#handler = handler;
		this.#summary = summary;
	}

	build(): DurableChannelCommandDefinition<TEnv> {
		if (this.#name === undefined || this.#params === undefined || this.#result === undefined || this.#handler === undefined) {
			throw new InvalidDefinitionError("Cannot build a command: name, params, result and handler must be defined");
		}
		return {
			name: this.#name,
			...(this.#summary !== undefined ? { summary: this.#summary } : {}),
			params: this.#params,
			result: this.#result,
			handler: this.#handler,
		};
	}

	name<TNewName extends string>(
		name: TNewName,
	): DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TNewName, TParams, TResult> {
		return new DurableChannelCommandBuilder(name, this.#params, this.#result, this.#handler, this.#summary);
	}

	/** A one-line description, carried into the generated document. */
	summary(summary: string): DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TName, TParams, TResult> {
		return new DurableChannelCommandBuilder(this.#name, this.#params, this.#result, this.#handler, summary);
	}

	params<TNewParams extends GenericSchema>(
		params: TNewParams,
	): DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TName, TNewParams, TResult> {
		return new DurableChannelCommandBuilder(this.#name, params, this.#result, this.#handler, this.#summary);
	}

	result<TNewResult extends GenericSchema>(
		result: TNewResult,
	): DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TName, TParams, TNewResult> {
		return new DurableChannelCommandBuilder(this.#name, this.#params, result, this.#handler, this.#summary);
	}

	/**
	 * The handler. `params` is the parsed output of the params schema and the return value is validated
	 * against the result schema, so it is typed as that schema's input.
	 */
	handler(
		handler: (
			params: InferOutput<TParams>,
			ctx: DurableChannelCommandContext<TEnv, TState, TActions, TNotifications>,
		) => InferInput<TResult> | Promise<InferInput<TResult>>,
	): DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TName, TParams, TResult> {
		return new DurableChannelCommandBuilder(this.#name, this.#params, this.#result, handler as never, this.#summary);
	}
}

/** Builds one notification. */
export class DurableChannelNotificationBuilder<
	TName extends string = string,
	TPayload extends GenericSchema = GenericSchema,
> {
	#name: string | undefined;
	#payload: GenericSchema | undefined;
	#summary: string | undefined;

	constructor(name?: string, payload?: GenericSchema, summary?: string) {
		this.#name = name;
		this.#payload = payload;
		this.#summary = summary;
	}

	build(): DurableChannelNotificationDefinition {
		if (this.#name === undefined || this.#payload === undefined) {
			throw new InvalidDefinitionError("Cannot build a notification: name and payload must be defined");
		}
		return { name: this.#name, ...(this.#summary !== undefined ? { summary: this.#summary } : {}), payload: this.#payload };
	}

	name<TNewName extends string>(name: TNewName): DurableChannelNotificationBuilder<TNewName, TPayload> {
		return new DurableChannelNotificationBuilder<TNewName, TPayload>(name, this.#payload, this.#summary);
	}

	/** A one-line description, carried into the generated document. */
	summary(summary: string): DurableChannelNotificationBuilder<TName, TPayload> {
		return new DurableChannelNotificationBuilder<TName, TPayload>(this.#name, this.#payload, summary);
	}

	payload<TNewPayload extends GenericSchema>(payload: TNewPayload): DurableChannelNotificationBuilder<TName, TNewPayload> {
		return new DurableChannelNotificationBuilder<TName, TNewPayload>(this.#name, payload, this.#summary);
	}
}

// ─── Channel builders ────────────────────────────────────────────────────────

type AnyActionBuilder = DurableChannelActionBuilder<never, never, string, GenericSchema>;
type AnyCommandBuilder = DurableChannelCommandBuilder<never, never, DurableChannelActionMap, DurableChannelNotificationMap>;
type AnyNotificationBuilder = DurableChannelNotificationBuilder<string, GenericSchema>;

function collect<TDefinition extends { name: string }>(
	builders: readonly { build(): TDefinition }[],
	kind: string,
): Record<string, TDefinition> {
	const collected: Record<string, TDefinition> = {};
	for (const builder of builders) {
		const definition = builder.build();
		if (Object.hasOwn(collected, definition.name)) {
			throw new InvalidDefinitionError(`Duplicate ${kind} "${definition.name}"`);
		}
		collected[definition.name] = definition;
	}
	return collected;
}

/**
 * A channel with no state: no snapshot, no actions, nothing persisted. This is the shape for a
 * real-time relay — a log, a telemetry stream, a chat feed — where every URI matching the route exists
 * implicitly. Calling {@link DurableChannelStatelessBuilder.state} turns it into a stateful builder,
 * which is the only one that has `.action()`.
 */
export class DurableChannelStatelessBuilder<
	TEnv = unknown,
	TCommands extends DurableChannelCommandMap = Record<never, never>,
	TNotifications extends DurableChannelNotificationMap = Record<never, never>,
> {
	#commands: readonly AnyCommandBuilder[];
	#notifications: readonly AnyNotificationBuilder[];
	#summary: string | undefined;

	constructor(commands: readonly AnyCommandBuilder[] = [], notifications: readonly AnyNotificationBuilder[] = [], summary?: string) {
		this.#commands = commands;
		this.#notifications = notifications;
		this.#summary = summary;
	}

	build(): DurableChannel<TEnv, undefined, Record<never, never>, TCommands, TNotifications> {
		return Object.freeze({
			...(this.#summary !== undefined ? { summary: this.#summary } : {}),
			state: undefined,
			initialState: undefined,
			actions: Object.freeze({}),
			commands: Object.freeze(collect<DurableChannelCommandDefinition<TEnv>>(this.#commands as never, "command")),
			notifications: Object.freeze(collect<DurableChannelNotificationDefinition>(this.#notifications, "notification")),
			types: undefined,
		});
	}

	env<TNewEnv>(): DurableChannelStatelessBuilder<TNewEnv, TCommands, TNotifications> {
		return new DurableChannelStatelessBuilder<TNewEnv, TCommands, TNotifications>(this.#commands, this.#notifications, this.#summary);
	}

	/** A one-line description of the channel, carried into the generated document. */
	summary(summary: string): DurableChannelStatelessBuilder<TEnv, TCommands, TNotifications> {
		return new DurableChannelStatelessBuilder<TEnv, TCommands, TNotifications>(this.#commands, this.#notifications, summary);
	}

	/** Declares the state and its initial value, which is what unlocks `.action()`. */
	state<TSchema extends GenericSchema>(
		schema: TSchema,
		initialState: InferInput<TSchema>,
	): DurableChannelBuilder<TEnv, InferOutput<TSchema>, Record<never, never>, TCommands, TNotifications> {
		return new DurableChannelBuilder(schema, initialState, [], this.#commands, this.#notifications, this.#summary);
	}

	command<TName extends string, TParams extends GenericSchema, TResult extends GenericSchema>(
		builder: (
			builder: DurableChannelCommandBuilder<TEnv, undefined, Record<never, never>, TNotifications>,
		) => DurableChannelCommandBuilder<TEnv, undefined, Record<never, never>, TNotifications, TName, TParams, TResult>,
	): DurableChannelStatelessBuilder<TEnv, TCommands & { [K in TName]: { params: TParams; result: TResult } }, TNotifications> {
		return new DurableChannelStatelessBuilder(
			[...this.#commands, builder(new DurableChannelCommandBuilder()) as never],
			this.#notifications,
			this.#summary,
		);
	}

	notification<TName extends string, TPayload extends GenericSchema>(
		builder: (builder: DurableChannelNotificationBuilder) => DurableChannelNotificationBuilder<TName, TPayload>,
	): DurableChannelStatelessBuilder<TEnv, TCommands, TNotifications & { [K in TName]: TPayload }> {
		return new DurableChannelStatelessBuilder(
			this.#commands,
			[...this.#notifications, builder(new DurableChannelNotificationBuilder())],
			this.#summary,
		);
	}
}

/** A channel with a state, its actions, its commands and its notifications. */
export class DurableChannelBuilder<
	TEnv = unknown,
	TState = unknown,
	TActions extends DurableChannelActionMap = Record<never, never>,
	TCommands extends DurableChannelCommandMap = Record<never, never>,
	TNotifications extends DurableChannelNotificationMap = Record<never, never>,
> {
	#state: GenericSchema;
	#initialState: unknown;
	#actions: readonly AnyActionBuilder[];
	#commands: readonly AnyCommandBuilder[];
	#notifications: readonly AnyNotificationBuilder[];
	#summary: string | undefined;

	constructor(
		state: GenericSchema,
		initialState: unknown,
		actions: readonly AnyActionBuilder[] = [],
		commands: readonly AnyCommandBuilder[] = [],
		notifications: readonly AnyNotificationBuilder[] = [],
		summary?: string,
	) {
		this.#state = state;
		this.#initialState = initialState;
		this.#actions = actions;
		this.#commands = commands;
		this.#notifications = notifications;
		this.#summary = summary;
	}

	/** Parses the initial state through the state schema, so defaults are applied once and for all. */
	build(): DurableChannel<TEnv, TState, TActions, TCommands, TNotifications> {
		const parsed = safeParse(this.#state, this.#initialState);
		if (!parsed.success) {
			throw new InvalidStateError("<initial>");
		}
		return Object.freeze({
			...(this.#summary !== undefined ? { summary: this.#summary } : {}),
			state: this.#state,
			initialState: parsed.output,
			actions: Object.freeze(collect<DurableChannelActionDefinition<TState, TEnv>>(this.#actions as never, "action")),
			commands: Object.freeze(collect<DurableChannelCommandDefinition<TEnv>>(this.#commands as never, "command")),
			notifications: Object.freeze(collect<DurableChannelNotificationDefinition>(this.#notifications, "notification")),
			types: undefined,
		});
	}

	env<TNewEnv>(): DurableChannelBuilder<TNewEnv, TState, TActions, TCommands, TNotifications> {
		return new DurableChannelBuilder(this.#state, this.#initialState, this.#actions, this.#commands, this.#notifications, this.#summary);
	}

	/** A one-line description of the channel, carried into the generated document. */
	summary(summary: string): DurableChannelBuilder<TEnv, TState, TActions, TCommands, TNotifications> {
		return new DurableChannelBuilder(this.#state, this.#initialState, this.#actions, this.#commands, this.#notifications, summary);
	}

	action<TName extends string, TPayload extends GenericSchema>(
		builder: (builder: DurableChannelActionBuilder<TEnv, TState>) => DurableChannelActionBuilder<TEnv, TState, TName, TPayload>,
	): DurableChannelBuilder<TEnv, TState, TActions & { [K in TName]: TPayload }, TCommands, TNotifications> {
		return new DurableChannelBuilder(
			this.#state,
			this.#initialState,
			[...this.#actions, builder(new DurableChannelActionBuilder<TEnv, TState>()) as never],
			this.#commands,
			this.#notifications,
			this.#summary,
		);
	}

	command<TName extends string, TParams extends GenericSchema, TResult extends GenericSchema>(
		builder: (
			builder: DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications>,
		) => DurableChannelCommandBuilder<TEnv, TState, TActions, TNotifications, TName, TParams, TResult>,
	): DurableChannelBuilder<TEnv, TState, TActions, TCommands & { [K in TName]: { params: TParams; result: TResult } }, TNotifications> {
		return new DurableChannelBuilder(
			this.#state,
			this.#initialState,
			this.#actions,
			[...this.#commands, builder(new DurableChannelCommandBuilder()) as never],
			this.#notifications,
			this.#summary,
		);
	}

	notification<TName extends string, TPayload extends GenericSchema>(
		builder: (builder: DurableChannelNotificationBuilder) => DurableChannelNotificationBuilder<TName, TPayload>,
	): DurableChannelBuilder<TEnv, TState, TActions, TCommands, TNotifications & { [K in TName]: TPayload }> {
		return new DurableChannelBuilder(
			this.#state,
			this.#initialState,
			this.#actions,
			this.#commands,
			[...this.#notifications, builder(new DurableChannelNotificationBuilder())],
			this.#summary,
		);
	}
}

/** Starts a channel definition. Call `.state()` to make it stateful; without it the channel is a relay. */
export function durableChannel(): DurableChannelStatelessBuilder {
	return new DurableChannelStatelessBuilder();
}
