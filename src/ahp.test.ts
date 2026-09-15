/**
 * The Durable Channel hub driven by the official Agent Host Protocol TypeScript client.
 *
 * This is the only file in the package allowed to know that AHP exists. It holds the whole adapter:
 * the root channel definition, the wire translation, the per-socket link, the hana collection whose
 * procedure names are the AHP methods, and a Hono + `Deno.serve` host. The library underneath it is
 * protocol-agnostic.
 */
import { describe, it } from "node:test";
import { assert, assertEquals, assertInstanceOf, assertNotEquals } from "@std/assert";
import * as v from "valibot";
import { Hono } from "hono";
import { upgradeWebSocket } from "hono/deno";
import { Hana, hana, JsonRpcError } from "./hana.ts";
import * as ahp from "@microsoft/agent-host-protocol";
import { MessageKind, PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS } from "@microsoft/agent-host-protocol";
import type {
	ActionEnvelope,
	ChatAction,
	ChatState,
	ChatSummary,
	InitializeResult,
	Message,
	ResponsePart,
	ResponsePartKind,
	RootAction,
	RootState,
	SessionAction,
	SessionState,
	SessionSummary,
	Snapshot,
	Turn,
	TurnState,
	UsageInfo,
} from "@microsoft/agent-host-protocol";
import { AhpClient, AhpStateMirror, type ClientEvent, RpcError, type Subscription } from "@microsoft/agent-host-protocol/client";
import { WebSocketTransport } from "@microsoft/agent-host-protocol/ws";
import { type HostClientHandle, immediateForeverPolicy, MultiHostClient } from "@microsoft/agent-host-protocol/hosts";
import {
	DenoKvStorage,
	durableChannel,
	type DurableChannelActionEffectContext,
	type DurableChannelConnection,
	DurableChannelError,
	DurableChannelHub,
	type DurableChannelHubOptions,
	type DurableChannelMessage,
	type DurableChannelStorage,
	durableRoutes,
	MemoryStorage,
	RejectAction,
} from "./mod.ts";

// ─── Channel definitions ─────────────────────────────────────────────────────

const AHP_ROOT = "ahp-root://";
const AHP_SESSION_TEMPLATE = "ahp-session:/:uid";
const AHP_CHAT_TEMPLATE = "ahp-chat:/:cid";

/**
 * A private singleton index, never subscribed by a client. It holds the two summary timestamps AHP
 * keeps outside `SessionState` — a reducer cannot read a clock, so `createdAt` and `modifiedAt` cannot
 * live in the reduced state — and `listSessions` and the `root/session*` notifications read it back.
 */
const X_CATALOG = "x-catalog://";

/**
 * What the host has beyond the protocol: a clock, a deterministic agent, and the pause a streaming
 * turn takes between two chunks so every dispatch is a separate commit a test can observe.
 */
type Env = {
	now(): string;
	agent: { reply(text: string): string[] };
	tick(): Promise<void>;
};

/** `SessionStatus`, whose `const enum` Deno cannot read as a value. Pinned against the runtime object. */
const STATUS = { Idle: 1, Error: 2, InProgress: 8, InputNeeded: 24, IsRead: 32, IsArchived: 64 };

/** The mutually-exclusive activity bits of a status; `IsRead` and `IsArchived` sit above them. */
const ACTIVITY_MASK = (1 << 5) - 1;

/** `SessionLifecycle`, same problem, same fix. */
const LIFECYCLE = { Creating: "creating", Ready: "ready", Failed: "failed" } as Record<string, SessionState["lifecycle"]>;

/** A refusal only the protocol has a code for. `guarded()` puts that code straight onto the wire. */
class AhpError extends Error {
	readonly code: number;

	constructor(code: number, message: string) {
		super(message);
		this.name = "AhpError";
		this.code = code;
	}
}

/** A field the adapter carries but never inspects: the official type is the only contract. */
function opaque<T>() {
	return v.custom<T>(() => true);
}

function withStatusFlag(status: number, flag: number, on: boolean): number {
	return on ? status | flag : status & ~flag;
}

/**
 * Drops every key whose value is `undefined` — which is what JSON already did to the same object on
 * its way to a peer. Reducers use it so the hub's own state stays comparable, field for field, with the
 * snapshot a client holds.
 */
function prune<T>(value: T): T {
	const kept: Record<string, unknown> = {};
	for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
		if (entry !== undefined) {
			kept[key] = entry;
		}
	}
	return kept as T;
}

const SessionModelInfo = v.object({ id: v.string(), provider: v.string(), name: v.string() });

const AgentInfo = v.object({
	provider: v.string(),
	displayName: v.string(),
	description: v.string(),
	models: v.array(SessionModelInfo),
});

const RootConfig = v.object({
	schema: v.object({
		type: v.literal("object"),
		properties: v.record(v.string(), v.unknown()),
		required: v.optional(v.array(v.string())),
	}),
	values: v.record(v.string(), v.unknown()),
});

const RootStateSchema = v.object({
	agents: v.array(AgentInfo),
	activeSessions: v.optional(v.number()),
	config: v.optional(RootConfig),
});

const demoAgent = { provider: "demo", displayName: "Demo agent", description: "In-memory demo provider", models: [] };

const initialRootState = {
	agents: [demoAgent],
	activeSessions: 0,
	config: { schema: { type: "object" as const, properties: {} }, values: { theme: "light", locked: "yes" } },
};

/** The state the hub persists has to be a legal `RootState` for the official client to mirror it. */
const _rootStateIsAssignable: RootState = initialRootState;

/**
 * The root channel. Action names are the AHP `type` strings and their payload schemas carry the
 * intrinsic fields only, so `{ type, ...payload }` reconstitutes the wire action.
 */
const root = durableChannel()
	.env<Env>()
	.state(RootStateSchema, initialRootState)
	.action((a) =>
		a.name("root/agentsChanged").payload(v.object({ agents: v.array(AgentInfo) })).reduce((state, payload) => ({
			...state,
			agents: payload.agents,
		}))
	)
	.action((a) =>
		a.name("root/activeSessionsChanged").payload(v.object({ activeSessions: v.number() })).reduce((state, payload) => ({
			...state,
			activeSessions: payload.activeSessions,
		}))
	)
	.action((a) =>
		a.name("root/configChanged")
			.payload(v.object({ config: v.record(v.string(), v.unknown()), replace: v.optional(v.boolean()) }))
			.client()
			.reduce((state, payload) => {
				if (state.config === undefined) {
					return state;
				}
				const values = payload.replace ? { ...payload.config } : { ...state.config.values, ...payload.config };
				// Host policy, not protocol: `locked` is read-only, so a client trying to change it is rejected.
				if (values.locked !== state.config.values.locked) {
					throw new RejectAction('config key "locked" is read-only');
				}
				return { ...state, config: { ...state.config, values } };
			})
	)
	.notification((n) => n.name("root/sessionAdded").payload(v.object({ summary: opaque<SessionSummary>() })))
	.notification((n) => n.name("root/sessionRemoved").payload(v.object({ session: v.string() })))
	.notification((n) =>
		n.name("root/sessionSummaryChanged").payload(v.object({ session: v.string(), changes: opaque<Partial<SessionSummary>>() }))
	)
	.command((c) =>
		c.name("createSession")
			.params(v.object({
				session: v.string(),
				provider: v.optional(v.string()),
				workingDirectories: v.optional(v.array(v.string())),
				config: v.optional(v.record(v.string(), v.unknown())),
				activeClient: v.optional(opaque<SessionState["activeClients"][number]>()),
			}))
			.result(v.null())
			.handler(async (params, ctx) => {
				const { agents } = await ctx.state();
				const provider = params.provider ?? agents[0]?.provider;
				if (!agents.some((agent) => agent.provider === provider)) {
					throw new AhpError(-32002, `PROVIDER_NOT_FOUND: no agent provider "${String(provider)}"`);
				}
				const at = ctx.env.now();
				await ctx.create(params.session, {
					provider,
					title: "New Session",
					status: STATUS.Idle,
					lifecycle: LIFECYCLE.Creating,
					activeClients: params.activeClient === undefined ? [] : [params.activeClient],
					chats: [],
					...(params.workingDirectories !== undefined ? { workingDirectories: params.workingDirectories } : {}),
					...(params.config !== undefined ? { config: { schema: { type: "object", properties: {} }, values: params.config } } : {}),
				});
				await ctx.dispatch(X_CATALOG, "catalog/sessionAdded", { session: params.session, createdAt: at, modifiedAt: at });
				await ctx.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: await countSessions(ctx) });
				const state = await ctx.get(params.session) as SessionStateShape;
				await ctx.notify(AHP_ROOT, "root/sessionAdded", { summary: summaryOf(params.session, state, { createdAt: at, modifiedAt: at }) });
				await ctx.dispatch(params.session, "session/ready", {});
				return null;
			})
	)
	.command((c) =>
		c.name("listSessions")
			.params(v.object({ limit: v.optional(v.number()), cursor: v.optional(v.string()) }))
			.result(v.object({ items: v.array(opaque<SessionSummary>()), nextCursor: v.optional(v.string()) }))
			.handler(async (params, ctx) => {
				const catalogue = await ctx.get(X_CATALOG) as CatalogueShape;
				const items: SessionSummary[] = [];
				for await (const instance of ctx.list(AHP_SESSION_TEMPLATE)) {
					items.push(summaryOf(instance.uri, instance.state as SessionStateShape, catalogue.sessions[instance.uri] ?? EPOCH));
				}
				items.sort((left, right) => right.modifiedAt.localeCompare(left.modifiedAt) || left.resource.localeCompare(right.resource));
				let from = 0;
				if (params.cursor !== undefined) {
					const at = items.findIndex((item) => item.resource === params.cursor);
					if (at < 0) {
						throw new AhpError(-32602, `INVALID_CURSOR: "${params.cursor}" is not a page boundary`);
					}
					from = at + 1;
				}
				const page = params.limit === undefined ? items.slice(from) : items.slice(from, from + params.limit);
				const last = page.at(-1);
				return {
					items: page,
					...(last !== undefined && from + page.length < items.length ? { nextCursor: last.resource } : {}),
				};
			})
	)
	.build();

// ─── The session channel and its catalogue ───────────────────────────────────

const EPOCH = { createdAt: "1970-01-01T00:00:00.000Z", modifiedAt: "1970-01-01T00:00:00.000Z" };

const CatalogueEntry = v.object({ createdAt: v.string(), modifiedAt: v.string() });

/**
 * The private index: the two summary timestamps AHP keeps outside `SessionState`, and the chat → session
 * link, which is nowhere on the wire either — a chat channel's own state does not name its session, so
 * an effect on a chat could not tell the parent session about itself without this.
 */
const CatalogueStateSchema = v.object({
	sessions: v.record(v.string(), CatalogueEntry),
	chats: v.record(v.string(), v.object({ session: v.string() })),
});

type CatalogueShape = v.InferOutput<typeof CatalogueStateSchema>;

const ChatSummarySchema = v.object({
	resource: v.string(),
	title: v.string(),
	status: v.number(),
	activity: v.optional(v.string()),
	modifiedAt: v.string(),
	origin: v.optional(opaque<NonNullable<ChatSummary["origin"]>>()),
	interactivity: v.optional(opaque<NonNullable<ChatSummary["interactivity"]>>()),
	workingDirectories: v.optional(v.array(v.string())),
});

type ChatSummaryShape = v.InferOutput<typeof ChatSummarySchema>;

// ─── The chat channel's state ────────────────────────────────────────────────

/** A message we model down to its origin; everything a client may hang off it stays opaque. */
const MessageSchema = v.object({
	text: v.string(),
	origin: v.object({ kind: v.enum(MessageKind) }),
	attachments: v.optional(v.array(opaque<NonNullable<Message["attachments"]>[number]>())),
	model: v.optional(opaque<NonNullable<Message["model"]>>()),
	agent: v.optional(opaque<NonNullable<Message["agent"]>>()),
});

const MarkdownResponsePartSchema = v.object({
	kind: v.custom<ResponsePartKind.Markdown>((value) => value === "markdown"),
	id: v.string(),
	content: v.string(),
});

const ErrorResponsePartSchema = v.object({
	kind: v.custom<ResponsePartKind.Error>((value) => value === "error"),
	error: v.object({ errorType: v.string(), message: v.string(), stack: v.optional(v.string()) }),
	resumable: v.optional(v.boolean()),
});

/**
 * The two response parts this host produces, and every other kind carried through untouched. The
 * opaque fallback comes last and is typed as the official union, so a `ChatState` built from this
 * schema stays assignable to the official one.
 */
const ResponsePartSchema = v.union([
	MarkdownResponsePartSchema,
	ErrorResponsePartSchema,
	v.custom<ResponsePart>((value) => typeof value === "object" && value !== null && "kind" in value),
]);

/** `usage` is `undefinedable`, not `optional`: the official `ActiveTurn` requires the key to be there. */
const ActiveTurnSchema = v.object({
	id: v.string(),
	startedAt: v.string(),
	message: MessageSchema,
	responseParts: v.array(ResponsePartSchema),
	usage: v.undefinedable(opaque<UsageInfo>()),
});

const TurnSchema = v.object({
	...ActiveTurnSchema.entries,
	duration: v.optional(v.number()),
	state: v.custom<TurnState>((value) => value === "complete" || value === "cancelled" || value === "error"),
});

const ChatStateSchema = v.object({
	resource: v.string(),
	title: v.string(),
	status: v.number(),
	activity: v.optional(v.string()),
	modifiedAt: v.string(),
	origin: v.optional(opaque<NonNullable<ChatState["origin"]>>()),
	interactivity: v.optional(opaque<NonNullable<ChatState["interactivity"]>>()),
	workingDirectories: v.optional(v.array(v.string())),
	turns: v.array(TurnSchema),
	turnsNextCursor: v.optional(v.string()),
	activeTurn: v.optional(ActiveTurnSchema),
	steeringMessage: v.optional(opaque<NonNullable<ChatState["steeringMessage"]>>()),
	queuedMessages: v.optional(v.array(opaque<NonNullable<ChatState["queuedMessages"]>[number]>())),
	draft: v.optional(MessageSchema),
});

type ChatStateShape = v.InferOutput<typeof ChatStateSchema>;
type ActiveTurnShape = v.InferOutput<typeof ActiveTurnSchema>;

/** `TurnState`, one more `const enum` Deno cannot read as a value. Pinned against the runtime object. */
const TURN = { Complete: "complete", Cancelled: "cancelled", Error: "error" } as Record<string, TurnState>;

/** Only a placeholder: every chat instance is created with an explicit state by `createChat`. */
const initialChatState: ChatStateShape = {
	resource: "",
	title: "New Chat",
	status: STATUS.Idle,
	modifiedAt: EPOCH.createdAt,
	turns: [],
};

/** The state the hub persists has to be a legal `ChatState` for `chatReducer` to run on it. */
const _chatStateIsAssignable: ChatState = initialChatState;

/** And a turn the hub builds has to be a legal `Turn`, which is what the fold in scenario 3 replays. */
const _turnIsAssignable: Turn = {
	id: "t1",
	startedAt: EPOCH.createdAt,
	duration: 0,
	message: { text: "", origin: { kind: MessageKind.User } },
	responseParts: [],
	usage: undefined,
	state: TURN.Complete,
} satisfies v.InferOutput<typeof TurnSchema>;

/** `addMillisecondsToTimestamp`, which the package keeps internal. `chat/turnComplete` needs it. */
function stampAfter(startedAt: string, duration: number): string {
	return new Date(Date.parse(startedAt) + duration).toISOString();
}

/**
 * The chat reducer's `summaryStatus`: the activity bits come from the live work — nothing in scope
 * opens an input request or a blocking tool call, so it is `Error`, then `InProgress`, then `Idle` —
 * and `IsRead` / `IsArchived` are preserved.
 */
function chatStatus(state: ChatStateShape, terminal?: number): number {
	const activity = terminal ?? (state.activeTurn !== undefined ? STATUS.InProgress : STATUS.Idle);
	return (state.status & ~ACTIVITY_MASK) | activity;
}

/** `endTurn`: finalize the active turn, clamp its duration, stamp `modifiedAt`, recompute the status. */
function endTurn(
	state: ChatStateShape,
	turnId: string,
	turnState: TurnState,
	duration: number,
	terminal?: number,
	errorPart?: v.InferOutput<typeof ErrorResponsePartSchema>,
): ChatStateShape {
	const active = state.activeTurn;
	if (active === undefined || active.id !== turnId) {
		return state;
	}
	const clamped = Math.max(0, duration);
	const turn: v.InferOutput<typeof TurnSchema> = {
		id: active.id,
		startedAt: active.startedAt,
		duration: clamped,
		message: active.message,
		responseParts: errorPart === undefined ? active.responseParts : [...active.responseParts, errorPart],
		usage: active.usage,
		state: turnState,
	};
	const { activeTurn: _finalized, ...rest } = state;
	const next: ChatStateShape = { ...rest, turns: [...state.turns, turn], modifiedAt: stampAfter(active.startedAt, clamped) };
	return { ...next, status: chatStatus(next, terminal) };
}

/** Replaces the active turn, leaving the rest of the state alone. A no-op when the turn is not live. */
function withActiveTurn(state: ChatStateShape, turnId: string, update: (turn: ActiveTurnShape) => ActiveTurnShape): ChatStateShape {
	if (state.activeTurn === undefined || state.activeTurn.id !== turnId) {
		return state;
	}
	return { ...state, activeTurn: update(state.activeTurn) };
}

/** The summary the parent session carries for a chat: `ChatState` denormalizes exactly these fields. */
function chatSummaryOf(state: ChatStateShape): ChatSummaryShape {
	return prune({
		resource: state.resource,
		title: state.title,
		status: state.status,
		activity: state.activity,
		modifiedAt: state.modifiedAt,
		origin: state.origin,
		interactivity: state.interactivity,
		workingDirectories: state.workingDirectories,
	});
}

// ─── The session channel ─────────────────────────────────────────────────────

const SessionStateSchema = v.object({
	provider: v.string(),
	title: v.string(),
	status: v.number(),
	activity: v.optional(v.string()),
	lifecycle: v.custom<SessionState["lifecycle"]>((value) => value === "creating" || value === "ready" || value === "failed"),
	creationError: v.optional(opaque<NonNullable<SessionState["creationError"]>>()),
	activeClients: v.array(opaque<SessionState["activeClients"][number]>()),
	chats: v.array(ChatSummarySchema),
	defaultChat: v.optional(v.string()),
	workingDirectories: v.optional(v.array(v.string())),
	config: v.optional(opaque<NonNullable<SessionState["config"]>>()),
});

type SessionStateShape = v.InferOutput<typeof SessionStateSchema>;

const initialSessionState: SessionStateShape = {
	provider: "demo",
	title: "New Session",
	status: STATUS.Idle,
	lifecycle: LIFECYCLE.Creating,
	activeClients: [],
	chats: [],
};

/** The state the hub persists has to be a legal `SessionState` for the official reducer to run on it. */
const _sessionStateIsAssignable: SessionState = initialSessionState;

/** The chat a summary speaks for: the default one, or the most recently modified. */
function primaryChat(state: SessionStateShape): v.InferOutput<typeof ChatSummarySchema> | undefined {
	return state.chats.find((chat) => chat.resource === state.defaultChat) ??
		[...state.chats].sort((left, right) => right.modifiedAt.localeCompare(left.modifiedAt))[0];
}

/**
 * A summary's `status` is **not** the session's own: it takes its activity bits from the primary chat
 * and promotes `InputNeeded`, then `Error`, if any chat has them, while `IsRead` and `IsArchived` stay
 * the session's. Nothing reduces this into `SessionState.status`, because `sessionReducer` does not —
 * a client mirroring the session would diverge from the server the moment the hub invented a field.
 */
function aggregateSessionStatus(state: SessionStateShape): number {
	const flags = state.status & ~ACTIVITY_MASK;
	const primary = primaryChat(state);
	const activity = state.chats.some((chat) => (chat.status & ACTIVITY_MASK) === STATUS.InputNeeded)
		? STATUS.InputNeeded
		: state.chats.some((chat) => (chat.status & ACTIVITY_MASK) === STATUS.Error)
		? STATUS.Error
		: primary === undefined
		? state.status & ACTIVITY_MASK
		: primary.status & ACTIVITY_MASK;
	return flags | activity;
}

/** A `SessionSummary` is the session's metadata plus its identity and the catalogue's timestamps. */
function summaryOf(uri: string, state: SessionStateShape, times: { createdAt: string; modifiedAt: string }): SessionSummary {
	return {
		resource: uri,
		provider: state.provider,
		title: state.title,
		status: aggregateSessionStatus(state),
		...(state.activity !== undefined ? { activity: state.activity } : {}),
		...(state.workingDirectories !== undefined ? { workingDirectories: state.workingDirectories } : {}),
		createdAt: times.createdAt,
		modifiedAt: times.modifiedAt,
	};
}

async function countSessions(ctx: { get(uri: string): Promise<unknown> }): Promise<number> {
	return Object.keys((await ctx.get(X_CATALOG) as CatalogueShape).sessions).length;
}

/**
 * What every summary-affecting action does once it is committed: stamp the catalogue with a fresh
 * `modifiedAt`, then tell the root channel's subscribers which mutable fields changed. Identity fields
 * never change, so `changes` never carries `resource`, `provider` or `createdAt`.
 */
function touched(
	field: "title" | "status" | "activity",
): (ctx: DurableChannelActionEffectContext<Env, SessionStateShape>) => Promise<void> {
	return async (ctx) => {
		const modifiedAt = ctx.env.now();
		await ctx.dispatch(X_CATALOG, "catalog/sessionTouched", { session: ctx.uri, modifiedAt });
		const changed = field === "title"
			? { title: ctx.state.title }
			: field === "status"
			? { status: aggregateSessionStatus(ctx.state) }
			: { activity: ctx.state.activity };
		await ctx.notify(AHP_ROOT, "root/sessionSummaryChanged", { session: ctx.uri, changes: { ...changed, modifiedAt } });
	};
}

/**
 * `ahp-session:/<uuid>`. Action names are the AHP `type` strings and every reducer reproduces
 * `sessionReducer` case by case, except that a key is deleted where the canonical reducer would set it
 * to `undefined`: the two are identical once JSON has been over the wire, and this way a snapshot and
 * the hub's own state stay comparable field for field.
 */
const session = durableChannel()
	.env<Env>()
	.state(SessionStateSchema, initialSessionState)
	.action((a) => a.name("session/ready").payload(v.object({})).reduce((state) => ({ ...state, lifecycle: LIFECYCLE.Ready })))
	.action((a) =>
		a.name("session/creationFailed")
			.payload(v.object({ error: opaque<NonNullable<SessionState["creationError"]>>() }))
			.reduce((state, payload) => ({ ...state, lifecycle: LIFECYCLE.Failed, creationError: payload.error }))
	)
	.action((a) =>
		a.name("session/chatAdded")
			.payload(v.object({ summary: ChatSummarySchema }))
			.reduce((state, payload) => {
				const at = state.chats.findIndex((chat) => chat.resource === payload.summary.resource);
				if (at < 0) {
					return { ...state, chats: [...state.chats, payload.summary] };
				}
				const chats = [...state.chats];
				chats[at] = payload.summary;
				return { ...state, chats };
			})
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/chatRemoved")
			.payload(v.object({ chat: v.string() }))
			.reduce((state, payload) => {
				const chats = state.chats.filter((chat) => chat.resource !== payload.chat);
				if (chats.length === state.chats.length) {
					return state;
				}
				const next = { ...state, chats };
				if (state.defaultChat === payload.chat) {
					delete next.defaultChat;
				}
				return next;
			})
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/chatUpdated")
			.payload(v.object({ chat: v.string(), changes: opaque<Partial<ChatSummary>>() }))
			.reduce((state, payload) => {
				const at = state.chats.findIndex((chat) => chat.resource === payload.chat);
				if (at < 0) {
					return state;
				}
				const { resource: _identity, ...changes } = payload.changes;
				const chats = [...state.chats];
				// `prune` on the *changes*, which is what JSON already did to them on the way to a peer:
				// a merge cannot express "remove this key", so an absent value means "leave it alone" on
				// both sides. A cleared chat activity therefore lingers in the parent's summary.
				chats[at] = { ...chats[at], ...prune(changes) };
				return { ...state, chats };
			})
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/defaultChatChanged")
			.payload(v.object({ defaultChat: v.optional(v.string()) }))
			.reduce((state, payload) => {
				if (payload.defaultChat === undefined) {
					const { defaultChat: _cleared, ...rest } = state;
					return rest;
				}
				return { ...state, defaultChat: payload.defaultChat };
			})
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/activityChanged")
			.payload(v.object({ activity: v.optional(v.string()) }))
			.reduce((state, payload) => {
				if (payload.activity === undefined) {
					const { activity: _cleared, ...rest } = state;
					return rest;
				}
				return { ...state, activity: payload.activity };
			})
			.effect(touched("activity"))
	)
	.action((a) =>
		a.name("session/titleChanged")
			.payload(v.object({ title: v.string() }))
			.client()
			.reduce((state, payload) => {
				// Host policy, not protocol: an empty title would leave the session unnameable in a list.
				if (payload.title.trim() === "") {
					throw new RejectAction("title must not be blank");
				}
				return { ...state, title: payload.title };
			})
			.effect(touched("title"))
	)
	.action((a) =>
		a.name("session/isReadChanged")
			.payload(v.object({ isRead: v.boolean() }))
			.client()
			.reduce((state, payload) => ({ ...state, status: withStatusFlag(state.status, STATUS.IsRead, payload.isRead) }))
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/isArchivedChanged")
			.payload(v.object({ isArchived: v.boolean() }))
			.client()
			.reduce((state, payload) => ({ ...state, status: withStatusFlag(state.status, STATUS.IsArchived, payload.isArchived) }))
			.effect(touched("status"))
	)
	.action((a) =>
		a.name("session/workingDirectorySet")
			.payload(v.object({ directory: v.string() }))
			.client()
			.reduce((state, payload) => {
				const directories = state.workingDirectories ?? [];
				if (directories.includes(payload.directory)) {
					return state;
				}
				return { ...state, workingDirectories: [...directories, payload.directory] };
			})
	)
	.action((a) =>
		a.name("session/workingDirectoryRemoved")
			.payload(v.object({ directory: v.string() }))
			.client()
			.reduce((state, payload) => {
				const directories = state.workingDirectories;
				if (directories === undefined || !directories.includes(payload.directory)) {
					return state;
				}
				return { ...state, workingDirectories: directories.filter((directory) => directory !== payload.directory) };
			})
	)
	.command((c) =>
		c.name("createChat")
			.params(v.object({
				chat: v.string(),
				initialMessage: v.optional(MessageSchema),
				source: v.optional(opaque<NonNullable<ahp.CreateChatParams["source"]>>()),
				workingDirectories: v.optional(v.array(v.string())),
			}))
			.result(v.null())
			.handler(async (params, ctx) => {
				const state = await ctx.state();
				if (state.lifecycle !== LIFECYCLE.Ready) {
					throw new AhpError(-32011, `CONFLICT: session "${ctx.uri}" is not ready`);
				}
				if (await ctx.has(params.chat)) {
					throw new AhpError(-32010, `ALREADY_EXISTS: chat "${params.chat}" already exists`);
				}
				await ctx.create(params.chat, {
					resource: params.chat,
					title: "New Chat",
					status: STATUS.Idle,
					modifiedAt: ctx.env.now(),
					turns: [],
					...(params.workingDirectories !== undefined ? { workingDirectories: params.workingDirectories } : {}),
				});
				await ctx.dispatch(X_CATALOG, "catalog/chatAdded", { chat: params.chat, session: ctx.uri });
				await ctx.dispatch(ctx.uri, "session/chatAdded", { summary: chatSummaryOf(await ctx.get(params.chat) as ChatStateShape) });
				if (state.defaultChat === undefined) {
					await ctx.dispatch(ctx.uri, "session/defaultChatChanged", { defaultChat: params.chat });
				}
				if (params.initialMessage !== undefined) {
					await ctx.dispatch(params.chat, "chat/turnStarted", {
						turnId: crypto.randomUUID(),
						startedAt: ctx.env.now(),
						message: params.initialMessage,
					});
				}
				return null;
			})
	)
	.command((c) =>
		c.name("disposeSession")
			.params(v.object({}))
			.result(v.null())
			.handler(async (_params, ctx) => {
				// Cascading destroy is the definition's business: the hub knows of no ownership.
				for (const chat of (await ctx.state()).chats) {
					await ctx.exec(chat.resource, "disposeChat", {});
				}
				await ctx.destroy(ctx.uri);
				await ctx.dispatch(X_CATALOG, "catalog/sessionRemoved", { session: ctx.uri });
				await ctx.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: await countSessions(ctx) });
				await ctx.notify(AHP_ROOT, "root/sessionRemoved", { session: ctx.uri });
				return null;
			})
	)
	.build();

// ─── The chat channel ────────────────────────────────────────────────────────

/** How long the fake agent claims a turn took. Producer-supplied and opaque to every reducer. */
const TURN_DURATION_MS = 42;

function isMarkdownPart(part: ResponsePart): part is v.InferOutput<typeof MarkdownResponsePartSchema> {
	return (part as { kind: string }).kind === "markdown";
}

function isErrorPart(part: ResponsePart): boolean {
	return (part as { kind: string }).kind === "error";
}

/** The session a chat belongs to. `ChatState` never names its parent, so the private index does. */
async function sessionOf(ctx: { get(uri: string): Promise<unknown> }, chat: string): Promise<string | undefined> {
	return (await ctx.get(X_CATALOG) as CatalogueShape).chats[chat]?.session;
}

/**
 * What a chat owes its parent after a mutation: `ChatState` denormalizes `ChatSummary`, and a producer
 * MUST keep the two consistent by announcing every change with `session/chatUpdated`.
 */
function announced(
	pick: (state: ChatStateShape) => Partial<ChatSummary>,
): (ctx: DurableChannelActionEffectContext<Env, ChatStateShape>) => Promise<void> {
	return async (ctx) => {
		const session = await sessionOf(ctx, ctx.uri);
		if (session === undefined) {
			return;
		}
		await ctx.dispatch(session, "session/chatUpdated", { chat: ctx.uri, changes: pick(ctx.state) });
	};
}

/** A turn starting, ending or being cancelled moves both the status and the modification stamp. */
const turnProgress = (state: ChatStateShape): Partial<ChatSummary> => ({ status: state.status, modifiedAt: state.modifiedAt });

/**
 * `ahp-chat:/<cid>`. The reducers reproduce `chatReducer` for the actions this host emits, with the
 * same key-deleting convention as the session channel, and the fake agent lives in the effect of
 * `chat/turnStarted`: it is background work owned by the chat's URI, so disposing the chat or
 * cancelling the turn stops it.
 */
const chat = durableChannel()
	.env<Env>()
	.state(ChatStateSchema, initialChatState)
	.action((a) =>
		a.name("chat/turnStarted")
			.payload(v.object({
				turnId: v.string(),
				startedAt: v.string(),
				message: MessageSchema,
				queuedMessageId: v.optional(v.string()),
			}))
			.client()
			.reduce((state, payload) => {
				// Host policy, not protocol: `dispatchAction` is a notification, so a busy chat can only
				// answer with a rejected echo — never AHP's `TurnInProgress` (-32004) error.
				if (state.activeTurn !== undefined) {
					throw new RejectAction("TURN_IN_PROGRESS: a turn is already active");
				}
				const next: ChatStateShape = {
					...state,
					activeTurn: {
						id: payload.turnId,
						startedAt: payload.startedAt,
						message: payload.message,
						responseParts: [],
						usage: undefined,
					},
				};
				return { ...next, status: withStatusFlag(chatStatus(next), STATUS.IsRead, false), modifiedAt: payload.startedAt };
			})
			.effect(async (ctx) => {
				await announced(turnProgress)(ctx);
				const { turnId, message } = ctx.payload;
				const chunks = ctx.env.agent.reply(message.text);
				ctx.background(async (signal) => {
					await ctx.dispatch(ctx.uri, "chat/activityChanged", { activity: "Thinking" });
					await ctx.dispatch(ctx.uri, "chat/responsePart", { turnId, part: { kind: "markdown", id: "p1", content: "" } });
					for (const chunk of chunks) {
						await ctx.env.tick();
						if (signal.aborted) {
							return;
						}
						await ctx.dispatch(ctx.uri, "chat/delta", { turnId, partId: "p1", content: chunk });
					}
					await ctx.dispatch(ctx.uri, "chat/usage", { turnId, usage: { inputTokens: 3, outputTokens: chunks.length } });
					await ctx.dispatch(ctx.uri, "chat/activityChanged", {});
					await ctx.dispatch(ctx.uri, "chat/turnComplete", { turnId, duration: TURN_DURATION_MS });
				});
			})
	)
	.action((a) =>
		a.name("chat/responsePart")
			.payload(v.object({ turnId: v.string(), part: ResponsePartSchema }))
			.reduce((state, payload) =>
				// An error part is ignored here exactly as in the canonical reducer: `chat/error` carries it.
				isErrorPart(payload.part) ? state : withActiveTurn(state, payload.turnId, (turn) => ({
					...turn,
					responseParts: [...turn.responseParts, payload.part],
				}))
			)
	)
	.action((a) =>
		a.name("chat/delta")
			.payload(v.object({ turnId: v.string(), partId: v.string(), content: v.string() }))
			.reduce((state, payload) =>
				withActiveTurn(state, payload.turnId, (turn) => ({
					...turn,
					responseParts: turn.responseParts.map((part) =>
						isMarkdownPart(part) && part.id === payload.partId ? { ...part, content: part.content + payload.content } : part
					),
				}))
			)
	)
	.action((a) =>
		a.name("chat/usage")
			.payload(v.object({ turnId: v.string(), usage: opaque<UsageInfo>() }))
			.reduce((state, payload) => withActiveTurn(state, payload.turnId, (turn) => ({ ...turn, usage: payload.usage })))
	)
	.action((a) =>
		a.name("chat/turnComplete")
			.payload(v.object({ turnId: v.string(), duration: v.number() }))
			.reduce((state, payload) => endTurn(state, payload.turnId, TURN.Complete, payload.duration))
			.effect(announced(turnProgress))
	)
	.action((a) =>
		a.name("chat/turnCancelled")
			.payload(v.object({ turnId: v.string(), duration: v.number() }))
			.client()
			.reduce((state, payload) => endTurn(state, payload.turnId, TURN.Cancelled, payload.duration))
			.effect(async (ctx) => {
				// The agent is background work owned by this URI; cancelling the turn is what stops it.
				ctx.abortBackground();
				await announced(turnProgress)(ctx);
			})
	)
	.action((a) =>
		a.name("chat/error")
			.payload(v.object({ turnId: v.string(), duration: v.number(), part: ErrorResponsePartSchema }))
			.reduce((state, payload) => endTurn(state, payload.turnId, TURN.Error, payload.duration, STATUS.Error, payload.part))
			.effect(announced(turnProgress))
	)
	.action((a) =>
		a.name("chat/activityChanged")
			.payload(v.object({ activity: v.optional(v.string()) }))
			.reduce((state, payload) => {
				const next = { ...state };
				if (payload.activity === undefined) {
					delete next.activity;
				} else {
					next.activity = payload.activity;
				}
				return next;
			})
			.effect(announced((state) => ({ activity: state.activity })))
	)
	.action((a) =>
		a.name("chat/truncated")
			.payload(v.object({ turnId: v.optional(v.string()) }))
			.client()
			.reduce((state, payload) => {
				let turns = state.turns;
				if (payload.turnId !== undefined) {
					const at = state.turns.findIndex((turn) => turn.id === payload.turnId);
					if (at < 0) {
						return state;
					}
					turns = state.turns.slice(0, at + 1);
				} else {
					turns = [];
				}
				const next: ChatStateShape = { ...state, turns };
				delete next.activeTurn;
				if (payload.turnId === undefined) {
					delete next.turnsNextCursor;
				}
				return { ...next, status: chatStatus(next) };
			})
			.effect(announced(turnProgress))
	)
	.action((a) =>
		a.name("chat/draftChanged")
			.payload(v.object({ draft: v.optional(MessageSchema) }))
			.client()
			.reduce((state, payload) => {
				const next = { ...state };
				if (payload.draft === undefined) {
					delete next.draft;
				} else {
					next.draft = payload.draft;
				}
				return next;
			})
	)
	.action((a) =>
		a.name("chat/workingDirectorySet")
			.payload(v.object({ directory: v.string() }))
			.client()
			.reduce((state, payload) => {
				const directories = state.workingDirectories ?? [];
				if (directories.includes(payload.directory)) {
					return state;
				}
				return { ...state, workingDirectories: [...directories, payload.directory] };
			})
	)
	.action((a) =>
		a.name("chat/workingDirectoryRemoved")
			.payload(v.object({ directory: v.string() }))
			.client()
			.reduce((state, payload) => {
				const directories = state.workingDirectories;
				if (directories === undefined || !directories.includes(payload.directory)) {
					return state;
				}
				return { ...state, workingDirectories: directories.filter((directory) => directory !== payload.directory) };
			})
	)
	.command((c) =>
		c.name("disposeChat")
			.params(v.object({}))
			.result(v.null())
			.handler(async (_params, ctx) => {
				const session = await sessionOf(ctx, ctx.uri);
				// `destroy` aborts the URI's background tasks, so a streaming agent stops here too.
				await ctx.destroy(ctx.uri);
				await ctx.dispatch(X_CATALOG, "catalog/chatRemoved", { chat: ctx.uri });
				if (session !== undefined) {
					await ctx.dispatch(session, "session/chatRemoved", { chat: ctx.uri });
				}
				return null;
			})
	)
	.command((c) =>
		c.name("fetchTurns")
			.params(v.object({ cursor: v.optional(v.string()) }))
			.result(v.null())
			.handler((params) => {
				// Every retained turn is already in the snapshot, so there is no older page and no cursor
				// this host could ever have handed out.
				if (params.cursor !== undefined) {
					throw new AhpError(-32602, `INVALID_CURSOR: "${params.cursor}" is not a page boundary`);
				}
				return null;
			})
	)
	.build();

const catalogue = durableChannel()
	.env<Env>()
	.state(CatalogueStateSchema, { sessions: {}, chats: {} })
	.action((a) =>
		a.name("catalog/sessionAdded")
			.payload(v.object({ session: v.string(), createdAt: v.string(), modifiedAt: v.string() }))
			.reduce((state, payload) => ({
				...state,
				sessions: { ...state.sessions, [payload.session]: { createdAt: payload.createdAt, modifiedAt: payload.modifiedAt } },
			}))
	)
	.action((a) =>
		a.name("catalog/sessionTouched").payload(v.object({ session: v.string(), modifiedAt: v.string() })).reduce((state, payload) => {
			const entry = state.sessions[payload.session];
			if (entry === undefined) {
				return state;
			}
			return { ...state, sessions: { ...state.sessions, [payload.session]: { ...entry, modifiedAt: payload.modifiedAt } } };
		})
	)
	.action((a) =>
		a.name("catalog/sessionRemoved").payload(v.object({ session: v.string() })).reduce((state, payload) => {
			const sessions = { ...state.sessions };
			delete sessions[payload.session];
			return { ...state, sessions };
		})
	)
	.action((a) =>
		a.name("catalog/chatAdded").payload(v.object({ chat: v.string(), session: v.string() })).reduce((state, payload) => ({
			...state,
			chats: { ...state.chats, [payload.chat]: { session: payload.session } },
		}))
	)
	.action((a) =>
		a.name("catalog/chatRemoved").payload(v.object({ chat: v.string() })).reduce((state, payload) => {
			const chats = { ...state.chats };
			delete chats[payload.chat];
			return { ...state, chats };
		})
	)
	.build();

const routes = durableRoutes()
	.env<Env>()
	.route(AHP_ROOT, root)
	.route(AHP_SESSION_TEMPLATE, session)
	.route(AHP_CHAT_TEMPLATE, chat)
	// Internal: the index needs the persistence, sequencing and validation of a durable channel, and no
	// client is ever told it exists — `subscribe` refuses it and `reconnect` reports it missing.
	.route(X_CATALOG, catalogue, { internal: true })
	.build();

function makeHub(options: DurableChannelHubOptions<Env>) {
	return new DurableChannelHub(routes, options);
}

type RootHub = ReturnType<typeof makeHub>;

// ─── Wire translation ────────────────────────────────────────────────────────

interface WireEnvelope {
	channel: string;
	action: { type: string } & Record<string, unknown>;
	serverSeq: number;
	origin?: { clientId: string; clientSeq: number };
	rejectionReason?: string;
}

function fields(payload: unknown): Record<string, unknown> {
	return typeof payload === "object" && payload !== null && !Array.isArray(payload) ? payload as Record<string, unknown> : {};
}

function toWireEnvelope(envelope: Extract<DurableChannelMessage, { type: "action" }>): WireEnvelope {
	return {
		channel: envelope.channel,
		action: { type: envelope.name, ...fields(envelope.payload) },
		serverSeq: envelope.serverSeq,
		...(envelope.origin !== undefined ? { origin: envelope.origin } : {}),
		...(envelope.rejectionReason !== undefined ? { rejectionReason: envelope.rejectionReason } : {}),
	};
}

/** An `action` envelope, or a channel notification whose name is the wire method. */
function toWire(message: DurableChannelMessage): Record<string, unknown> {
	return message.type === "action"
		? { jsonrpc: "2.0", method: "action", params: toWireEnvelope(message) }
		: { jsonrpc: "2.0", method: message.name, params: { channel: message.channel, ...fields(message.payload) } };
}

function fromAction(action: { type: string } & Record<string, unknown>): [string, Record<string, unknown>] {
	const { type, ...payload } = action;
	return [type, payload];
}

// ─── Link ────────────────────────────────────────────────────────────────────

/** Binds one socket to one `clientId`, and lets a handler ask for the socket to close afterwards. */
interface AhpLink {
	readonly clientId: string | undefined;
	bind(clientId: string): void;
	closeAfterResponse(): void;
}

// ─── JSON-RPC surface ────────────────────────────────────────────────────────

const WireSnapshot = v.object({ resource: v.string(), state: v.unknown(), fromSeq: v.number() });

const WireEnvelopeSchema = v.object({
	channel: v.string(),
	action: v.looseObject({ type: v.string() }),
	serverSeq: v.number(),
	origin: v.optional(v.object({ clientId: v.string(), clientSeq: v.number() })),
	rejectionReason: v.optional(v.string()),
});

/**
 * AHP's error table for the durable channel codes this milestone can raise. A session URI has its own
 * two codes — `SessionNotFound` and `SessionAlreadyExists` — where any other resource gets the generic
 * `NotFound` and `AlreadyExists`, so the mapping needs the URI the call was about.
 */
function rpcCodeFor(code: string, uri: string | undefined): number {
	const session = uri !== undefined && uri.startsWith("ahp-session:");
	switch (code) {
		case "CHANNEL_NOT_FOUND":
		case "ROUTE_NOT_FOUND":
			return session ? -32001 : -32008;
		case "CHANNEL_ALREADY_EXISTS":
			return session ? -32003 : -32010;
		case "INVALID_PAYLOAD":
			return -32602;
		default:
			return -32000;
	}
}

async function guarded<T>(task: () => Promise<T>, uri?: string): Promise<T> {
	try {
		return await task();
	} catch (error) {
		if (error instanceof AhpError) {
			throw new JsonRpcError(error.code, error.message);
		}
		if (!(error instanceof DurableChannelError)) {
			throw error;
		}
		throw new JsonRpcError(rpcCodeFor(error.code, uri), `${error.code}: ${error.message}`);
	}
}

function boundClientId(link: AhpLink | undefined): string {
	if (link?.clientId === undefined) {
		throw new JsonRpcError(-32000, "NOT_INITIALIZED: initialize or reconnect first");
	}
	return link.clientId;
}

type RpcEnv = { hub: RootHub; link?: AhpLink; calls: Record<string, number> };

const ahpRpc = new Hana(
	hana()
		.env<RpcEnv>()
		.def((b) =>
			b.name("initialize")
				.params(v.object({
					channel: v.literal(AHP_ROOT),
					clientId: v.pipe(v.string(), v.minLength(1)),
					protocolVersions: v.pipe(v.array(v.string()), v.minLength(1)),
					clientInfo: v.optional(v.unknown()),
					initialSubscriptions: v.optional(v.array(v.string()), []),
					locale: v.optional(v.string()),
					capabilities: v.optional(v.unknown()),
				}))
				.result(v.object({
					protocolVersion: v.string(),
					serverSeq: v.number(),
					serverInfo: v.object({ name: v.string(), version: v.string() }),
					snapshots: v.array(WireSnapshot),
				}))
				.handler(async (params, ctx) => {
					ctx.env.calls.initialize = (ctx.env.calls.initialize ?? 0) + 1;
					const protocolVersion = params.protocolVersions.find((offer) => SUPPORTED_PROTOCOL_VERSIONS.includes(offer as never));
					if (protocolVersion === undefined) {
						ctx.env.link?.closeAfterResponse();
						throw new JsonRpcError(-32005, "UNSUPPORTED_PROTOCOL_VERSION: none of the offered versions is supported", {
							supportedVersions: [...SUPPORTED_PROTOCOL_VERSIONS],
						});
					}
					ctx.env.link?.bind(params.clientId);
					const clientId = boundClientId(ctx.env.link);
					const snapshots = [];
					for (const uri of params.initialSubscriptions) {
						try {
							const snapshot = await ctx.env.hub.subscribe(clientId, uri);
							if (snapshot !== undefined) {
								snapshots.push(snapshot);
							}
						} catch (error) {
							if (!(error instanceof DurableChannelError)) {
								throw error;
							}
						}
					}
					return {
						protocolVersion,
						serverSeq: ctx.env.hub.serverSeq,
						serverInfo: { name: "durable-channel-ahp-test", version: "0.1.0" },
						snapshots,
					};
				})
		)
		.def((b) =>
			b.name("ping")
				.params(v.object({ channel: v.optional(v.string()) }))
				.result(v.null())
				.handler((_params, ctx) => {
					ctx.env.calls.ping = (ctx.env.calls.ping ?? 0) + 1;
					return null;
				})
		)
		.def((b) =>
			b.name("reconnect")
				.params(v.object({
					channel: v.literal(AHP_ROOT),
					clientId: v.pipe(v.string(), v.minLength(1)),
					lastSeenServerSeq: v.number(),
					subscriptions: v.array(v.string()),
				}))
				.result(v.union([
					v.object({ type: v.literal("replay"), actions: v.array(WireEnvelopeSchema), missing: v.array(v.string()) }),
					v.object({ type: v.literal("snapshot"), snapshots: v.array(WireSnapshot), missing: v.array(v.string()) }),
				]))
				.handler((params, ctx) => {
					ctx.env.calls.reconnect = (ctx.env.calls.reconnect ?? 0) + 1;
					ctx.env.link?.bind(params.clientId);
					const clientId = boundClientId(ctx.env.link);
					return guarded(async () => {
						const result = await ctx.env.hub.reconnect(clientId, params.lastSeenServerSeq, params.subscriptions);
						const missing = [...result.missing];
						return result.type === "replay"
							? { type: "replay" as const, actions: result.actions.map(toWireEnvelope), missing }
							: { type: "snapshot" as const, snapshots: [...result.snapshots], missing };
					});
				})
		)
		.def((b) =>
			b.name("subscribe")
				.params(v.object({ channel: v.string(), delivery: v.optional(v.unknown()), view: v.optional(v.unknown()) }))
				.result(v.object({ snapshot: v.optional(WireSnapshot) }))
				.handler((params, ctx) => {
					ctx.env.calls.subscribe = (ctx.env.calls.subscribe ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					return guarded(async () => {
						const snapshot = await ctx.env.hub.subscribe(clientId, params.channel);
						return snapshot === undefined ? {} : { snapshot };
					}, params.channel);
				})
		)
		.def((b) =>
			b.name("unsubscribe")
				.params(v.object({ channel: v.string() }))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.calls.unsubscribe = (ctx.env.calls.unsubscribe ?? 0) + 1;
					ctx.env.hub.unsubscribe(boundClientId(ctx.env.link), params.channel);
					return null;
				})
		)
		.def((b) =>
			b.name("dispatchAction")
				.params(v.object({ channel: v.string(), clientSeq: v.number(), action: v.looseObject({ type: v.string() }) }))
				.result(v.nullable(WireEnvelopeSchema))
				.handler((params, ctx) => {
					ctx.env.calls.dispatchAction = (ctx.env.calls.dispatchAction ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					const [name, payload] = fromAction(params.action);
					return guarded(async () => {
						const envelope = await ctx.env.hub.dispatchFrom(clientId, params.channel, name, payload, params.clientSeq, { lenient: true });
						return envelope === undefined ? null : toWireEnvelope(envelope);
					}, params.channel);
				})
		)
		.def((b) =>
			b.name("listSessions")
				.params(v.object({ channel: v.literal(AHP_ROOT), limit: v.optional(v.number()), cursor: v.optional(v.string()) }))
				.result(v.object({ items: v.array(v.unknown()), nextCursor: v.optional(v.string()) }))
				.handler((params, ctx) => {
					ctx.env.calls.listSessions = (ctx.env.calls.listSessions ?? 0) + 1;
					return guarded(() => ctx.env.hub.exec(AHP_ROOT, "listSessions", { limit: params.limit, cursor: params.cursor }));
				})
		)
		.def((b) =>
			b.name("createSession")
				.params(v.object({
					channel: v.string(),
					provider: v.optional(v.string()),
					workingDirectories: v.optional(v.array(v.string())),
					config: v.optional(v.record(v.string(), v.unknown())),
					activeClient: v.optional(opaque<SessionState["activeClients"][number]>()),
					progressToken: v.optional(v.unknown()),
				}))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.calls.createSession = (ctx.env.calls.createSession ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					return guarded(
						() =>
							ctx.env.hub.exec(AHP_ROOT, "createSession", {
								session: params.channel,
								provider: params.provider,
								workingDirectories: params.workingDirectories,
								config: params.config,
								activeClient: params.activeClient,
							}, { connectionId: clientId }),
						params.channel,
					);
				})
		)
		.def((b) =>
			b.name("disposeSession")
				.params(v.object({ channel: v.string() }))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.calls.disposeSession = (ctx.env.calls.disposeSession ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					return guarded(async () => {
						await ctx.env.hub.exec(params.channel, "disposeSession", {}, { connectionId: clientId });
						return null;
					}, params.channel);
				})
		)
		.def((b) =>
			b.name("createChat")
				.params(v.object({
					channel: v.string(),
					chat: v.string(),
					initialMessage: v.optional(v.unknown()),
					source: v.optional(v.unknown()),
					workingDirectories: v.optional(v.array(v.string())),
				}))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.calls.createChat = (ctx.env.calls.createChat ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					// `channel` is the owning session; `chat` is the new chat's own URI.
					return guarded(async () => {
						await ctx.env.hub.exec(params.channel, "createChat", {
							chat: params.chat,
							initialMessage: params.initialMessage,
							source: params.source,
							workingDirectories: params.workingDirectories,
						}, { connectionId: clientId });
						return null;
					}, params.channel);
				})
		)
		.def((b) =>
			b.name("disposeChat")
				.params(v.object({ channel: v.string() }))
				.result(v.null())
				.handler((params, ctx) => {
					ctx.env.calls.disposeChat = (ctx.env.calls.disposeChat ?? 0) + 1;
					const clientId = boundClientId(ctx.env.link);
					return guarded(async () => {
						await ctx.env.hub.exec(params.channel, "disposeChat", {}, { connectionId: clientId });
						return null;
					}, params.channel);
				})
		)
		.def((b) =>
			b.name("fetchTurns")
				.params(v.object({ channel: v.string(), cursor: v.optional(v.string()) }))
				.result(v.object({}))
				.handler((params, ctx) => {
					ctx.env.calls.fetchTurns = (ctx.env.calls.fetchTurns ?? 0) + 1;
					return guarded(async () => {
						await ctx.env.hub.exec(params.channel, "fetchTurns", { cursor: params.cursor });
						return {};
					}, params.channel);
				})
		)
		.build(),
);

// ─── Host ────────────────────────────────────────────────────────────────────

interface Socket {
	send(data: string): void;
	close(code?: number, reason?: string): void;
}

/** The deterministic agent: the same three chunks for any prompt, and no clock and no randomness. */
const fakeAgent = { reply: () => ["Hello", ", ", "world"] };

function boot(
	options?: {
		storage?: DurableChannelStorage;
		replayLimit?: number;
		now?: () => string;
		tick?: () => Promise<void>;
	},
) {
	const hub = makeHub({
		storage: options?.storage ?? new MemoryStorage(),
		env: {
			now: options?.now ?? (() => new Date().toISOString()),
			agent: fakeAgent,
			// One macrotask between two chunks: fast, yet every dispatch is a commit of its own.
			tick: options?.tick ?? (() => new Promise<void>((resolve) => setTimeout(resolve, 0))),
		},
		...(options?.replayLimit !== undefined ? { replayLimit: options.replayLimit } : {}),
	});
	const calls: Record<string, number> = {};
	const bindings = new Map<string, Socket>();
	const live = new Set<Socket>();
	const app = new Hono();
	app.get(
		"/rpc",
		upgradeWebSocket(() => {
			const aborter = new AbortController();
			let socket: Socket | undefined;
			let bound: { clientId: string; connection: DurableChannelConnection } | undefined;
			let closing = false;
			const release = (): void => {
				if (bound === undefined) {
					return;
				}
				const stale = bound;
				bound = undefined;
				hub.disconnect(stale.clientId, stale.connection);
				if (bindings.get(stale.clientId) === socket) {
					bindings.delete(stale.clientId);
				}
			};
			const link: AhpLink = {
				get clientId(): string | undefined {
					return bound?.clientId;
				},
				bind(clientId: string): void {
					release();
					const connection = hub.connect({ id: clientId, send: (message) => socket?.send(JSON.stringify(toWire(message))) });
					bound = { clientId, connection };
					if (socket !== undefined) {
						bindings.set(clientId, socket);
					}
				},
				closeAfterResponse(): void {
					closing = true;
				},
			};
			const drop = (): void => {
				aborter.abort();
				release();
			};
			return {
				onOpen(_event: Event, ws: Socket) {
					socket = ws;
					live.add(ws);
				},
				async onMessage(event: MessageEvent, ws: Socket) {
					socket = ws;
					let frame: unknown;
					try {
						frame = JSON.parse(typeof event.data === "string" ? event.data : await (event.data as Blob).text());
					} catch {
						ws.send(JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }));
						return;
					}
					const response = await ahpRpc.handle(frame, { env: { hub, link, calls } }, aborter.signal);
					if (response !== undefined) {
						ws.send(JSON.stringify(response));
					}
					if (closing) {
						ws.close(1000);
					}
				},
				onClose(_event: CloseEvent, ws: Socket) {
					live.delete(ws);
					drop();
				},
				onError(_event: Event, ws: Socket) {
					live.delete(ws);
					drop();
				},
			};
		}),
	);
	const listener = Deno.serve({ port: 0, hostname: "127.0.0.1", onListen() {} }, app.fetch);
	const { port } = listener.addr as Deno.NetAddr;
	return {
		hub,
		calls,
		url: `ws://127.0.0.1:${port}/rpc`,
		/** Unclean close, so the official transport reports a drop and `MultiHostClient` reconnects. */
		drop(clientId: string): void {
			bindings.get(clientId)?.close(1012);
		},
		async stop(): Promise<void> {
			// Before the sockets: a background task still streaming would otherwise outlive the test and
			// trip the op sanitizer.
			await withTimeout(hub.close(), "the hub's background tasks to settle");
			for (const socket of [...live]) {
				socket.close(1000);
			}
			await withTimeout(listener.shutdown(), "the listener to shut down");
		},
	};
}

// ─── Client helpers ──────────────────────────────────────────────────────────

async function connect(url: string): Promise<{ client: AhpClient; transport: WebSocketTransport }> {
	const transport = await WebSocketTransport.connect(url);
	const client = new AhpClient(transport, { requestTimeoutMs: 5000 });
	client.connect();
	return { client, transport };
}

/** Fails fast with a message instead of hanging, and leaves no timer behind for the op sanitizer. */
function withTimeout<T>(promise: Promise<T>, what: string, ms = 5000): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const guard = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms);
	});
	return Promise.race([promise, guard]).finally(() => clearTimeout(timer)) as Promise<T>;
}

/** The handshake every scenario but the negotiation ones starts from. */
function handshake(client: AhpClient, clientId: string, initialSubscriptions: readonly string[] = []): Promise<InitializeResult> {
	return withTimeout(client.initialize({ clientId, protocolVersions: [PROTOCOL_VERSION], initialSubscriptions }), "the handshake");
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function nextEvent<T extends string>(
	subscription: Subscription,
	type: T,
	what: string,
): Promise<Extract<Awaited<ReturnType<Subscription["next"]>>["value"], { type: T }>> {
	for (;;) {
		const step = await withTimeout(subscription.next(), what);
		assert(!step.done, `subscription closed while waiting for ${what}`);
		if (step.value.type === type) {
			return step.value as never;
		}
	}
}

function nextAction(subscription: Subscription, what: string): Promise<ActionEnvelope> {
	return nextEvent(subscription, "action", what).then((event) => event.params);
}

/** `"quiet"` when nothing arrives within `ms`, otherwise a description of what did. */
async function quiet(events: AsyncIterableIterator<ClientEvent>, ms: number): Promise<string> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const idle = new Promise<string>((resolve) => {
		timer = setTimeout(() => resolve("quiet"), ms);
	});
	const arrived = events.next().then((step) => (step.done ? "quiet" : `${step.value.event.type} on ${step.value.channel}`));
	try {
		return await Promise.race([idle, arrived]);
	} finally {
		clearTimeout(timer);
		await events.return?.(undefined);
	}
}

/**
 * Drives a bare socket so the test can assert on frames the official client would drop. A frame
 * carrying an `id` is a request and the next frame waits for its response; a notification has no
 * response, so it gets a short grace period instead.
 */
async function rawExchange(url: string, frames: Record<string, unknown>[], quietMs = 250): Promise<Record<string, unknown>[]> {
	const socket = new WebSocket(url);
	const received: Record<string, unknown>[] = [];
	const waiting = new Map<number, () => void>();
	socket.onmessage = (event) => {
		const frame = JSON.parse(event.data as string) as Record<string, unknown>;
		received.push(frame);
		if (typeof frame.id === "number") {
			waiting.get(frame.id)?.();
			waiting.delete(frame.id);
		}
	};
	await withTimeout(
		new Promise<void>((resolve) => {
			socket.onopen = () => resolve();
		}),
		`the raw socket to ${url}`,
	);
	for (const frame of frames) {
		const answered = typeof frame.id === "number" ? new Promise<void>((resolve) => waiting.set(frame.id as number, resolve)) : sleep(100);
		socket.send(JSON.stringify(frame));
		await withTimeout(answered, `the response to frame ${String(frame.id)}`);
	}
	await sleep(quietMs);
	socket.close(1000);
	return received;
}

function sessionUri(): string {
	return `ahp-session:/${crypto.randomUUID()}`;
}

function chatUri(): string {
	return `ahp-chat:/${crypto.randomUUID()}`;
}

function message(text: string): Message {
	return { text, origin: { kind: MessageKind.User } };
}

/** What JSON already did to a state on its way to a peer: every `undefined` key is gone. */
function wire<T>(value: T): T {
	return JSON.parse(JSON.stringify(value)) as T;
}

function typeOf(envelope: ActionEnvelope): string {
	return (envelope.action as unknown as { type: string }).type;
}

function changesOf(envelope: ActionEnvelope): Record<string, unknown> {
	return (envelope.action as unknown as { changes?: Record<string, unknown> }).changes ?? {};
}

/** Collects echoes until one satisfies `done`, so interleaved traffic cannot derail a scenario. */
async function actionsUntil(
	subscription: Subscription,
	done: (envelope: ActionEnvelope) => boolean,
	what: string,
): Promise<ActionEnvelope[]> {
	const collected: ActionEnvelope[] = [];
	for (;;) {
		const envelope = await nextAction(subscription, what);
		collected.push(envelope);
		if (done(envelope)) {
			return collected;
		}
	}
}

/** A `tick` the test drives: the agent parks between two chunks until the test lets it through. */
function pausedTicks() {
	const gates: (() => void)[] = [];
	return {
		tick: (): Promise<void> => new Promise<void>((resolve) => gates.push(resolve)),
		async release(): Promise<void> {
			await until("the agent to park between two chunks", () => gates.length > 0);
			gates.shift()?.();
		},
		releaseAll(): void {
			for (const gate of gates.splice(0)) {
				gate();
			}
		},
	};
}

/** A clock that advances one step per read, so `createdAt` and `modifiedAt` order deterministically. */
function ticking(stepMs = 1000): () => string {
	let at = Date.parse("2026-09-07T12:00:00.000Z") - stepMs;
	return () => {
		at += stepMs;
		return new Date(at).toISOString();
	};
}

/** The live client of a `MultiHostClient` host, or a failure naming the host that has none. */
function hostClient(multi: MultiHostClient, id: string): HostClientHandle {
	const client = multi.client(id);
	assert(client !== undefined, `host "${id}" has no live client`);
	return client;
}

async function until(what: string, predicate: () => boolean, ms = 2000): Promise<void> {
	for (let waited = 0; waited < ms; waited += 10) {
		if (predicate()) {
			return;
		}
		await sleep(10);
	}
	assert(predicate(), `timed out waiting for ${what}`);
}

// ─── Scenarios ───────────────────────────────────────────────────────────────

describe("AHP root channel", () => {
	const otherAgent = { provider: "other", displayName: "Other agent", description: "Second provider", models: [] };

	it("pins the protocol constants the test relies on", () => {
		const actionType = (ahp as unknown as { ActionType: Record<string, string> }).ActionType;
		const errorCodes = (ahp as unknown as { AhpErrorCodes: Record<string, number> }).AhpErrorCodes;
		assertEquals(PROTOCOL_VERSION, "0.9.0");
		assertEquals(actionType.RootAgentsChanged, "root/agentsChanged");
		assertEquals(actionType.RootActiveSessionsChanged, "root/activeSessionsChanged");
		assertEquals(actionType.RootConfigChanged, "root/configChanged");
		assertEquals(errorCodes.UnsupportedProtocolVersion, -32005);
	});

	it("negotiates the handshake", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			const result = await withTimeout(
				client.initialize({
					clientId: crypto.randomUUID(),
					protocolVersions: [PROTOCOL_VERSION, "0.1.0"],
					initialSubscriptions: [AHP_ROOT],
				}),
				"the initialize result",
			);
			assertEquals(result.protocolVersion, "0.9.0");
			assertEquals(typeof result.serverSeq, "number");
			assertEquals(result.serverInfo, { name: "durable-channel-ahp-test", version: "0.1.0" });
			assertEquals(result.snapshots.length, 1);
			assertEquals(result.snapshots[0].resource, AHP_ROOT);
			assertEquals((result.snapshots[0].state as RootState).agents.length, 1);
			assertEquals(result.snapshots[0].fromSeq, result.serverSeq);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("answers ping before initialize and refuses an unknown version", async () => {
		const host = boot();
		const first = await connect(host.url);
		const unversioned = await connect(host.url);
		const rejected = await connect(host.url);
		try {
			await handshake(first.client, crypto.randomUUID());
			await withTimeout(unversioned.client.ping(), "a ping before initialize");
			const error = await rejected.client
				.initialize({ clientId: crypto.randomUUID(), protocolVersions: ["9.9.9"] })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(error, RpcError);
			assertEquals(error.code, -32005);
			assertEquals((error.data as { supportedVersions: string[] }).supportedVersions, [...SUPPORTED_PROTOCOL_VERSIONS]);
			await until("the server to close the socket after -32005", () => rejected.transport.lastClose !== null, 1000);
		} finally {
			await Promise.all([first, unversioned, rejected].map((entry) => entry.client.shutdown()));
			await host.stop();
		}
	});

	it("rejects a malformed initialize", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			const clientId = crypto.randomUUID();
			const wrongChannel = await client
				.request("initialize", { channel: "ahp-session:/x", clientId, protocolVersions: [PROTOCOL_VERSION] } as never)
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(wrongChannel, RpcError);
			assertEquals(wrongChannel.code, -32602);
			const tooEarly = await client.subscribe(AHP_ROOT).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(tooEarly, RpcError);
			assertEquals(tooEarly.code, -32000);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("streams server-origin actions to root subscribers and stops after unsubscribe", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const subscription = client.attachSubscription(AHP_ROOT);
			const agents = [demoAgent, otherAgent];
			const committed = await host.hub.dispatch(AHP_ROOT, "root/agentsChanged", { agents });
			const envelope = await nextAction(subscription, "the agents echo");
			assertEquals(envelope.channel, AHP_ROOT);
			assertEquals(envelope.action as unknown as Record<string, unknown>, { type: "root/agentsChanged", agents });
			assertEquals(envelope.serverSeq, committed.serverSeq);
			assert(!("origin" in envelope) || envelope.origin === undefined, "a server-origin envelope carries no origin");
			await client.unsubscribe(AHP_ROOT);
			await sleep(100);
			await host.hub.dispatch(AHP_ROOT, "root/agentsChanged", { agents: [demoAgent] });
			assertEquals(await quiet(client.events(), 250), "quiet");
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("echoes a client action with its origin and keeps AhpStateMirror equal to the server", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			const clientId = crypto.randomUUID();
			const result = await handshake(client, clientId, [AHP_ROOT]);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(result.snapshots[0] as Snapshot);
			const subscription = client.attachSubscription(AHP_ROOT);
			const handle = client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "dark" } } as never);
			assertEquals(handle.clientSeq, 1);
			const echo = await nextAction(subscription, "the configChanged echo");
			assertEquals(echo.origin, { clientId, clientSeq: 1 });
			assertEquals(echo.rejectionReason, undefined);
			mirror.apply(echo);
			const resubscribed = await client.subscribe(AHP_ROOT);
			assertEquals(mirror.root, resubscribed.result.snapshot?.state as RootState);
			assertEquals(mirror.root, await host.hub.get(AHP_ROOT) as RootState);
			assertEquals((mirror.root.config as { values: Record<string, unknown> }).values, { theme: "dark", locked: "yes" });

			client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "light", locked: "yes" }, replace: true } as never);
			const replaced = await nextAction(subscription, "the replacing configChanged echo");
			assertEquals(replaced.rejectionReason, undefined);
			mirror.apply(replaced);
			assertEquals(mirror.root, await host.hub.get(AHP_ROOT) as RootState);
			assertEquals(mirror.root, ahp.rootReducer(mirror.root, replaced.action as RootAction));
			await resubscribed.subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("answers unacceptable actions with rejected echoes, never error frames", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const before = await host.hub.get(AHP_ROOT);
			const subscription = client.attachSubscription(AHP_ROOT);
			const reasons: string[] = [];
			const sequences: number[] = [];
			for (
				const action of [
					{ type: "root/configChanged", config: { locked: "no" } },
					{ type: "root/bogus", whatever: 1 },
					{ type: "root/agentsChanged", agents: [] },
					{ type: "root/configChanged", config: "nope" },
				]
			) {
				client.dispatch(AHP_ROOT, action as never);
				const echo = await nextAction(subscription, `the rejected echo of ${action.type}`);
				reasons.push(echo.rejectionReason ?? "");
				sequences.push(echo.serverSeq);
			}
			assertEquals(reasons[0], 'config key "locked" is read-only');
			assert(reasons[1].startsWith("UNKNOWN_ACTION"), `expected UNKNOWN_ACTION, got ${reasons[1]}`);
			assert(reasons[2].startsWith("NOT_CLIENT_DISPATCHABLE"), `expected NOT_CLIENT_DISPATCHABLE, got ${reasons[2]}`);
			assert(reasons[3].startsWith("INVALID_PAYLOAD"), `expected INVALID_PAYLOAD, got ${reasons[3]}`);
			assertEquals(sequences, [...sequences].sort((a, b) => a - b));
			assertEquals(new Set(sequences).size, 4);
			assertEquals(await host.hub.get(AHP_ROOT), before);
			assertEquals(await quiet(client.events(), 250), "quiet");

			const frames = await rawExchange(host.url, [
				{
					jsonrpc: "2.0",
					id: 1,
					method: "initialize",
					params: { channel: AHP_ROOT, clientId: crypto.randomUUID(), protocolVersions: [PROTOCOL_VERSION] },
				},
				{ jsonrpc: "2.0", id: 2, method: "subscribe", params: { channel: AHP_ROOT } },
				{
					jsonrpc: "2.0",
					method: "dispatchAction",
					params: { channel: AHP_ROOT, clientSeq: 1, action: { type: "root/bogus", whatever: 1 } },
				},
				{
					jsonrpc: "2.0",
					method: "dispatchAction",
					params: { channel: AHP_ROOT, clientSeq: 2, action: { type: "root/configChanged", config: { theme: "dark" } } },
				},
				{ jsonrpc: "2.0", method: "unsubscribe", params: { channel: AHP_ROOT } },
			]);
			assertEquals(frames.filter((frame) => "error" in frame), []);
			assertEquals(frames.filter((frame) => frame.method === "action").length, 2);
			assertEquals(frames.filter((frame) => "id" in frame).map((frame) => frame.id), [1, 2]);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("ignores a duplicate clientSeq on the same link", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const before = host.hub.serverSeq;
			const subscription = client.attachSubscription(AHP_ROOT);
			client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "dark" } } as never, 1);
			const echo = await nextAction(subscription, "the only echo");
			assertEquals(echo.origin?.clientSeq, 1);
			client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "solar" } } as never, 1);
			assertEquals(await quiet(client.events(), 250), "quiet");
			assertEquals(host.hub.serverSeq, before + 1);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("replays what a reconnecting client missed and accepts a clientSeq that restarted at 1", async () => {
		const host = boot();
		const clientId = crypto.randomUUID();
		const first = await connect(host.url);
		try {
			await handshake(first.client, clientId, [AHP_ROOT]);
			const subscription = first.client.attachSubscription(AHP_ROOT);
			first.client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "dark" } } as never);
			const echo = await nextAction(subscription, "the first echo");
			const lastSeen = echo.serverSeq;
			await first.client.shutdown();
			await host.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: 1 });
			await host.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: 2 });

			const second = await connect(host.url);
			try {
				const resumed = await withTimeout(
					second.client.reconnect({ clientId, lastSeenServerSeq: lastSeen, subscriptions: [AHP_ROOT, "ahp-session:/none"] }),
					"the reconnect result",
				);
				assertEquals(resumed.type, "replay");
				assert(resumed.type === "replay");
				assertEquals(resumed.missing, ["ahp-session:/none"]);
				assertEquals(resumed.actions.map((envelope) => envelope.serverSeq), [lastSeen + 1, lastSeen + 2]);
				assertEquals(resumed.actions.map((envelope) => (envelope.action as unknown as { activeSessions: number }).activeSessions), [1, 2]);

				const resubscribed = second.client.attachSubscription(AHP_ROOT);
				const handle = second.client.dispatch(AHP_ROOT, { type: "root/configChanged", config: { theme: "dusk" } } as never);
				assertEquals(handle.clientSeq, 1);
				const again = await nextAction(resubscribed, "the echo after the reconnect");
				assertEquals(again.origin, { clientId, clientSeq: 1 });
				assertEquals(again.rejectionReason, undefined);
				assert(again.serverSeq > lastSeen + 2, "the resent action must consume a fresh serverSeq");
			} finally {
				await second.client.shutdown();
			}
		} finally {
			await first.client.shutdown();
			await host.stop();
		}
	});

	it("falls back to snapshots when the replay ring cannot cover the gap", async () => {
		const host = boot({ replayLimit: 2 });
		const clientId = crypto.randomUUID();
		const first = await connect(host.url);
		try {
			const lastSeen = (await handshake(first.client, clientId, [AHP_ROOT])).serverSeq;
			await first.client.shutdown();
			for (const activeSessions of [1, 2, 3, 4]) {
				await host.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions });
			}
			const second = await connect(host.url);
			try {
				const resumed = await withTimeout(
					second.client.reconnect({ clientId, lastSeenServerSeq: lastSeen, subscriptions: [AHP_ROOT] }),
					"the reconnect result",
				);
				assertEquals(resumed.type, "snapshot");
				assert(resumed.type === "snapshot");
				assertEquals((resumed as unknown as { missing: string[] }).missing, []);
				assertEquals(resumed.snapshots.length, 1);
				assertEquals(resumed.snapshots[0].resource, AHP_ROOT);
				assertEquals((resumed.snapshots[0].state as RootState).activeSessions, 4);
				assertEquals(resumed.snapshots[0].fromSeq, host.hub.serverSeq);
			} finally {
				await second.client.shutdown();
			}
		} finally {
			await first.client.shutdown();
			await host.stop();
		}
	});

	it("replaces the link when a second socket claims the same clientId", async () => {
		const host = boot();
		const clientId = crypto.randomUUID();
		const first = await connect(host.url);
		const second = await connect(host.url);
		try {
			await handshake(first.client, clientId, [AHP_ROOT]);
			first.client.attachSubscription(AHP_ROOT);
			await handshake(second.client, clientId, [AHP_ROOT]);
			const live = second.client.attachSubscription(AHP_ROOT);
			await host.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: 1 });
			assertEquals((await nextAction(live, "the echo on the surviving link")).serverSeq, host.hub.serverSeq);
			assertEquals(await quiet(first.client.events(), 250), "quiet");

			await first.client.shutdown();
			await sleep(100);
			await host.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: 2 });
			const survived = await nextAction(live, "the echo after the stale socket closed");
			assertEquals((survived.action as unknown as { activeSessions: number }).activeSessions, 2);
		} finally {
			await Promise.all([first, second].map((entry) => entry.client.shutdown()));
			await host.stop();
		}
	});

	it("survives a hub restart on the same storage", async () => {
		const storage = new MemoryStorage();
		const clientId = crypto.randomUUID();
		const first = boot({ storage });
		let lastSeen = 0;
		try {
			const { client } = await connect(first.url);
			try {
				await handshake(client, clientId, [AHP_ROOT]);
				const committed = await first.hub.dispatch(AHP_ROOT, "root/activeSessionsChanged", { activeSessions: 7 });
				lastSeen = committed.serverSeq;
			} finally {
				await client.shutdown();
			}
		} finally {
			await first.stop();
		}
		const second = boot({ storage });
		try {
			await second.hub.ready();
			assertEquals(second.hub.serverSeq, lastSeen);
			assertEquals((await second.hub.get(AHP_ROOT) as RootState).activeSessions, 7);
			const caughtUp = await connect(second.url);
			try {
				const resumed = await withTimeout(
					caughtUp.client.reconnect({ clientId, lastSeenServerSeq: lastSeen, subscriptions: [AHP_ROOT] }),
					"the caught-up reconnect",
				);
				assertEquals(resumed.type, "replay");
				assertEquals(resumed.type === "replay" ? resumed.actions : undefined, []);
			} finally {
				await caughtUp.client.shutdown();
			}
			const behind = await connect(second.url);
			try {
				const resumed = await withTimeout(
					behind.client.reconnect({ clientId, lastSeenServerSeq: lastSeen - 1, subscriptions: [AHP_ROOT] }),
					"the lagging reconnect",
				);
				assertEquals(resumed.type, "snapshot");
				assertEquals(resumed.type === "snapshot" ? resumed.snapshots.length : 0, 1);
			} finally {
				await behind.client.shutdown();
			}
		} finally {
			await second.stop();
		}
	});

	it("lets MultiHostClient reconnect through a dropped link", async () => {
		const host = boot();
		const clientId = crypto.randomUUID();
		const { multi } = await MultiHostClient.single({
			id: "h",
			label: "h",
			clientId,
			initialSubscriptions: [AHP_ROOT],
			transportFactory: () => WebSocketTransport.connect(host.url),
			reconnectPolicy: immediateForeverPolicy(),
			clientConfig: { requestTimeoutMs: 5000 },
		});
		try {
			await until("the host to connect", () => multi.host("h")?.state.status === "connected");
			assertEquals(multi.host("h")?.agents.length, 1);
			assertEquals(host.calls.listSessions, 1);

			await host.hub.dispatch(AHP_ROOT, "root/agentsChanged", { agents: [demoAgent, otherAgent] });
			await until("the mirrored agents to grow to two", () => multi.host("h")?.agents.length === 2);

			host.drop(clientId);
			await until(
				"a second generation after the drop",
				() => (multi.host("h")?.generation ?? 0) >= 2 && multi.host("h")?.state.status === "connected",
				5000,
			);
			assertEquals(host.calls.reconnect, 1);

			await host.hub.dispatch(AHP_ROOT, "root/agentsChanged", { agents: [demoAgent] });
			await until("the resubscribed link to see the third change", () => multi.host("h")?.agents.length === 1);
		} finally {
			await multi.shutdown();
			await host.stop();
		}
	});

	it("lists no session on a fresh host", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID());
			assertEquals(await withTimeout(client.request("listSessions", { channel: AHP_ROOT }), "the session list"), { items: [] });
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});
});

describe("AHP session channel", () => {
	it("pins the session constants and the client-dispatchable table", () => {
		const runtime = ahp as unknown as {
			SessionStatus: Record<string, unknown>;
			SessionLifecycle: Record<string, unknown>;
			IS_CLIENT_DISPATCHABLE: Record<string, boolean>;
		};
		for (const [name, value] of Object.entries(STATUS)) {
			assertEquals(runtime.SessionStatus[name], value, `SessionStatus.${name}`);
		}
		for (const [name, value] of Object.entries(LIFECYCLE)) {
			assertEquals(runtime.SessionLifecycle[name], value, `SessionLifecycle.${name}`);
		}
		for (const [name, definition] of Object.entries(session.actions)) {
			assertEquals(definition.client, runtime.IS_CLIENT_DISPATCHABLE[name] === true, `client flag of ${name}`);
		}
	});

	it("creates a session and announces it on root", async () => {
		const at = "2026-09-07T12:00:00.000Z";
		const host = boot({ now: () => at });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const rootFeed = client.attachSubscription(AHP_ROOT);
			const uri = sessionUri();
			assertEquals(await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), "createSession"), null);
			const counted = await nextAction(rootFeed, "the activeSessions echo");
			assertEquals(counted.action as unknown as Record<string, unknown>, { type: "root/activeSessionsChanged", activeSessions: 1 });
			const added = await nextEvent(rootFeed, "sessionAdded", "the sessionAdded notification");
			assertEquals(added.params.summary, {
				resource: uri,
				provider: "demo",
				title: "New Session",
				status: STATUS.Idle,
				createdAt: at,
				modifiedAt: at,
			});

			const { result, subscription } = await client.subscribe(uri);
			const snapshot = result.snapshot as Snapshot;
			const state = snapshot.state as SessionState;
			assertEquals(snapshot.resource, uri);
			assertEquals(snapshot.fromSeq, host.hub.serverSeq);
			assertEquals(state.lifecycle, LIFECYCLE.Ready);
			assertEquals(state.chats, []);
			assertEquals(state.activeClients, []);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(snapshot);
			assertEquals(mirror.getSession(uri), state);
			assertEquals(await host.hub.get(uri), state);
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("refuses bad creations", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const uri = sessionUri();
			await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), "the first createSession");
			const twice = await client.request("createSession", { channel: uri, provider: "demo" }).then(
				() => undefined,
				(caught: unknown) => caught,
			);
			assertInstanceOf(twice, RpcError);
			assertEquals(twice.code, -32003);
			const unknownProvider = await client
				.request("createSession", { channel: sessionUri(), provider: "nope" })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(unknownProvider, RpcError);
			assertEquals(unknownProvider.code, -32002);
			const absent = await client.subscribe(sessionUri()).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(absent, RpcError);
			assertEquals(absent.code, -32001);
			const disposed = await client
				.request("disposeSession", { channel: sessionUri() })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(disposed, RpcError);
			assertEquals(disposed.code, -32001);
			assertEquals((await host.hub.get(AHP_ROOT) as RootState).activeSessions, 1);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("echoes client session actions and matches the canonical reducer", async () => {
		const host = boot({ now: ticking() });
		const { client } = await connect(host.url);
		try {
			const clientId = crypto.randomUUID();
			await handshake(client, clientId, [AHP_ROOT]);
			const uri = sessionUri();
			await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), "createSession");
			const { result, subscription } = await client.subscribe(uri);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(result.snapshot as Snapshot);
			for (
				const action of [
					{ type: "session/titleChanged", title: "Renamed" },
					{ type: "session/isReadChanged", isRead: true },
					{ type: "session/isArchivedChanged", isArchived: true },
					{ type: "session/isArchivedChanged", isArchived: false },
					{ type: "session/workingDirectorySet", directory: "file:///tmp" },
				]
			) {
				const before = await host.hub.get(uri) as SessionState;
				client.dispatch(uri, action as never);
				const echo = await nextAction(subscription, `the echo of ${action.type}`);
				assertEquals(echo.rejectionReason, undefined);
				assertEquals(echo.origin?.clientId, clientId);
				mirror.apply(echo);
				assertEquals(await host.hub.get(uri), ahp.sessionReducer(before, echo.action as SessionAction));
				const fresh = await client.subscribe(uri);
				assertEquals(mirror.getSession(uri), fresh.result.snapshot?.state as SessionState);
				await fresh.subscription.close();
			}
			const final = await host.hub.get(uri) as SessionState;
			assertEquals(final.title, "Renamed");
			assertEquals(final.status, STATUS.Idle | STATUS.IsRead);
			assertEquals(final.workingDirectories, ["file:///tmp"]);
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("rejects a blank title and leaves both sides untouched", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			const clientId = crypto.randomUUID();
			await handshake(client, clientId, [AHP_ROOT]);
			const uri = sessionUri();
			await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), "createSession");
			const { result, subscription } = await client.subscribe(uri);
			const before = result.snapshot?.state as SessionState;
			client.dispatch(uri, { type: "session/titleChanged", title: "   " } as never);
			const echo = await nextAction(subscription, "the rejected echo");
			assertEquals(echo.rejectionReason, "title must not be blank");
			assertEquals(echo.origin, { clientId, clientSeq: 1 });
			assertEquals(await host.hub.get(uri), before);
			assertEquals(await quiet(client.events(), 250), "quiet");
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("propagates summary changes to the root channel", async () => {
		const host = boot({ now: ticking() });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const rootFeed = client.attachSubscription(AHP_ROOT);
			const uri = sessionUri();
			await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), "createSession");
			client.dispatch(uri, { type: "session/titleChanged", title: "Renamed" } as never);
			const renamed = await nextEvent(rootFeed, "sessionSummaryChanged", "the summary change after the rename");
			assertEquals(renamed.params.session, uri);
			assertEquals(Object.keys(renamed.params.changes).sort(), ["modifiedAt", "title"]);
			assertEquals(renamed.params.changes.title, "Renamed");
			const renamedAt = renamed.params.changes.modifiedAt as string;

			client.dispatch(uri, { type: "session/isArchivedChanged", isArchived: true } as never);
			const archived = await nextEvent(rootFeed, "sessionSummaryChanged", "the summary change after the archive");
			assertEquals(Object.keys(archived.params.changes).sort(), ["modifiedAt", "status"]);
			assertEquals(archived.params.changes.status, STATUS.Idle | STATUS.IsArchived);
			assert((archived.params.changes.modifiedAt as string) > renamedAt, "each change carries a fresher modifiedAt");

			const listed = await withTimeout(client.request("listSessions", { channel: AHP_ROOT }), "the session list");
			assertEquals(listed.items.map((item) => [item.title, item.status, item.modifiedAt]), [[
				"Renamed",
				STATUS.Idle | STATUS.IsArchived,
				archived.params.changes.modifiedAt,
			]]);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("lists sessions most-recently-modified first with paging", async () => {
		const host = boot({ now: ticking() });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const rootFeed = client.attachSubscription(AHP_ROOT);
			const [first, second, third] = [sessionUri(), sessionUri(), sessionUri()];
			for (const uri of [first, second, third]) {
				await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), `createSession for ${uri}`);
			}
			client.dispatch(first, { type: "session/titleChanged", title: "Freshest" } as never);
			await nextEvent(rootFeed, "sessionSummaryChanged", "the rename to land in the catalogue");

			const page = await withTimeout(client.request("listSessions", { channel: AHP_ROOT, limit: 2 }), "the first page");
			assertEquals(page.items.map((item) => item.resource), [first, third]);
			assertEquals(page.nextCursor, third);
			const rest = await withTimeout(client.request("listSessions", { channel: AHP_ROOT, cursor: page.nextCursor }), "the second page");
			assertEquals(rest.items.map((item) => item.resource), [second]);
			assertEquals(rest.nextCursor, undefined);
			const bad = await client
				.request("listSessions", { channel: AHP_ROOT, cursor: "garbage" })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(bad, RpcError);
			assertEquals(bad.code, -32602);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("disposes a session and reports it missing on reconnect", async () => {
		const host = boot({ now: ticking() });
		const clientId = crypto.randomUUID();
		const first = await connect(host.url);
		try {
			await handshake(first.client, clientId, [AHP_ROOT]);
			const rootFeed = first.client.attachSubscription(AHP_ROOT);
			const uri = sessionUri();
			await withTimeout(first.client.request("createSession", { channel: uri, provider: "demo" }), "createSession");
			await nextAction(rootFeed, "the activeSessions echo of the creation");
			await withTimeout(first.client.request("disposeSession", { channel: uri }), "disposeSession");
			const counted = await nextAction(rootFeed, "the activeSessions echo of the disposal");
			assertEquals(counted.action as unknown as Record<string, unknown>, { type: "root/activeSessionsChanged", activeSessions: 0 });
			const removed = await nextEvent(rootFeed, "sessionRemoved", "the sessionRemoved notification");
			assertEquals(removed.params.session, uri);
			const gone = await first.client.subscribe(uri).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(gone, RpcError);
			assertEquals(gone.code, -32001);
			assertEquals(await withTimeout(first.client.request("listSessions", { channel: AHP_ROOT }), "the empty list"), { items: [] });

			const second = await connect(host.url);
			try {
				const resumed = await withTimeout(
					second.client.reconnect({ clientId, lastSeenServerSeq: host.hub.serverSeq, subscriptions: [AHP_ROOT, uri] }),
					"the reconnect result",
				);
				assertEquals(resumed.type, "replay");
				assert(resumed.type === "replay");
				assertEquals(resumed.missing, [uri]);
			} finally {
				await second.client.shutdown();
			}
		} finally {
			await first.client.shutdown();
			await host.stop();
		}
	});

	it("keeps MultiHostClient's session cache in sync", async () => {
		const host = boot({ now: ticking() });
		const clientId = crypto.randomUUID();
		const { multi } = await MultiHostClient.single({
			id: "h",
			label: "h",
			clientId,
			initialSubscriptions: [AHP_ROOT],
			transportFactory: () => WebSocketTransport.connect(host.url),
			reconnectPolicy: immediateForeverPolicy(),
			clientConfig: { requestTimeoutMs: 5000 },
		});
		try {
			await until("the host to connect", () => multi.host("h")?.state.status === "connected");
			const first = sessionUri();
			await withTimeout(hostClient(multi, "h").request("createSession", { channel: first, provider: "demo" }), "the first createSession");
			await until("the aggregated cache to hold the session", () => multi.aggregatedSessions().length === 1);
			assertEquals(multi.aggregatedSessions().map((entry) => [entry.hostId, entry.summary.resource, entry.summary.title]), [[
				"h",
				first,
				"New Session",
			]]);
			await withTimeout(hostClient(multi, "h").request("disposeSession", { channel: first }), "disposeSession");
			await until("the aggregated cache to empty", () => multi.aggregatedSessions().length === 0);

			const second = sessionUri();
			await withTimeout(hostClient(multi, "h").request("createSession", { channel: second, provider: "demo" }), "the second createSession");
			await until("the aggregated cache to hold the second session", () => multi.aggregatedSessions().length === 1);
			host.drop(clientId);
			await until(
				"a second generation after the drop",
				() => (multi.host("h")?.generation ?? 0) >= 2 && multi.host("h")?.state.status === "connected",
				5000,
			);
			assertEquals(multi.aggregatedSessions().map((entry) => entry.summary.resource), [second]);
			assertEquals(host.calls.listSessions, 2);
		} finally {
			await multi.shutdown();
			await host.stop();
		}
	});

	it("runs the whole lifecycle on Deno KV storage", async () => {
		using storage = await DenoKvStorage.open(":memory:");
		const now = ticking();
		const kept = sessionUri();
		const dropped = sessionUri();
		const first = boot({ storage, now });
		let lastSeen = 0;
		try {
			const { client } = await connect(first.url);
			try {
				await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
				const rootFeed = client.attachSubscription(AHP_ROOT);
				for (const uri of [kept, dropped]) {
					await withTimeout(client.request("createSession", { channel: uri, provider: "demo" }), `createSession for ${uri}`);
				}
				client.dispatch(kept, { type: "session/titleChanged", title: "Persisted" } as never);
				const renamed = await nextEvent(rootFeed, "sessionSummaryChanged", "the rename to reach the catalogue");
				assertEquals(renamed.params.changes.title, "Persisted");
				await withTimeout(client.request("disposeSession", { channel: dropped }), "disposeSession");
				lastSeen = first.hub.serverSeq;
			} finally {
				await client.shutdown();
			}
		} finally {
			await first.stop();
		}

		const second = boot({ storage, now });
		try {
			await second.hub.ready();
			assertEquals(second.hub.serverSeq, lastSeen);
			assertEquals((await second.hub.get(AHP_ROOT) as RootState).activeSessions, 1);
			const { client } = await connect(second.url);
			try {
				await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
				const listed = await withTimeout(client.request("listSessions", { channel: AHP_ROOT }), "the session list after the restart");
				assertEquals(listed.items.map((item) => [item.resource, item.title]), [[kept, "Persisted"]]);
				await withTimeout(
					client.request("createSession", { channel: sessionUri(), provider: "demo" }),
					"a createSession after the restart",
				);
				assert(second.hub.serverSeq > lastSeen, "the sequence keeps climbing across a restart");
				assertEquals((await withTimeout(client.request("listSessions", { channel: AHP_ROOT }), "the grown list")).items.length, 2);
			} finally {
				await client.shutdown();
			}
		} finally {
			await second.stop();
		}
	});
});

describe("AHP chat channel", () => {
	/** Every scenario starts from a ready session the client is subscribed to. */
	async function openSession(client: AhpClient): Promise<{ session: string; feed: Subscription; snapshot: Snapshot }> {
		const session = sessionUri();
		await withTimeout(client.request("createSession", { channel: session, provider: "demo" }), "createSession");
		const { result, subscription } = await withTimeout(client.subscribe(session), "the session subscription");
		return { session, feed: subscription, snapshot: result.snapshot as Snapshot };
	}

	async function createChat(client: AhpClient, session: string, chat: string): Promise<void> {
		assertEquals(await withTimeout(client.request("createChat", { channel: session, chat }), `createChat for ${chat}`), null);
	}

	/** Starts a turn and returns every echo up to and including the agent's `chat/turnComplete`. */
	function runTurn(client: AhpClient, chat: string, subscription: Subscription, turnId: string, text = "hi"): Promise<ActionEnvelope[]> {
		client.dispatch(chat, { type: "chat/turnStarted", turnId, startedAt: new Date().toISOString(), message: message(text) } as never);
		return actionsUntil(subscription, (envelope) => typeOf(envelope) === "chat/turnComplete", `turn ${turnId} to complete`);
	}

	it("pins the chat constants and the client-dispatchable table", () => {
		const runtime = ahp as unknown as { TurnState: Record<string, unknown>; IS_CLIENT_DISPATCHABLE: Record<string, boolean> };
		for (const [name, value] of Object.entries(TURN)) {
			assertEquals(runtime.TurnState[name], value, `TurnState.${name}`);
		}
		for (const [name, definition] of Object.entries(chat.actions)) {
			assertEquals(definition.client, runtime.IS_CLIENT_DISPATCHABLE[name] === true, `client flag of ${name}`);
		}
	});

	it("creates a chat inside a ready session", async () => {
		const at = "2026-09-07T12:00:00.000Z";
		const host = boot({ now: () => at });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session, feed, snapshot } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);

			const added = await nextAction(feed, "the chatAdded echo");
			assertEquals(added.action as unknown as Record<string, unknown>, {
				type: "session/chatAdded",
				summary: { resource: uri, title: "New Chat", status: STATUS.Idle, modifiedAt: at },
			});
			const defaulted = await nextAction(feed, "the defaultChatChanged echo");
			assertEquals(defaulted.action as unknown as Record<string, unknown>, { type: "session/defaultChatChanged", defaultChat: uri });

			const { result, subscription } = await client.subscribe(uri);
			assertEquals(result.snapshot?.state, { resource: uri, title: "New Chat", status: STATUS.Idle, modifiedAt: at, turns: [] });
			assertEquals(result.snapshot?.fromSeq, host.hub.serverSeq);

			const mirror = new AhpStateMirror();
			mirror.applySnapshot(snapshot);
			mirror.apply(added);
			mirror.apply(defaulted);
			assertEquals(mirror.getSession(session)?.chats.length, 1);
			assertEquals(mirror.getSession(session), wire(await host.hub.get(session)) as SessionState);
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("refuses chats on unknown, duplicate or unready targets", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);

			const unknownSession = await client
				.request("createChat", { channel: sessionUri(), chat: chatUri() })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(unknownSession, RpcError);
			assertEquals(unknownSession.code, -32001);
			const twice = await client.request("createChat", { channel: session, chat: uri }).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(twice, RpcError);
			assertEquals(twice.code, -32010);
			const absent = await client.subscribe(chatUri()).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(absent, RpcError);
			assertEquals(absent.code, -32008);
			const gone = await client.request("disposeChat", { channel: chatUri() }).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(gone, RpcError);
			assertEquals(gone.code, -32008);
			const badCursor = await client
				.request("fetchTurns", { channel: uri, cursor: "garbage" })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(badCursor, RpcError);
			assertEquals(badCursor.code, -32602);
			assertEquals(await withTimeout(client.request("fetchTurns", { channel: uri }), "a cursorless fetchTurns"), {});

			// `-32011` never reaches the wire, because `createSession` dispatches `session/ready` itself:
			// a session a client can name is always ready. The refusal is still there underneath.
			const creating = sessionUri();
			await host.hub.create(creating, { ...initialSessionState });
			const conflict = await host.hub
				.exec(creating, "createChat", { chat: chatUri() })
				.then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(conflict, AhpError);
			assertEquals(conflict.code, -32011);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("streams a full turn and matches chatReducer step by step", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const { result, subscription } = await client.subscribe(uri);
			const startedAt = new Date().toISOString();
			client.dispatch(uri, { type: "chat/turnStarted", turnId: "t1", startedAt, message: message("hi") } as never);
			const echoes = await actionsUntil(subscription, (envelope) => typeOf(envelope) === "chat/turnComplete", "the turn to complete");
			assertEquals(echoes.map(typeOf), [
				"chat/turnStarted",
				"chat/activityChanged",
				"chat/responsePart",
				"chat/delta",
				"chat/delta",
				"chat/delta",
				"chat/usage",
				"chat/activityChanged",
				"chat/turnComplete",
			]);

			let folded = result.snapshot?.state as ChatState;
			for (const echo of echoes) {
				assertEquals(echo.rejectionReason, undefined);
				folded = ahp.chatReducer(folded, echo.action as ChatAction);
			}
			const fresh = await client.subscribe(uri);
			assertEquals(wire(folded), fresh.result.snapshot?.state);
			const state = await host.hub.get(uri) as ChatState;
			assertEquals(wire(state), fresh.result.snapshot?.state);
			assertEquals(state.turns.length, 1);
			assertEquals(state.turns[0].responseParts as unknown, [{ kind: "markdown", id: "p1", content: "Hello, world" }]);
			assertEquals(state.turns[0].state, TURN.Complete);
			assertEquals(state.turns[0].duration, TURN_DURATION_MS);
			assertEquals(state.turns[0].usage?.outputTokens, 3);
			assertEquals(state.status, STATUS.Idle);
			assertEquals(state.activeTurn, undefined);
			assertEquals(state.modifiedAt, stampAfter(startedAt, TURN_DURATION_MS));
			await fresh.subscription.close();
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("mirrors turn progress on the session and the root", async () => {
		const host = boot({ now: ticking() });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const rootFeed = client.attachSubscription(AHP_ROOT);
			const { session, feed, snapshot } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(snapshot);
			const { subscription } = await client.subscribe(uri);

			client.dispatch(
				uri,
				{ type: "chat/turnStarted", turnId: "t1", startedAt: new Date().toISOString(), message: message("hi") } as never,
			);
			const echoes = await actionsUntil(
				feed,
				(envelope) => typeOf(envelope) === "session/chatUpdated" && changesOf(envelope).status === STATUS.Idle,
				"the session to hear the turn end",
			);
			const statuses: number[] = [];
			for (const echo of echoes) {
				assertEquals(echo.rejectionReason, undefined);
				assertEquals("resource" in changesOf(echo), false);
				mirror.apply(echo);
				const status = changesOf(echo).status;
				if (typeof status === "number" && typeOf(echo) === "session/chatUpdated") {
					statuses.push(status);
				}
			}
			assertEquals(statuses, [STATUS.InProgress, STATUS.Idle]);

			const seen: number[] = [];
			for (;;) {
				const changed = await nextEvent(rootFeed, "sessionSummaryChanged", "a session summary change");
				assertEquals(changed.params.session, session);
				assertEquals("resource" in changed.params.changes, false);
				seen.push(changed.params.changes.status as number);
				if (seen.includes(STATUS.InProgress) && seen.at(-1) === STATUS.Idle) {
					break;
				}
			}
			assertEquals(mirror.getSession(session), wire(await host.hub.get(session)) as SessionState);
			const refreshed = await client.subscribe(session);
			assertEquals(mirror.getSession(session), refreshed.result.snapshot?.state as SessionState);
			await refreshed.subscription.close();
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("rejects a second turn while one is active", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const { subscription } = await client.subscribe(uri);
			const startedAt = new Date().toISOString();
			client.dispatch(uri, { type: "chat/turnStarted", turnId: "t1", startedAt, message: message("hi") } as never);
			client.dispatch(uri, { type: "chat/turnStarted", turnId: "t2", startedAt, message: message("again") } as never);
			const echoes = await actionsUntil(subscription, (envelope) => typeOf(envelope) === "chat/turnComplete", "the first turn to complete");

			const refused = echoes.filter((envelope) => envelope.rejectionReason !== undefined);
			assertEquals(refused.map(typeOf), ["chat/turnStarted"]);
			assert(refused[0].rejectionReason?.startsWith("TURN_IN_PROGRESS"), `unexpected reason ${refused[0].rejectionReason}`);
			const state = await host.hub.get(uri) as ChatState;
			assertEquals(state.turns.map((turn) => turn.id), ["t1"]);
			assertEquals(state.status, STATUS.Idle);
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("cancels an active turn", async () => {
		const clock = pausedTicks();
		const host = boot({ tick: clock.tick });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const { result, subscription } = await client.subscribe(uri);
			client.dispatch(
				uri,
				{ type: "chat/turnStarted", turnId: "t1", startedAt: new Date().toISOString(), message: message("hi") } as never,
			);
			const echoes = await actionsUntil(subscription, (envelope) => typeOf(envelope) === "chat/responsePart", "the empty markdown part");
			await clock.release();
			echoes.push(await nextAction(subscription, "the first delta"));
			assertEquals(typeOf(echoes[echoes.length - 1]), "chat/delta");

			client.dispatch(uri, { type: "chat/turnCancelled", turnId: "t1", duration: 7 } as never);
			echoes.push(await nextAction(subscription, "the cancellation echo"));
			assertEquals(typeOf(echoes[echoes.length - 1]), "chat/turnCancelled");
			clock.releaseAll();
			assertEquals(await quiet(client.events(), 250), "quiet");

			const state = await host.hub.get(uri) as ChatState;
			assertEquals(state.turns.length, 1);
			assertEquals(state.turns[0].state, TURN.Cancelled);
			assertEquals(state.turns[0].duration, 7);
			assertEquals(state.turns[0].responseParts as unknown, [{ kind: "markdown", id: "p1", content: "Hello" }]);
			assertEquals(state.status, STATUS.Idle);
			assertEquals(state.activeTurn, undefined);
			let folded = result.snapshot?.state as ChatState;
			for (const echo of echoes) {
				folded = ahp.chatReducer(folded, echo.action as ChatAction);
			}
			assertEquals(wire(folded), wire(state));
			await subscription.close();
		} finally {
			clock.releaseAll();
			await client.shutdown();
			await host.stop();
		}
	});

	it("persists the draft a client is composing", async () => {
		const host = boot();
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const { subscription } = await client.subscribe(uri);

			client.dispatch(uri, { type: "chat/draftChanged", draft: message("wip") } as never);
			const saved = await nextAction(subscription, "the draft echo");
			assertEquals(saved.rejectionReason, undefined);
			const withDraft = await client.subscribe(uri);
			assertEquals((withDraft.result.snapshot?.state as ChatState).draft, message("wip"));
			await withDraft.subscription.close();

			client.dispatch(uri, { type: "chat/draftChanged" } as never);
			const cleared = await nextAction(subscription, "the cleared draft echo");
			assertEquals(cleared.rejectionReason, undefined);
			const withoutDraft = await client.subscribe(uri);
			assertEquals("draft" in (withoutDraft.result.snapshot?.state as object), false);
			await withoutDraft.subscription.close();
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("disposes chats with their session", async () => {
		const host = boot({ now: ticking() });
		const clientId = crypto.randomUUID();
		const { client } = await connect(host.url);
		try {
			await handshake(client, clientId, [AHP_ROOT]);
			const { session, feed, snapshot } = await openSession(client);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(snapshot);
			const first = chatUri();
			await createChat(client, session, first);
			for (
				const echo of await actionsUntil(feed, (envelope) => typeOf(envelope) === "session/defaultChatChanged", "the chat to be announced")
			) {
				mirror.apply(echo);
			}
			assertEquals(mirror.getSession(session)?.defaultChat, first);

			await withTimeout(client.request("disposeChat", { channel: first }), "disposeChat");
			for (const echo of await actionsUntil(feed, (envelope) => typeOf(envelope) === "session/chatRemoved", "the chatRemoved echo")) {
				mirror.apply(echo);
			}
			assertEquals(mirror.getSession(session)?.chats, []);
			assertEquals(mirror.getSession(session)?.defaultChat, undefined);
			const gone = await client.subscribe(first).then(() => undefined, (caught: unknown) => caught);
			assertInstanceOf(gone, RpcError);
			assertEquals(gone.code, -32008);

			const second = chatUri();
			const third = chatUri();
			await createChat(client, session, second);
			await createChat(client, session, third);
			await withTimeout(client.request("disposeSession", { channel: session }), "disposeSession");
			assertEquals(await host.hub.has(second), false);
			assertEquals(await host.hub.has(third), false);
			const resumed = await withTimeout(
				client.reconnect({ clientId, lastSeenServerSeq: host.hub.serverSeq, subscriptions: [AHP_ROOT, session, second, third] }),
				"the reconnect after the disposal",
			);
			assert(resumed.type === "replay");
			assertEquals(resumed.missing, [session, second, third]);
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});

	it("replays a turn to a reconnecting client, and snapshots it when the ring is too small", async () => {
		for (const replayLimit of [undefined, 3]) {
			const host = boot(replayLimit === undefined ? {} : { replayLimit });
			const clientId = crypto.randomUUID();
			const away = await connect(host.url);
			try {
				await handshake(away.client, clientId, [AHP_ROOT]);
				const session = sessionUri();
				await withTimeout(away.client.request("createSession", { channel: session, provider: "demo" }), "createSession");
				const uri = chatUri();
				await createChat(away.client, session, uri);
				const parked = await away.client.subscribe(uri);
				const baseline = parked.result.snapshot?.state as ChatState;
				const lastSeen = parked.result.snapshot?.fromSeq as number;
				await away.client.shutdown();

				const busy = await connect(host.url);
				try {
					await handshake(busy.client, crypto.randomUUID(), []);
					const { subscription } = await busy.client.subscribe(uri);
					await runTurn(busy.client, uri, subscription, "t1");
					await subscription.close();
				} finally {
					await busy.client.shutdown();
				}

				const back = await connect(host.url);
				try {
					const resumed = await withTimeout(
						back.client.reconnect({ clientId, lastSeenServerSeq: lastSeen, subscriptions: [uri] }),
						"the reconnect of the parked client",
					);
					const fresh = await back.client.subscribe(uri);
					const current = fresh.result.snapshot?.state as ChatState;
					if (replayLimit === undefined) {
						assert(resumed.type === "replay", `expected a replay, got ${resumed.type}`);
						let folded = baseline;
						for (const envelope of resumed.actions) {
							assertEquals(envelope.channel, uri);
							folded = ahp.chatReducer(folded, envelope.action as ChatAction);
						}
						assertEquals(wire(folded), current);
					} else {
						assert(resumed.type === "snapshot", `expected a snapshot, got ${resumed.type}`);
						assertEquals(resumed.snapshots.map((snapshot) => snapshot.resource), [uri]);
						assertEquals(resumed.snapshots[0].state, current);
					}
					assertEquals((current.turns[0].responseParts[0] as { content: string }).content, "Hello, world");
					await fresh.subscription.close();
				} finally {
					await back.client.shutdown();
				}
			} finally {
				await host.stop();
			}
		}
	});

	it("keeps AhpStateMirror unaware of chats but consistent for sessions", async () => {
		const host = boot({ now: ticking() });
		const { client } = await connect(host.url);
		try {
			await handshake(client, crypto.randomUUID(), [AHP_ROOT]);
			const { session, snapshot } = await openSession(client);
			const uri = chatUri();
			await createChat(client, session, uri);
			const { result, subscription } = await client.subscribe(uri);
			const mirror = new AhpStateMirror();
			mirror.applySnapshot(snapshot);
			// A chat snapshot goes nowhere: the mirror has no chat branch at all.
			mirror.applySnapshot(result.snapshot as Snapshot);
			assertEquals(mirror.sessions.size, 1);

			const echoes = await runTurn(client, uri, subscription, "t1");
			for (const echo of echoes) {
				mirror.apply(echo);
			}
			assertEquals(mirror.sessions.size, 1);
			assertEquals(mirror.getSession(session), snapshot.state as SessionState);
			assertNotEquals(wire(await host.hub.get(session)) as SessionState, mirror.getSession(session));

			let folded = result.snapshot?.state as ChatState;
			for (const echo of echoes) {
				folded = ahp.chatReducer(folded, echo.action as ChatAction);
			}
			assertEquals(wire(folded), wire(await host.hub.get(uri)));
			await subscription.close();
		} finally {
			await client.shutdown();
			await host.stop();
		}
	});
});
