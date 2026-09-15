/**
 * The route map: URI templates to channel definitions.
 *
 * A template is split on `/` into segments. A literal segment matches itself; a `:name` segment
 * matches exactly one non-empty segment and becomes a parameter. That is all the machinery needed —
 * `counter://` is the three segments `["counter:", "", ""]` and `doc:/:id` is `["doc:", ":id"]`, so no
 * scheme ever needs special-casing. A template with no `:name` segment is a **singleton**; one with at
 * least one is a **family** whose instances have to be created explicitly.
 *
 * A route may also be declared **internal**, which only means "no connection may reach it": the flag
 * lives on the route and the hub enforces it, so the matcher stays the same on both sides.
 *
 * The same route map value is given to a hub on the server and to a client, so nothing here does I/O.
 */
import type { DurableChannel, DurableChannelActionMap, DurableChannelCommandMap, DurableChannelNotificationMap } from "./channel.ts";
import { InvalidDefinitionError } from "./error.ts";

/** The parameter names of a template, as an object type. A singleton template yields `{}`. */
export type PathToParams<TTemplate extends string> = { [K in ParamNames<TTemplate>]: string };

type ParamNames<TTemplate extends string> = TTemplate extends `${infer Head}/${infer Rest}` ? SegmentName<Head> | ParamNames<Rest>
	: SegmentName<TTemplate>;

type SegmentName<TSegment extends string> = TSegment extends `:${infer Name}` ? Name : never;

/** The type-level description of one route, accumulated by {@link DurableChannelRoutesBuilder.route}. */
export interface DurableChannelRouteTypes {
	readonly state: unknown;
	readonly actions: DurableChannelActionMap;
	readonly commands: DurableChannelCommandMap;
	readonly notifications: DurableChannelNotificationMap;
}

/** Template to route types. Phantom on a built route map, and what makes a hub's calls typed. */
export type DurableChannelRouteMap = Record<string, DurableChannelRouteTypes>;

/** `true` when one segment of a template accepts one segment of a URI. */
type SegmentMatches<TSegment extends string, TValue extends string> = TSegment extends `:${string}` ? TValue extends "" ? false : true
	: [TSegment] extends [TValue] ? [TValue] extends [TSegment] ? true : false
	: false;

/**
 * The type-level twin of {@link matchRoute} for one template: `true` when the URI literal matches it.
 * A URI that is only known to be a `string` never matches, which is what makes a client's `state(uri)`
 * fall back to `unknown` instead of guessing.
 */
export type UriMatchesTemplate<TTemplate extends string, TUri extends string> = TTemplate extends `${infer THead}/${infer TRest}`
	? TUri extends `${infer UHead}/${infer URest}` ? SegmentMatches<THead, UHead> extends true ? UriMatchesTemplate<TRest, URest> : false
	: false
	: TUri extends `${string}/${string}` ? false
	: SegmentMatches<TTemplate, TUri>;

/** The route types a URI literal resolves to, or `never` when no template in the map matches it. */
export type MatchRoute<TRoutes extends DurableChannelRouteMap, TUri extends string> = {
	[K in keyof TRoutes & string]: UriMatchesTemplate<K, TUri> extends true ? TRoutes[K] : never;
}[keyof TRoutes & string];

/** One compiled route. */
export interface DurableChannelRoute<TEnv = unknown> {
	readonly template: string;
	readonly definition: DurableChannel<TEnv>;
	/** The template's segments, with `:name` entries left as-is. */
	readonly segments: readonly string[];
	/** The parameter names, in the order they appear. */
	readonly params: readonly string[];
	/** `true` when no connection may reach the route: the hub hides it as if it did not exist. */
	readonly internal: boolean;
	/** A one-line description of what lives at this template, for the generated document. */
	readonly summary?: string;
}

/** What a route may be declared with beyond its definition. */
export interface DurableChannelRouteOptions {
	/**
	 * Hides the route from every connection: `subscribe` and `dispatchFrom` refuse it with
	 * {@link RouteNotFoundError} and `reconnect` lists it in `missing`, while commands, effects and the
	 * hub's own methods keep using it. This is how a private index stays a durable channel without ever
	 * being told to a peer.
	 */
	readonly internal?: boolean;
	/** Overrides the definition's own summary in the generated document. */
	readonly summary?: string;
}

/** What a match produced: the route it hit and the parameters it bound. */
export interface DurableChannelRouteMatch<TEnv = unknown> {
	readonly route: DurableChannelRoute<TEnv>;
	readonly params: Readonly<Record<string, string>>;
}

/** A built route map. `TRoutes` is a phantom parameter carrying every route's types. */
export interface DurableChannelRoutes<TEnv = unknown, TRoutes extends DurableChannelRouteMap = DurableChannelRouteMap> {
	readonly routes: readonly DurableChannelRoute<TEnv>[];
}

/** Splits a template or a URI into its segments. */
function segmentsOf(value: string): string[] {
	return value.split("/");
}

/** The route whose template matches `uri`, with its bound parameters, or `undefined`. */
export function matchRoute<TEnv>(
	routes: DurableChannelRoutes<TEnv, DurableChannelRouteMap>,
	uri: string,
): DurableChannelRouteMatch<TEnv> | undefined {
	const segments = segmentsOf(uri);
	for (const route of routes.routes) {
		if (route.segments.length !== segments.length) {
			continue;
		}
		const params: Record<string, string> = {};
		let matched = true;
		for (let index = 0; index < segments.length; index++) {
			const expected = route.segments[index];
			if (expected.startsWith(":")) {
				if (segments[index] === "") {
					matched = false;
					break;
				}
				params[expected.slice(1)] = segments[index];
			} else if (expected !== segments[index]) {
				matched = false;
				break;
			}
		}
		if (matched) {
			return { route, params };
		}
	}
	return undefined;
}

/** Fills a template's `:name` segments. Throws {@link InvalidDefinitionError} when a value is missing. */
export function resolveUri(template: string, params: Readonly<Record<string, string>>): string {
	return segmentsOf(template)
		.map((segment) => {
			if (!segment.startsWith(":")) {
				return segment;
			}
			const name = segment.slice(1);
			const value = params[name];
			if (value === undefined || value === "") {
				throw new InvalidDefinitionError(`Template "${template}" needs a value for ":${name}"`);
			}
			return value;
		})
		.join("/");
}

/** `true` when the template binds no parameter, which makes its single instance auto-created on first touch. */
export function isSingleton(route: { readonly params: readonly string[] }): boolean {
	return route.params.length === 0;
}

/** Accumulates routes copy-on-write, so every `.route()` call widens the map's type. */
export class DurableChannelRoutesBuilder<TEnv = unknown, TRoutes extends DurableChannelRouteMap = Record<never, never>> {
	#routes: readonly DurableChannelRoute<TEnv>[];

	constructor(routes: readonly DurableChannelRoute<TEnv>[] = []) {
		this.#routes = routes;
	}

	build(): DurableChannelRoutes<TEnv, TRoutes> {
		return Object.freeze({ routes: Object.freeze([...this.#routes]) });
	}

	env<TNewEnv>(): DurableChannelRoutesBuilder<TNewEnv, TRoutes> {
		return new DurableChannelRoutesBuilder<TNewEnv, TRoutes>(this.#routes as never);
	}

	route<
		TTemplate extends string,
		TState,
		TActions extends DurableChannelActionMap,
		TCommands extends DurableChannelCommandMap,
		TNotifications extends DurableChannelNotificationMap,
	>(
		template: TTemplate,
		definition: DurableChannel<TEnv, TState, TActions, TCommands, TNotifications>,
		options?: DurableChannelRouteOptions,
	): DurableChannelRoutesBuilder<
		TEnv,
		& TRoutes
		& {
			[K in TTemplate]: { state: TState; actions: TActions; commands: TCommands; notifications: TNotifications };
		}
	> {
		if (this.#routes.some((route) => route.template === template)) {
			throw new InvalidDefinitionError(`Duplicate route "${template}"`);
		}
		const segments = segmentsOf(template);
		return new DurableChannelRoutesBuilder([
			...this.#routes,
			{
				template,
				definition: definition as never,
				segments: Object.freeze(segments),
				params: Object.freeze(segments.filter((segment) => segment.startsWith(":")).map((segment) => segment.slice(1))),
				internal: options?.internal === true,
				...(options?.summary !== undefined ? { summary: options.summary } : {}),
			},
		]);
	}
}

/** Starts a route map. */
export function durableRoutes(): DurableChannelRoutesBuilder {
	return new DurableChannelRoutesBuilder();
}
