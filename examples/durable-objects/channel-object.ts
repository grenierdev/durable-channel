import { DurableObject } from "cloudflare:workers";
import {
	type DistributedActorEndpoint,
	type DistributedCaller,
	type DistributedDispatchRequest,
	type DistributedMemberRequest,
	DurableChannelActor,
	DurableChannelError,
	SqliteChannelStore,
} from "../../src/mod.ts";
import {
	alarmScheduler,
	channelName,
	type Env,
	type OwnerCall,
	type OwnerIdentity,
	type OwnerResult,
	routerFor,
	safeOwnerCall,
} from "./runtime.ts";
import { routes } from "./routes.ts";

export class ChannelObject extends DurableObject<Env> {
	#identity: OwnerIdentity | undefined;
	#actor: DurableChannelActor | undefined;
	#ready: Promise<void>;
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#ready = ctx.blockConcurrencyWhile(async () => {
			this.#identity = await ctx.storage.get<OwnerIdentity>("host/identity");
			if (this.#identity) this.build(this.#identity);
		});
	}
	private build(identity: OwnerIdentity): void {
		this.#actor = new DurableChannelActor<unknown>(identity.uri, routes, {
			env: {},
			store: new SqliteChannelStore(this.ctx.storage),
			router: routerFor(this.env, identity.hub),
			scheduler: alarmScheduler(this.ctx.storage),
			historyLimit: 8,
			retryMs: 100,
			leaseMs: 30_000,
			deliveryTimeoutMs: 5000,
			waitUntil: (task) => this.ctx.waitUntil(task),
			gateways: (id) => ({
				deliver: async (message) => {
					const result = await this.env.GATEWAYS.getByName(id).deliver(message);
					return { revision: result.revision, generation: result.generation, channelSeq: result.channelSeq, clients: result.clients };
				},
				notify: (channel, generation, name, payload) => this.env.GATEWAYS.getByName(id).notify(channel, generation, name, payload),
			}),
		});
	}
	async invoke(input: OwnerCall): Promise<OwnerResult> {
		await this.#ready;
		return await safeOwnerCall(async () => {
			if (this.env.CHANNELS.idFromName(channelName(input.hub, input.uri)).toString() !== this.ctx.id.toString()) {
				throw new DurableChannelError("OWNER_MISMATCH", "OWNER_MISMATCH");
			}
			if (!this.#identity) {
				// Identity is durable before actor work can arm an alarm.
				this.#identity = { hub: input.hub, uri: input.uri };
				await this.ctx.storage.put("host/identity", this.#identity);
				await this.ctx.storage.sync();
				this.build(this.#identity);
			}
			const actor: DistributedActorEndpoint = this.#actor!;
			const args = input.args;
			switch (input.method) {
				case "snapshot":
					return await actor.snapshot(args[0] as string | undefined);
				case "create":
					return await actor.create(args[0]);
				case "destroy":
					return await actor.destroy(args[0] as string);
				case "dispatch":
					return await actor.dispatch(args[0] as string, args[1] as string, args[2]);
				case "dispatchFrom":
					return await actor.dispatchFrom(args[0] as DistributedCaller, args[1] as DistributedDispatchRequest);
				case "exec":
					return await actor.exec(args[0] as string, args[1], args[2] as string | undefined, args[3] as DistributedCaller | undefined);
				case "execFrom":
					return await actor.execFrom(args[0] as DistributedCaller, args[1] as string, args[2], args[3] as string | undefined);
				case "notify":
					return await actor.notify(args[0] as string, args[1], args[2] as string | undefined);
				case "resume":
					return await actor.resume(args[0] as DistributedMemberRequest);
				case "renew":
					return await actor.renew(args[0] as DistributedMemberRequest);
				case "remove":
					return await actor.remove(args[0] as DistributedMemberRequest);
				case "alarm":
					return await actor.alarm();
				default:
					throw new DurableChannelError("UNSUPPORTED_OPERATION", "UNSUPPORTED_OPERATION");
			}
		});
	}
	override async alarm(): Promise<void> {
		await this.#ready;
		await this.#actor?.alarm();
	}
}
