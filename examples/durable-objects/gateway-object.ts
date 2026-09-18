import { DurableObject } from "cloudflare:workers";
import {
	createDistributedLink,
	createDistributedRpc,
	type DistributedDelivery,
	type DistributedDeliveryAck,
	distributedMessageToFrame,
	type DistributedRpcLink,
	type DistributedSession,
	DurableChannelGateway,
	SqliteChannelStore,
} from "../../src/mod.ts";
import { alarmScheduler, type Env, routerFor } from "./runtime.ts";
interface GatewayIdentity {
	hub: string;
	gatewayId: string;
}
interface Attachment {
	connectionId: string;
	clientId: string;
	binding?: number;
}
export class GatewayObject extends DurableObject<Env> {
	#identity: GatewayIdentity | undefined;
	#gateway: DurableChannelGateway | undefined;
	#links = new WeakMap<WebSocket, DistributedRpcLink>();
	#ready: Promise<void>;
	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		this.#ready = ctx.blockConcurrencyWhile(async () => {
			this.#identity = await ctx.storage.get<GatewayIdentity>("host/identity");
			if (!this.#identity) return;
			this.build(this.#identity);
			const sessions = ctx.getWebSockets().filter((socket) => (socket.deserializeAttachment() as Attachment)?.binding !== undefined).map((
				socket,
			) => this.session(socket));
			await this.#gateway!.restore(sessions, { recover: false });
		});
		ctx.waitUntil(this.#ready.then(() => this.#gateway?.alarm()));
	}
	private build(identity: GatewayIdentity): void {
		this.#gateway = new DurableChannelGateway({
			id: identity.gatewayId,
			router: routerFor(this.env, identity.hub),
			store: new SqliteChannelStore(this.ctx.storage),
			scheduler: alarmScheduler(this.ctx.storage),
			renewMs: 10_000,
			timeoutMs: 5000,
		});
	}
	private session(socket: WebSocket): DistributedSession {
		const attachment = socket.deserializeAttachment() as Attachment;
		return {
			id: attachment.connectionId,
			clientId: attachment.clientId,
			send: (message) => socket.send(JSON.stringify(distributedMessageToFrame(message))),
			close: (reason) => socket.close(1012, reason.slice(0, 120)),
		};
	}
	private link(socket: WebSocket): DistributedRpcLink {
		let link = this.#links.get(socket);
		if (!link) {
			const attachment = socket.deserializeAttachment() as Attachment;
			link = createDistributedLink(this.#gateway!, this.session(socket), attachment.binding);
			this.#links.set(socket, link);
		}
		return link;
	}
	override async fetch(request: Request): Promise<Response> {
		await this.#ready;
		if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") return new Response("WebSocket required", { status: 426 });
		const hub = request.headers.get("X-Distributed-Hub"),
			gatewayId = request.headers.get("X-Distributed-Gateway"),
			clientId = request.headers.get("X-Distributed-Client");
		if (!hub || !gatewayId || !clientId || this.env.GATEWAYS.idFromName(gatewayId).toString() !== this.ctx.id.toString()) {
			return new Response("Invalid binding", { status: 400 });
		}
		if (!this.#identity) {
			this.#identity = { hub, gatewayId };
			await this.ctx.storage.put("host/identity", this.#identity);
			await this.ctx.storage.sync();
			this.build(this.#identity);
		}
		const pair = new WebSocketPair(), client = pair[0], server = pair[1];
		server.serializeAttachment({ connectionId: crypto.randomUUID(), clientId } satisfies Attachment);
		this.ctx.acceptWebSocket(server);
		return new Response(null, { status: 101, webSocket: client, headers: { "X-Distributed-Gateway": gatewayId } });
	}
	override async webSocketMessage(socket: WebSocket, data: string | ArrayBuffer): Promise<void> {
		await this.#ready;
		try {
			const link = this.link(socket), frame: unknown = JSON.parse(typeof data === "string" ? data : new TextDecoder().decode(data));
			const response = await createDistributedRpc(this.#gateway!).handle(frame, link);
			const attachment = socket.deserializeAttachment() as Attachment;
			socket.serializeAttachment({ ...attachment, binding: link.binding });
			if (response) socket.send(JSON.stringify(response));
		} catch {
			socket.send(JSON.stringify({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Invalid frame" } }));
		}
	}
	override async webSocketClose(socket: WebSocket, code: number, reason: string): Promise<void> {
		await this.#ready;
		await this.link(socket).detach();
		socket.close(code, reason);
	}
	override async webSocketError(socket: WebSocket): Promise<void> {
		await this.#ready;
		await this.link(socket).detach();
	}
	async deliver(message: DistributedDelivery): Promise<DistributedDeliveryAck> {
		await this.#ready;
		if (await this.ctx.storage.get<boolean>("test/fail-delivery")) {
			await this.ctx.storage.delete("test/fail-delivery");
			await this.ctx.storage.sync();
			throw new Error("Injected local delivery failure");
		}
		if (!this.#gateway) return { revision: message.revision, generation: message.generation, channelSeq: 0, clients: 0 };
		return await this.#gateway.deliver(message);
	}
	async notify(channel: string, generation: string, name: string, payload: unknown): Promise<void> {
		await this.#ready;
		await this.#gateway?.notify(channel, generation, name, payload);
	}
	override async alarm(): Promise<void> {
		await this.#ready;
		await this.#gateway?.alarm();
	}
	async failNextDelivery(): Promise<void> {
		await this.ctx.storage.put("test/fail-delivery", true);
		await this.ctx.storage.sync();
	}
}
