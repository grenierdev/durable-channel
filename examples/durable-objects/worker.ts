import { channelName, type Env, gatewayName, ownerEndpoint, safeOwnerCall } from "./runtime.ts";
export { ChannelObject } from "./channel-object.ts";
export { GatewayObject } from "./gateway-object.ts";
export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const url = new URL(request.url), hub = env.LOGICAL_HUB;
		if (url.pathname === "/test") {
			const body = await request.json() as { op: string; uri?: string; generation?: string; bucket?: number; by?: number };
			const uri = body.uri ?? "counter:/a", owner = ownerEndpoint(env, hub, uri);
			const result = await safeOwnerCall(async () => {
				switch (body.op) {
					case "create":
						return await owner.create();
					case "snapshot":
						return await owner.snapshot();
					case "destroy":
						return await owner.destroy(body.generation!);
					case "dispatch":
						return await owner.dispatch(body.generation ?? (await owner.snapshot())!.cursor.generation, "add", body.by ?? 1);
					case "failDelivery":
						return await env.GATEWAYS.getByName(gatewayName(hub, body.bucket ?? 0)).failNextDelivery();
					case "identities":
						return { channel: channelName(hub, uri), gateway: gatewayName(hub, body.bucket ?? 0) };
					default:
						throw new Error("Unknown test operation");
				}
			});
			return Response.json(result);
		}
		if (url.pathname !== "/connect") return new Response("Distributed Durable Channel example", { status: 200 });
		let clientId = url.searchParams.get("client") ?? undefined;
		if (!clientId && env.AUTH) {
			const result = await env.AUTH.fetch(request);
			if (result.ok) {
				const identity = await result.json() as { clientId?: string };
				clientId = identity.clientId;
			}
		}
		if (!clientId) return new Response("Authenticated identity required", { status: 401 });
		const requested = url.searchParams.get("gateway"), supplied = requested === null ? undefined : Number(requested);
		let hash = 0;
		for (const char of clientId) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
		const bucket = supplied !== undefined && Number.isSafeInteger(supplied) && supplied >= 0 && supplied < 4 ? supplied : hash % 4;
		const gatewayId = gatewayName(hub, bucket), headers = new Headers(request.headers);
		headers.set("X-Distributed-Hub", hub);
		headers.set("X-Distributed-Gateway", gatewayId);
		headers.set("X-Distributed-Client", clientId);
		return await env.GATEWAYS.getByName(gatewayId).fetch(new Request(request, { headers }));
	},
} satisfies ExportedHandler<Env>;
