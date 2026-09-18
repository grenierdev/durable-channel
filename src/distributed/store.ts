import { DurableChannelError } from "../error.ts";
export { type ChannelStore, type ChannelTransaction } from "./interfaces.ts";

/** JSON cloning is part of the store contract, including reads and transaction results. */
export function cloneRecord<T>(value: T): T {
	return value === undefined ? value : JSON.parse(JSON.stringify(value));
}
export function distributedError(code: string, message = code): DurableChannelError {
	return new DurableChannelError(code, message);
}
/** Stable JSON comparison for immutable retry payloads (object property order is irrelevant). */
export function canonicalJson(value: unknown): string {
	const walk = (entry: unknown): unknown => {
		if (Array.isArray(entry)) return entry.map(walk);
		if (entry !== null && typeof entry === "object") {
			return Object.fromEntries(Object.keys(entry).sort().map((key) => [key, walk((entry as Record<string, unknown>)[key])]));
		}
		return entry;
	};
	return JSON.stringify(walk(value));
}
/** An ID is a decimal Unix-millisecond deadline, a dot, and an opaque random nonce. */
export function createDistributedActionId(deadline: number, nonce: string = crypto.randomUUID()): string {
	if (!Number.isSafeInteger(deadline) || deadline < 0 || !/^[A-Za-z0-9_-]{16,100}$/.test(nonce)) {
		throw distributedError("INVALID_ACTION_ID");
	}
	return `${deadline}.${nonce}`;
}
export function distributedActionDeadline(id: string): number {
	const match = /^(0|[1-9][0-9]*)\.([A-Za-z0-9_-]{16,100})$/.exec(id);
	if (!match) throw distributedError("INVALID_ACTION_ID");
	const deadline = Number(match[1]);
	if (!Number.isSafeInteger(deadline)) throw distributedError("INVALID_ACTION_ID");
	return deadline;
}
