import type { DistributedClientScheduler } from "./interfaces.ts";
import { distributedClientScheduler } from "./interfaces.ts";
import type { ChannelCursor, DistributedResumeEntry } from "./protocol.ts";
import { distributedError } from "./store.ts";

/** Runtime timeout only. Durable retry scheduling must already be armed by the host. */
export function distributedTimeout<T>(
	task: Promise<T>,
	milliseconds: number,
	scheduler: DistributedClientScheduler = distributedClientScheduler,
): Promise<T> {
	return new Promise((resolve, reject) => {
		const cancel = scheduler.schedule(milliseconds, () => reject(distributedError("DELIVERY_TIMEOUT")));
		task.then((value) => {
			cancel();
			resolve(value);
		}, (error) => {
			cancel();
			reject(error);
		});
	});
}
export function resumeCursor(entry: DistributedResumeEntry): ChannelCursor | undefined {
	return entry.type === "snapshot" ? entry.snapshot.cursor : entry.type === "replay" ? entry.cursor : undefined;
}
