/** Deno-only resolution of the ambient Worker types; esbuild keeps the real runtime import external. */
export const DurableObject: typeof CloudflareWorkersModule.DurableObject;
