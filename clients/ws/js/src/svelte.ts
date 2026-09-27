// Svelte store contract adapters: `$prices` in a component just works.
//
//   import { latestByKey } from "@brahmaputra/ws-client";
//   import { readable } from "@brahmaputra/ws-client/svelte";
//   const prices = readable(latestByKey(client, "prices.us"));

import type { ReadableStore } from "./stores.js";

export interface SvelteReadable<T> {
  subscribe(run: (value: T) => void): () => void;
}

/** Wrap a store in Svelte's contract (call the subscriber now and on every change). */
export function readable<T>(store: ReadableStore<T>): SvelteReadable<T> {
  return {
    subscribe(run) {
      run(store.get());
      return store.subscribe(() => run(store.get()));
    },
  };
}
