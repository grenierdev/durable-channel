# Durable Channel landing page

A TanStack Start + React site with Tailwind CSS, statically prerendered at build time. The interactive example runs the actual Durable Channel hub and two client instances inside the browser.

## Develop

The site reads the library source from `../src` and the documentation from `../docs`, so install both packages:

```sh
npm install          # at the repository root
cd website
npm install
npm run dev
```

Open http://localhost:3000.

## Build and deploy

```sh
npm run build
```

Deploy **`dist/client/`** to any static host. TanStack Start prerenders `/` into `dist/client/index.html`; no application server or environment variables are needed for this page. Prerender configuration follows the [TanStack Start static prerendering guide](https://tanstack.com/start/latest/docs/framework/react/guide/static-prerendering).

For a local check of the exact static deployment:

```sh
python3 -m http.server 3000 --directory dist/client
```

## Verify

```sh
npm run typecheck
npm test
npx playwright install chromium
npm run test:e2e
```

The browser test expects a static server at http://127.0.0.1:3000; override with `SITE_URL`. It exercises both clients, disconnect/reconnect, replay, reset, keyboard tabs and copying, runs axe accessibility checks at five viewport widths, and verifies the content without JavaScript. Screenshots and results go to `.impeccable/review/`.

## Demo implementation

The demo imports the library straight from its source in `../src` through the `@lib` alias (see `vite.config.ts` and `tsconfig.json`), so it always runs the current code. The demo orchestrator uses real `DurableChannelClient`, `DurableChannelHub`, `MemoryStorage`, JSON-RPC and paired `InMemoryTransport` instances. A simulated 220 ms request delay makes optimistic updates visible. All state stays in the current browser page and resets on reload; this is not a multi-user network deployment.

The demo's replay ring holds 64 actions. Reconnecting after a larger gap falls back to a snapshot. Tests cover both paths. Source snippets use the published npm package, and the WebSocket client snippet explicitly requires the reader's own server.

## Documentation and llms.txt

The pages under `/docs` come from the Markdown files in `../docs`. The build also writes `/llms.txt` (an index of the pages) and `/llms-full.txt` (every page in reading order), following https://llmstxt.org.

## Design

The selected synchronization-score direction, product facts and tokens live in `PRODUCT.md`, `DESIGN.md` and `.impeccable/` at the repository root. Fonts are self-hosted from Fontsource packages; icons use Lucide and a small geometric brand mark. No third-party image service is required.
