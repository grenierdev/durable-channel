import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import { tanstackStart } from '@tanstack/react-start/plugin/vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import contentCollections from '@content-collections/vite'

export default defineConfig({
  resolve: {
    alias: {
      // The demo runs the real library, straight from its source.
      '@lib': fileURLToPath(new URL('../src', import.meta.url)),
    },
    dedupe: ['valibot', '@valibot/to-json-schema'],
  },
  server: { fs: { allow: ['..'] } },
  plugins: [
    contentCollections(),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
        autoStaticPathsDiscovery: true,
        crawlLinks: true,
        failOnError: true,
      },
      // The llms.txt files are listed too, as no page links to them with a router `Link`.
      pages: [{ path: '/' }, { path: '/llms.txt' }, { path: '/llms-full.txt' }],
    }),
    react(),
  ],
})
