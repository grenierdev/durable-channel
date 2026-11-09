import { createRootRoute, HeadContent, Outlet, Scripts } from '@tanstack/react-router'
import stylesheet from '../styles.css?url'

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Durable Channel — State changes. Everyone follows.' },
      { name: 'description', content: 'Build and sync realtime state with shared TypeScript definitions, optimistic clients, and an authoritative hub. Try Durable Channel in your browser.' },
      { name: 'theme-color', content: '#2449e9' },
      { property: 'og:title', content: 'Durable Channel — State changes. Everyone follows.' },
      { property: 'og:description', content: 'One shared definition. An authoritative hub. Realtime state that stays in sync.' },
      { property: 'og:type', content: 'website' },
    ],
    links: [{ rel: 'stylesheet', href: stylesheet }, { rel: 'icon', type: 'image/svg+xml', href: '/favicon.svg' }],
  }),
  component: Root,
  notFoundComponent: () => <main className="error-page"><h1>This channel doesn’t exist.</h1><a href="/">Return to Durable Channel</a></main>,
})

function Root() {
  return <html lang="en"><head><HeadContent /></head><body><Outlet /><Scripts /></body></html>
}
