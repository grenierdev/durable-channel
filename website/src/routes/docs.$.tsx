import { createFileRoute, notFound } from '@tanstack/react-router'
import { DocsShell } from '../components/docs/DocsShell'
import { findDoc } from '../lib/docs'

export const Route = createFileRoute('/docs/$')({
  loader: ({ params }) => {
    const slug = (params._splat ?? '').replace(/\/$/, '')
    const doc = slug ? findDoc(slug) : undefined
    if (!doc) throw notFound()
    return doc
  },
  head: ({ loaderData }) => ({
    meta: loaderData ? [
      { title: `${loaderData.title} — Durable Channel` },
      { name: 'description', content: loaderData.description },
    ] : [],
  }),
  component: Guide,
})

function Guide() {
  return <DocsShell doc={Route.useLoaderData()} />
}
