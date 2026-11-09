import { createFileRoute, notFound } from '@tanstack/react-router'
import { allDocs } from 'content-collections'
import { DocsShell, type DocRecord } from '../components/docs/DocsShell'

const docs = [...allDocs].sort((a, b) => a.order - b.order) as DocRecord[]

export const Route = createFileRoute('/docs/$slug')({
  loader: ({ params }) => {
    const doc = docs.find((entry) => entry.slug === params.slug)
    if (!doc || doc.slug === 'overview') throw notFound()
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
  const doc = Route.useLoaderData()
  return <DocsShell doc={doc as DocRecord} docs={docs} />
}
