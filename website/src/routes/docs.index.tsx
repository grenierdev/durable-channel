import { createFileRoute } from '@tanstack/react-router'
import { allDocs } from 'content-collections'
import { DocsShell, type DocRecord } from '../components/docs/DocsShell'

const docs = [...allDocs].sort((a, b) => a.order - b.order) as DocRecord[]
const overview = docs.find((doc) => doc.slug === 'overview')!

export const Route = createFileRoute('/docs/')({
  head: () => ({
    meta: [
      { title: 'Documentation — Durable Channel' },
      { name: 'description', content: overview.description },
    ],
  }),
  component: () => <DocsShell doc={overview} docs={docs} />,
})
