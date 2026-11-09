import { createFileRoute } from '@tanstack/react-router'
import { DocsShell } from '../components/docs/DocsShell'
import { overview } from '../lib/docs'

export const Route = createFileRoute('/docs/')({
  head: () => ({
    meta: [
      { title: 'Documentation — Durable Channel' },
      { name: 'description', content: overview.description },
    ],
  }),
  component: () => <DocsShell doc={overview} />,
})
