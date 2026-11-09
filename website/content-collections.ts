import { defineCollection, defineConfig } from '@content-collections/core'
import { compileMarkdown } from '@content-collections/markdown'
import * as v from 'valibot'
import remarkGfm from 'remark-gfm'

type HastNode = {
  type?: string
  tagName?: string
  value?: string
  properties?: Record<string, unknown>
  children?: HastNode[]
}

function textContent(node: HastNode): string {
  if (node.type === 'text') return node.value ?? ''
  return node.children?.map(textContent).join('') ?? ''
}

function slugify(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9\s-]/g, '').trim().replace(/\s+/g, '-')
}

function headingIds() {
  return (tree: HastNode) => {
    const visit = (node: HastNode) => {
      if (node.type === 'element' && /^h[2-3]$/.test(node.tagName ?? '')) {
        node.properties = { ...node.properties, id: slugify(textContent(node)) }
      }
      if (node.type === 'element' && (node.tagName === 'pre' || node.tagName === 'table')) {
        node.properties = { ...node.properties, tabIndex: 0 }
      }
      node.children?.forEach(visit)
    }
    visit(tree)
  }
}

function extractHeadings(content: string) {
  return content.split('\n').flatMap((line) => {
    const match = /^(##|###)\s+(.+)$/.exec(line)
    if (!match) return []
    const title = match[2].replace(/[`*_]/g, '').trim()
    return [{ title, id: slugify(title), level: match[1].length }]
  })
}

const docs = defineCollection({
  name: 'docs',
  directory: '../docs',
  include: '**/*.md',
  schema: v.object({
    content: v.string(),
    title: v.string(),
    description: v.string(),
    order: v.number(),
    visual: v.picklist(['sync', 'definition', 'commit', 'effects', 'auth', 'routes', 'replay', 'transport', 'schema']),
  }),
  transform: async (document, context) => ({
    ...document,
    /** The folder of a page under `docs/` names its section; `index.md` is the introduction. */
    section: document._meta.directory === '.' || document._meta.directory === '' ? 'introduction' : document._meta.directory.split('/')[0],
    slug: document._meta.path === 'index' ? '' : document._meta.path,
    url: document._meta.path === 'index' ? '/docs' : `/docs/${document._meta.path}`,
    file: `docs/${document._meta.filePath}`,
    html: await compileMarkdown(context, document, { remarkPlugins: [remarkGfm], rehypePlugins: [headingIds] }),
    headings: extractHeadings(document.content),
    /** The page body as written, for `/llms-full.txt`. */
    markdown: document.content.trim(),
  }),
})

export default defineConfig({ content: [docs] })
