import { overview, pagesOf, readingOrder, SECTIONS } from './docs'

/** The llms.txt files follow the proposal at https://llmstxt.org. */
const SITE = 'https://durable.channel'
const NAME = 'Durable Channel'
const SUMMARY = 'Durable Channel is a TypeScript library for realtime state. You define the state and every allowed change once; an authoritative hub commits each change in order, and clients mirror it by running the same pure reducers.'

export const textResponse = (body: string) => new Response(body, { headers: { 'content-type': 'text/plain; charset=utf-8' } })

/** The index: the project, then one link list per section of the docs. */
export function llmsIndex() {
  const item = (doc: { title: string; url: string; description: string }) => `- [${doc.title}](${SITE}${doc.url}): ${doc.description}`
  const lists = [`## Introduction\n\n${item(overview)}`, ...SECTIONS.map((section) => `## ${section.title}\n\n${pagesOf(section.id).map(item).join('\n')}`)]
  const head = `# ${NAME}\n\n> ${SUMMARY}\n\nThe full text of every page below is in one file: [llms-full.txt](${SITE}/llms-full.txt).`
  return `${[head, ...lists].join('\n\n')}\n`
}

/** Every documentation page as one Markdown file, in reading order. */
export function llmsFull() {
  const pages = readingOrder.map((doc) => {
    // Site-relative links become absolute, as the file is read outside the site.
    const body = doc.markdown.replace(/\]\(\/(?!\/)/g, `](${SITE}/`)
    return `# ${doc.title}\n\nSource: ${SITE}${doc.url}\n\n> ${doc.description}\n\n${body}\n`
  })
  const head = `# ${NAME}\n\n> ${SUMMARY}\n\nThis file holds the full documentation of ${NAME}, one page after the other, in reading order.\n`
  return [head, ...pages].join('\n---\n\n')
}
