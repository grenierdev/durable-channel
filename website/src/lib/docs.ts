import { allDocs } from 'content-collections'

export type Doc = (typeof allDocs)[number]

/** The four Diátaxis sections, in sidebar order. The folder of a page under `docs/` names its section. */
export const SECTIONS = [
  { id: 'tutorial', title: 'Tutorial' },
  { id: 'concepts', title: 'Concepts' },
  { id: 'guides', title: 'Guides' },
  { id: 'reference', title: 'Reference' },
] as const

const byOrder = (a: Doc, b: Doc) => a.order - b.order || a.title.localeCompare(b.title)

export const overview = allDocs.find((doc) => doc.slug === '')!

export const pagesOf = (section: string): Doc[] => allDocs.filter((doc) => doc.section === section).sort(byOrder)

/** Reading order of the whole site: the introduction, then each section in sidebar order. */
export const readingOrder: Doc[] = [overview, ...SECTIONS.flatMap((section) => pagesOf(section.id))]

export const findDoc = (slug: string) => allDocs.find((doc) => doc.slug === slug)

export const sectionTitle = (id: string) => SECTIONS.find((section) => section.id === id)?.title ?? 'Introduction'
