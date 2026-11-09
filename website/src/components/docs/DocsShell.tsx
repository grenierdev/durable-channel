import { useEffect, useState } from 'react'
import { ArrowLeft, ArrowRight, ArrowUpRight, GitBranch, Menu, Search, X } from 'lucide-react'
import { DocsSyncLab } from './DocsSyncLab'
import { GuideVisual } from './GuideVisual'

export type DocRecord = {
  title: string
  description: string
  section: 'Start' | 'Build' | 'Operate' | 'Reference'
  order: number
  visual: 'sync' | 'definition' | 'commit' | 'effects' | 'auth' | 'routes' | 'replay' | 'transport' | 'schema'
  slug: string
  html: string
  headings: { title: string; id: string; level: number }[]
}

const repo = 'https://github.com/grenierdev/durable-channel'
const sections: DocRecord['section'][] = ['Start', 'Build', 'Operate', 'Reference']

function Brand() {
  return <a className="brand docs-brand" href="/" aria-label="Durable Channel home"><svg viewBox="0 0 42 30" fill="none" aria-hidden="true"><path d="M3 5h12a10 10 0 0 1 0 20H3V5Zm24 0h12v20H27" stroke="currentColor" strokeWidth="3.5" /><path d="M19 15h11" stroke="currentColor" strokeWidth="3.5" /></svg><span>durable<span className="brand-light">channel</span></span></a>
}

function GuideNavigation({ docs, current }: { docs: DocRecord[]; current: string }) {
  const [query, setQuery] = useState('')
  const filtered = docs.filter((doc) => `${doc.title} ${doc.description}`.toLowerCase().includes(query.toLowerCase()))
  const content = <>
    <label className="docs-search"><Search size={15} aria-hidden="true" /><span className="sr-only">Filter documentation</span><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Find a guide" />{query && <button type="button" onClick={() => setQuery('')} aria-label="Clear filter"><X size={13} /></button>}</label>
    <nav aria-label="Documentation guides">
      {sections.map((section) => {
        const matches = filtered.filter((doc) => doc.section === section)
        if (!matches.length) return null
        return <div className="docs-nav-group" key={section}><h2>{section}</h2>{matches.map((doc) => <a key={doc.slug} href={doc.slug === 'overview' ? '/docs' : `/docs/${doc.slug}`} aria-current={doc.slug === current ? 'page' : undefined}><span>{doc.title}</span><small>{String(doc.order).padStart(2, '0')}</small></a>)}</div>
      })}
      {!filtered.length && <p className="docs-nav-empty">No guide matches “{query}”.</p>}
    </nav>
  </>
  return <>
    <div className="docs-nav-desktop">{content}</div>
    <details className="docs-nav-mobile"><summary><Menu size={16} />Browse guides</summary>{content}</details>
  </>
}

function CodeEnhancer({ pageKey }: { pageKey: string }) {
  useEffect(() => {
    const article = document.querySelector('.docs-prose')
    if (!article) return
    const cleanups: (() => void)[] = []
    article.querySelectorAll('pre').forEach((pre) => {
      if (pre.querySelector('.docs-copy')) return
      const button = document.createElement('button')
      button.type = 'button'
      button.className = 'docs-copy'
      button.setAttribute('aria-label', 'Copy code')
      button.innerHTML = '<span>Copy</span>'
      const copy = async () => {
        const code = pre.querySelector('code')?.textContent ?? pre.textContent ?? ''
        try {
          await navigator.clipboard.writeText(code)
          button.innerHTML = '<span>Copied</span>'
          button.dataset.copied = 'true'
          window.setTimeout(() => { button.innerHTML = '<span>Copy</span>'; delete button.dataset.copied }, 1800)
        } catch { button.innerHTML = '<span>Select to copy</span>' }
      }
      button.addEventListener('click', copy)
      pre.append(button)
      cleanups.push(() => button.removeEventListener('click', copy))
    })
    return () => cleanups.forEach((cleanup) => cleanup())
  }, [pageKey])
  return null
}

function OverviewGuideList({ docs }: { docs: DocRecord[] }) {
  return <section className="docs-guide-index" aria-labelledby="guide-index-title">
    <div className="docs-index-heading"><h2 id="guide-index-title">Follow the signal from definition to wire.</h2><p>Ten guides, ordered by the decisions you make while building.</p></div>
    <div className="docs-index-list">
      {docs.filter((doc) => doc.slug !== 'overview').map((doc) => <a href={`/docs/${doc.slug}`} key={doc.slug}><span className="docs-index-order">{String(doc.order).padStart(2, '0')}</span><span><strong>{doc.title}</strong><small>{doc.description}</small></span><ArrowRight size={18} /></a>)}
    </div>
  </section>
}

export function DocsShell({ doc, docs }: { doc: DocRecord; docs: DocRecord[] }) {
  const ordered = [...docs].sort((a, b) => a.order - b.order)
  const currentIndex = ordered.findIndex((entry) => entry.slug === doc.slug)
  const previous = currentIndex > 0 ? ordered[currentIndex - 1] : undefined
  const next = currentIndex < ordered.length - 1 ? ordered[currentIndex + 1] : undefined
  const isOverview = doc.slug === 'overview'
  const mobileOutline = <details className="docs-mobile-toc"><summary>On this page <span>{doc.headings.length}</span></summary><nav aria-label="On this page">{doc.headings.map((heading) => <a className={heading.level === 3 ? 'is-nested' : ''} href={`#${heading.id}`} key={heading.id}>{heading.title}</a>)}</nav></details>

  return <div className="docs-page">
    <a className="skip-link" href="#docs-main">Skip to documentation</a>
    <header className="docs-header">
      <div className="docs-header-inner"><div className="docs-header-brand"><Brand /><span>Documentation</span></div><nav aria-label="Utility navigation"><a href="/">Overview</a><a href={repo}>GitHub <GitBranch size={15} /></a></nav></div>
    </header>
    <main id="docs-main">
      {isOverview && <section className="docs-opening">
        <div className="docs-opening-copy"><p className="docs-breadcrumb"><span>Docs</span><span>/</span><span>Start here</span></p><h1>{doc.title}</h1><p>{doc.description}</p><div className="docs-opening-actions"><a href="/docs/quick-start">Build your first channel <ArrowRight size={17} /></a><a href={repo}>View source <ArrowUpRight size={15} /></a></div></div>
        <DocsSyncLab />
      </section>}
      <div className={`docs-shell ${isOverview ? 'is-overview' : ''}`}>
        <aside className="docs-sidebar"><GuideNavigation docs={ordered} current={doc.slug} /></aside>
        <article className="docs-article">
          {!isOverview && <><nav className="docs-breadcrumb" aria-label="Breadcrumb"><a href="/docs">Docs</a><span>/</span><span>{doc.section}</span></nav><header className="docs-article-head"><h1>{doc.title}</h1><p>{doc.description}</p></header>{mobileOutline}{doc.visual === 'replay' ? <DocsSyncLab compact /> : <GuideVisual visual={doc.visual as Exclude<DocRecord['visual'], 'sync' | 'replay'>} />}</>}
          {isOverview && mobileOutline}
          <div className="docs-prose" dangerouslySetInnerHTML={{ __html: doc.html }} />
          {isOverview && <OverviewGuideList docs={ordered} />}
          <nav className="docs-pagination" aria-label="Guide pagination">
            {previous ? <a href={previous.slug === 'overview' ? '/docs' : `/docs/${previous.slug}`}><ArrowLeft size={16} /><span><small>Previous</small>{previous.title}</span></a> : <span />}
            {next ? <a href={`/docs/${next.slug}`}><span><small>Next</small>{next.title}</span><ArrowRight size={16} /></a> : <a href={repo}><span><small>Continue</small>Explore the source</span><ArrowUpRight size={16} /></a>}
          </nav>
        </article>
        <aside className="docs-toc" aria-label="On this page"><h2>On this page</h2>{doc.headings.map((heading) => <a className={heading.level === 3 ? 'is-nested' : ''} href={`#${heading.id}`} key={heading.id}>{heading.title}</a>)}<a className="docs-edit-link" href={`${repo}/blob/main/docs/${doc.slug}.md`}>Source material <ArrowUpRight size={13} /></a></aside>
      </div>
    </main>
    <footer className="docs-footer"><div><Brand /><p>Shared state. One source of truth.</p><a href={repo}>GitHub <ArrowUpRight size={13} /></a><a href="/llms.txt">llms.txt <ArrowUpRight size={13} /></a></div></footer>
    <CodeEnhancer pageKey={doc.slug} />
  </div>
}
