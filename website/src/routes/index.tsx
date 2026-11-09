import { createFileRoute } from '@tanstack/react-router'
import { ArrowDown, ArrowRight, ArrowUpRight, GitBranch, MoveRight } from 'lucide-react'
import { SyncDemo } from '../components/SyncDemo'
import { CodeExample } from '../components/CodeExample'

const repo = 'https://github.com/grenierdev/durable-channel'
const docs = '/docs'
export const Route = createFileRoute('/')({ component: Landing })

function Logo({ compact = false }: { compact?: boolean }) {
  return <a className="brand" href="/" aria-label="Durable Channel home"><svg viewBox="0 0 42 30" fill="none" aria-hidden="true"><path d="M3 5h12a10 10 0 0 1 0 20H3V5Zm24 0h12v20H27" stroke="currentColor" strokeWidth="3.5" /><path d="M19 15h11" stroke="currentColor" strokeWidth="3.5" /></svg>{!compact && <span>durable<span className="brand-light">channel</span></span>}</a>
}

function Landing() {
  return <>
    <a className="skip-link" href="#main">Skip to content</a>
    <div className="blue-stage">
      <header className="site-header container"><Logo /><nav aria-label="Main navigation" className="flex items-center gap-7"><a href="#how-it-works" className="nav-how">How it works</a><a href={docs}><span className="docs-long">Documentation</span><span className="docs-short">Docs</span> <ArrowUpRight size={13} /></a><a href={repo} className="github-link" aria-label="Durable Channel on GitHub"><GitBranch size={18} /><span>GitHub</span></a></nav></header>
    </div>
    <main id="main">
    <div className="blue-stage">
        <section className="hero container" aria-labelledby="hero-heading">
          <div className="hero-opening"><h1 id="hero-heading">State changes.<br /><span>Everyone follows.</span></h1><div className="hero-intro"><p>Build realtime apps around shared state.<br className="desktop-break" /> Define it once. Keep every client in sync.</p><a className="button button-lime" href="#quick-start">Start building <ArrowRight size={18} /></a><a className="hero-secondary" href={docs}>A TypeScript library for shared state <ArrowUpRight size={13} /></a></div></div>
          <SyncDemo />
        </section>
    </div>
    <div className="capabilities-strip"><div className="container flex flex-wrap items-center justify-between gap-5"><span>One shared definition</span><span>Optimistic by default</span><span>Server-authoritative</span><span>Replay on reconnect</span></div></div>
    <section className="definition-section section-space container" id="quick-start" aria-labelledby="definition-heading">
      <div className="definition-copy"><h2 id="definition-heading">One definition.<br />{' '}Every side<br className="desktop-break" /> of your app.</h2><p>Your state and the actions that change it belong together. Write a pure reducer. Run it on the server and every connected client.</p><p>The hub commits the change.<br />Your clients follow the same logic.</p><a className="text-link" href="/docs/tutorial/first-channel">Read the quick start <ArrowUpRight size={17} /></a><div className="setup-note"><span className="setup-mark" aria-hidden="true">{`{ }`}</span><p><a href={`${repo}#install`}>Install the npm package</a>, then add this channel definition.</p></div></div>
      <CodeExample />
    </section>
    <section className="mechanism-section" id="how-it-works" aria-labelledby="mechanism-heading"><div className="container section-space">
      <div className="mechanism-heading"><h2 id="mechanism-heading">Instant here.<br />Consistent everywhere.</h2><p>Your interface responds immediately. Behind the scenes, the hub gives every committed action a place in the sequence.</p></div>
      <div className="mechanism-layout"><div className="sequence-diagram" aria-label="An action updates local state, is committed by the hub, then reaches subscribed clients"><div className="sequence-head"><span>Client</span><span>Hub</span><span>Other clients</span></div><div className="sequence-body"><div className="sequence-rail rail-left" /><div className="sequence-rail rail-center" /><div className="sequence-rail rail-right" /><div className="sequence-event event-local"><span className="event-node" /><span>Apply locally</span></div><div className="sequence-event event-send"><span>dispatch(action)</span><MoveRight size={20} /></div><div className="sequence-event event-commit"><span className="event-node" /><span>Validate & persist</span></div><div className="sequence-event event-broadcast"><span>committed envelope</span><MoveRight size={20} /></div><div className="sequence-event event-sync"><span className="event-node" /><span>Run the same reducer</span></div></div><div className="sequence-footer"><span className="diagram-dot" /> One ordered sequence. Shared state.</div></div>
      <div className="mechanism-details"><article><h3>Local feels immediate.</h3><p>The client applies your action optimistically, then reconciles with the hub’s response. If the hub rejects a change, the optimistic update rolls back.</p></article><article><h3>The hub has the final word.</h3><p>Validate the payload. Run the reducer. Persist the state. Broadcast the envelope. Each commit happens in order.</p></article><article><h3>A dropped connection isn’t the end.</h3><p>Reconnect to replay missed actions. If the gap exceeds the replay window, a fresh snapshot brings the client up to date. Your app decides when to reconnect.</p><a href="#demo" className="text-link">Try disconnecting a client <ArrowUpRight size={16} /></a></article></div></div>
    </div></section>
    <section className="primitives-section section-space container" aria-labelledby="primitives-heading"><div className="primitives-intro"><h2 id="primitives-heading">Give every change<br />a clear purpose.</h2><p>A small vocabulary for the moving parts<br className="desktop-break" /> of your realtime application.</p></div><dl className="primitives-list"><div><dt><span className="primitive-symbol" aria-hidden="true">{`{ }`}</span>State</dt><dd>The data at a URI, with a schema and an initial value.</dd><dd className="primitive-code">.state()</dd></div><div><dt><span className="primitive-symbol" aria-hidden="true"><ArrowRight size={23} /></span>Actions</dt><dd>Named changes, reduced on the hub and mirrored by clients.</dd><dd className="primitive-code">.action()</dd></div><div><dt><span className="primitive-symbol" aria-hidden="true"><svg viewBox="0 0 24 24" width="23" height="23" fill="none"><path d="m5 5 7 7-7 7M13 19h7" stroke="currentColor" strokeWidth="1.8" /></svg></span>Commands</dt><dd>Server-side work with typed parameters and results.</dd><dd className="primitive-code">.command()</dd></div><div><dt><span className="primitive-symbol" aria-hidden="true"><svg viewBox="0 0 24 24" width="23" height="23" fill="none"><circle cx="12" cy="12" r="3" stroke="currentColor" strokeWidth="1.8" /><path d="M6 6a8.5 8.5 0 0 0 0 12M18 6a8.5 8.5 0 0 1 0 12" stroke="currentColor" strokeWidth="1.8" /></svg></span>Notifications</dt><dd>Ephemeral messages. Delivered live, never persisted or replayed.</dd><dd className="primitive-code">.notification()</dd></div></dl></section>
    <section className="closing-section"><div className="container closing-inner"><div><h2>Your next idea.<br />Already in sync.</h2><p>Start with a channel. Build from there.</p></div><a className="button button-dark" href="/docs/tutorial/first-channel">Build your first channel <ArrowUpRight size={19} /></a><ArrowDown className="closing-arrow" aria-hidden="true" /></div></section>
    </main>
    <footer className="site-footer container"><Logo /><p>Shared state. One source of truth.</p><div className="flex items-center gap-6"><a href={docs}>Documentation <ArrowUpRight size={13} /></a><a href={repo}>GitHub <ArrowUpRight size={13} /></a><a href="/llms.txt">llms.txt <ArrowUpRight size={13} /></a></div></footer>
  </>
}
