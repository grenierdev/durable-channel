import { useEffect, useRef, useState } from 'react'
import { Check, CirclePause, Plus, RotateCcw, Unplug, Wifi } from 'lucide-react'
import { initialDemoState, type DemoState } from '../../lib/demo-state'
import type { SyncDemo as DemoEngine } from '../../lib/demo'

export function DocsSyncLab({ compact = false }: { compact?: boolean }) {
  const [state, setState] = useState<DemoState>(initialDemoState)
  const [generation, setGeneration] = useState(0)
  const engine = useRef<DemoEngine | null>(null)

  useEffect(() => {
    let cancelled = false
    let instance: DemoEngine | undefined
    let unsubscribe: (() => void) | undefined
    setState(initialDemoState)
    void import('../../lib/demo').then(async ({ SyncDemo }) => {
      if (cancelled) return
      instance = new SyncDemo(compact ? 120 : 220)
      engine.current = instance
      unsubscribe = instance.subscribe(setState)
      try { await instance.start() }
      catch (error) {
        if (!cancelled) setState((current) => ({ ...current, error: error instanceof Error ? error.message : 'The lab could not start.' }))
      }
    })
    return () => {
      cancelled = true
      engine.current = null
      unsubscribe?.()
      void instance?.dispose()
    }
  }, [generation, compact])

  const reset = () => setGeneration((value) => value + 1)
  const statusMessage = state.message === 'Try +1 on either client. Both share the same state.'
    ? 'Dispatch from Client A. Both clients run the same reducer.'
    : state.message
  const value = (label: string, count: number, muted = false) => (
    <div className={`docs-lab-node ${muted ? 'is-held' : ''}`}>
      <span>{label}</span>
      <strong>{String(count).padStart(2, '0')}</strong>
    </div>
  )

  return <section className={`docs-lab ${compact ? 'docs-lab-compact' : ''}`} aria-label="Interactive client and hub synchronization lab">
    <div className="docs-lab-head">
      <div><span className="docs-live-dot" aria-hidden="true" /><strong>Live protocol trace</strong><code>counter://</code></div>
      <button type="button" onClick={reset} disabled={state.busy}><RotateCcw size={13} />Reset</button>
    </div>
    <div className={`docs-lab-track ${state.busy ? 'is-running' : ''}`} data-phase={state.phase}>
      {value('Client A', state.a)}
      <span className="docs-lab-line"><i /><small>dispatch</small></span>
      {value('Hub', state.hub)}
      <span className={`docs-lab-line ${!state.bConnected ? 'is-broken' : ''}`}><i /><small>{state.bConnected ? 'broadcast' : 'held'}</small></span>
      {value('Client B', state.b, !state.bConnected)}
    </div>
    <div className="docs-lab-actions">
      <button type="button" className="docs-lab-primary" onClick={() => void engine.current?.increment('a')} disabled={!state.ready || state.busy}><Plus size={15} />Client A +1</button>
      <button type="button" onClick={() => void engine.current?.toggleConnection()} disabled={!state.ready || state.busy}>{state.bConnected ? <Unplug size={14} /> : <Wifi size={14} />}{state.bConnected ? 'Disconnect B' : 'Reconnect B'}</button>
      <p role="status" className={state.error ? 'is-error' : ''}>{state.error ? <Unplug size={14} /> : state.bConnected ? <Check size={14} /> : <CirclePause size={14} />}{state.error ?? statusMessage}</p>
    </div>
    {!compact && <div className="docs-lab-log" aria-label="Recent committed actions">
      <div><span>SEQ</span><span>ORIGIN</span><span>ENVELOPE</span></div>
      {state.events.length ? state.events.map((event) => <div key={event.seq}><code>{String(event.seq).padStart(3, '0')}</code><span>{event.from}</span><code>{event.name}</code></div>) : <div className="is-empty"><code>—</code><span>Waiting for a dispatch</span><code>—</code></div>}
    </div>}
    <noscript><p className="docs-lab-noscript">The guide remains available without JavaScript. Enable it to run the synchronization trace.</p></noscript>
  </section>
}
