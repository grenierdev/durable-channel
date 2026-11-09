import { useEffect, useRef, useState } from 'react'
import { ArrowRight, Check, CirclePause, Plus, Radio, RotateCcw, Unplug, Wifi } from 'lucide-react'
import { initialDemoState, type DemoState, type Peer } from '../lib/demo-state'
import type { SyncDemo as DemoEngine } from '../lib/demo'

export function SyncDemo() {
  const [state, setState] = useState<DemoState>(initialDemoState)
  const [generation, setGeneration] = useState(0)
  const engine = useRef<DemoEngine | null>(null)
  useEffect(() => {
    let cancelled = false
    let instance: DemoEngine | undefined
    let unsubscribe: (() => void) | undefined
    setState(initialDemoState)
    void import('../lib/demo').then(async ({ SyncDemo }) => {
      if (cancelled) return
      instance = new SyncDemo()
      engine.current = instance
      unsubscribe = instance.subscribe(setState)
      try { await instance.start() }
      catch (error) {
        if (!cancelled) setState(s => ({ ...s, error: error instanceof Error ? error.message : 'Unable to start the demo. Try resetting it.' }))
      }
    }).catch(() => {
      if (!cancelled) setState(s => ({ ...s, error: 'The demo could not load. Check your connection and reset to retry.' }))
    })
    return () => { cancelled = true; engine.current = null; unsubscribe?.(); void instance?.dispose() }
  }, [generation])

  const client = (peer: Peer) => {
    const connected = peer === 'a' || state.bConnected
    return <div className={`client-lane client-${peer} ${!connected ? 'is-disconnected' : ''}`}>
      <div className="lane-title"><span className="client-symbol" aria-hidden="true">{peer.toUpperCase()}</span><span>Client {peer.toUpperCase()}</span><span className="connection-label">{connected ? <><span className="status-dot" />Connected</> : <><CirclePause size={12} />Offline</>}</span></div>
      <div className="client-state"><span className="state-key">count</span><output className="count" data-testid={`count-${peer}`} aria-label={`Client ${peer.toUpperCase()} count`}>{String(state[peer]).padStart(2, '0')}</output><button type="button" className="increment" onClick={() => void engine.current?.increment(peer)} disabled={!state.ready || state.busy || !connected} aria-label={`Increment Client ${peer.toUpperCase()}`}><Plus size={20} /><span>1</span></button></div>
      <div className="lane-caption">{!connected ? 'Holding the last known state' : state.phase === peer ? 'Updated locally. Awaiting confirmation.' : 'Same definition. Same reducer.'}</div>
    </div>
  }

  return <section className="sync-demo" id="demo" aria-label="Interactive synchronization example">
    <div className="demo-toolbar flex flex-wrap items-center justify-between gap-4"><div className="flex items-center gap-3"><Radio size={16} aria-hidden="true" /><span className="font-semibold">Play with shared state</span><code>counter://</code></div><button className="reset-button" onClick={() => setGeneration(g => g + 1)} disabled={state.busy} type="button"><RotateCcw size={13} />Reset</button></div>
    <div className={`score ${state.busy ? 'is-active' : ''}`} data-phase={state.phase}>
      {client('a')}
      <div className="score-connection connection-a" aria-hidden="true"><span className="connection-dot" /><span className="connection-line" /><ArrowRight size={16} /></div>
      <div className="hub-lane"><div className="hub-title"><span className="hub-mark" aria-hidden="true"><i /><i /><i /></span>Authoritative hub</div><div className="hub-value"><span className="state-key">count</span><output data-testid="count-hub" aria-label="Authoritative hub count">{String(state.hub).padStart(2, '0')}</output></div><div className="hub-sequence"><span className="status-dot" />Committed state <code>seq {String(state.seq).padStart(3, '0')}</code></div></div>
      <div className={`score-connection connection-b ${!state.bConnected ? 'broken' : ''}`} aria-hidden="true"><span className="connection-dot" /><span className="connection-line" /><ArrowRight size={16} /></div>
      {client('b')}
    </div>
    <div className="demo-bottom"><p role="status" className={state.error ? 'demo-error' : ''}><span aria-hidden="true">{state.error ? <Unplug size={15} /> : state.busy ? <Radio size={15} /> : <Check size={15} />}</span>{state.error ?? state.message}</p><button type="button" className="disconnect-button" disabled={!state.ready || state.busy} onClick={() => void engine.current?.toggleConnection()}>{state.bConnected ? <Unplug size={14} /> : <Wifi size={14} />}{state.bConnected ? 'Disconnect Client B' : 'Reconnect Client B'}</button></div>
    <noscript><p className="demo-footnote">Enable JavaScript to try the live demo. The quick start below is available without it.</p></noscript>
    <div className="demo-footnote"><span>Real library. In-memory transport. Simulated 220 ms latency.</span><span>Click +1. Break the connection. Watch it catch up.</span></div>
  </section>
}
