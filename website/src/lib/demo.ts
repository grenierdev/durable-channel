import * as v from 'valibot'
import { durableChannel } from '@lib/channel.ts'
import { DurableChannelClient } from '@lib/client.ts'
import { DurableChannelHub, type DurableChannelConnection } from '@lib/hub.ts'
import { durableRoutes } from '@lib/routes.ts'
import { createRpc, toRpcNotification, type DurableChannelLink, type DurableChannelRpcFrame } from '@lib/rpc.ts'
import { MemoryStorage } from '@lib/storage.ts'
import { InMemoryTransport } from '@lib/transport.ts'

export const counter = durableChannel()
  .state(v.object({ count: v.number() }), { count: 0 })
  .action(a => a.name('counter/incremented')
    .payload(v.object({ by: v.number() }))
    .client()
    .reduce((state, { by }) => ({ count: state.count + by })))
  .build()

const routes = durableRoutes().route('counter://', counter).build()
import { initialDemoState, type Peer, type DemoState } from './demo-state'

// Keep the concrete route-map type inferred rather than exposing library internals.
type CounterClient = ReturnType<typeof makeClient>
function makeClient(transport: InMemoryTransport, clientId: string) {
  return new DurableChannelClient(routes, transport, { clientId, requestTimeoutMs: 5000 })
}

function createHub(replayLimit: number) {
  return new DurableChannelHub(routes, { storage: new MemoryStorage(), env: {}, replayLimit })
}

function createDemoRpc(hub: ReturnType<typeof createHub>) { return createRpc(hub) }

export class SyncDemo {
  private hub: ReturnType<typeof createHub>
  private rpc: ReturnType<typeof createDemoRpc>
  private clients = {} as Record<Peer, CounterClient>
  private links = {} as Record<Peer, ReturnType<SyncDemo['serve']>>
  private allLinks = new Set<ReturnType<SyncDemo['serve']>>()
  private listeners = new Set<(state: DemoState) => void>()
  private disposed = false
  private state = initialDemoState

  constructor(private latency = 220, replayLimit = 64) {
    this.hub = createHub(replayLimit)
    this.rpc = createRpc(this.hub)
  }

  get snapshot() { return this.state }
  subscribe(listener: (state: DemoState) => void) {
    this.listeners.add(listener)
    listener(this.state)
    return () => { this.listeners.delete(listener) }
  }
  private update(patch: Partial<DemoState>) {
    if (this.disposed) return
    this.state = { ...this.state, ...patch }
    this.listeners.forEach(listener => listener(this.state))
  }
  private serve() {
    const [transport, server] = InMemoryTransport.pair()
    const abort = new AbortController()
    let binding: DurableChannelConnection | undefined
    let closed = false
    const out = (frame: DurableChannelRpcFrame) => { if (!closed) server.send(frame) }
    const unbind = () => {
      if (!binding) return
      const stale = binding
      binding = undefined
      this.hub.disconnect(stale)
    }
    const link: DurableChannelLink = {
      get clientId() { return binding?.id },
      get connection() { return binding },
      unbind,
      bind: id => {
        unbind()
        binding = this.hub.connect({ id, send: message => out(toRpcNotification(message)) })
      },
    }
    const loop = (async () => {
      try {
        while (!closed) {
          const frame = await server.recv()
          if (!frame || closed) break
          if (this.latency) await new Promise(resolve => setTimeout(resolve, this.latency))
          if (closed) break
          const response = await this.rpc.handle(frame, link, abort.signal)
          if (response !== undefined) out(response)
        }
      } catch (error) {
        if (!closed && !this.disposed) this.fail(error)
      } finally {
        unbind()
      }
    })()
    const result = {
      transport, loop,
      close: async () => {
        closed = true
        abort.abort()
        server.close()
        await loop
      },
    }
    this.allLinks.add(result)
    return result
  }
  private async watch(peer: Peer) {
    for await (const { event } of this.clients[peer].events()) {
      if (this.disposed) break
      if (event.type === 'state') this.update({ [peer]: this.clients[peer].state('counter://').count })
    }
  }
  async start() {
    for (const peer of ['a', 'b'] as const) {
      if (this.disposed) return
      this.links[peer] = this.serve()
      this.clients[peer] = makeClient(this.links[peer].transport, `client-${peer}`)
      void this.watch(peer)
      this.clients[peer].connect()
      await this.clients[peer].hello({ subscriptions: ['counter://'] })
      if (this.disposed) return
      this.update({ [peer]: this.clients[peer].state('counter://').count })
    }
    this.update({ ready: true, message: 'Try +1 on either client. Both share the same state.' })
  }
  async increment(peer: Peer) {
    if (!this.state.ready || this.state.busy || (peer === 'b' && !this.state.bConnected)) return
    this.update({ busy: true, phase: peer, message: `Client ${peer.toUpperCase()} updates optimistically. The hub is confirming…`, error: null })
    try {
      const handle = this.clients[peer].dispatch('counter://', 'counter/incremented', { by: 1 })
      this.update({ [peer]: this.clients[peer].state('counter://').count })
      const outcome = await handle.settled
      if (outcome.status !== 'confirmed') throw new Error(`Action ${outcome.status}. Reset the demo to try again.`)
      const { count } = await this.hub.get('counter://')
      this.update({
        hub: count, seq: this.hub.serverSeq,
        events: [{ seq: this.hub.serverSeq, from: `Client ${peer.toUpperCase()}`, name: 'counter/incremented' }, ...this.state.events].slice(0, 4),
        message: this.state.bConnected ? 'Committed by the hub. Both clients are in sync.' : 'Committed. Client B stays at its last known state until you reconnect.',
      })
    } catch (error) { this.fail(error) }
    finally { this.update({ busy: false, phase: 'idle' }) }
  }
  async toggleConnection() {
    if (!this.state.ready || this.state.busy) return
    this.update({ busy: true, error: null })
    try {
      if (this.state.bConnected) {
        await this.links.b.close()
        this.update({ bConnected: false, message: 'Client B is disconnected. Add a few changes on Client A, then reconnect.' })
      } else {
        this.update({ phase: 'replay', message: 'Reconnecting Client B from its last seen sequence…' })
        this.links.b = this.serve()
        const result = await this.clients.b.reconnect(this.links.b.transport)
        this.update({
          bConnected: true, b: this.clients.b.state('counter://').count,
          message: result.type === 'replay'
            ? `Replayed ${result.actions.length} missed action${result.actions.length === 1 ? '' : 's'}. Both clients are in sync.`
            : 'The replay window was exceeded. A fresh snapshot brought Client B back in sync.',
        })
      }
    } catch (error) { this.fail(error) }
    finally { this.update({ busy: false, phase: 'idle' }) }
  }
  private fail(error: unknown) {
    this.update({ error: error instanceof Error ? error.message : 'The demo could not connect. Reset it to try again.', busy: false })
  }
  async dispose() {
    this.disposed = true
    this.listeners.clear()
    await Promise.all(Object.values(this.clients).map(client => client.shutdown()))
    await Promise.all([...this.allLinks].map(link => link.close()))
    await this.hub.close()
  }
}
