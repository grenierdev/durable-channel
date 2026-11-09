export type Peer = 'a' | 'b'
export type DemoState = {
  ready: boolean
  busy: boolean
  a: number
  b: number
  hub: number
  seq: number
  bConnected: boolean
  phase: 'idle' | 'a' | 'b' | 'replay'
  message: string
  error: string | null
  events: { seq: number; from: string; name: string }[]
}
export const initialDemoState: DemoState = {
  ready: false, busy: false, a: 0, b: 0, hub: 0, seq: 0,
  bConnected: true, phase: 'idle', message: 'Starting the shared counter…', error: null, events: [],
}

