import { ArrowRight, Braces, CircleDot, Database, RadioTower, Route, ServerCog } from 'lucide-react'

type Visual = 'definition' | 'commit' | 'effects' | 'auth' | 'routes' | 'transport' | 'schema'

const configs: Record<Visual, { title: string; nodes: { label: string; detail: string }[] }> = {
  definition: { title: 'One definition, every runtime', nodes: [
    { label: 'State', detail: 'schema + initial' }, { label: 'Actions', detail: 'pure reducers' }, { label: 'Commands', detail: 'server work' }, { label: 'Notifications', detail: 'live only' },
  ] },
  commit: { title: 'One ordered commit path', nodes: [
    { label: 'Validate', detail: 'payload' }, { label: 'Reduce', detail: 'next state' }, { label: 'Persist', detail: 'state + seq' }, { label: 'Broadcast', detail: 'envelope' },
  ] },
  effects: { title: 'Commit first, work after', nodes: [
    { label: 'Action', detail: 'pure change' }, { label: 'Commit', detail: 'durable point' }, { label: 'Effect', detail: 'server side' }, { label: 'Background', detail: 'detached task' },
  ] },
  auth: { title: 'Identity enters through the trusted host', nodes: [
    { label: 'Session', detail: 'verify credentials' }, { label: 'Principal', detail: 'typed identity' }, { label: 'Connection', detail: 'host attached' }, { label: 'Policy', detail: 'allow operations' },
  ] },
  routes: { title: 'Templates become typed instances', nodes: [
    { label: 'doc:/:id', detail: 'route family' }, { label: 'doc:/42', detail: 'instance' }, { label: 'hub.of()', detail: 'typed handle' }, { label: 'internal', detail: 'server only' },
  ] },
  transport: { title: 'The core stays wire-agnostic', nodes: [
    { label: 'Client', detail: 'decoded frames' }, { label: 'Transport', detail: 'send / recv / close' }, { label: 'RPC', detail: 'JSON-RPC 2.0' }, { label: 'Storage', detail: 'state + sequence' },
  ] },
  schema: { title: 'Mounted routes become a document', nodes: [
    { label: 'Routes', detail: 'templates' }, { label: 'Definitions', detail: 'members' }, { label: 'JSON Schema', detail: 'input + output' }, { label: 'Document', detail: 'tooling contract' },
  ] },
}

const icons = [Braces, CircleDot, ServerCog, RadioTower, Route, Database]

export function GuideVisual({ visual }: { visual: Visual }) {
  const config = configs[visual]
  return <figure className="guide-visual">
    <figcaption>{config.title}</figcaption>
    <div className="guide-visual-flow">
      {config.nodes.map((node, index) => {
        const Icon = icons[index]
        return <div className="guide-visual-step" key={node.label}>
          <div><Icon size={17} aria-hidden="true" /><span><strong>{node.label}</strong><small>{node.detail}</small></span></div>
          {index < config.nodes.length - 1 && <ArrowRight className="guide-visual-arrow" size={17} aria-hidden="true" />}
        </div>
      })}
    </div>
  </figure>
}
