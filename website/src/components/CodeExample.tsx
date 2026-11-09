import { useEffect, useRef, useState } from 'react'
import { Check, Copy } from 'lucide-react'

const definition = `import * as v from "valibot";
import { durableChannel, durableRoutes } from "durable-channel";

export const counter = durableChannel()
  .state(v.object({ count: v.number() }), { count: 0 })
  .action((a) =>
    a.name("counter/incremented")
      .payload(v.object({ by: v.number() }))
      .client()
      .reduce((state, { by }) => ({
        count: state.count + by,
      }))
  )
  .build();

export const routes = durableRoutes()
  .route("counter://", counter).build();`
const hub = `import {
  DurableChannelHub, MemoryStorage,
} from "durable-channel";
import { routes } from "./counter.ts";

const hub = new DurableChannelHub(routes, {
  storage: new MemoryStorage(),
  env: {},
});

await hub.dispatch("counter://", "counter/incremented", { by: 1 });
console.log(await hub.get("counter://")); // { count: 1 }`
const client = `import {
  DurableChannelClient, WebSocketTransport,
} from "durable-channel";
import { routes } from "./counter.ts";

// Requires a hub exposed at your WebSocket endpoint.
const transport = await WebSocketTransport.connect(
  "ws://localhost:8000/rpc"
);
const client = new DurableChannelClient(routes, transport);
client.connect();
await client.hello({ subscriptions: ["counter://"] });

const action = client.dispatch(
  "counter://", "counter/incremented", { by: 1 }
);
await action.settled; // confirmed by the hub`
const examples = [
  { label: 'counter.ts', source: definition, note: 'A schema, an initial state, and the only way this counter can change.' },
  { label: 'server.ts', source: hub, note: 'Mount the definition at a URI. MemoryStorage is process-local; use persistent storage in production.' },
  { label: 'client.ts', source: client, note: 'Use the same routes on the client. This snippet requires your own WebSocket server; see the serving guide.' },
]

function Highlight({ code }: { code: string }) {
  return <>{code.split('\n').map((line, i) => <span className="code-line" key={i}><span className="line-number" aria-hidden="true">{i + 1}</span><span>{line.split(/("[^"\n]*"|\/\/.*$|\b(?:import|from|export|const|new|await|return)\b|\b\d+\b)/g).map((token, j) => <span className={token.startsWith('"') ? 'token-string' : token.startsWith('//') ? 'token-comment' : /^(import|from|export|const|new|await|return)$/.test(token) ? 'token-keyword' : /^\d+$/.test(token) ? 'token-number' : undefined} key={j}>{token}</span>)}</span></span>)}</>
}

export function CodeExample() {
  const [active, setActive] = useState(0)
  const [copied, setCopied] = useState(false)
  const [copyError, setCopyError] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const tabs = useRef<(HTMLButtonElement | null)[]>([])
  useEffect(() => () => clearTimeout(timer.current), [])
  async function copy() {
    try {
      await navigator.clipboard.writeText(examples[active].source)
      setCopied(true); setCopyError(false)
      clearTimeout(timer.current)
      timer.current = setTimeout(() => setCopied(false), 2000)
    } catch { setCopyError(true) }
  }
  return <div className="code-example">
    <div className="code-toolbar"><div role="tablist" aria-label="Quick start code files" className="code-tabs">{examples.map((example, i) => <button ref={node => { tabs.current[i] = node }} type="button" role="tab" aria-selected={active === i} aria-controls="code-panel" id={`code-tab-${i}`} tabIndex={active === i ? 0 : -1} key={example.label} onClick={() => { setActive(i); setCopied(false); setCopyError(false) }} onKeyDown={event => {
      if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
      event.preventDefault()
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? 2 : (active + (event.key === 'ArrowRight' ? 1 : 2)) % 3
      setActive(next); setCopied(false); setCopyError(false); tabs.current[next]?.focus()
    }}>{example.label}</button>)}</div><button className="copy-button" type="button" onClick={() => void copy()} aria-label={copied ? 'Code copied' : 'Copy code'}>{copied ? <Check size={15} /> : <Copy size={15} />}<span>{copied ? 'Copied' : 'Copy'}</span></button></div>
    <div role="tabpanel" id="code-panel" aria-labelledby={`code-tab-${active}`}><pre tabIndex={0} aria-label={`${examples[active].label} source code`}><code><Highlight code={examples[active].source} /></code></pre></div>
    <div className="code-note" role="status">{copyError ? 'Clipboard unavailable. Select the code above and copy it manually.' : examples[active].note}</div>
  </div>
}
