import { test } from 'node:test'
import assert from 'node:assert/strict'
import { SyncDemo } from '../src/lib/demo'

test('real clients apply optimistic state and converge on the authoritative commit', async () => {
  const demo = new SyncDemo(20)
  try {
    await demo.start()
    const update = demo.increment('a')
    assert.equal(demo.snapshot.a, 1, 'origin changes before the roundtrip')
    assert.equal(demo.snapshot.hub, 0, 'hub has not committed yet')
    assert.equal(demo.snapshot.b, 0)
    await update
    assert.deepEqual([demo.snapshot.a, demo.snapshot.hub, demo.snapshot.b], [1, 1, 1])
    await demo.increment('b')
    assert.deepEqual([demo.snapshot.a, demo.snapshot.hub, demo.snapshot.b], [2, 2, 2])
    assert.equal(demo.snapshot.seq, 2)
  } finally { await demo.dispose() }
})

test('disconnected client retains state and catches up through real replay', async () => {
  const demo = new SyncDemo(0)
  try {
    await demo.start()
    await demo.toggleConnection()
    await demo.increment('a')
    await demo.increment('a')
    assert.equal(demo.snapshot.b, 0)
    assert.equal(demo.snapshot.hub, 2)
    await demo.increment('b')
    assert.equal(demo.snapshot.seq, 2, 'offline client cannot dispatch')
    await demo.toggleConnection()
    assert.equal(demo.snapshot.b, 2)
    assert.match(demo.snapshot.message, /Replayed 2 missed actions/)
    assert.equal(demo.snapshot.error, null)
  } finally { await demo.dispose() }
})

test('reconnect falls back to a snapshot when the replay ring no longer covers the gap', async () => {
  const demo = new SyncDemo(0, 1)
  try {
    await demo.start()
    await demo.toggleConnection()
    await demo.increment('a')
    await demo.increment('a')
    await demo.toggleConnection()
    assert.equal(demo.snapshot.b, 2)
    assert.match(demo.snapshot.message, /fresh snapshot/)
    assert.equal(demo.snapshot.error, null)
  } finally { await demo.dispose() }
})

test('disposal during startup closes pending connections', async () => {
  const demo = new SyncDemo(5)
  const starting = demo.start().catch(() => undefined)
  await demo.dispose()
  await starting
  assert.equal(demo.snapshot.ready, false)
})
