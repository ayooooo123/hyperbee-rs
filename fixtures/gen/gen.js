#!/usr/bin/env node
// Writes reference bees with npm hyperbee2, one JSON fixture per scenario.
'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee2')
const { encodeBlock } = require('hyperbee2/lib/encoding.js')

const OUT = path.join(__dirname, '..')

function mulberry32(seed) {
  let a = seed >>> 0
  return function () {
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function key(i) {
  return Buffer.from('key-' + String(i).padStart(4, '0'))
}

function value(i) {
  return Buffer.from('value-' + String(i).padStart(4, '0'))
}

function bigValue(i, len) {
  const out = Buffer.alloc(len)
  out.fill((i + 1) % 251)
  out.write('big-' + i, 0)
  return out
}

function hex(b) {
  return Buffer.from(b).toString('hex')
}

function put(k, v) {
  return ['put', hex(k), hex(v)]
}

function del(k) {
  return ['del', hex(k)]
}

function scenarios() {
  const list = []

  list.push({ name: 'empty', seed: 1, batches: [] })
  list.push({
    name: 'single',
    seed: 2,
    batches: [[put(Buffer.from('a'), Buffer.from('1'))]]
  })

  {
    const rand = mulberry32(3)
    const keys = []
    for (let i = 0; i < 320; i++) keys.push(i)
    for (let i = keys.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1))
      ;[keys[i], keys[j]] = [keys[j], keys[i]]
    }
    const batches = []
    for (let b = 0; b < 4; b++) {
      const ops = []
      for (let i = b * 80; i < (b + 1) * 80; i++) {
        ops.push(put(key(keys[i]), value(keys[i])))
      }
      batches.push(ops)
    }
    list.push({ name: 'random-320', seed: 3, batches })
  }

  {
    const batches = []
    const ops = []
    for (let i = 0; i < 400; i++) ops.push(put(key(i), value(i)))
    for (let b = 0; b < 4; b++) batches.push(ops.slice(b * 100, (b + 1) * 100))

    let next = 0
    let remaining = 400
    while (remaining > 5) {
      const ops = []
      const count = Math.min(remaining - 5, 97)
      for (let i = 0; i < count; i++) ops.push(del(key(next++)))
      remaining -= count
      batches.push(ops)
    }
    list.push({ name: 'deletes-to-five', seed: 4, batches })
  }

  {
    const batches = []
    for (let b = 0; b < 2; b++) {
      const ops = []
      for (let i = b * 100; i < (b + 1) * 100; i++) ops.push(put(key(i), value(i)))
      batches.push(ops)
    }
    const ops = []
    for (let i = 0; i < 100; i++) ops.push(put(key(i * 2), value(1000 + i)))
    batches.push(ops)
    const more = []
    for (let i = 0; i < 40; i++) more.push(put(key(i * 5), bigValue(i, 1100 + i * 7)))
    batches.push(more)
    list.push({ name: 'overwrites', seed: 5, batches })
  }

  {
    const ops = [put(Buffer.from('small'), Buffer.from('v'))]
    ops.push(put(Buffer.from('big'), bigValue(1, 10000)))
    for (let i = 0; i < 12; i++) {
      ops.push(put(Buffer.from('v-' + i), bigValue(i, 1020 + i * 3)))
    }
    ops.push(put(Buffer.from('exactly-inline'), bigValue(2, 1024)))
    ops.push(put(Buffer.from('just-over'), bigValue(3, 1025)))
    list.push({ name: 'large-values', seed: 6, batches: [ops] })
  }

  {
    const prefix = Buffer.alloc(64, 0xab)
    const batches = []
    for (let b = 0; b < 3; b++) {
      const ops = []
      for (let i = b * 100; i < (b + 1) * 100; i++) {
        const k = Buffer.concat([prefix, Buffer.from(String(i).padStart(3, '0'))])
        ops.push(put(k, value(i)))
      }
      batches.push(ops)
    }
    list.push({ name: 'prefixes', seed: 7, batches })
  }

  {
    const k1 = Buffer.from('same-key')
    const k2 = Buffer.from('other-key')
    const batches = [
      [
        put(k1, Buffer.from('a')),
        del(k1),
        put(k1, Buffer.from('b')),
        del(Buffer.from('missing')),
        put(k1, Buffer.from('c')),
        put(k2, Buffer.from('x')),
        put(k2, Buffer.from('x')),
        put(k2, Buffer.from('y'))
      ],
      [del(k1), put(k1, Buffer.from('d')), del(k2), del(k2)]
    ]
    list.push({ name: 'same-batch-upsert', seed: 8, batches })
  }

  {
    const batches = []
    for (let b = 0; b < 5; b++) {
      const ops = []
      for (let i = b * 80; i < (b + 1) * 80; i++) {
        const k = Buffer.from('k/' + String(i).padStart(5, '0') + '/' + 'x'.repeat(40))
        ops.push(put(k, i % 7 === 0 ? bigValue(i, 4200) : value(i)))
      }
      batches.push(ops)
    }
    const dels = []
    for (let i = 0; i < 300; i += 2) {
      const k = Buffer.from('k/' + String(i).padStart(5, '0') + '/' + 'x'.repeat(40))
      dels.push(del(k))
    }
    batches.push(dels)
    list.push({ name: 'mixed-large-delete', seed: 9, batches })
  }

  list.push({ name: 'multi-core', seed: 10, batches: [], multi: true })

  list.push({
    name: 'compat',
    seed: 11,
    compat: true,
    batches: [
      [put(Buffer.from('a'), Buffer.from('1')), put(Buffer.from('b'), Buffer.from('2'))],
      [put(Buffer.from('c'), Buffer.from('3'))],
      [del(Buffer.from('a')), put(Buffer.from('d'), Buffer.from('4'))]
    ]
  })

  list.push({ name: 'block-0', seed: 12, batches: [], block0: true })

  {
    const batches = []
    for (let b = 0; b < 10; b++) {
      const ops = []
      for (let i = b * 300; i < (b + 1) * 300; i++) {
        ops.push(put(key(i), value(i)))
      }
      batches.push(ops)
    }
    let nextKey = 0
    let remaining = 3000
    while (remaining > 60) {
      const ops = []
      const count = Math.min(remaining - 60, 500)
      for (let i = 0; i < count; i++) ops.push(del(key(nextKey++)))
      remaining -= count
      batches.push(ops)
    }
    list.push({ name: 'deep-3000', seed: 13, batches })
  }

  list.push({ name: 'mappings', seed: 15, batches: [], mapped: true })

  {
    const batches = []
    for (let b = 0; b < 70; b++) {
      const ops = []
      for (let i = b * 1000; i < (b + 1) * 1000; i++) {
        const k = Buffer.from('p/' + String(i).padStart(6, '0'))
        ops.push(put(k, Buffer.from([i % 251])))
      }
      batches.push(ops)
    }
    list.push({ name: 'parent-split', seed: 14, batches })
  }

  return list
}

function fixtureSeed(seed) {
  const primaryKey = Buffer.alloc(32)
  primaryKey.writeUInt32BE(seed, 28)
  primaryKey[0] = 0x42
  return primaryKey
}

async function runMulti(scenario, dir) {
  const primaryKey = fixtureSeed(scenario.seed)
  const store = new Corestore(dir, { primaryKey, unsafe: true })

  const coreA = store.get({ name: 'a' })
  const a = new Hyperbee(store, { core: coreA })
  await a.ready()
  let batch = a.write()
  batch.tryPut(Buffer.from('a'), Buffer.from('1'))
  batch.tryPut(Buffer.from('big'), bigValue(9, 3000))
  await batch.flush()
  const aMain = coreA.length - 1

  const coreB = store.get({ name: 'b' })
  const b = new Hyperbee(store, { core: coreB })
  await b.ready()
  batch = b.write()
  batch.tryPut(Buffer.from('b'), Buffer.from('2'))
  await batch.flush()

  const coreC = store.get({ name: 'c' })
  await coreC.ready()
  const block = {
    type: 1,
    checkpoint: 1,
    batch: { start: 0, end: 0 },
    previous: null,
    metadata: {
      cores: [
        { key: coreA.key, fork: 0, length: 0, treeHash: null },
        { key: coreB.key, fork: 0, length: 0, treeHash: null }
      ],
      mappings: null
    },
    tree: [
      {
        keys: [{ type: 3, index: 0, pointer: { core: 0, seq: 0, offset: 0 } }],
        children: []
      }
    ],
    keys: null,
    values: null,
    cohorts: [
      [
        { type: 1, index: 0, pointer: { core: 1, seq: aMain, offset: 0 } },
        { type: 1, index: 1, pointer: { core: 2, seq: 0, offset: 0 } },
        { type: 1, index: 2, pointer: { core: 1, seq: aMain, offset: 1 } }
      ]
    ],
    t: 128,
    invalidation: 1
  }
  await coreC.append([encodeBlock(block)])

  const view = new Hyperbee(store, { core: coreC })
  await view.ready()
  const entries = []
  for await (const entry of view.createReadStream()) {
    entries.push({ seq: entry.seq, key: hex(entry.key), value: hex(entry.value) })
  }

  const blocksOf = async (core) => {
    const blocks = []
    for (let i = 0; i < core.length; i++) blocks.push(hex(await core.get(i)))
    return blocks
  }

  const fixture = {
    name: scenario.name,
    primaryKey: hex(primaryKey),
    rootKey: hex(coreC.key),
    replayable: false,
    batches: [],
    cores: [
      { name: 'a', key: hex(coreA.key), blocks: await blocksOf(coreA) },
      { name: 'b', key: hex(coreB.key), blocks: await blocksOf(coreB) },
      { name: 'c', key: hex(coreC.key), blocks: await blocksOf(coreC) }
    ],
    entries
  }

  fs.writeFileSync(path.join(OUT, scenario.name + '.json'), JSON.stringify(fixture))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })

  console.log(
    `${scenario.name}: ${fixture.cores.length} cores, ${fixture.entries.length} entries`
  )
}

async function runBlock0(scenario, dir) {
  const primaryKey = fixtureSeed(scenario.seed)
  const store = new Corestore(dir, { primaryKey, unsafe: true })
  const core = store.get({ name: 'bee' })
  await core.ready()
  const block = {
    type: 0,
    checkpoint: 0,
    batch: { start: 0, end: 0 },
    previous: null,
    tree: [{ keys: [{ pointer: { offset: 0 } }], children: [] }],
    keys: [
      { key: Buffer.from('alpha'), value: Buffer.from('1') },
      { key: Buffer.from('beta'), value: Buffer.from('2') }
    ],
    cohorts: [
      [
        { pointer: { core: 0, seq: 0, offset: 0 } },
        { pointer: { core: 0, seq: 0, offset: 1 } }
      ]
    ],
    metadata: { cores: [], mappings: null }
  }
  await core.append([encodeBlock(block)])

  const view = new Hyperbee(store, { core })
  await view.ready()
  const entries = []
  for await (const entry of view.createReadStream()) {
    entries.push({ seq: entry.seq, key: hex(entry.key), value: hex(entry.value) })
  }

  const fixture = {
    name: scenario.name,
    primaryKey: hex(primaryKey),
    rootKey: hex(core.key),
    replayable: false,
    batches: [],
    cores: [{ name: 'bee', key: hex(core.key), blocks: [hex(await core.get(0))] }],
    entries
  }
  fs.writeFileSync(path.join(OUT, scenario.name + '.json'), JSON.stringify(fixture))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })
  console.log(`${scenario.name}: type-0 block, ${entries.length} entries`)
}

async function runMapped(scenario, dir) {
  const primaryKey = fixtureSeed(scenario.seed)
  const store = new Corestore(dir, { primaryKey, unsafe: true })

  const mapped = store.get({ name: 'mapped' })
  const bee = new Hyperbee(store, { core: mapped })
  await bee.ready()
  const batch = bee.write()
  batch.tryPut(Buffer.from('m'), Buffer.from('9'))
  await batch.flush()

  const oldKey = Buffer.alloc(32, 0x77)
  const root = store.get({ name: 'root' })
  await root.ready()
  const block = {
    type: 1,
    checkpoint: 1,
    batch: { start: 0, end: 0 },
    previous: null,
    metadata: {
      cores: [{ key: oldKey, fork: 0, length: 0, treeHash: null }],
      mappings: [{ from: oldKey, to: mapped.key }]
    },
    tree: [
      {
        keys: [{ type: 3, index: 0, pointer: { core: 0, seq: 0, offset: 0 } }],
        children: []
      }
    ],
    keys: null,
    values: null,
    cohorts: [[{ type: 1, index: 0, pointer: { core: 1, seq: 0, offset: 0 } }]],
    t: 128,
    invalidation: 1
  }
  await root.append([encodeBlock(block)])

  const view = new Hyperbee(store, { core: root })
  await view.ready()
  const entries = []
  for await (const entry of view.createReadStream()) {
    entries.push({ seq: entry.seq, key: hex(entry.key), value: hex(entry.value) })
  }
  if (entries.length !== 1) throw new Error('mapping fixture did not resolve')

  const fixture = {
    name: scenario.name,
    primaryKey: hex(primaryKey),
    rootKey: hex(root.key),
    replayable: false,
    batches: [],
    cores: [
      { name: 'mapped', key: hex(mapped.key), blocks: [hex(await mapped.get(0))] },
      { name: 'root', key: hex(root.key), blocks: [hex(await root.get(0))] }
    ],
    entries
  }
  fs.writeFileSync(path.join(OUT, scenario.name + '.json'), JSON.stringify(fixture))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })
  console.log(`${scenario.name}: mapped core, ${entries.length} entry`)
}

async function run(scenario) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hyperbee-fixture-'))
  const primaryKey = fixtureSeed(scenario.seed)
  if (scenario.multi) return runMulti(scenario, dir)
  if (scenario.mapped) return runMapped(scenario, dir)
  if (scenario.block0) return runBlock0(scenario, dir)

  const store = new Corestore(dir, { primaryKey, unsafe: true })
  const bee = new Hyperbee(store)
  await bee.ready()

  for (const ops of scenario.batches) {
    const batch = bee.write({ compat: scenario.compat === true })
    for (const op of ops) {
      if (op[0] === 'put') batch.tryPut(Buffer.from(op[1], 'hex'), Buffer.from(op[2], 'hex'))
      else batch.tryDelete(Buffer.from(op[1], 'hex'))
    }
    await batch.flush()
  }

  const blocks = []
  for (let i = 0; i < bee.core.length; i++) {
    const block = await bee.core.get(i)
    blocks.push(hex(block))
  }

  const entries = []
  for await (const entry of bee.createReadStream()) {
    entries.push({ seq: entry.seq, key: hex(entry.key), value: hex(entry.value) })
  }

  const fixture = {
    name: scenario.name,
    primaryKey: hex(primaryKey),
    coreKey: hex(bee.core.key),
    replayable: scenario.compat !== true,
    batches: scenario.batches,
    blocks,
    entries
  }

  fs.writeFileSync(path.join(OUT, scenario.name + '.json'), JSON.stringify(fixture))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })

  console.log(
    `${scenario.name}: ${fixture.batches.length} batches, ${blocks.length} blocks, ${entries.length} entries`
  )
}

;(async () => {
  for (const scenario of scenarios()) await run(scenario)
})().catch((err) => {
  console.error(err)
  process.exit(1)
})
