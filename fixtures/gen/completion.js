'use strict'
const fs = require('fs')
const os = require('os')
const path = require('path')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee2')
const hex = b => Buffer.from(b).toString('hex')

async function main () {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bee-completion-'))
  const primaryKey = Buffer.alloc(32, 41)
  const store = new Corestore(dir, { primaryKey, unsafe: true })
  const a = store.get({ name: 'a' })
  const b = store.get({ name: 'b' })
  const bee = new Hyperbee(store, { core: a })
  const other = new Hyperbee(store, { core: b })
  await Promise.all([bee.ready(), other.ready()])
  let batch = bee.write()
  batch.tryPut(Buffer.from('local'), Buffer.from('one'))
  await batch.flush()
  batch = other.write()
  batch.tryPut(Buffer.from('remote'), Buffer.from('two'))
  await batch.flush()
  const remoteHead = { key: hex(b.key), length: b.length }
  bee.move({ key: b.key, length: b.length })
  batch = bee.write()
  batch.tryPut(Buffer.from('after'), Buffer.from('three'))
  await batch.flush()
  const changes = []
  for await (const c of bee.createChangesStream()) {
    changes.push({ head: { key: hex(c.head.key), length: c.head.length }, tail: c.tail && { key: hex(c.tail.key), length: c.tail.length }, keys: c.batch.map(block => (block.keys || []).map(k => hex(k.key))) })
  }
  const blocks = async core => {
    const result = []
    for (let i = 0; i < core.length; i++) result.push(hex(await core.get(i)))
    return result
  }
  const movedBlocks = await blocks(a)
  const cores = (await bee.cores()).map(hex)
  const count = await bee.reindex(() => false)
  const reindexedBlocks = await blocks(a)
  const mappedCount = await bee.reindex(() => false, { map: (key, value) => Buffer.concat([value, Buffer.from('!')]) })
  const fixture = { primaryKey: hex(primaryKey), a: hex(a.key), b: hex(b.key), remoteHead, movedBlocks, changes, cores, count, reindexedBlocks, mappedCount, mappedBlocks: await blocks(a) }
  fs.writeFileSync(path.join(__dirname, 'completion.json'), JSON.stringify(fixture))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })
}
main().catch(err => { console.error(err); process.exitCode = 1 })
