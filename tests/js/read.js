#!/usr/bin/env node
// Append Rust-written blocks to a JS core and print hyperbee2's read stream.
'use strict'

const fs = require('fs')
const os = require('os')
const path = require('path')
const Corestore = require('corestore')
const Hyperbee = require('hyperbee2')

async function main() {
  const input = JSON.parse(fs.readFileSync(process.argv[2]))
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'hyperbee-rust-'))
  const store = new Corestore(dir, {
    primaryKey: Buffer.from(input.primaryKey, 'hex'),
    unsafe: true
  })
  const core = store.get({ name: 'bee' })
  await core.ready()
  await core.append(input.blocks.map((b) => Buffer.from(b, 'hex')))

  const bee = new Hyperbee(store)
  await bee.ready()

  const entries = []
  for await (const entry of bee.createReadStream()) {
    entries.push({
      seq: entry.seq,
      key: entry.key.toString('hex'),
      value: entry.value.toString('hex')
    })
  }
  process.stdout.write(JSON.stringify(entries))
  await store.close()
  fs.rmSync(dir, { recursive: true, force: true })
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
