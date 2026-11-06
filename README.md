# hyperbee-rs

A Rust port of the npm `hyperbee2` B-tree (reference version 2.21.1) on top of
[`hypercore-rs`](../hypercore-rs) and [`corestore-rs`](../corestore-rs).

The reader accepts every block type hyperbee2 2.21.1 reads (latest block type 1,
legacy block type 0 and bee1 compat blocks). The writer produces byte-identical
blocks to hyperbee2's default (`type 1`) writer for the same batch sequence,
including the same splits, merges, cohorts, value blocks and checkpoint/
invalidation fields.

## Usage

```rust
# async fn demo() -> Result<(), Box<dyn std::error::Error>> {
use corestore::Corestore;
use hyperbee::Bee;

let store = Corestore::open(std::path::Path::new("./db"))?;
let bee = Bee::open(&store, None).await?; // core name "bee"

let mut batch = bee.batch();
batch.put(&b"name"[..], &b"example"[..]);
batch.flush().await?;

let entry = bee.get(b"name").await?.unwrap();
assert_eq!(entry.value, &b"example"[..]);

let mut stream = bee.range(Default::default());
while let Some(entry) = stream.next().await? {
    println!("{:?}", entry.key);
}
# Ok(())
# }
```

## API

- `Bee::open(&Corestore, Option<[u8; 32]>)`: opens the core with that key, or
  the core named `bee`.
- `Bee::head() -> Option<Head>`: root block count and root core key.
- `Bee::get(&[u8]) -> Result<Option<Entry>>`.
- `Bee::range(RangeOptions) -> RangeStream`: `gt`, `gte`, `lt`, `lte`,
  `reverse`, `limit`; call `next()` until it returns `None`.
- `Bee::batch() -> Batch`: `put`, `del`, then `flush()`. Ops apply in call
  order; a put followed by a del in one batch leaves the key deleted.
- `Bee::checkout(length) -> Bee`: read-only snapshot at a root-core length.
- `Bee::update() -> Result<bool>`: move to the root core's latest block.

`Entry` carries the data block sequence number, key and value. Blocks that
reference other cores (the `metadata.cores` table) are resolved through the
same `Corestore`, as in the JS implementation.

## Compatibility notes

- Block type 0 is decoded, including its cohort layout.
- Bee1 `TYPE_COMPAT` (10) blocks are decoded.
- Only latest blocks are written. There is no API to request compat or type 0
  writes, matching the default hyperbee2 writer.
- Values larger than 1024 bytes are stored in separate value blocks exactly as
  hyperbee2 does; value pointers, offsets and block splitting follow the JS
  rules.
- The in-memory tree uses append-only arenas; the JS node cache's LRU eviction
  and its tuning knobs are not ported (nodes stay resident for the lifetime of
  the `Bee`).

## Not ported

- Diff streams (`createDiffStream`) and changes streams (`createChangesStream`).
- Copying or moving a tree between cores (`copy`, `move`, `reindex`). Reading
  trees whose blocks point into other cores is supported.
- Downloader, preload hooks, request cancellation/timeouts/tracing, encryption
  providers beyond what hypercore already handles.
- Node cache eviction/tuning, range prefetching, rollback (`unbatch`).
- Mappings can be read from block metadata, but there is no write API that
  supplies new mappings.

## Fixtures and tests

Generated fixtures live in `fixtures/`; `fixtures/gen/gen.js` writes them with
npm `hyperbee2 2.21.1`, `corestore 7.16.1` and `hypercore 11.40.3`:

```sh
cd fixtures/gen && npm install && node gen.js
```

- `tests/read_js.rs`: appends the reference blocks and compares `get` and
  range results with hyperbee2.
- `tests/write_js.rs`: replays every batch sequence and compares every appended
  block byte for byte.
- `tests/js_reads_rust.rs` (`#[ignore]`): Rust writes a fresh bee, a node script
  appends its blocks and prints hyperbee2's read stream for comparison. Run it
  with `cargo test -- --include-ignored` after `npm install` in `tests/js`.
- `tests/smoke.rs`: single-key block byte check.

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test -- --include-ignored
```
