//! Range iteration, ported from `lib/ranges.js`.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::inflate::{inflate, inflate_value};
use crate::tree::{KeyId, Locked, MAX_DEPTH, NodeId, PtrRef};
use crate::{Bee, Entry, Error, Result};

/// Bounds for [`Bee::range`]. Both edges are inclusive or exclusive exactly as
/// in `createReadStream`.
#[derive(Clone, Debug, Default)]
pub struct RangeOptions {
    pub gt: Option<Bytes>,
    pub gte: Option<Bytes>,
    pub lt: Option<Bytes>,
    pub lte: Option<Bytes>,
    pub reverse: bool,
    /// `None` is unlimited, like JS `limit: -1`.
    pub limit: Option<usize>,
}

/// A frame keeps its node's location, because the tree lock is released
/// between `next` calls and the node cache may free the pointer meanwhile.
struct Frame {
    node: PtrRef,
    offset: usize,
}

async fn inflate_frame(st: &mut Locked, node: &mut PtrRef) -> Result<NodeId> {
    let mut ptr = st.resolve(node)?;
    let id = inflate(st, &mut ptr).await?;
    if ptr != node.id {
        *node = st.ptr_ref(ptr);
    }
    Ok(id)
}

pub struct RangeStream {
    bee: Bee,
    stack: Vec<Frame>,
    reverse: bool,
    limit: Option<usize>,
    start: Option<Bytes>,
    end: Option<Bytes>,
    inclusive: bool,
    compare: i32,
    opened: bool,
    read_options: crate::ReadOptions,
}

impl RangeStream {
    pub(crate) fn new(bee: Bee, opts: RangeOptions) -> Self {
        let start = opts.gte.clone().or(opts.gt.clone());
        let end = opts.lte.clone().or(opts.lt.clone());
        let inclusive = if opts.reverse {
            opts.lte.is_some()
        } else {
            opts.gte.is_some()
        };
        let compare = if if opts.reverse {
            opts.gte.is_some()
        } else {
            opts.lte.is_some()
        } {
            0
        } else {
            -1
        };
        Self {
            read_options: bee.read_options(),
            bee,
            stack: Vec::new(),
            reverse: opts.reverse,
            limit: opts.limit,
            start,
            end,
            inclusive,
            compare,
            opened: false,
        }
    }
    pub(crate) fn with_read_options(mut self, options: crate::ReadOptions) -> Self {
        self.read_options = options;
        self
    }

    pub async fn next(&mut self) -> Result<Option<Entry>> {
        let options = self.read_options.clone();
        crate::READ_OPTIONS.scope(options, self.next_inner()).await
    }

    async fn next_inner(&mut self) -> Result<Option<Entry>> {
        let inner = self.bee.inner.clone();
        let mut st = Locked::new(inner.state.clone()).await;
        if !self.opened {
            self.open(&mut st).await?;
            self.opened = true;
        }
        let Some(key) = self.next_key(&mut st).await? else {
            return Ok(None);
        };
        let (seq, key, value) = {
            let (seq, k) = (st.keys[key].seq, st.keys[key].key.clone());
            (seq, k, inflate_value(&mut st, key).await?)
        };
        Ok(Some(Entry {
            seq,
            key,
            value: value.unwrap_or_default(),
        }))
    }

    async fn open(&mut self, st: &mut Locked) -> Result<()> {
        if self.limit == Some(0) {
            return Ok(());
        }
        let Some(root) = self.bee.root_ptr(st)? else {
            return Ok(());
        };
        self.stack.push(Frame {
            node: st.ptr_ref(root),
            offset: 0,
        });
        if if self.reverse {
            self.end.is_none()
        } else {
            self.start.is_none()
        } {
            return Ok(());
        }

        let adjust = if self.inclusive { 0 } else { 1 };
        loop {
            let top = self.stack.len() - 1;
            let node = inflate_frame(st, &mut self.stack[top].node).await?;
            let (child_len, offset) = {
                let n = &st.nodes[node];
                let key_len = n.keys.length();
                let child_len = n.children.length();
                let mut offset = self.stack[top].offset;
                for i in 0..key_len {
                    let j = if self.reverse { key_len - 1 - i } else { i };
                    let kid = n.keys.get(j).ok_or(Error::Message("null key pointer"))?;
                    let c = if self.reverse {
                        cmp_end(&st.keys[kid].key, self.end.as_ref())
                    } else {
                        cmp_start(self.start.as_ref(), &st.keys[kid].key)
                    };
                    if c == Ordering::Less {
                        break;
                    }
                    offset = 2 * i + 1 + if c == Ordering::Equal { adjust } else { 1 };
                }
                (child_len, offset)
            };
            let child = offset & 1 == 0;
            let k = offset >> 1;
            if !child || k >= child_len {
                self.stack[top].offset = offset;
                break;
            }
            let j = if self.reverse { child_len - 1 - k } else { k };
            let next = st.nodes[node]
                .children
                .get(j)
                .ok_or(Error::Message("child index out of range"))?;
            if self.stack.len() >= MAX_DEPTH {
                return Err(Error::Message("tree too deep"));
            }
            self.stack.push(Frame {
                node: st.ptr_ref(next),
                offset: 0,
            });
            self.stack[top].offset = offset + 1;
        }
        Ok(())
    }

    async fn next_key(&mut self, st: &mut Locked) -> Result<Option<KeyId>> {
        while !self.stack.is_empty() && self.limit != Some(0) {
            let mut top = self.stack.pop().expect("non-empty");
            let node = inflate_frame(st, &mut top.node).await?;
            let offset = top.offset;
            top.offset += 1;
            let child = offset & 1 == 0;
            let k = offset >> 1;

            if child {
                self.stack.push(top);
                let child_len = st.nodes[node].children.length();
                if k < child_len {
                    let j = if self.reverse { child_len - 1 - k } else { k };
                    let next = st.nodes[node]
                        .children
                        .get(j)
                        .ok_or(Error::Message("child index out of range"))?;
                    if self.stack.len() >= MAX_DEPTH {
                        return Err(Error::Message("tree too deep"));
                    }
                    self.stack.push(Frame {
                        node: st.ptr_ref(next),
                        offset: 0,
                    });
                }
                continue;
            }

            let key_len = st.nodes[node].keys.length();
            if k < key_len {
                let j = if self.reverse { key_len - 1 - k } else { k };
                let kid = st.nodes[node]
                    .keys
                    .get(j)
                    .ok_or(Error::Message("null key pointer"))?;
                let c = if self.reverse {
                    cmp_start(self.start.as_ref(), &st.keys[kid].key)
                } else {
                    cmp_end(&st.keys[kid].key, self.end.as_ref())
                };
                let c = match c {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                };
                if c > self.compare {
                    break;
                }
                self.stack.push(top);
                if let Some(limit) = &mut self.limit {
                    *limit -= 1;
                }
                return Ok(Some(kid));
            }
        }
        Ok(None)
    }
}

fn cmp_start(start: Option<&Bytes>, key: &Bytes) -> Ordering {
    match start {
        Some(s) => s.as_ref().cmp(key.as_ref()),
        None => Ordering::Less,
    }
}

fn cmp_end(key: &Bytes, end: Option<&Bytes>) -> Ordering {
    match end {
        Some(e) => key.as_ref().cmp(e.as_ref()),
        None => Ordering::Less,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn node_cache_bounds_the_arenas_and_open_ranges_survive() {
        const KEYS: usize = 20_000;
        const MAX: usize = 8;
        let item = |i: usize| Bytes::from(format!("key/{i:05}"));
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        // Two batches: the range below walks the first root, the reads walk
        // the second, so the first root's node and pointer get freed.
        let half = KEYS / 2;
        let mut heads = Vec::new();
        for part in [0..half, half..KEYS] {
            let mut batch = writer.batch();
            for i in part {
                batch.put(item(i), &b"v"[..]);
            }
            batch.flush().await.unwrap();
            heads.push(writer.head().unwrap());
        }
        let first_head = heads[0];

        let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
        reader.inner.state.lock().await.max_nodes = MAX;
        let old = reader.checkout(first_head).await.unwrap();
        let mut range = old.range(RangeOptions::default());
        let mut seen = Vec::new();
        for _ in 0..10 {
            seen.push(range.next().await.unwrap().unwrap().key);
        }
        let frames: Vec<_> = range.stack.iter().map(|f| f.node.id).collect();

        let (mut nodes, mut ptrs, mut keys) = (0, 0, 0);
        for i in 0..KEYS {
            let entry = reader.get(&item(i)).await.unwrap().unwrap();
            assert_eq!(entry.key, item(i));
            let st = reader.inner.state.lock().await;
            nodes = nodes.max(st.nodes.len());
            ptrs = ptrs.max(st.ptrs.len());
            keys = keys.max(st.keys.len());
        }
        eprintln!("PEAK nodes={nodes} ptrs={ptrs} keys={keys}");
        // The cache, plus the node being installed.
        assert!(nodes <= MAX + 1, "{nodes} live nodes");
        // Live nodes hold at most 2T pointers and 2T - 1 keys each; up to MAX
        // dropped nodes wait for the next sweep.
        let per_node = 2 * crate::encoding::T as usize;
        assert!(ptrs <= (2 * MAX + 1) * per_node + 8, "{ptrs} live pointers");
        assert!(keys <= (2 * MAX + 1) * per_node, "{keys} live keys");

        {
            let st = reader.inner.state.lock().await;
            assert!(
                frames.iter().any(|id| !st.has_ptr(*id)),
                "the open range's pointers were freed"
            );
        }
        while let Some(entry) = range.next().await.unwrap() {
            seen.push(entry.key);
        }
        assert_eq!(seen, (0..half).map(item).collect::<Vec<_>>());
        store.close().await;
    }
}
