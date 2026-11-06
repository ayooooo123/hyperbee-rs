//! Range iteration, ported from `lib/ranges.js`.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::inflate::{inflate, inflate_value};
use crate::tree::{KeyId, PtrId, TreeState};
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

struct Frame {
    node: PtrId,
    offset: usize,
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

    pub async fn next(&mut self) -> Result<Option<Entry>> {
        let inner = self.bee.inner.clone();
        let mut st = inner.state.lock().await;
        if !self.opened {
            self.open(&mut st).await?;
            self.opened = true;
        }
        let Some(key) = self.next_key(&mut st).await? else {
            return Ok(None);
        };
        let (seq, key, value) = {
            let k = &st.keys[key];
            let value = match &k.value {
                Some(v) => Some(v.clone()),
                None => inflate_value(&st, key).await?,
            };
            (k.seq, k.key.clone(), value)
        };
        Ok(Some(Entry {
            seq,
            key,
            value: value.unwrap_or_default(),
        }))
    }

    async fn open(&mut self, st: &mut TreeState) -> Result<()> {
        if self.limit == Some(0) {
            return Ok(());
        }
        let Some(root) = self.bee.root_ptr(st).await? else {
            return Ok(());
        };
        self.stack.push(Frame {
            node: root,
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
            let node = inflate(st, self.stack[top].node).await?;
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
            self.stack.push(Frame {
                node: next,
                offset: 0,
            });
            self.stack[top].offset = offset + 1;
        }
        Ok(())
    }

    async fn next_key(&mut self, st: &mut TreeState) -> Result<Option<KeyId>> {
        while !self.stack.is_empty() && self.limit != Some(0) {
            let mut top = self.stack.pop().expect("non-empty");
            let node = inflate(st, top.node).await?;
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
                    self.stack.push(Frame {
                        node: next,
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
