//! Write batches, ported from `lib/write.js`.

use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{
    Block, EncDelta, KeyData, Loc, Metadata, OP_COHORT, RawTree, T, TYPE_LATEST, ValuePtr,
};
use crate::inflate::{inflate, inflate_value};
use crate::tree::{CompressedArray, Delta, DeltaRef, KeyId, Node, PtrId, TreeState, ValuePointer};
use crate::{Bee, Error, Result};

const PREFERRED_BLOCK_SIZE: usize = 4096;
const INLINE_VALUE_SIZE: usize = 1024;
const ESTIMATED_POINTER_SIZE: usize = 12;
const DELTA_MAX: usize = 16;
const DELTA_MIN: usize = 1;

pub(crate) enum Op {
    Put(Bytes, Bytes),
    Del(Bytes),
}

/// A write batch. Ops apply in call order when [`Batch::flush`] runs.
pub struct Batch {
    bee: Bee,
    ops: Vec<Op>,
}

impl Batch {
    pub(crate) fn new(bee: Bee) -> Self {
        Self {
            bee,
            ops: Vec::new(),
        }
    }

    pub fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.ops.push(Op::Put(key.into(), value.into()));
    }

    pub fn del(&mut self, key: impl Into<Bytes>) {
        self.ops.push(Op::Del(key.into()));
    }

    pub async fn flush(mut self) -> Result<()> {
        if !self.bee.writable() {
            return Err(Error::NotWritable);
        }
        let inner = self.bee.inner.clone();
        let mut st = inner.state.lock().await;
        let result = self.flush_inner(&mut st).await;
        st.flush_cache();
        result
    }

    async fn flush_inner(&mut self, st: &mut TreeState) -> Result<()> {
        let ctx = self.bee.context().clone();
        ctx.update(true).await?;

        let mut root = self.bee.root_ptr(st).await?;
        let length = root.map(|p| st.ptrs[p].seq + 1).unwrap_or(0);

        if root.is_none() {
            let a = st.array_id();
            let b = st.array_id();
            let node = st.nodes.len();
            st.nodes.push(Node {
                t: T,
                keys: CompressedArray::empty(a),
                children: CompressedArray::empty(b),
            });
            root = Some(st.create_ptr(ctx.clone(), 0, 0, 0, true, Some(node))?);
        }
        let mut root = root.expect("root present");

        let mut applied = 0usize;
        for op in &self.ops {
            let changed = match op {
                Op::Put(k, v) => put(st, &ctx, &mut root, k.clone(), v.clone()).await?,
                Op::Del(k) => delete(st, &ctx, &mut root, k).await?,
            };
            if changed {
                applied += 1;
            }
        }

        root = self.flush_write(st, &ctx, root, length, applied).await?;

        let new_length = st.ptrs[root].seq + 1;
        self.bee
            .set_root_ptr(st, Some(root), new_length, *ctx.core.key());
        Ok(())
    }

    async fn flush_write(
        &self,
        st: &mut TreeState,
        ctx: &Arc<Context>,
        mut root: PtrId,
        length: u64,
        applied: usize,
    ) -> Result<PtrId> {
        if !st.ptrs[root].changed {
            return Ok(root);
        }
        if st.ptrs[root].value.is_none() {
            inflate(st, root).await?;
        }

        let mut values: Vec<KeyId> = Vec::new();
        let mut updates: Vec<Update> = vec![Update::default()];

        root = commit(st, root)?;
        let t = st.nodes[st.ptrs[root].value.expect("committed")].t;
        let mut stack = vec![root];

        while let Some(node_ptr) = stack.pop() {
            if updates.last().expect("one update").size >= PREFERRED_BLOCK_SIZE {
                updates.push(Update::default());
            }
            let ui = updates.len() - 1;
            updates[ui].nodes.push(node_ptr);
            updates[ui].size += est_node_size(st, node_ptr);

            let node_id = st.ptrs[node_ptr].value.expect("committed node");
            let key_len = st.nodes[node_id].keys.length();
            let child_len = st.nodes[node_id].children.length();

            for i in 0..key_len {
                let kid = st.nodes[node_id]
                    .keys
                    .entries
                    .get(i)
                    .copied()
                    .flatten()
                    .ok_or(Error::Message("null key pointer"))?;
                if !st.keys[kid].changed {
                    continue;
                }
                if !should_inline(st, kid) {
                    values.push(kid);
                    st.keys[kid].value_ptr = Some(ValuePointer {
                        context: ctx.clone(),
                        core: 0,
                        seq: 0,
                        offset: 0,
                        split: 0,
                    });
                }
                updates[ui].keys.push(kid);
                let extra = usize::from(st.keys[kid].value_ptr.is_some()) * ESTIMATED_POINTER_SIZE;
                updates[ui].size += st.keys[kid].key.len() + extra;
            }

            for i in 0..child_len {
                let child = st.nodes[node_id]
                    .children
                    .entries
                    .get(i)
                    .copied()
                    .flatten()
                    .ok_or(Error::Message("null child pointer"))?;
                if st.ptrs[child].value.is_none() || !st.ptrs[child].changed {
                    continue;
                }
                let committed = commit(st, child)?;
                st.nodes[node_id].children.entries[i] = Some(committed);
                let array_id = st.nodes[node_id].children.id;
                if st.ptrs[committed].changed_by != Some(array_id) {
                    st.nodes[node_id].children.set(i, committed);
                    st.ptrs[committed].changed_by = Some(array_id);
                }
                stack.push(committed);
            }
        }

        if applied == 0 && length > 0 {
            return Ok(root);
        }

        if !values.is_empty() {
            let mut update = Update::default();
            for (i, kid) in values.iter().enumerate() {
                update.size += st.keys[*kid].value.as_ref().map_or(0, Bytes::len);
                update.values.push(*kid);
                if i == values.len() - 1 || update.size >= PREFERRED_BLOCK_SIZE {
                    updates.push(update);
                    update = Update::default();
                }
            }
        }

        let n = updates.len();
        let mut blocks: Vec<Option<PendingBlock>> = (0..n).map(|_| None).collect();
        for (i, update) in updates.iter().enumerate() {
            let seq = length + n as u64 - 1 - i as u64;
            let mut pending_keys = Vec::new();
            let mut pending_trees = Vec::new();
            let mut pending_cohorts = Vec::new();
            let mut block = Block {
                type_: TYPE_LATEST,
                seq,
                checkpoint: 0,
                batch: crate::encoding::BatchPtr {
                    start: n as u64 - 1 - i as u64,
                    end: i as u64,
                },
                previous: None,
                metadata: None,
                tree: Vec::new(),
                keys: Vec::new(),
                values: Vec::new(),
                cohorts: Vec::new(),
                t,
                invalidation: ctx.invalidation(),
            };

            for kid in &update.values {
                let key = &mut st.keys[*kid];
                let vp = key
                    .value_ptr
                    .as_mut()
                    .ok_or(Error::Message("missing value pointer"))?;
                vp.core = 0;
                vp.context = ctx.clone();
                vp.seq = seq;
                vp.offset = block.values.len() as u64;
                vp.split = 0;
                block.values.push(key.value.take().unwrap_or_default());
            }

            for kid in &update.keys {
                let key = &mut st.keys[*kid];
                key.core = 0;
                key.context = ctx.clone();
                key.seq = seq;
                key.offset = pending_keys.len() as u64;
                if let Some(vp) = &mut key.value_ptr {
                    vp.core = ctx.get_core_offset_local(&vp.context, vp.core)?;
                    vp.context = ctx.clone();
                }
                key.changed = false;
                pending_keys.push(*kid);
            }

            for ptr in &update.nodes {
                let node_id = st.ptrs[*ptr].value.expect("committed node");
                st.ptrs[*ptr].core = 0;
                st.ptrs[*ptr].context = ctx.clone();
                st.ptrs[*ptr].seq = seq;
                st.ptrs[*ptr].offset = pending_trees.len() as u64;

                let mut keys = st.nodes[node_id].keys.flush(DELTA_MAX, DELTA_MIN);
                let mut children = st.nodes[node_id].children.flush(DELTA_MAX, DELTA_MIN);
                prepare_keys(st, ctx, seq, &mut keys, &mut pending_cohorts)?;
                prepare_children(st, ctx, seq, &mut children, &mut pending_cohorts)?;
                st.nodes[node_id].keys.delta = keys.clone();
                st.nodes[node_id].children.delta = children.clone();
                st.unmark(*ptr);

                pending_trees.push((keys, children));
            }

            blocks[(seq - length) as usize] = Some(PendingBlock {
                block,
                keys: pending_keys,
                trees: pending_trees,
                cohorts: pending_cohorts,
            });
        }
        let mut blocks: Vec<PendingBlock> = blocks
            .into_iter()
            .map(|b| b.expect("every block filled"))
            .collect();

        if !blocks.is_empty() && length > 0 {
            let core = ctx.get_core_offset_by_key(*ctx.core.key()).await?;
            blocks.last_mut().expect("non-empty").block.previous = Some(Loc {
                core,
                seq: length - 1,
                offset: 0,
            });
        }

        if ctx.needs_flush() {
            let checkpoint = ctx.core.length() + blocks.len() as u64;
            let (cores, mappings) = ctx.flush();
            blocks
                .last_mut()
                .expect("at least one block")
                .block
                .metadata = Some(Metadata { cores, mappings });
            ctx.set_checkpoint(checkpoint);
        }

        let checkpoint = ctx.checkpoint();
        let invalidation = ctx.invalidation();
        for pending in &mut blocks {
            pending.block.checkpoint = checkpoint;
            pending.block.invalidation = invalidation;
            for kid in pending.keys.drain(..) {
                let key = &st.keys[kid];
                let external = key.value_ptr.is_some();
                pending.block.keys.push(KeyData {
                    key: key.key.clone(),
                    value: if external { None } else { key.value.clone() },
                    value_ptr: key.value_ptr.as_ref().map(|vp| ValuePtr {
                        core: vp.core,
                        seq: vp.seq,
                        offset: vp.offset,
                        split: vp.split,
                    }),
                });
            }
            for cohort in pending.cohorts.drain(..) {
                pending.block.cohorts.push(match cohort {
                    PendingCohort::Keys(deltas) => deltas
                        .iter()
                        .map(|d| EncDelta::from_delta(d, |k| key_loc(st, *k)))
                        .collect(),
                    PendingCohort::Children(deltas) => deltas
                        .iter()
                        .map(|d| EncDelta::from_delta(d, |p| ptr_loc(st, *p)))
                        .collect(),
                });
            }
            for (keys, children) in pending.trees.drain(..) {
                pending.block.tree.push(RawTree {
                    keys: keys
                        .iter()
                        .map(|d| EncDelta::from_delta(d, |k| key_loc(st, *k)))
                        .collect(),
                    children: children
                        .iter()
                        .map(|d| EncDelta::from_delta(d, |p| ptr_loc(st, *p)))
                        .collect(),
                });
            }
        }

        let buffers: Vec<Bytes> = blocks
            .iter()
            .map(|pending| crate::encoding::encode_block(&pending.block))
            .collect();
        ctx.core.append(&buffers).await?;
        Ok(root)
    }
}

enum PendingCohort {
    Keys(Vec<Delta<KeyId>>),
    Children(Vec<Delta<PtrId>>),
}

type PendingTree = (Vec<Delta<KeyId>>, Vec<Delta<PtrId>>);
type FoundKey = Option<(usize, KeyId)>;
type KeySearch = (FoundKey, usize, usize, Ordering);

struct PendingBlock {
    block: Block,
    keys: Vec<KeyId>,
    trees: Vec<PendingTree>,
    cohorts: Vec<PendingCohort>,
}

#[derive(Default)]
struct Update {
    size: usize,
    nodes: Vec<PtrId>,
    keys: Vec<KeyId>,
    values: Vec<KeyId>,
}

enum Status {
    Existing(usize),
    Inserted,
    NeedsSplit,
}

fn search(st: &TreeState, array: &CompressedArray<KeyId>, key: &[u8]) -> Result<KeySearch> {
    let mut s = 0;
    let mut e = array.ulength();
    let mut c = Ordering::Equal;
    while s < e {
        let mid = (s + e) >> 1;
        let kid = array.uget(mid).ok_or(Error::Message("null key pointer"))?;
        c = key.cmp(st.keys[kid].key.as_ref());
        if c == Ordering::Equal {
            return Ok((Some((mid, kid)), s, e, c));
        }
        if c == Ordering::Less {
            e = mid;
        } else {
            s = mid + 1;
        }
    }
    Ok((None, s, e, c))
}

async fn put(
    st: &mut TreeState,
    ctx: &Arc<Context>,
    root: &mut PtrId,
    target: Bytes,
    value: Bytes,
) -> Result<bool> {
    let mut stack = Vec::new();
    let mut ptr = *root;

    loop {
        let node = inflate(st, ptr).await?;
        let has_children = st.nodes[node].children.ulength() > 0;
        if !has_children {
            break;
        }
        stack.push(ptr);
        let (found, s, e, c) = search(st, &st.nodes[node].keys, &target)?;
        if let Some((mid, kid)) = found {
            let existing = existing_value(st, kid).await?;
            if existing.as_deref() == Some(value.as_ref()) {
                return Ok(false);
            }
            set_value(st, node, ctx, mid, target, value)?;
            for p in &stack {
                st.mark(*p);
            }
            return Ok(true);
        }
        let i = if c == Ordering::Less { e } else { s };
        ptr = st.nodes[node]
            .children
            .uget(i)
            .ok_or(Error::Message("child index out of range"))?;
    }

    let node = inflate(st, ptr).await?;
    let status = insert_leaf(st, node, ctx, target.clone(), value.clone())?;
    if let Status::Existing(index) = status {
        let kid = st.nodes[node]
            .keys
            .uget(index)
            .ok_or(Error::Message("null key pointer"))?;
        let existing = existing_value(st, kid).await?;
        if existing != Some(value.clone()) {
            set_value(st, node, ctx, index, target, value)?;
        } else {
            return Ok(false);
        }
    }

    st.mark(ptr);
    for p in &stack {
        st.mark(*p);
    }

    let mut status = status;
    while matches!(status, Status::NeedsSplit) {
        let new_t = st.ptrs[ptr]
            .value
            .map(|n| st.nodes[n].t)
            .ok_or(Error::Message("split without node"))?;
        let (median, right) = split(st, ptr, ctx)?;
        match stack.pop() {
            Some(parent) => {
                let pnode = inflate(st, parent).await?;
                status = insert_node(st, pnode, median, right)?;
                ptr = parent;
            }
            None => {
                let a = st.array_id();
                let b = st.array_id();
                let node_id = st.nodes.len();
                st.nodes.push(Node {
                    t: new_t,
                    keys: CompressedArray::empty(a),
                    children: CompressedArray::empty(b),
                });
                let new_root = st.create_ptr(ctx.clone(), 0, 0, 0, true, Some(node_id))?;
                st.nodes[node_id].keys.push(median);
                st.nodes[node_id].children.push(ptr);
                st.nodes[node_id].children.push(right);
                *root = new_root;
                status = Status::Inserted;
            }
        }
    }
    Ok(true)
}

async fn existing_value(st: &TreeState, key: KeyId) -> Result<Option<Bytes>> {
    if let Some(v) = &st.keys[key].value {
        return Ok(Some(v.clone()));
    }
    inflate_value(st, key).await
}

fn set_value(
    st: &mut TreeState,
    node: usize,
    ctx: &Arc<Context>,
    index: usize,
    key: Bytes,
    value: Bytes,
) -> Result<()> {
    let kid = st.add_key(ctx.clone(), 0, 0, 0, true, key, Some(value), None);
    st.nodes[node].keys.set(index, kid);
    Ok(())
}

fn insert_leaf(
    st: &mut TreeState,
    node: usize,
    ctx: &Arc<Context>,
    key: Bytes,
    value: Bytes,
) -> Result<Status> {
    let max_keys = 2 * st.nodes[node].t as usize - 1;
    let (found, s, e, c) = search(st, &st.nodes[node].keys, &key)?;
    if let Some((mid, _)) = found {
        return Ok(Status::Existing(mid));
    }
    let i = if c == Ordering::Less { e } else { s };
    let kid = st.add_key(ctx.clone(), 0, 0, 0, true, key, Some(value), None);
    st.nodes[node].keys.insert(i, kid);
    Ok(if st.nodes[node].keys.ulength() <= max_keys {
        Status::Inserted
    } else {
        Status::NeedsSplit
    })
}

fn insert_node(st: &mut TreeState, node: usize, key: KeyId, child: PtrId) -> Result<Status> {
    let max_keys = 2 * st.nodes[node].t as usize - 1;
    let (found, s, e, c) = search(st, &st.nodes[node].keys, &st.keys[key].key)?;
    if let Some((mid, _)) = found {
        return Ok(Status::Existing(mid));
    }
    let i = if c == Ordering::Less { e } else { s };
    st.nodes[node].keys.insert(i, key);
    st.nodes[node].children.insert(i + 1, child);
    Ok(if st.nodes[node].keys.ulength() <= max_keys {
        Status::Inserted
    } else {
        Status::NeedsSplit
    })
}

fn split(st: &mut TreeState, ptr: PtrId, ctx: &Arc<Context>) -> Result<(KeyId, PtrId)> {
    let node = st.ptrs[ptr]
        .value
        .ok_or(Error::Message("split without node"))?;
    let t = st.nodes[node].t;
    let len = st.nodes[node].keys.ulength() >> 1;

    let a = st.array_id();
    let b = st.array_id();
    let right_node = st.nodes.len();
    st.nodes.push(Node {
        t,
        keys: CompressedArray::empty(a),
        children: CompressedArray::empty(b),
    });
    let right = st.create_ptr(ctx.clone(), 0, 0, 0, true, Some(right_node))?;

    let mut keys = Vec::with_capacity(len);
    for _ in 0..len {
        keys.push(
            st.nodes[node]
                .keys
                .pop()
                .ok_or(Error::Message("split keys underflow"))?,
        );
    }
    for k in keys.iter().rev() {
        st.nodes[right_node].keys.push(*k);
    }
    let median = st.nodes[node]
        .keys
        .pop()
        .ok_or(Error::Message("split median underflow"))?;

    if st.nodes[node].children.ulength() > 0 {
        let mut children = Vec::with_capacity(len + 1);
        for _ in 0..len + 1 {
            children.push(
                st.nodes[node]
                    .children
                    .pop()
                    .ok_or(Error::Message("split children underflow"))?,
            );
        }
        for c in children.iter().rev() {
            st.nodes[right_node].children.push(*c);
        }
    }

    Ok((median, right))
}

async fn delete(
    st: &mut TreeState,
    ctx: &Arc<Context>,
    root: &mut PtrId,
    key: &[u8],
) -> Result<bool> {
    let mut ptr = *root;
    let mut stack = Vec::new();

    loop {
        let node = inflate(st, ptr).await?;
        stack.push(ptr);
        let (found, s, e, c) = search(st, &st.nodes[node].keys, key)?;
        if let Some((mid, _)) = found {
            if st.nodes[node].children.ulength() > 0 {
                set_key_to_nearest_leaf(st, node, mid, &mut stack, ctx).await?;
            } else {
                remove_key(st, node, mid);
            }
            for p in &stack {
                st.mark(*p);
            }
            *root = rebalance(st, stack).await?;
            return Ok(true);
        }
        if st.nodes[node].children.ulength() == 0 {
            return Ok(false);
        }
        let i = if c == Ordering::Less { e } else { s };
        ptr = st.nodes[node]
            .children
            .uget(i)
            .ok_or(Error::Message("child index out of range"))?;
    }
}

async fn set_key_to_nearest_leaf(
    st: &mut TreeState,
    parent: usize,
    index: usize,
    stack: &mut Vec<PtrId>,
    _ctx: &Arc<Context>,
) -> Result<()> {
    let left = st.nodes[parent]
        .children
        .uget(index)
        .ok_or(Error::Message("missing left child"))?;
    let right = st.nodes[parent]
        .children
        .uget(index + 1)
        .ok_or(Error::Message("missing right child"))?;

    let ls = leaf_size(st, left, false).await?;
    let rs = leaf_size(st, right, true).await?;

    if ls < rs {
        stack.push(right);
        let mut node = inflate(st, right).await?;
        loop {
            if st.nodes[node].children.ulength() == 0 {
                break;
            }
            let child = st.nodes[node]
                .children
                .uget(0)
                .ok_or(Error::Message("missing first child"))?;
            stack.push(child);
            node = inflate(st, child).await?;
        }
        let key = st.nodes[node]
            .keys
            .shift()
            .ok_or(Error::Message("empty leaf"))?;
        st.nodes[parent].keys.set(index, key);
    } else {
        stack.push(left);
        let mut node = inflate(st, left).await?;
        loop {
            if st.nodes[node].children.ulength() == 0 {
                break;
            }
            let last = st.nodes[node].children.ulength() - 1;
            let child = st.nodes[node]
                .children
                .uget(last)
                .ok_or(Error::Message("missing last child"))?;
            stack.push(child);
            node = inflate(st, child).await?;
        }
        let key = st.nodes[node]
            .keys
            .pop()
            .ok_or(Error::Message("empty leaf"))?;
        st.nodes[parent].keys.set(index, key);
    }
    Ok(())
}

async fn leaf_size(st: &mut TreeState, mut ptr: PtrId, go_left: bool) -> Result<usize> {
    loop {
        let node = inflate(st, ptr).await?;
        let len = st.nodes[node].children.ulength();
        if len == 0 {
            return Ok(st.nodes[node].keys.ulength());
        }
        let i = if go_left { 0 } else { len - 1 };
        ptr = st.nodes[node]
            .children
            .uget(i)
            .ok_or(Error::Message("missing child"))?;
    }
}

fn remove_key(st: &mut TreeState, node: usize, index: usize) {
    st.nodes[node].keys.delete(index);
    if st.nodes[node].children.ulength() > 0 {
        st.nodes[node].children.delete(index + 1);
    }
}

fn siblings(
    st: &TreeState,
    parent: usize,
    node: usize,
) -> Result<(Option<PtrId>, usize, Option<PtrId>)> {
    let len = st.nodes[parent].children.ulength();
    let list: Vec<PtrId> = (0..len)
        .map(|i| st.nodes[parent].children.uget(i))
        .collect::<Option<Vec<_>>>()
        .ok_or(Error::Message("null child pointer"))?;
    let index = list
        .iter()
        .position(|p| st.ptrs[*p].value == Some(node))
        .ok_or(Error::Message("Bad parent"))?;
    Ok((
        (index > 0).then(|| list[index - 1]),
        index,
        (index + 1 < list.len()).then(|| list[index + 1]),
    ))
}

fn merge(st: &mut TreeState, left: usize, right: usize, median: KeyId) {
    let right_node = st.ptrs[right].value.expect("right node");
    let mut keys = Vec::new();
    for i in 0..st.nodes[right_node].keys.ulength() {
        keys.push(st.nodes[right_node].keys.uget(i).expect("key"));
    }
    let mut children = Vec::new();
    for i in 0..st.nodes[right_node].children.ulength() {
        children.push(st.nodes[right_node].children.uget(i).expect("child"));
    }

    let left_node = st.ptrs[left].value.expect("left node");
    st.nodes[left_node].keys.push(median);
    for k in keys {
        st.nodes[left_node].keys.push(k);
    }
    for c in children {
        st.nodes[left_node].children.push(c);
    }
}

async fn rebalance(st: &mut TreeState, mut stack: Vec<PtrId>) -> Result<PtrId> {
    let root = stack[0];

    while stack.len() > 1 {
        let ptr = stack.pop().expect("non-empty");
        let parent = *stack.last().expect("non-empty");

        let node = inflate(st, ptr).await?;
        let min_keys = st.nodes[node].t as usize - 1;
        if st.nodes[node].keys.ulength() >= min_keys {
            return Ok(root);
        }

        let pnode = inflate(st, parent).await?;
        let (left, mut index, right) = siblings(st, pnode, node)?;

        if let Some(left) = left {
            let l = inflate(st, left).await?;
            if st.nodes[l].keys.ulength() > min_keys {
                st.mark(left);
                let key = st.nodes[pnode]
                    .keys
                    .uget(index - 1)
                    .ok_or(Error::Message("missing left separator"))?;
                st.nodes[node].keys.unshift(key);
                if st.nodes[l].children.ulength() > 0 {
                    let child = st.nodes[l].children.pop().expect("child");
                    st.nodes[node].children.unshift(child);
                }
                let moved = st.nodes[l].keys.pop().ok_or(Error::Message("empty left"))?;
                st.nodes[pnode].keys.set(index - 1, moved);
                return Ok(root);
            }
        }

        if let Some(right) = right {
            let r = inflate(st, right).await?;
            if st.nodes[r].keys.ulength() > min_keys {
                st.mark(right);
                let key = st.nodes[pnode]
                    .keys
                    .uget(index)
                    .ok_or(Error::Message("missing right separator"))?;
                st.nodes[node].keys.push(key);
                if st.nodes[r].children.ulength() > 0 {
                    let child = st.nodes[r].children.shift().expect("child");
                    st.nodes[node].children.push(child);
                }
                let moved = st.nodes[r]
                    .keys
                    .shift()
                    .ok_or(Error::Message("empty right"))?;
                st.nodes[pnode].keys.set(index, moved);
                return Ok(root);
            }
        }

        let (left, right) = if let Some(left) = left {
            index -= 1;
            (left, ptr)
        } else {
            (ptr, right.expect("one sibling exists"))
        };

        st.mark(left);
        let median = st.nodes[pnode]
            .keys
            .uget(index)
            .ok_or(Error::Message("missing merge separator"))?;
        merge(st, left, right, median);
        st.mark(parent);
        remove_key(st, pnode, index);
    }

    let r = inflate(st, root).await?;
    if st.nodes[r].keys.ulength() == 0 && st.nodes[r].children.ulength() > 0 {
        return Ok(st.nodes[r].children.uget(0).expect("child"));
    }
    Ok(root)
}

fn commit(st: &mut TreeState, ptr: PtrId) -> Result<PtrId> {
    let node = st.ptrs[ptr]
        .value
        .ok_or(Error::Message("commit without node"))?;
    let t = st.nodes[node].t;
    let a = st.array_id();
    let b = st.array_id();
    let keys = st.nodes[node].keys.commit(a);
    let children = st.nodes[node].children.commit(b);
    let new_node = st.nodes.len();
    st.nodes.push(Node { t, keys, children });
    let context = st.ptrs[ptr].context.clone();
    let core = st.ptrs[ptr].core;
    let new_ptr = st.add_ptr(context, core, 0, 0, true, Some(new_node));
    st.unmark(ptr);
    Ok(new_ptr)
}

fn est_node_size(st: &TreeState, ptr: PtrId) -> usize {
    let node = st.ptrs[ptr].value.expect("committed node");
    (st.nodes[node].keys.delta.len() + st.nodes[node].children.delta.len()) * ESTIMATED_POINTER_SIZE
}

fn should_inline(st: &TreeState, key: KeyId) -> bool {
    let k = &st.keys[key];
    match &k.value {
        None => true,
        Some(_) if k.value_ptr.is_some() => true,
        Some(v) => v.len() <= INLINE_VALUE_SIZE,
    }
}

fn key_loc(st: &TreeState, key: KeyId) -> Loc {
    let k = &st.keys[key];
    Loc {
        core: k.core,
        seq: k.seq,
        offset: k.offset,
    }
}

fn ptr_loc(st: &TreeState, ptr: PtrId) -> Loc {
    let p = &st.ptrs[ptr];
    Loc {
        core: p.core,
        seq: p.seq,
        offset: p.offset,
    }
}

fn prepare_keys(
    st: &mut TreeState,
    ctx: &Arc<Context>,
    seq: u64,
    deltas: &mut [Delta<KeyId>],
    cohorts: &mut Vec<PendingCohort>,
) -> Result<()> {
    for d in deltas.iter_mut() {
        if d.changed
            && let Some(DeltaRef::Id(k)) = d.ptr
            && st.keys[k].changed
        {
            d.ptr = None;
        }
        if let Some(DeltaRef::Id(k)) = d.ptr {
            relocate_key(st, ctx, k)?;
        }
        if d.kind != OP_COHORT || !d.changed {
            continue;
        }
        d.changed = false;
        d.ptr = Some(DeltaRef::Loc(Loc {
            core: 0,
            seq,
            offset: cohorts.len() as u64,
        }));

        for dd in d.cohort.iter_mut() {
            if dd.changed
                && let Some(DeltaRef::Id(k)) = dd.ptr
                && st.keys[k].changed
            {
                dd.ptr = None;
            }
            dd.changed = false;
            if let Some(DeltaRef::Id(k)) = dd.ptr {
                relocate_key(st, ctx, k)?;
            }
        }
        cohorts.push(PendingCohort::Keys(d.cohort.clone()));
    }
    Ok(())
}

fn prepare_children(
    st: &mut TreeState,
    ctx: &Arc<Context>,
    seq: u64,
    deltas: &mut [Delta<PtrId>],
    cohorts: &mut Vec<PendingCohort>,
) -> Result<()> {
    for d in deltas.iter_mut() {
        if let Some(DeltaRef::Id(p)) = d.ptr {
            relocate_ptr(st, ctx, p)?;
        }
        if d.kind != OP_COHORT || !d.changed {
            continue;
        }
        d.changed = false;
        d.ptr = Some(DeltaRef::Loc(Loc {
            core: 0,
            seq,
            offset: cohorts.len() as u64,
        }));

        for dd in d.cohort.iter_mut() {
            dd.changed = false;
            if let Some(DeltaRef::Id(p)) = dd.ptr {
                relocate_ptr(st, ctx, p)?;
            }
        }
        cohorts.push(PendingCohort::Children(d.cohort.clone()));
    }
    Ok(())
}

fn relocate_key(st: &mut TreeState, ctx: &Arc<Context>, key: KeyId) -> Result<()> {
    if !Arc::ptr_eq(&st.keys[key].context, ctx) {
        st.keys[key].core =
            ctx.get_core_offset_local(&st.keys[key].context.clone(), st.keys[key].core)?;
        st.keys[key].context = ctx.clone();
    }
    Ok(())
}

fn relocate_ptr(st: &mut TreeState, ctx: &Arc<Context>, ptr: PtrId) -> Result<()> {
    if !Arc::ptr_eq(&st.ptrs[ptr].context, ctx) {
        st.ptrs[ptr].core =
            ctx.get_core_offset_local(&st.ptrs[ptr].context.clone(), st.ptrs[ptr].core)?;
        st.ptrs[ptr].context = ctx.clone();
    }
    Ok(())
}
