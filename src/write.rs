//! Write batches, ported from `lib/write.js`.

use std::cmp::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{
    Block, EncDelta, KeyData, Loc, Metadata, OP_COHORT, RawTree, T, TYPE_LATEST, ValuePtr,
};
use crate::inflate::{inflate_value, retain_and_inflate};
use crate::tree::{
    CompressedArray, Delta, DeltaRef, KeyId, Locked, MAX_DEPTH, Node, NodeId, PtrId, TreeState,
    ValuePointer,
};
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
        let _writer = inner.writer.lock().await;
        let options = self.bee.read_options();
        crate::READ_OPTIONS
            .scope(options, async {
                inner.context.update(true).await?;
                let mut st = Locked::new(inner.state.clone()).await;
                let result = self.flush_inner(&mut st).await;
                st.flush_cache();
                result
            })
            .await
    }

    async fn flush_inner(&mut self, st: &mut Locked) -> Result<()> {
        let ctx = self.bee.inner.context.clone();
        let mut root = self.bee.root_ptr(st)?;
        let previous = self.bee.head().filter(|head| head.length > 0);
        let length = ctx.core.length();

        if root.is_none() {
            let a = st.array_id();
            let b = st.array_id();
            let node = st.add_node(Node {
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

        root = self
            .flush_write(st, &ctx, root, length, previous, applied)
            .await?;

        let new_length = st.ptrs[root].seq + 1;
        self.bee.set_root_ptr(
            st,
            Some(root),
            new_length,
            *st.ptrs[root].context.core.key(),
        );
        Ok(())
    }

    async fn flush_write(
        &self,
        st: &mut Locked,
        ctx: &Arc<Context>,
        mut root: PtrId,
        length: u64,
        previous: Option<crate::Head>,
        applied: usize,
    ) -> Result<PtrId> {
        if !st.ptrs[root].changed {
            return Ok(root);
        }
        if st.ptrs[root].value.is_none() {
            retain_and_inflate(st, root).await?;
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

        // JS tests the root length (`this.length`), not the local core length.
        if applied == 0 && previous.is_some() {
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

        if let Some(previous) = previous {
            let core = ctx.get_core_offset_by_key(previous.key).await?;
            blocks.last_mut().expect("non-empty").block.previous = Some(Loc {
                core,
                seq: previous.length - 1,
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
            .into_iter()
            .map(|pending| crate::encoding::encode_block(pending.block))
            .collect();
        ctx.core.append(&buffers).await?;
        Ok(root)
    }
}

enum PendingCohort {
    Keys(Vec<Delta<KeyId>>),
    Children(Vec<Delta<PtrId>>),
}

type PendingTree = (Vec<Arc<Delta<KeyId>>>, Vec<Arc<Delta<PtrId>>>);
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
    st: &mut Locked,
    ctx: &Arc<Context>,
    root: &mut PtrId,
    target: Bytes,
    value: Bytes,
) -> Result<bool> {
    let mut stack = Vec::new();
    let mut ptr = *root;

    loop {
        if stack.len() >= MAX_DEPTH {
            return Err(Error::Message("tree too deep"));
        }
        let node = retain_and_inflate(st, ptr).await?;
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

    let node = retain_and_inflate(st, ptr).await?;
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
                let pnode = retain_and_inflate(st, parent).await?;
                status = insert_node(st, pnode, median, right)?;
                ptr = parent;
            }
            None => {
                let a = st.array_id();
                let b = st.array_id();
                let node_id = st.add_node(Node {
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

async fn existing_value(st: &mut Locked, key: KeyId) -> Result<Option<Bytes>> {
    if let Some(v) = &st.keys[key].value {
        return Ok(Some(v.clone()));
    }
    inflate_value(st, key).await
}

fn set_value(
    st: &mut TreeState,
    node: NodeId,
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
    node: NodeId,
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

fn insert_node(st: &mut TreeState, node: NodeId, key: KeyId, child: PtrId) -> Result<Status> {
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
    let right_node = st.add_node(Node {
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

async fn delete(st: &mut Locked, ctx: &Arc<Context>, root: &mut PtrId, key: &[u8]) -> Result<bool> {
    let mut ptr = *root;
    let mut stack = Vec::new();

    loop {
        if stack.len() >= MAX_DEPTH {
            return Err(Error::Message("tree too deep"));
        }
        let node = retain_and_inflate(st, ptr).await?;
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
    st: &mut Locked,
    parent: NodeId,
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
        let mut node = retain_and_inflate(st, right).await?;
        loop {
            if stack.len() >= MAX_DEPTH {
                return Err(Error::Message("tree too deep"));
            }
            if st.nodes[node].children.ulength() == 0 {
                break;
            }
            let child = st.nodes[node]
                .children
                .uget(0)
                .ok_or(Error::Message("missing first child"))?;
            stack.push(child);
            node = retain_and_inflate(st, child).await?;
        }
        let key = st.nodes[node]
            .keys
            .shift()
            .ok_or(Error::Message("empty leaf"))?;
        st.nodes[parent].keys.set(index, key);
    } else {
        stack.push(left);
        let mut node = retain_and_inflate(st, left).await?;
        loop {
            if stack.len() >= MAX_DEPTH {
                return Err(Error::Message("tree too deep"));
            }
            if st.nodes[node].children.ulength() == 0 {
                break;
            }
            let last = st.nodes[node].children.ulength() - 1;
            let child = st.nodes[node]
                .children
                .uget(last)
                .ok_or(Error::Message("missing last child"))?;
            stack.push(child);
            node = retain_and_inflate(st, child).await?;
        }
        let key = st.nodes[node]
            .keys
            .pop()
            .ok_or(Error::Message("empty leaf"))?;
        st.nodes[parent].keys.set(index, key);
    }
    Ok(())
}

async fn leaf_size(st: &mut Locked, mut ptr: PtrId, go_left: bool) -> Result<usize> {
    for _ in 0..MAX_DEPTH {
        let node = retain_and_inflate(st, ptr).await?;
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
    Err(Error::Message("tree too deep"))
}

fn remove_key(st: &mut TreeState, node: NodeId, index: usize) {
    st.nodes[node].keys.delete(index);
    if st.nodes[node].children.ulength() > 0 {
        st.nodes[node].children.delete(index + 1);
    }
}

fn siblings(
    st: &TreeState,
    parent: NodeId,
    node: NodeId,
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

fn merge(st: &mut TreeState, left: PtrId, right: PtrId, median: KeyId) -> Result<()> {
    let right_node = st.ptrs[right]
        .value
        .ok_or(Error::Message("malformed node"))?;
    let keys = (0..st.nodes[right_node].keys.ulength())
        .map(|i| st.nodes[right_node].keys.uget(i))
        .collect::<Option<Vec<_>>>()
        .ok_or(Error::Message("malformed node"))?;
    let children = (0..st.nodes[right_node].children.ulength())
        .map(|i| st.nodes[right_node].children.uget(i))
        .collect::<Option<Vec<_>>>()
        .ok_or(Error::Message("malformed node"))?;

    let left_node = st.ptrs[left]
        .value
        .ok_or(Error::Message("malformed node"))?;
    st.nodes[left_node].keys.push(median);
    for k in keys {
        st.nodes[left_node].keys.push(k);
    }
    for c in children {
        st.nodes[left_node].children.push(c);
    }
    Ok(())
}

async fn rebalance(st: &mut Locked, mut stack: Vec<PtrId>) -> Result<PtrId> {
    let root = stack[0];

    while stack.len() > 1 {
        let ptr = stack.pop().expect("non-empty");
        let parent = *stack.last().expect("non-empty");

        let node = retain_and_inflate(st, ptr).await?;
        let min_keys = st.nodes[node].t as usize - 1;
        if st.nodes[node].keys.ulength() >= min_keys {
            return Ok(root);
        }

        let pnode = retain_and_inflate(st, parent).await?;
        let (left, mut index, right) = siblings(st, pnode, node)?;

        if let Some(left) = left {
            let l = retain_and_inflate(st, left).await?;
            if st.nodes[l].keys.ulength() > min_keys {
                st.mark(left);
                let key = st.nodes[pnode]
                    .keys
                    .uget(index - 1)
                    .ok_or(Error::Message("missing left separator"))?;
                st.nodes[node].keys.unshift(key);
                if st.nodes[l].children.ulength() > 0 {
                    let child = st.nodes[l]
                        .children
                        .pop()
                        .ok_or(Error::Message("malformed node"))?;
                    st.nodes[node].children.unshift(child);
                }
                let moved = st.nodes[l].keys.pop().ok_or(Error::Message("empty left"))?;
                st.nodes[pnode].keys.set(index - 1, moved);
                return Ok(root);
            }
        }

        if let Some(right) = right {
            let r = retain_and_inflate(st, right).await?;
            if st.nodes[r].keys.ulength() > min_keys {
                st.mark(right);
                let key = st.nodes[pnode]
                    .keys
                    .uget(index)
                    .ok_or(Error::Message("missing right separator"))?;
                st.nodes[node].keys.push(key);
                if st.nodes[r].children.ulength() > 0 {
                    let child = st.nodes[r]
                        .children
                        .shift()
                        .ok_or(Error::Message("malformed node"))?;
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
            (ptr, right.ok_or(Error::Message("malformed node"))?)
        };

        st.mark(left);
        let median = st.nodes[pnode]
            .keys
            .uget(index)
            .ok_or(Error::Message("missing merge separator"))?;
        merge(st, left, right, median)?;
        st.mark(parent);
        remove_key(st, pnode, index);
    }

    let r = retain_and_inflate(st, root).await?;
    if st.nodes[r].keys.ulength() == 0 && st.nodes[r].children.ulength() > 0 {
        return st.nodes[r]
            .children
            .uget(0)
            .ok_or(Error::Message("malformed node"));
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
    let new_node = st.add_node(Node { t, keys, children });
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

/// Whether `d` points into a block of another context and so must be rewritten
/// for `ctx` (`relocate_loc`).
fn foreign_loc<T>(d: &Delta<T>, ctx: &Arc<Context>) -> bool {
    matches!(&d.ptr, Some(DeltaRef::Loc(from, _)) if !Arc::ptr_eq(from, ctx))
}

// The deltas are shared with the arrays that `commit` left behind (JS shares the objects). Only a delta that is
// actually rewritten here is copied first (`Arc::make_mut`), so the base arrays keep their values, as before.
fn prepare_keys(
    st: &mut TreeState,
    ctx: &Arc<Context>,
    seq: u64,
    deltas: &mut [Arc<Delta<KeyId>>],
    cohorts: &mut Vec<PendingCohort>,
) -> Result<()> {
    for shared in deltas.iter_mut() {
        let drop_id =
            shared.changed && matches!(shared.ptr, Some(DeltaRef::Id(k)) if st.keys[k].changed);
        if drop_id {
            Arc::make_mut(shared).ptr = None;
        }
        if foreign_loc(shared, ctx)
            && let Some(DeltaRef::Loc(from, loc)) = &mut Arc::make_mut(shared).ptr
        {
            relocate_loc(from, ctx, loc)?;
        }
        if let Some(DeltaRef::Id(k)) = shared.ptr {
            relocate_key(st, ctx, k)?;
        }
        if shared.kind != OP_COHORT || !shared.changed {
            continue;
        }
        let d = Arc::make_mut(shared);
        d.changed = false;
        d.ptr = Some(DeltaRef::Loc(
            ctx.clone(),
            Loc {
                core: 0,
                seq,
                offset: cohorts.len() as u64,
            },
        ));

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
    deltas: &mut [Arc<Delta<PtrId>>],
    cohorts: &mut Vec<PendingCohort>,
) -> Result<()> {
    for shared in deltas.iter_mut() {
        if foreign_loc(shared, ctx)
            && let Some(DeltaRef::Loc(from, loc)) = &mut Arc::make_mut(shared).ptr
        {
            relocate_loc(from, ctx, loc)?;
        }
        if let Some(DeltaRef::Id(p)) = shared.ptr {
            relocate_ptr(st, ctx, p)?;
        }
        if shared.kind != OP_COHORT || !shared.changed {
            continue;
        }
        let d = Arc::make_mut(shared);
        d.changed = false;
        d.ptr = Some(DeltaRef::Loc(
            ctx.clone(),
            Loc {
                core: 0,
                seq,
                offset: cohorts.len() as u64,
            },
        ));

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

/// A cohort pointer carries the context it was decoded from, so its core index
/// has to be translated like any other pointer before it can be re-emitted.
fn relocate_loc(from: &mut Arc<Context>, ctx: &Arc<Context>, loc: &mut Loc) -> Result<()> {
    if !Arc::ptr_eq(from, ctx) {
        loc.core = ctx.get_core_offset_local(from, loc.core)?;
        *from = ctx.clone();
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

#[cfg(test)]
mod tests {
    use corestore::Corestore;

    use crate::Bee;

    /// Writes on evicted and re-inflated nodes produce the same blocks as
    /// writes on nodes that stayed in memory.
    #[tokio::test]
    async fn evicted_nodes_write_the_same_blocks() {
        let mut cores = Vec::new();
        let mut dirs = Vec::new();
        for max_nodes in [2, crate::tree::NODE_CACHE_SIZE] {
            let dir = tempfile::tempdir().unwrap();
            let store = Corestore::open_with_primary_key(dir.path(), [7; 32]).unwrap();
            let bee = Bee::open(&store, None).await.unwrap();
            bee.inner.state.lock().await.max_nodes = max_nodes;
            let mut seed = 1u64;
            for round in 0..6 {
                let mut batch = bee.batch();
                for _ in 0..800 {
                    seed = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let key = format!("key/{:04}", (seed >> 33) % 3000);
                    if round > 1 && seed.is_multiple_of(3) {
                        batch.del(key);
                    } else {
                        batch.put(
                            key,
                            format!("value/{round}/{}", "x".repeat((seed % 1500) as usize)),
                        );
                    }
                }
                batch.flush().await.unwrap();
                let live = bee.inner.state.lock().await.nodes.len();
                assert!(live <= max_nodes, "{live} nodes after flush");
            }
            let core = bee.inner.context.core.clone();
            let mut blocks = Vec::new();
            for seq in 0..core.length() {
                blocks.push(core.get(seq, Default::default()).await.unwrap().unwrap());
            }
            cores.push(blocks);
            dirs.push((dir, store));
        }
        assert!(cores[0].len() > 6);
        assert_eq!(cores[0], cores[1]);
        for (_, store) in dirs {
            store.close().await;
        }
    }
}
