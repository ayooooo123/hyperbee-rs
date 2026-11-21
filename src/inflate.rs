//! Inflating serialized tree deltas into nodes, keyed by `ptr` position.
//!
//! Inflation has two phases, as in `lib/inflate.js`: the blocks a node
//! references are read and decoded without the tree lock, under the caller's
//! read options; the node is then installed under the lock, unless another
//! caller installed it first.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{Block, EncDelta, KeyData, Loc, OP_COHORT};
use crate::tree::{
    CompressedArray, Delta, DeltaRef, KeyId, Locked, Node, NodeId, PtrId, TreeState, ValuePointer,
};

type Blocks = HashMap<([u8; 32], u64, u64), Arc<Block>>;

/// A key decoded from its block, not yet in the arena.
struct KeySpec {
    context: Arc<Context>,
    loc: Loc,
    key: Bytes,
    value: Option<Bytes>,
    value_ptr: Option<ValuePointer>,
}

/// A child location in the context it was decoded from.
type ChildSpec = (Arc<Context>, Loc);

/// Everything a node needs, read without the tree lock.
struct Fetched {
    base: Arc<Context>,
    /// Fork of the node's core before its block was read.
    fork: u64,
    t: u64,
    keys: Vec<Delta<KeySpec>>,
    children: Vec<Delta<ChildSpec>>,
}

/// Fill `ptr`'s value for a reader, reading every block the node references
/// with the tree unlocked. If `ptr`'s slot was freed meanwhile, `ptr` is
/// re-created from its location. If the core forked during the read, the
/// bytes may be of the old fork and are read again.
pub(crate) async fn inflate(st: &mut Locked, ptr: &mut PtrId) -> crate::Result<NodeId> {
    loop {
        if let Some(node) = st.ptrs[*ptr].value {
            st.touch(*ptr);
            return Ok(node);
        }
        let mut at = st.ptr_ref(*ptr);
        let fetched = st
            .unlocked(fetch(at.context.clone(), at.core, at.seq, at.offset))
            .await?;
        // Drop cache entries of an old fork before re-creating `ptr`.
        st.check_fork(&fetched.base.core);
        *ptr = st.resolve(&mut at)?;
        if fetched.base.core.fork() == fetched.fork {
            return install(st, *ptr, fetched);
        }
    }
}

/// Writer inflation, JS `retainAndInflate`: the pointer and its node stay in
/// the arena until the batch flushes, so ids a write holds stay valid.
pub(crate) async fn retain_and_inflate(st: &mut Locked, ptr: PtrId) -> crate::Result<NodeId> {
    st.retain(ptr);
    let mut id = ptr;
    let node = inflate(st, &mut id).await?;
    if id != ptr {
        // A retained pointer is never freed; refuse to write through a copy.
        return Err(crate::Error::Message("retained pointer was freed"));
    }
    Ok(node)
}

async fn fetch(context: Arc<Context>, core: u64, seq: u64, offset: u64) -> crate::Result<Fetched> {
    // What `get_block` does first, so the core's fork can be read before the block.
    if !context.has_core(core) {
        context.update_maybe(core, None).await?;
    }
    let base = context.get_loaded_context(core).await?;
    let fork = base.core.fork();
    let block = Arc::new(context.get_block(seq, core, None).await?);
    let mut blocks = Blocks::new();
    let tree = block
        .tree
        .get(offset as usize)
        .ok_or(crate::Error::Message("tree offset out of range"))?;

    let mut keys = Vec::with_capacity(tree.keys.len());
    for d in &tree.keys {
        keys.push(key_delta(&base, d, &block, core, &mut blocks).await?);
    }
    let mut children = Vec::with_capacity(tree.children.len());
    for d in &tree.children {
        children.push(child_delta(&base, d, &block, core, &mut blocks).await?);
    }
    Ok(Fetched {
        base,
        fork,
        t: block.t,
        keys,
        children,
    })
}

/// Build the node of `ptr` from fetched data, unless it is already there.
fn install(st: &mut TreeState, ptr: PtrId, fetched: Fetched) -> crate::Result<NodeId> {
    st.check_fork(&fetched.base.core);
    if let Some(node) = st.ptrs[ptr].value {
        st.touch(ptr);
        return Ok(node);
    }
    let mut keys = Vec::with_capacity(fetched.keys.len());
    for d in fetched.keys {
        keys.push(map_delta(d, &mut |k: KeySpec| {
            Ok(st.add_key(
                k.context,
                k.loc.core,
                k.loc.seq,
                k.loc.offset,
                false,
                k.key,
                k.value,
                k.value_ptr,
            ))
        })?);
    }
    let mut children = Vec::with_capacity(fetched.children.len());
    for d in fetched.children {
        children.push(map_delta(d, &mut |(ctx, loc): ChildSpec| {
            st.create_ptr(ctx, loc.core, loc.seq, loc.offset, false, None)
        })?);
    }

    let kid = st.array_id();
    let cid = st.array_id();
    let node = st.add_node(Node {
        t: fetched.t,
        keys: CompressedArray::from_delta(keys, kid),
        children: CompressedArray::from_delta(children, cid),
    });
    st.ptrs[ptr].value = Some(node);
    // Collect before the new node enters the cache, so it is never the one dropped.
    st.gc();
    st.touch(ptr);
    Ok(node)
}

fn map_delta<A, B>(
    d: Delta<A>,
    f: &mut impl FnMut(A) -> crate::Result<B>,
) -> crate::Result<Delta<B>> {
    let ptr = match d.ptr {
        Some(DeltaRef::Id(a)) => Some(DeltaRef::Id(f(a)?)),
        Some(DeltaRef::Loc(context, loc)) => Some(DeltaRef::Loc(context, loc)),
        None => None,
    };
    let mut cohort = Vec::with_capacity(d.cohort.len());
    for dd in d.cohort {
        cohort.push(map_delta(dd, f)?);
    }
    Ok(Delta {
        changed: d.changed,
        kind: d.kind,
        index: d.index,
        ptr,
        cohort,
    })
}

async fn key_delta(
    base: &Arc<Context>,
    d: &EncDelta,
    origin: &Arc<Block>,
    origin_core: u64,
    blocks: &mut Blocks,
) -> crate::Result<Delta<KeySpec>> {
    if d.kind == OP_COHORT {
        let co = d
            .ptr
            .ok_or(crate::Error::Message("cohort without pointer"))?;
        let cohort_block = load_block(base, co, origin, origin_core, blocks).await?;
        let ctx = base.get_loaded_context(co.core).await?;
        let entries = cohort_block
            .cohorts
            .get(co.offset as usize)
            .ok_or(crate::Error::Message("cohort offset out of range"))?;
        let mut deltas = Vec::with_capacity(entries.len());
        for p in entries {
            let key = match p.ptr {
                Some(loc) => {
                    let target = load_block(&ctx, loc, &cohort_block, co.core, blocks).await?;
                    Some(build_key(&ctx, loc, &target).await?)
                }
                None => None,
            };
            deltas.push(Delta::new(false, p.kind, p.index, key.map(DeltaRef::Id)));
        }
        return Ok(Delta::cohort(
            false,
            Some(DeltaRef::Loc(base.clone(), co)),
            deltas,
        ));
    }

    let key = match d.ptr {
        Some(loc) => {
            let target = load_block(base, loc, origin, origin_core, blocks).await?;
            Some(build_key(base, loc, &target).await?)
        }
        None => None,
    };
    Ok(Delta::new(false, d.kind, d.index, key.map(DeltaRef::Id)))
}

async fn child_delta(
    base: &Arc<Context>,
    d: &EncDelta,
    origin: &Arc<Block>,
    origin_core: u64,
    blocks: &mut Blocks,
) -> crate::Result<Delta<ChildSpec>> {
    if d.kind == OP_COHORT {
        let co = d
            .ptr
            .ok_or(crate::Error::Message("cohort without pointer"))?;
        let cohort_block = load_block(base, co, origin, origin_core, blocks).await?;
        let ctx = base.get_loaded_context(co.core).await?;
        let entries = cohort_block
            .cohorts
            .get(co.offset as usize)
            .ok_or(crate::Error::Message("cohort offset out of range"))?;
        let mut deltas = Vec::with_capacity(entries.len());
        for p in entries {
            let child = match p.ptr {
                Some(loc) => {
                    if !ctx.has_core(loc.core) {
                        ctx.update_maybe(loc.core, Some(&cohort_block)).await?;
                    }
                    Some(DeltaRef::Id((ctx.clone(), loc)))
                }
                None => None,
            };
            deltas.push(Delta::new(false, p.kind, p.index, child));
        }
        return Ok(Delta::cohort(
            false,
            Some(DeltaRef::Loc(base.clone(), co)),
            deltas,
        ));
    }

    let child = match d.ptr {
        Some(loc) => {
            if !base.has_core(loc.core) {
                base.update_maybe(loc.core, Some(origin)).await?;
            }
            Some(DeltaRef::Id((base.clone(), loc)))
        }
        None => None,
    };
    Ok(Delta::new(false, d.kind, d.index, child))
}

/// Reuse decoded blocks only for this node, not across forks or later reads.
async fn load_block(
    base: &Arc<Context>,
    loc: Loc,
    origin: &Arc<Block>,
    origin_core: u64,
    blocks: &mut Blocks,
) -> crate::Result<Arc<Block>> {
    if loc.core == 0 && loc.seq == origin.seq && origin_core == 0 {
        return Ok(origin.clone());
    }
    let key = (*base.core.key(), loc.core, loc.seq);
    if let Some(block) = blocks.get(&key) {
        return Ok(block.clone());
    }
    let block = Arc::new(base.get_block(loc.seq, loc.core, Some(origin)).await?);
    blocks.insert(key, block.clone());
    Ok(block)
}

async fn build_key(base: &Arc<Context>, loc: Loc, target: &Block) -> crate::Result<KeySpec> {
    let data: &KeyData = target
        .keys
        .get(loc.offset as usize)
        .ok_or(crate::Error::Message("key offset out of range"))?;
    let key_context = base.get_loaded_context(loc.core).await?;
    let value_ptr = data.value_ptr.as_ref().map(|p| ValuePointer {
        context: key_context,
        core: p.core,
        seq: p.seq,
        offset: p.offset,
        split: p.split,
    });
    Ok(KeySpec {
        context: base.clone(),
        loc,
        key: data.key.clone(),
        value: data.value.clone(),
        value_ptr,
    })
}

/// Read a key's value, following external value pointers with the tree
/// unlocked. Readers copy what they need from `key` before calling.
pub(crate) async fn inflate_value(st: &mut Locked, key: KeyId) -> crate::Result<Option<Bytes>> {
    let k = &st.keys[key];
    if let Some(v) = &k.value {
        return Ok(Some(v.clone()));
    }
    let Some(vp) = k.value_ptr.clone() else {
        return Ok(None);
    };
    st.unlocked(fetch_value(vp)).await
}

async fn fetch_value(vp: ValuePointer) -> crate::Result<Option<Bytes>> {
    if vp.split == 0 {
        let block = vp.context.get_block(vp.seq, vp.core, None).await?;
        return Ok(block.values.get(vp.offset as usize).cloned());
    }

    // A split value is the first value of `split` blocks followed by the tail
    // of the last one, starting at `offset`.
    let first_seq = vp
        .seq
        .checked_sub(vp.split)
        .ok_or(crate::Error::Message("value split before core start"))?;
    let mut out = Vec::new();
    for i in 0..=vp.split {
        let block = vp.context.get_block(first_seq + i, vp.core, None).await?;
        let first = block
            .values
            .first()
            .ok_or(crate::Error::Message("missing split value block"))?;
        if i < vp.split {
            out.extend_from_slice(first);
        } else {
            let start = (vp.offset as usize).min(first.len());
            out.extend_from_slice(&first[start..]);
        }
    }
    Ok(Some(Bytes::from(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn same_block_references_share_the_decoded_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = corestore::Corestore::open(dir.path()).unwrap();
        let bee = crate::Bee::open(&store, None).await.unwrap();
        let origin = Arc::new(Block {
            seq: 7,
            keys: vec![KeyData::default(); 256],
            ..Default::default()
        });
        let loaded = load_block(
            &bee.inner.context,
            Loc {
                seq: 7,
                ..Default::default()
            },
            &origin,
            0,
            &mut Blocks::new(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&loaded, &origin));

        let mut write = bee.batch();
        write.put(&b"key"[..], &b"value"[..]);
        write.flush().await.unwrap();
        let mut blocks = Blocks::new();
        let first = load_block(&bee.inner.context, Loc::default(), &origin, 0, &mut blocks)
            .await
            .unwrap();
        let second = load_block(&bee.inner.context, Loc::default(), &origin, 0, &mut blocks)
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(blocks.len(), 1);
        store.close().await;
    }
}
