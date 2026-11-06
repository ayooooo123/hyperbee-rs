//! Inflating serialized tree deltas into nodes, keyed by `ptr` position.

use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{Block, EncDelta, KeyData, Loc, OP_COHORT};
use crate::tree::{
    CompressedArray, Delta, DeltaRef, KeyId, Node, NodeId, PtrId, TreeState, ValuePointer,
};

/// Fill `ptr`'s value, reading every block the node references.
pub(crate) async fn inflate(state: &mut TreeState, ptr: PtrId) -> crate::Result<NodeId> {
    if let Some(node) = state.ptrs[ptr].value {
        return Ok(node);
    }

    let context = state.ptrs[ptr].context.clone();
    let core = state.ptrs[ptr].core;
    let seq = state.ptrs[ptr].seq;
    let offset = state.ptrs[ptr].offset;

    let block = context.get_block(seq, core, None).await?;
    let base = context.get_loaded_context(core)?;
    let tree = block
        .tree
        .get(offset as usize)
        .ok_or(crate::Error::Message("tree offset out of range"))?;

    let mut keys = Vec::with_capacity(tree.keys.len());
    for d in &tree.keys {
        keys.push(key_delta(state, &base, d, &block, core, &block).await?);
    }
    let mut children = Vec::with_capacity(tree.children.len());
    for d in &tree.children {
        children.push(child_delta(state, &base, d, &block, core).await?);
    }

    let kid = state.array_id();
    let cid = state.array_id();
    let node = state.nodes.len();
    state.nodes.push(Node {
        t: block.t,
        keys: CompressedArray::from_delta(keys, kid),
        children: CompressedArray::from_delta(children, cid),
    });
    state.ptrs[ptr].value = Some(node);
    Ok(node)
}

async fn key_delta(
    state: &mut TreeState,
    base: &Arc<Context>,
    d: &EncDelta,
    origin: &Block,
    origin_core: u64,
    _current: &Block,
) -> crate::Result<Delta<KeyId>> {
    if d.kind == OP_COHORT {
        let co = d
            .ptr
            .ok_or(crate::Error::Message("cohort without pointer"))?;
        let (cohort_block, _) = load_block(base, co, origin, origin_core).await?;
        let ctx = base.get_loaded_context(co.core)?;
        let entries = cohort_block
            .cohorts
            .get(co.offset as usize)
            .ok_or(crate::Error::Message("cohort offset out of range"))?;
        let mut deltas = Vec::with_capacity(entries.len());
        for p in entries {
            let key = match p.ptr {
                Some(loc) => {
                    let (target, _) = load_block(&ctx, loc, &cohort_block, co.core).await?;
                    Some(build_key(state, &ctx, loc, &target)?)
                }
                None => None,
            };
            deltas.push(Delta::new(false, p.kind, p.index, key.map(DeltaRef::Id)));
        }
        return Ok(Delta::cohort(false, Some(DeltaRef::Loc(co)), deltas));
    }

    let key = match d.ptr {
        Some(loc) => {
            let (target, _) = load_block(base, loc, origin, origin_core).await?;
            Some(build_key(state, base, loc, &target)?)
        }
        None => None,
    };
    Ok(Delta::new(false, d.kind, d.index, key.map(DeltaRef::Id)))
}

async fn child_delta(
    state: &mut TreeState,
    base: &Arc<Context>,
    d: &EncDelta,
    origin: &Block,
    origin_core: u64,
) -> crate::Result<Delta<PtrId>> {
    if d.kind == OP_COHORT {
        let co = d
            .ptr
            .ok_or(crate::Error::Message("cohort without pointer"))?;
        let (cohort_block, _) = load_block(base, co, origin, origin_core).await?;
        let ctx = base.get_loaded_context(co.core)?;
        let entries = cohort_block
            .cohorts
            .get(co.offset as usize)
            .ok_or(crate::Error::Message("cohort offset out of range"))?;
        let mut deltas = Vec::with_capacity(entries.len());
        for p in entries {
            let ptr = match p.ptr {
                Some(loc) => {
                    if !ctx.has_core(loc.core) {
                        ctx.update_maybe(loc.core, Some(&cohort_block)).await?;
                    }
                    Some(state.create_ptr(
                        ctx.clone(),
                        loc.core,
                        loc.seq,
                        loc.offset,
                        false,
                        None,
                    )?)
                }
                None => None,
            };
            deltas.push(Delta::new(false, p.kind, p.index, ptr.map(DeltaRef::Id)));
        }
        return Ok(Delta::cohort(false, Some(DeltaRef::Loc(co)), deltas));
    }

    let ptr = match d.ptr {
        Some(loc) => {
            if !base.has_core(loc.core) {
                base.update_maybe(loc.core, Some(origin)).await?;
            }
            Some(state.create_ptr(base.clone(), loc.core, loc.seq, loc.offset, false, None)?)
        }
        None => None,
    };
    Ok(Delta::new(false, d.kind, d.index, ptr.map(DeltaRef::Id)))
}

/// Resolve a referenced block, reusing `origin` for the same-block fast path.
async fn load_block(
    base: &Arc<Context>,
    loc: Loc,
    origin: &Block,
    origin_core: u64,
) -> crate::Result<(Block, u64)> {
    if loc.core == 0 && loc.seq == origin.seq && origin_core == 0 {
        return Ok((origin.clone(), 0));
    }
    let block = base.get_block(loc.seq, loc.core, Some(origin)).await?;
    Ok((block, loc.core))
}

fn build_key(
    state: &mut TreeState,
    base: &Arc<Context>,
    loc: Loc,
    target: &Block,
) -> crate::Result<KeyId> {
    let data: &KeyData = target
        .keys
        .get(loc.offset as usize)
        .ok_or(crate::Error::Message("key offset out of range"))?;
    let key_context = base.get_loaded_context(loc.core)?;
    let value_ptr = data.value_ptr.as_ref().map(|p| ValuePointer {
        context: key_context,
        core: p.core,
        seq: p.seq,
        offset: p.offset,
        split: p.split,
    });
    Ok(state.add_key(
        base.clone(),
        loc.core,
        loc.seq,
        loc.offset,
        false,
        data.key.clone(),
        data.value.clone(),
        value_ptr,
    ))
}

/// Read a key's value, following external value pointers.
pub(crate) async fn inflate_value(state: &TreeState, key: KeyId) -> crate::Result<Option<Bytes>> {
    let k = &state.keys[key];
    if let Some(v) = &k.value {
        return Ok(Some(v.clone()));
    }
    let Some(vp) = &k.value_ptr else {
        return Ok(None);
    };

    if vp.split == 0 {
        let block = vp.context.get_block(vp.seq, vp.core, None).await?;
        return Ok(block.values.get(vp.offset as usize).cloned());
    }

    // A split value is the first value of `split` blocks followed by the tail
    // of the last one, starting at `offset`.
    let mut out = Vec::new();
    for i in 0..=vp.split {
        let block = vp
            .context
            .get_block(vp.seq - vp.split + i, vp.core, None)
            .await?;
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
