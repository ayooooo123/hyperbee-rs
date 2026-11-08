//! Inflating serialized tree deltas into nodes, keyed by `ptr` position.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{Block, EncDelta, KeyData, Loc, OP_COHORT};
use crate::tree::{
    CompressedArray, Delta, DeltaRef, KeyId, Node, NodeId, PtrId, TreeState, ValuePointer,
};

type Blocks = HashMap<([u8; 32], u64, u64), Arc<Block>>;

/// Fill `ptr`'s value, reading every block the node references.
pub(crate) async fn inflate(state: &mut TreeState, ptr: PtrId) -> crate::Result<NodeId> {
    if let Some(node) = state.ptrs[ptr].value {
        return Ok(node);
    }

    let context = state.ptrs[ptr].context.clone();
    let core = state.ptrs[ptr].core;
    let seq = state.ptrs[ptr].seq;
    let offset = state.ptrs[ptr].offset;

    let block = Arc::new(context.get_block(seq, core, None).await?);
    let mut blocks = Blocks::new();
    let base = context.get_loaded_context(core).await?;
    let tree = block
        .tree
        .get(offset as usize)
        .ok_or(crate::Error::Message("tree offset out of range"))?;

    let mut keys = Vec::with_capacity(tree.keys.len());
    for d in &tree.keys {
        keys.push(key_delta(state, &base, d, &block, core, &mut blocks).await?);
    }
    let mut children = Vec::with_capacity(tree.children.len());
    for d in &tree.children {
        children.push(child_delta(state, &base, d, &block, core, &mut blocks).await?);
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
    origin: &Arc<Block>,
    origin_core: u64,
    blocks: &mut Blocks,
) -> crate::Result<Delta<KeyId>> {
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
                    Some(build_key(state, &ctx, loc, &target).await?)
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
            Some(build_key(state, base, loc, &target).await?)
        }
        None => None,
    };
    Ok(Delta::new(false, d.kind, d.index, key.map(DeltaRef::Id)))
}

async fn child_delta(
    state: &mut TreeState,
    base: &Arc<Context>,
    d: &EncDelta,
    origin: &Arc<Block>,
    origin_core: u64,
    blocks: &mut Blocks,
) -> crate::Result<Delta<PtrId>> {
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
        return Ok(Delta::cohort(
            false,
            Some(DeltaRef::Loc(base.clone(), co)),
            deltas,
        ));
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

async fn build_key(
    state: &mut TreeState,
    base: &Arc<Context>,
    loc: Loc,
    target: &Block,
) -> crate::Result<KeyId> {
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
