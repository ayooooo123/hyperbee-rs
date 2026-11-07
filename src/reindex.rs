use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{Block, Loc, Metadata, TYPE_COMPAT, encode_block};
use crate::{Bee, Change, Error, Head, Result};
/// Optional value transform used when copying changes.
pub type ValueMap<'a> = dyn FnMut(&[u8], &[u8], &Change) -> Option<Bytes> + 'a;

async fn remap(
    local: &Arc<Context>,
    src: &Arc<Context>,
    ptr: &mut Loc,
    origin: &Block,
    copied: &HashMap<([u8; 32], u64), u64>,
) -> Result<()> {
    if ptr.core != 0 && !src.has_core(ptr.core) {
        src.update_maybe(ptr.core, Some(origin)).await?;
    }
    let key = src.core_key(ptr.core)?;
    if let Some(seq) = copied.get(&(key, ptr.seq)) {
        ptr.core = 0;
        ptr.seq = *seq;
    } else {
        ptr.core = local.get_core_offset_local(src, ptr.core)?;
    }
    Ok(())
}

impl Bee {
    /// Copy changes newer than `until` into the local core, optionally mapping values.
    pub async fn reindex<U>(
        &self,
        mut until: U,
        prefetch: u64,
        map: Option<&mut ValueMap<'_>>,
    ) -> Result<usize>
    where
        U: FnMut(&Change) -> bool,
    {
        if !self.writable() {
            return Err(Error::NotWritable);
        }
        let mut stream = self.changes(None, prefetch).await?;
        let mut changes = Vec::new();
        while let Some(change) = stream.next().await? {
            if until(&change) {
                break;
            }
            changes.push(change);
        }
        self.copy_changes(changes, map).await
    }

    /// Reindex with an asynchronous, fallible head predicate.
    pub async fn reindex_async<U, F>(
        &self,
        mut until: U,
        prefetch: u64,
        map: Option<&mut ValueMap<'_>>,
    ) -> Result<usize>
    where
        U: FnMut(Head) -> F,
        F: Future<Output = Result<bool>>,
    {
        if !self.writable() {
            return Err(Error::NotWritable);
        }
        let mut stream = self.changes(None, prefetch).await?;
        let mut changes = Vec::new();
        while let Some(change) = stream.next().await? {
            if until(change.head).await? {
                break;
            }
            changes.push(change);
        }
        self.copy_changes(changes, map).await
    }

    async fn copy_changes(
        &self,
        changes: Vec<Change>,
        mut map: Option<&mut ValueMap<'_>>,
    ) -> Result<usize> {
        if changes.is_empty() {
            return Ok(0);
        }
        let state = self.inner.state.lock().await;
        let local = self.context_by_key(self.inner.context.local).await?;
        local.update(true).await?;
        let base = local.core.length();
        let mut copied = HashMap::new();
        let mut blocks = Vec::new();
        let count = changes.len();
        for (i, mut change) in changes.into_iter().rev().enumerate() {
            let src = self.context_by_key(change.head.key).await?;
            let start = base + blocks.len() as u64;
            let first = change
                .head
                .length
                .checked_sub(change.batch.len() as u64)
                .ok_or(Error::Message("invalid batch length"))?;
            for j in 0..change.batch.len() {
                copied.insert((change.head.key, first + j as u64), start + j as u64);
            }
            if let Some(map) = map.as_mut() {
                for j in 0..change.batch.len() {
                    for k in 0..change.batch[j].keys.len() {
                        let key = change.batch[j].keys[k].key.clone();
                        let ptr = change.batch[j].keys[k].value_ptr;
                        if let Some(value) = change.batch[j].keys[k].value.clone() {
                            if let Some(value) = map(&key, &value, &change) {
                                change.batch[j].keys[k].value = Some(value);
                            }
                        } else if let Some(ptr) = ptr {
                            let idx = ptr
                                .seq
                                .checked_sub(first)
                                .and_then(|v| usize::try_from(v).ok())
                                .filter(|&v| v < change.batch.len());
                            let idx = idx
                                .filter(|_| ptr.core == 0 && ptr.split == 0)
                                .ok_or(Error::Message("value outside batch"))?;
                            let offset = usize::try_from(ptr.offset)
                                .map_err(|_| Error::Message("invalid value offset"))?;
                            let value = change.batch[idx]
                                .values
                                .get(offset)
                                .ok_or(Error::Message("invalid value offset"))?
                                .clone();
                            if let Some(value) = map(&key, &value, &change) {
                                change.batch[idx].values[offset] = value;
                            }
                        }
                    }
                }
            }
            for mut block in change.batch {
                if block.type_ == TYPE_COMPAT {
                    return Err(Error::Message("cannot index compat blocks"));
                }
                let origin = block.clone();
                for tree in &mut block.tree {
                    for delta in tree.keys.iter_mut().chain(tree.children.iter_mut()) {
                        if let Some(ptr) = &mut delta.ptr {
                            remap(&local, &src, ptr, &origin, &copied).await?;
                        }
                    }
                }
                for cohort in &mut block.cohorts {
                    for delta in cohort {
                        if let Some(ptr) = &mut delta.ptr {
                            remap(&local, &src, ptr, &origin, &copied).await?;
                        }
                    }
                }
                for key in &mut block.keys {
                    if let Some(ptr) = &mut key.value_ptr {
                        let mut loc = Loc {
                            core: ptr.core,
                            seq: ptr.seq,
                            offset: ptr.offset,
                        };
                        remap(&local, &src, &mut loc, &origin, &copied).await?;
                        ptr.core = loc.core;
                        ptr.seq = loc.seq;
                    }
                }
                block.metadata = None;
                block.previous = None;
                blocks.push(block);
            }
            let last = blocks
                .last_mut()
                .ok_or(Error::Message("empty change batch"))?;
            last.previous = if i == 0 {
                if let Some(tail) = change.tail {
                    Some(Loc {
                        core: local.get_core_offset_by_key(tail.key).await?,
                        seq: tail.length - 1,
                        offset: 0,
                    })
                } else {
                    None
                }
            } else {
                Some(Loc {
                    core: 0,
                    seq: start - 1,
                    offset: 0,
                })
            };
        }
        if local.needs_flush() {
            local.set_checkpoint(base + blocks.len() as u64);
            let (cores, mappings) = local.flush();
            blocks
                .last_mut()
                .ok_or(Error::Message("empty reindex"))?
                .metadata = Some(Metadata { cores, mappings });
        }
        let mut buffers = Vec::with_capacity(blocks.len());
        for mut block in blocks {
            block.checkpoint = local.checkpoint();
            block.invalidation = local.invalidation();
            buffers.push(encode_block(block));
        }
        local.core.append(&buffers).await?;
        drop(state);
        self.move_to(Head {
            key: *local.core.key(),
            length: local.core.length(),
        })
        .await?;
        Ok(count)
    }
}
