//! Per-core context: core table, checkpoint, mappings and block loading.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use bytes::Bytes;
use compact_encoding::{decode, encode};
use corestore::Corestore;
use corestore::GetOptions as StoreGetOptions;
use hypercore::GetOptions;
use hypercore::Session;

use crate::Error;
use crate::encoding::{Block, CoreInfo, decode_block};

pub(crate) const CHECKPOINT_KEY: &str = "hyperbee/checkpoint";

pub(crate) type MappingsFlush = Option<Vec<([u8; 32], [u8; 32])>>;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}
fn read_options() -> GetOptions {
    crate::READ_OPTIONS
        .try_with(Clone::clone)
        .unwrap_or_default()
}

#[derive(Default)]
pub(crate) struct Mappings {
    known: HashMap<[u8; 32], [u8; 32]>,
    reverse: HashMap<[u8; 32], [u8; 32]>,
    order: Vec<([u8; 32], [u8; 32])>,
    dirty: bool,
    flushed: Option<Vec<([u8; 32], [u8; 32])>>,
}

impl Mappings {
    pub fn resolve(&self, key: [u8; 32]) -> [u8; 32] {
        self.known.get(&key).copied().unwrap_or(key)
    }

    pub fn unresolve(&self, key: [u8; 32]) -> [u8; 32] {
        self.reverse.get(&key).copied().unwrap_or(key)
    }

    pub fn add(&mut self, mappings: &[([u8; 32], [u8; 32])], persisted: bool) {
        for &(from, to) in mappings {
            if from == to || self.known.contains_key(&from) {
                continue;
            }
            self.known.insert(from, to);
            self.reverse.insert(to, from);
            self.order.push((from, to));
            self.flushed = None;
            if !persisted {
                self.dirty = true;
            }
        }
    }

    pub fn flush(&mut self) -> Option<Vec<([u8; 32], [u8; 32])>> {
        self.dirty = false;
        if self.order.is_empty() {
            return None;
        }
        if self.flushed.is_none() {
            self.flushed = Some(self.order.clone());
        }
        self.flushed.clone()
    }
}

pub(crate) struct Shared {
    pub store: Corestore,
    pub mappings: Mutex<Mappings>,
    // Contexts own Shared; the lookup table must not keep its owner alive.
    pub other: Mutex<HashMap<[u8; 32], Weak<Context>>>,
    pub encryption: Option<crate::EncryptionProvider>,
}

pub(crate) struct CtxState {
    pub length: u64,
    pub checkpoint: u64,
    pub invalidation: u64,
    pub opened: Vec<Option<Arc<Session>>>,
    pub cores: Vec<CoreInfo>,
    pub changed: bool,
}

pub(crate) struct Context {
    pub shared: Arc<Shared>,
    pub core: Arc<Session>,
    pub local: [u8; 32],
    state: Mutex<CtxState>,
}

impl Context {
    pub fn new(shared: Arc<Shared>, core: Arc<Session>, local: [u8; 32]) -> Arc<Self> {
        Arc::new(Self {
            shared,
            core,
            local,
            state: Mutex::new(CtxState {
                length: 0,
                checkpoint: 0,
                invalidation: 0,
                opened: Vec::new(),
                cores: Vec::new(),
                changed: false,
            }),
        })
    }

    pub fn length(&self) -> u64 {
        lock(&self.state).length
    }

    pub fn invalidation(&self) -> u64 {
        lock(&self.state).invalidation
    }

    fn state_snapshot(&self) -> (u64, u64, u64, Vec<CoreInfo>) {
        let s = lock(&self.state);
        (s.length, s.checkpoint, s.invalidation, s.cores.clone())
    }

    fn raw_core_key(&self, index: u64) -> crate::Result<[u8; 32]> {
        if index == 0 {
            return Ok(*self.core.key());
        }
        let s = lock(&self.state);
        s.cores
            .get(index as usize - 1)
            .map(|c| c.key)
            .ok_or(Error::BadCoreIndex(index))
    }

    /// Key of a core index, with persisted mappings applied.
    pub fn core_key(&self, index: u64) -> crate::Result<[u8; 32]> {
        let raw = self.raw_core_key(index)?;
        Ok(lock(&self.shared.mappings).resolve(raw))
    }
    pub fn core_keys(&self) -> crate::Result<Vec<[u8; 32]>> {
        let count = lock(&self.state).cores.len();
        (0..=count).map(|i| self.core_key(i as u64)).collect()
    }

    /// The session for a core index, opening it through the store on first use.
    pub async fn core(&self, index: u64) -> crate::Result<Arc<Session>> {
        if index == 0 {
            return Ok(self.core.clone());
        }
        let (key, opened) = {
            let s = lock(&self.state);
            let key = s
                .cores
                .get(index as usize - 1)
                .map(|c| c.key)
                .ok_or(Error::BadCoreIndex(index))?;
            (key, s.opened[index as usize - 1].clone())
        };
        if let Some(session) = opened {
            return Ok(session);
        }
        let key = lock(&self.shared.mappings).resolve(key);
        let mut options = StoreGetOptions::key(key);
        options.encryption_key = self
            .shared
            .encryption
            .as_ref()
            .and_then(|provider| provider(&key));
        let session = Arc::new(self.shared.store.get(options).await?);
        let mut s = lock(&self.state);
        if index as usize <= s.opened.len() {
            if let Some(existing) = &s.opened[index as usize - 1] {
                return Ok(existing.clone());
            }
            s.opened[index as usize - 1] = Some(session.clone());
        }
        Ok(session)
    }

    pub fn has_core(&self, index: u64) -> bool {
        index as usize <= lock(&self.state).opened.len()
    }

    fn needs_update(&self, core: u64, origin_invalidation: u64) -> bool {
        if !self.has_core(core) {
            return true;
        }
        origin_invalidation > self.invalidation()
    }

    pub async fn update(&self, persist: bool) -> crate::Result<()> {
        let core_length = self.core.length();
        if self.length() >= core_length {
            return Ok(());
        }
        self.inflate_checkpoint_from_length(core_length, persist)
            .await
    }

    async fn inflate_checkpoint_from_length(
        &self,
        length: u64,
        persist: bool,
    ) -> crate::Result<()> {
        let seq = length - 1;
        let buffer = self
            .core
            .get(seq, read_options())
            .await?
            .ok_or(Error::BlockNotAvailable)?;
        let block = decode_block(buffer, seq)?;
        self.inflate_checkpoint(length, &block, persist).await
    }

    async fn inflate_checkpoint(
        &self,
        length: u64,
        block: &Block,
        persist: bool,
    ) -> crate::Result<()> {
        let (cur_length, cur_checkpoint, _, _) = self.state_snapshot();
        if length <= cur_length {
            return Ok(());
        }

        let checkpoint = block.checkpoint;
        let mut cores: Option<Vec<CoreInfo>> = None;

        if checkpoint > 0 && checkpoint != cur_checkpoint {
            let buffer = self
                .core
                .get(checkpoint - 1, read_options())
                .await?
                .ok_or(Error::BlockNotAvailable)?;
            let cp_block = decode_block(buffer, checkpoint - 1)?;
            if length <= self.length() {
                return Ok(());
            }
            let meta = cp_block.metadata.unwrap_or_default();
            if let Some(mappings) = &meta.mappings {
                let persisted = self.core.key() == &self.local;
                lock(&self.shared.mappings).add(mappings, persisted);
            }
            cores = Some(meta.cores);
        }

        if length <= self.length() {
            return Ok(());
        }

        {
            let mut s = lock(&self.state);
            if length <= s.length {
                return Ok(());
            }
            if let Some(cores) = cores {
                s.cores = cores;
                let n = s.cores.len();
                s.opened.resize(n, None);
            }
            s.checkpoint = checkpoint;
            s.invalidation = block.invalidation;
            s.length = length;
        }

        if persist {
            self.core
                .set_user_data(CHECKPOINT_KEY, Some(&encode(&length)))
                .await?;
        }
        Ok(())
    }

    pub async fn update_maybe(&self, core: u64, origin: Option<&Block>) -> crate::Result<()> {
        let origin_invalidation = origin.map_or(0, |b| b.invalidation);
        if !self.needs_update(core, origin_invalidation) {
            return Ok(());
        }

        if let Some(block) = origin {
            self.inflate_checkpoint(block.seq + 1, block, false).await?;
            if !self.needs_update(core, origin_invalidation) {
                return Ok(());
            }
        }

        let cached = self
            .core
            .user_data(CHECKPOINT_KEY)?
            .map(decode::<u64>)
            .transpose()?
            .unwrap_or(0);
        if cached > self.length() && cached <= self.core.length() {
            self.inflate_checkpoint_from_length(cached, false).await?;
            if !self.needs_update(core, origin_invalidation) {
                return Ok(());
            }
        }

        self.update(true).await
    }

    pub async fn get_block(
        self: &Arc<Self>,
        seq: u64,
        core: u64,
        origin: Option<&Block>,
    ) -> crate::Result<Block> {
        let buffer = self.get_block_buffer(seq, core, origin).await?;
        let block = decode_block(buffer, seq)?;
        if block.invalidation != 0 {
            let ctx = self.get_loaded_context(core).await?;
            if block.invalidation > ctx.invalidation() {
                ctx.update_maybe(0, Some(&block)).await?;
            }
        }
        Ok(block)
    }

    pub async fn get_block_buffer(
        &self,
        seq: u64,
        core: u64,
        origin: Option<&Block>,
    ) -> crate::Result<Bytes> {
        if core > lock(&self.state).cores.len() as u64 {
            self.update_maybe(core, origin).await?;
        }
        let session = self.core(core).await?;
        session
            .get(seq, read_options())
            .await?
            .ok_or(Error::BlockNotAvailable)
    }

    pub async fn get_loaded_context(self: &Arc<Self>, core: u64) -> crate::Result<Arc<Context>> {
        if core == 0 {
            return Ok(self.clone());
        }
        if core as usize > lock(&self.state).cores.len() {
            return Err(Error::BadCoreIndex(core));
        }
        let session = self.core(core).await?;
        let key = *session.key();
        let mut other = lock(&self.shared.other);
        if let Some(ctx) = other.get(&key).and_then(Weak::upgrade) {
            return Ok(ctx);
        }
        let ctx = Context::new(self.shared.clone(), session, self.local);
        other.insert(key, Arc::downgrade(&ctx));
        Ok(ctx)
    }

    pub fn get_core_offset_local(
        self: &Arc<Self>,
        context: &Arc<Context>,
        core: u64,
    ) -> crate::Result<u64> {
        if Arc::ptr_eq(context, self) {
            return Ok(core);
        }
        let key = context.raw_core_key(core)?;
        let key = lock(&self.shared.mappings).unresolve(key);
        if let Some(offset) = self.find_core(key) {
            return Ok(offset);
        }
        let mut s = lock(&self.state);
        s.changed = true;
        s.cores.push(CoreInfo {
            key,
            ..Default::default()
        });
        s.opened.push(None);
        Ok(s.cores.len() as u64)
    }

    fn find_core(&self, key: [u8; 32]) -> Option<u64> {
        if *self.core.key() == key {
            return Some(0);
        }
        lock(&self.state)
            .cores
            .iter()
            .position(|c| c.key == key)
            .map(|i| i as u64 + 1)
    }

    pub async fn get_core_offset_by_key(&self, key: [u8; 32]) -> crate::Result<u64> {
        let key = lock(&self.shared.mappings).unresolve(key);
        if let Some(offset) = self.find_core(key) {
            return Ok(offset);
        }
        self.update(true).await?;
        if let Some(offset) = self.find_core(key) {
            return Ok(offset);
        }
        let mut s = lock(&self.state);
        s.changed = true;
        s.cores.push(CoreInfo {
            key,
            ..Default::default()
        });
        s.opened.push(None);
        Ok(s.cores.len() as u64)
    }

    pub fn checkpoint(&self) -> u64 {
        lock(&self.state).checkpoint
    }

    pub fn set_checkpoint(&self, checkpoint: u64) {
        lock(&self.state).checkpoint = checkpoint;
    }

    pub fn needs_flush(&self) -> bool {
        if lock(&self.state).changed {
            return true;
        }
        lock(&self.shared.mappings).dirty
    }

    pub fn flush(&self) -> (Vec<CoreInfo>, MappingsFlush) {
        let (mappings, dirty) = {
            let mut m = lock(&self.shared.mappings);
            let dirty = m.dirty;
            (m.flush(), dirty)
        };
        let mut s = lock(&self.state);
        if dirty {
            s.invalidation += 1;
        }
        s.changed = false;
        (s.cores.clone(), mappings)
    }
}

impl std::fmt::Debug for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Context")
            .field("core", &self.core.key())
            .finish_non_exhaustive()
    }
}
