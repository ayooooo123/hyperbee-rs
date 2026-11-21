//! Hyperbee on hypercore: a B-tree compatible with npm hyperbee2 2.21.4.

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard, Weak};

use bytes::Bytes;
use corestore::{Corestore, GetOptions as StoreGetOptions};
use tokio::sync::Mutex as AsyncMutex;

mod changes;
mod compat;
pub(crate) mod context;
pub(crate) mod encoding;
mod inflate;
mod ranges;
mod reindex;
mod tree;
mod write;

pub use changes::{Change, ChangesStream};
pub use encoding::{
    BatchPtr, Block, CoreInfo, EncDelta, KeyData, Loc, Metadata, RawTree, ValuePtr,
};
pub use ranges::{RangeOptions, RangeStream};
pub use reindex::ValueMap;
pub use write::Batch;

use context::{Context, Mappings, Shared};
use tree::{KeyId, Locked, Node, PtrId, TreeState};

fn lock<T>(m: &StdMutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug)]
pub enum Error {
    Core(hypercore::CoreError),
    Store(corestore::Error),
    Storage(hypercore::storage::StorageError),
    Encoding(compact_encoding::Error),
    BlockNotAvailable,
    BadCoreIndex(u64),
    UnknownBlockType(u64),
    NotWritable,
    Message(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Core(e) => write!(f, "{e}"),
            Error::Store(e) => write!(f, "{e}"),
            Error::Storage(e) => write!(f, "{e}"),
            Error::Encoding(e) => write!(f, "{e}"),
            Error::BlockNotAvailable => f.write_str("Block not available"),
            Error::BadCoreIndex(i) => write!(f, "Bad core index: {i}"),
            Error::UnknownBlockType(t) => write!(f, "Unknown block type: {t}"),
            Error::NotWritable => f.write_str("Not writable"),
            Error::Message(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

impl From<hypercore::CoreError> for Error {
    fn from(e: hypercore::CoreError) -> Self {
        Error::Core(e)
    }
}

impl From<corestore::Error> for Error {
    fn from(e: corestore::Error) -> Self {
        Error::Store(e)
    }
}

impl From<hypercore::storage::StorageError> for Error {
    fn from(e: hypercore::storage::StorageError) -> Self {
        Error::Storage(e)
    }
}

impl From<compact_encoding::Error> for Error {
    fn from(e: compact_encoding::Error) -> Self {
        Error::Encoding(e)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn search_node(
    st: &TreeState,
    node: &Node,
    key: &[u8],
) -> Result<(Option<KeyId>, usize, usize, std::cmp::Ordering)> {
    let mut s = 0;
    let mut e = node.keys.length();
    let mut c = std::cmp::Ordering::Equal;
    while s < e {
        let mid = (s + e) >> 1;
        let kid = node
            .keys
            .get(mid)
            .ok_or(Error::Message("null key pointer"))?;
        c = key.cmp(st.keys[kid].key.as_ref());
        if c == std::cmp::Ordering::Equal {
            return Ok((Some(kid), s, e, c));
        }
        if c == std::cmp::Ordering::Less {
            e = mid;
        } else {
            s = mid + 1;
        }
    }
    Ok((None, s, e, c))
}

/// `head()` of a bee: the root block's length and the root core's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Head {
    pub length: u64,
    pub key: [u8; 32],
}

/// One entry as returned by `get` and the range stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub seq: u64,
    pub key: Bytes,
    pub value: Bytes,
}
/// Options passed to each Hypercore block read.
pub type ReadOptions = hypercore::GetOptions;
/// Select a block-encryption key for each non-root core.
pub type EncryptionProvider = Arc<dyn Fn(&[u8; 32]) -> Option<[u8; 32]> + Send + Sync>;

tokio::task_local! {
    pub(crate) static READ_OPTIONS: ReadOptions;
}

#[derive(Default)]
pub(crate) struct RootState {
    pub loaded: bool,
    pub ptr: Option<PtrId>,
    pub loc: Option<(u64, u64, u64)>,
    pub length: u64,
    pub key: [u8; 32],
    pub context: Option<Arc<Context>>,
}

pub(crate) struct BeeInner {
    pub shared: Arc<Shared>,
    pub context: Arc<Context>,
    pub state: Arc<AsyncMutex<TreeState>>,
    /// Serializes batches, reindex and root moves on one tree, like JS
    /// `context.lock`; taken before `state`. Readers never take it.
    pub writer: Arc<AsyncMutex<()>>,
    pub root: StdMutex<RootState>,
    pub writable: bool,
    pub view: bool,
    /// Read options for calls that pass none; see `set_default_read_options`.
    pub read_options: StdMutex<ReadOptions>,
}

/// A hyperbee. Cheap to clone: clones share the tree and storage.
#[derive(Clone)]
pub struct Bee {
    pub(crate) inner: Arc<BeeInner>,
}

impl Bee {
    /// JS `new Hyperbee(store, { key })`. `None` opens the core named `bee`.
    pub async fn open(store: &Corestore, key: Option<[u8; 32]>) -> Result<Bee> {
        let opts = match key {
            Some(k) => StoreGetOptions::key(k),
            None => StoreGetOptions::name("bee"),
        };
        let session = Arc::new(store.get(opts).await?);
        Self::from_session(store, session).await
    }

    /// Open a bee on an existing core session without opening another root core.
    pub async fn from_session(store: &Corestore, session: Arc<hypercore::Session>) -> Result<Bee> {
        Self::from_session_with_encryption(store, session, None).await
    }

    /// Open an existing session with per-batch-core encryption keys.
    pub async fn from_session_with_encryption(
        store: &Corestore,
        session: Arc<hypercore::Session>,
        encryption: Option<EncryptionProvider>,
    ) -> Result<Bee> {
        let shared = Arc::new(Shared {
            store: store.clone(),
            mappings: StdMutex::new(Mappings::default()),
            other: StdMutex::new(HashMap::new()),
            encryption,
        });
        let context = Context::new(shared.clone(), session.clone(), *session.key());
        let state = Arc::new(AsyncMutex::new(TreeState::new()));
        let inner = Arc::new(BeeInner {
            shared,
            context: context.clone(),
            state,
            writer: Arc::new(AsyncMutex::new(())),
            root: StdMutex::new(RootState::default()),
            writable: session.writable(),
            view: false,
            read_options: StdMutex::new(ReadOptions::default()),
        });
        let bee = Bee { inner };
        bee.set_root_from_core().await?;
        Ok(bee)
    }

    async fn set_root_from_core(&self) -> Result<()> {
        let length = self.inner.context.core.length();
        let mut st = self.inner.state.lock().await;
        st.check_fork(&self.inner.context.core);
        let ptr = if length == 0 {
            None
        } else {
            Some(st.create_ptr(self.inner.context.clone(), 0, length - 1, 0, false, None)?)
        };
        let mut root = lock(&self.inner.root);
        root.loaded = true;
        root.ptr = ptr;
        root.loc = ptr.map(|_| (0, length - 1, 0));
        root.length = length;
        root.key = *self.inner.context.core.key();
        root.context = Some(self.inner.context.clone());
        Ok(())
    }

    /// JS `head()`. `None` only before the tree is ready.
    pub fn head(&self) -> Option<Head> {
        let root = lock(&self.inner.root);
        root.loaded.then_some(Head {
            length: root.length,
            key: root.key,
        })
    }

    /// Number of blocks in the root core.
    pub fn length(&self) -> u64 {
        self.context().core.length()
    }

    /// The root core's key.
    pub fn key(&self) -> [u8; 32] {
        *self.context().core.key()
    }

    pub(crate) fn root_loc(&self) -> (bool, Option<(u64, u64, u64)>) {
        let root = lock(&self.inner.root);
        (root.loaded, root.loc)
    }

    /// The root pointer, created again from the root location if its slot
    /// was freed by the node cache.
    pub(crate) fn root_ptr(&self, st: &mut TreeState) -> Result<Option<PtrId>> {
        let (loaded, loc) = self.root_loc();
        if !loaded {
            return Ok(None);
        }
        if let Some(ptr) = lock(&self.inner.root).ptr
            && st.has_ptr(ptr)
        {
            return Ok(Some(ptr));
        }
        let Some((core, seq, offset)) = loc else {
            return Ok(None);
        };
        let context = self.context();
        st.check_fork(&context.core);
        let ptr = st.create_ptr(context, core, seq, offset, false, None)?;
        lock(&self.inner.root).ptr = Some(ptr);
        Ok(Some(ptr))
    }

    pub(crate) fn set_root_ptr(
        &self,
        st: &TreeState,
        ptr: Option<PtrId>,
        length: u64,
        key: [u8; 32],
    ) {
        let mut root = lock(&self.inner.root);
        root.loaded = true;
        root.ptr = ptr;
        root.loc = ptr.map(|p| (st.ptrs[p].core, st.ptrs[p].seq, st.ptrs[p].offset));
        if let Some(ptr) = ptr {
            root.context = Some(st.ptrs[ptr].context.clone());
        }
        root.length = ptr.map_or(0, |_| length);
        root.key = key;
    }

    /// Read options used by calls that pass none: `get`, `range`, `changes`,
    /// batch flushes, `cores` and `reindex`. `checkout` and `snapshot`
    /// handles start with the options of the handle they come from.
    pub fn set_default_read_options(&self, options: ReadOptions) {
        *lock(&self.inner.read_options) = options;
    }

    pub(crate) fn read_options(&self) -> ReadOptions {
        lock(&self.inner.read_options).clone()
    }

    /// JS `get`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        self.get_with_options(key, self.read_options()).await
    }

    /// Get with Hypercore timeout, wait and request options.
    pub async fn get_with_options(
        &self,
        key: &[u8],
        options: ReadOptions,
    ) -> Result<Option<Entry>> {
        READ_OPTIONS.scope(options, self.get_inner(key)).await
    }

    async fn get_inner(&self, key: &[u8]) -> Result<Option<Entry>> {
        let mut st = Locked::new(self.inner.state.clone()).await;
        let Some(mut ptr) = self.root_ptr(&mut st)? else {
            return Ok(None);
        };

        for _ in 0..tree::MAX_DEPTH {
            let node = inflate::inflate(&mut st, &mut ptr).await?;
            let (found, s, e, cmp) = {
                let n = &st.nodes[node];
                search_node(&st, n, key)?
            };
            let has_children = st.nodes[node].children.length() > 0;

            if let Some(kid) = found {
                let (seq, key) = (st.keys[kid].seq, st.keys[kid].key.clone());
                let value = inflate::inflate_value(&mut st, kid).await?;
                return Ok(Some(Entry {
                    seq,
                    key,
                    value: value.unwrap_or_default(),
                }));
            }

            if !has_children {
                return Ok(None);
            }
            let index = if cmp == std::cmp::Ordering::Less {
                e
            } else {
                s
            };
            let n = &st.nodes[node];
            ptr = n
                .children
                .get(index)
                .ok_or(Error::Message("child index out of range"))?;
        }
        Err(Error::Message("tree too deep"))
    }

    /// JS `createReadStream`.
    pub fn range(&self, opts: RangeOptions) -> RangeStream {
        RangeStream::new(self.clone(), opts)
    }

    /// Range stream with Hypercore read options.
    pub fn range_with_options(&self, opts: RangeOptions, read: ReadOptions) -> RangeStream {
        self.range(opts).with_read_options(read)
    }
    /// Walk historical batches from newest to oldest.
    pub async fn changes(&self, head: Option<Head>, prefetch: u64) -> Result<ChangesStream> {
        ChangesStream::new(self.clone(), head, prefetch).await
    }

    /// Changes stream with Hypercore read options.
    pub async fn changes_with_options(
        &self,
        head: Option<Head>,
        prefetch: u64,
        read: ReadOptions,
    ) -> Result<ChangesStream> {
        Ok(self.changes(head, prefetch).await?.with_read_options(read))
    }

    /// JS `write()`.
    pub fn batch(&self) -> Batch {
        Batch::new(self.clone())
    }

    /// Close the backing store for an owning bee; snapshots do not own it.
    pub async fn close(&self) {
        if !self.inner.view {
            self.inner.shared.store.close().await;
        }
    }

    /// Read-only snapshot at `length`.
    pub async fn checkout(&self, head: Head) -> Result<Bee> {
        let context = self.context_by_key(head.key).await?;
        let inner = Arc::new(BeeInner {
            shared: self.inner.shared.clone(),
            context: context.clone(),
            state: self.inner.state.clone(),
            writer: self.inner.writer.clone(),
            root: StdMutex::new(RootState {
                loaded: true,
                ptr: None,
                loc: (head.length > 0).then(|| (0, head.length - 1, 0)),
                length: head.length,
                key: head.key,
                context: Some(context),
            }),
            writable: false,
            view: true,
            read_options: StdMutex::new(self.read_options()),
        });
        Ok(Bee { inner })
    }

    /// A read-only view pinned to the current root.
    pub async fn snapshot(&self) -> Result<Bee> {
        self.checkout(self.head().ok_or(Error::Message("root not loaded"))?)
            .await
    }

    /// Move this view to a head, retaining its write permission.
    pub async fn move_to(&self, head: Head) -> Result<()> {
        let context = self.context_by_key(head.key).await?;
        let _writer = self.inner.writer.lock().await;
        let _state = self.inner.state.lock().await;
        let mut root = lock(&self.inner.root);
        root.loaded = true;
        root.ptr = None;
        root.loc = (head.length > 0).then(|| (0, head.length - 1, 0));
        root.length = head.length;
        root.key = head.key;
        root.context = Some(context);
        Ok(())
    }

    async fn context_by_key(&self, key: [u8; 32]) -> Result<Arc<Context>> {
        let key = lock(&self.inner.shared.mappings).resolve(key);
        if key == *self.inner.context.core.key() {
            return Ok(self.inner.context.clone());
        }
        if let Some(context) = lock(&self.inner.shared.other)
            .get(&key)
            .and_then(Weak::upgrade)
        {
            return Ok(context);
        }
        let mut options = StoreGetOptions::key(key);
        options.encryption_key = self
            .inner
            .shared
            .encryption
            .as_ref()
            .and_then(|provider| provider(&key));
        let session = Arc::new(self.inner.shared.store.get(options).await?);
        let mut other = lock(&self.inner.shared.other);
        if let Some(context) = other.get(&key).and_then(Weak::upgrade) {
            return Ok(context);
        }
        let context = Context::new(self.inner.shared.clone(), session, self.inner.context.local);
        other.insert(key, Arc::downgrade(&context));
        Ok(context)
    }

    /// List keys in the current context's core table.
    pub async fn cores(&self, local: bool) -> Result<Vec<[u8; 32]>> {
        let context = self.context();
        READ_OPTIONS
            .scope(self.read_options(), context.update(true))
            .await?;
        let mut keys = context.core_keys()?;
        if !local {
            keys.retain(|key| key != &context.local);
        }
        Ok(keys)
    }

    /// Move to the root core's latest length.
    pub async fn update(&self) -> Result<bool> {
        let context = self.context();
        let _writer = self.inner.writer.lock().await;
        context.reset_if_forked();
        let length = context.core.length();
        let mut st = self.inner.state.lock().await;
        st.check_fork(&context.core);
        let ptr = if length == 0 {
            None
        } else {
            Some(st.create_ptr(context.clone(), 0, length - 1, 0, false, None)?)
        };
        let changed = lock(&self.inner.root).ptr != ptr;
        self.set_root_ptr(&st, ptr, length, *context.core.key());
        Ok(changed)
    }

    pub(crate) fn context(&self) -> Arc<Context> {
        lock(&self.inner.root)
            .context
            .clone()
            .unwrap_or_else(|| self.inner.context.clone())
    }

    pub(crate) fn writable(&self) -> bool {
        self.inner.writable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cross_core_contexts_close_with_the_last_bee() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let source = Bee::from_session(
            &store,
            Arc::new(store.get(StoreGetOptions::name("source")).await.unwrap()),
        )
        .await
        .unwrap();
        let mut write = source.batch();
        for i in 0..64 {
            write.put(format!("item/{i:03}"), format!("value/{i}"));
        }
        write.flush().await.unwrap();
        let source_key = source.key();
        let source_core = source.inner.context.core.core().clone();

        let bee = Bee::from_session(
            &store,
            Arc::new(store.get(StoreGetOptions::name("view")).await.unwrap()),
        )
        .await
        .unwrap();
        let local_key = bee.key();
        let local_core = bee.inner.context.core.core().clone();
        bee.move_to(source.head().unwrap()).await.unwrap();
        let mut write = bee.batch();
        write.put(&b"item/064"[..], &b"value/64"[..]);
        write.flush().await.unwrap();

        let reader = Bee::open(&store, Some(local_key)).await.unwrap();
        assert_eq!(
            reader.get(b"item/000").await.unwrap().unwrap().value,
            "value/0"
        );
        assert_eq!(
            reader.get(b"item/064").await.unwrap().unwrap().value,
            "value/64"
        );
        let foreign = reader.context_by_key(source_key).await.unwrap();
        let again = reader.context_by_key(source_key).await.unwrap();
        assert!(Arc::ptr_eq(&foreign, &again));
        let unused = store.get(StoreGetOptions::name("unused")).await.unwrap();
        let unused_key = *unused.key();
        let unused_core = unused.core().clone();
        drop(unused);
        let unused = reader.context_by_key(unused_key).await.unwrap();
        let expired = Arc::downgrade(&unused);
        drop(unused);
        assert_eq!(expired.strong_count(), 0);
        let reloaded = reader.context_by_key(unused_key).await.unwrap();
        assert_eq!(*reloaded.core.key(), unused_key);
        drop(reloaded);
        assert_eq!(unused_core.session_count(), 0);
        let shared = [
            Arc::downgrade(&source.inner.shared),
            Arc::downgrade(&bee.inner.shared),
            Arc::downgrade(&reader.inner.shared),
        ];
        let contexts = [
            Arc::downgrade(&reader.inner.context),
            Arc::downgrade(&foreign),
        ];
        drop(again);
        drop(foreign);
        drop(reader);
        drop(bee);
        drop(source);

        let shared_alive = shared.iter().filter(|weak| weak.strong_count() > 0).count();
        let contexts_alive = contexts
            .iter()
            .filter(|weak| weak.strong_count() > 0)
            .count();
        let sessions = source_core.session_count() + local_core.session_count();
        eprintln!("RETENTION shared={shared_alive} contexts={contexts_alive} sessions={sessions}");
        assert_eq!(shared_alive, 0, "cross-core cache retains its owner");
        assert_eq!(
            contexts_alive, 0,
            "all tree and context handles were dropped"
        );
        assert_eq!(
            sessions, 0,
            "cross-core sessions must close without store.close"
        );
        store.close().await;
    }

    #[tokio::test]
    async fn mapped_keys_share_the_resolved_context() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        let target = store.get(StoreGetOptions::name("target")).await.unwrap();
        let target_key = *target.key();
        drop(target);
        let alias = [0x42; 32];
        let root_alias = [0x43; 32];
        lock(&bee.inner.shared.mappings).add(&[(alias, target_key), (root_alias, bee.key())], true);

        let direct = bee.context_by_key(target_key).await.unwrap();
        let mapped = bee.context_by_key(alias).await.unwrap();
        assert!(Arc::ptr_eq(&direct, &mapped));
        assert_eq!(*mapped.core.key(), target_key);
        let index = bee
            .inner
            .context
            .get_core_offset_by_key(alias)
            .await
            .unwrap();
        let indexed = bee.inner.context.get_loaded_context(index).await.unwrap();
        assert!(Arc::ptr_eq(&direct, &indexed));
        let local = bee.context_by_key(root_alias).await.unwrap();
        assert!(Arc::ptr_eq(&local, &bee.inner.context));
        store.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_cold_core_calls_share_one_session() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let target = store.get(StoreGetOptions::name("target")).await.unwrap();
        let target_key = *target.key();
        drop(target);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let provider: EncryptionProvider = Arc::new(move |key| {
            assert_eq!(*key, target_key);
            // Both callers pass the cold-cache check before either opens a session.
            barrier.wait();
            None
        });
        let session = Arc::new(store.get(StoreGetOptions::name("view")).await.unwrap());
        let bee = Bee::from_session_with_encryption(&store, session, Some(provider))
            .await
            .unwrap();
        let index = bee
            .inner
            .context
            .get_core_offset_by_key(target_key)
            .await
            .unwrap();
        let first = tokio::spawn({
            let context = bee.inner.context.clone();
            async move { context.core(index).await.unwrap() }
        });
        let second = tokio::spawn({
            let context = bee.inner.context.clone();
            async move { context.core(index).await.unwrap() }
        });
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        let target_core = first.core().clone();
        assert_eq!(target_core.session_count(), 1);
        drop(first);
        drop(second);
        assert_eq!(target_core.session_count(), 1);
        drop(bee);
        assert_eq!(target_core.session_count(), 0);
        store.close().await;
    }

    fn self_referencing_root() -> Block {
        let own = Some(Loc::default());
        let insert = |index| EncDelta {
            kind: encoding::OP_INSERT,
            index,
            ptr: own,
        };
        Block {
            type_: encoding::TYPE_LATEST,
            tree: vec![RawTree {
                keys: vec![insert(0)],
                // Both children name this node's own (seq 0, offset 0).
                children: vec![insert(0), insert(1)],
            }],
            keys: vec![KeyData {
                key: Bytes::from_static(b"m"),
                value: Some(Bytes::from_static(b"v")),
                value_ptr: None,
            }],
            t: encoding::T,
            ..Default::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn child_pointer_cycle_errors_and_releases_the_tree() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        let block = encoding::encode_block(self_referencing_root());
        bee.inner.context.core.append(&[block]).await.unwrap();
        bee.update().await.unwrap();

        let get = tokio::spawn({
            let bee = bee.clone();
            async move { bee.get(b"a").await }
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), get)
            .await
            .expect("cycle must not spin")
            .unwrap();
        assert!(matches!(result, Err(Error::Message("tree too deep"))));

        let mut batch = bee.batch();
        batch.put(&b"a"[..], &b"x"[..]);
        assert!(batch.flush().await.is_err());

        let other = bee.snapshot().await.unwrap();
        let entry = other.get(b"m").await.unwrap().unwrap();
        assert_eq!(entry.value, &b"v"[..]);
        store.close().await;
    }

    #[tokio::test]
    async fn update_after_truncate_reads_the_new_fork() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        let mut batch = writer.batch();
        batch.put(&b"a"[..], &b"old"[..]);
        batch.flush().await.unwrap();

        let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
        assert_eq!(reader.get(b"a").await.unwrap().unwrap().value, "old");

        writer.inner.context.core.truncate(0, None).await.unwrap();
        let rewriter = Bee::open(&store, Some(writer.key())).await.unwrap();
        let mut batch = rewriter.batch();
        batch.put(&b"a"[..], &b"new"[..]);
        batch.flush().await.unwrap();
        assert_eq!(rewriter.length(), 1);

        reader.update().await.unwrap();
        assert_eq!(reader.get(b"a").await.unwrap().unwrap().value, "new");
        store.close().await;
    }

    #[tokio::test]
    async fn empty_flush_after_move_to_genesis_appends_a_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        let mut batch = bee.batch();
        for key in ["a", "b", "c"] {
            batch.put(key, "value");
        }
        batch.flush().await.unwrap();
        let length = bee.length();

        bee.move_to(Head {
            key: bee.key(),
            length: 0,
        })
        .await
        .unwrap();
        bee.batch().flush().await.unwrap();
        assert_eq!(bee.length(), length + 1);
        assert_eq!(
            bee.head(),
            Some(Head {
                key: bee.key(),
                length: length + 1,
            })
        );
        let reopened = Bee::open(&store, Some(bee.key())).await.unwrap();
        assert!(reopened.get(b"a").await.unwrap().is_none());
        store.close().await;
    }

    #[tokio::test]
    async fn reindex_of_type_0_source_returns_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        // `fixtures/block-0.json`: one legacy type 0 block.
        let block = Bytes::from_static(&[
            0x00, 0x00, 0x00, 0x00, 0x0e, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
            0x02, 0x05, 0x61, 0x6c, 0x70, 0x68, 0x61, 0x01, 0x31, 0x04, 0x62, 0x65, 0x74, 0x61,
            0x01, 0x32, 0x00,
        ]);
        let source = store.get(StoreGetOptions::name("source")).await.unwrap();
        source.append(&[block]).await.unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        bee.move_to(Head {
            key: *source.key(),
            length: 1,
        })
        .await
        .unwrap();
        assert_eq!(bee.get(b"alpha").await.unwrap().unwrap().value, "1");
        assert!(bee.reindex(|_| false, 0, None).await.is_err());
        store.close().await;
    }

    #[tokio::test]
    async fn default_read_options_time_out_missing_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        let mut batch = writer.batch();
        batch.put(&b"key"[..], &b"value"[..]);
        batch.flush().await.unwrap();
        let core = &writer.inner.context.core;
        core.clear(0, core.length()).await.unwrap();

        let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
        reader.set_default_read_options(ReadOptions {
            timeout: Some(std::time::Duration::from_millis(50)),
            ..Default::default()
        });
        let snapshot = reader.snapshot().await.unwrap();
        for bee in [&reader, &snapshot] {
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), bee.get(b"key"))
                .await
                .expect("the default timeout applies");
            assert!(matches!(
                result,
                Err(Error::Core(hypercore::CoreError::Timeout))
            ));
        }
        store.close().await;
    }

    fn item(i: usize) -> Bytes {
        Bytes::from(format!("key/{i:05}"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_block_does_not_stall_cached_reads_or_writes() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        let mut batch = writer.batch();
        for i in 0..1000 {
            batch.put(item(i), &b"v"[..]);
        }
        batch.flush().await.unwrap();
        let root_seq = writer.length() - 1;
        let seq_of = async |i| writer.get(&item(i)).await.unwrap().unwrap().seq;
        let cached = item(0);
        let cached_seq = seq_of(0).await;
        let mut missing = None;
        for i in 1..1000 {
            let seq = seq_of(i).await;
            if seq != cached_seq && seq != root_seq {
                missing = Some((item(i), seq));
                break;
            }
        }
        let (missing, missing_seq) = missing.expect("a leaf outside the root and cached blocks");
        assert_ne!(cached_seq, root_seq);

        let bee = Bee::open(&store, Some(writer.key())).await.unwrap();
        assert!(bee.writable());
        bee.get(&cached).await.unwrap().unwrap();
        let core = &bee.inner.context.core;
        core.clear(missing_seq, missing_seq + 1).await.unwrap();

        let stuck_get = tokio::spawn({
            let bee = bee.clone();
            let missing = missing.clone();
            async move { bee.get(&missing).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!stuck_get.is_finished(), "no peer has the cleared block");

        let wait = Duration::from_secs(5);
        let entry = tokio::time::timeout(wait, bee.get(&cached))
            .await
            .expect("a cached read does not wait for another read's block")
            .unwrap();
        assert_eq!(entry.unwrap().value, "v");
        let added = Bytes::from(format!("{}x", String::from_utf8_lossy(&cached)));
        let mut batch = bee.batch();
        batch.put(added.clone(), &b"w"[..]);
        tokio::time::timeout(wait, batch.flush())
            .await
            .expect("a flush on cached nodes does not wait for a read's block")
            .unwrap();

        let stuck_flush = tokio::spawn({
            let bee = bee.clone();
            async move {
                let mut batch = bee.batch();
                batch.put(missing, &b"w"[..]);
                batch.flush().await
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!stuck_flush.is_finished(), "no peer has the cleared block");
        for key in [&cached, &added] {
            tokio::time::timeout(wait, bee.get(key))
                .await
                .expect("a cached read does not wait for a flush's block")
                .unwrap()
                .unwrap();
        }

        stuck_get.abort();
        stuck_flush.abort();
        tokio::time::timeout(wait, store.close())
            .await
            .expect("store closes");
    }

    /// A read that got its bytes before a truncate and rewrite must not install
    /// them under the pointer of the new fork's block at the same location.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fork_during_a_fetch_installs_the_new_fork() {
        use context::{FETCH_GATE, FetchGate};
        let wait = std::time::Duration::from_secs(5);
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        let mut batch = writer.batch();
        batch.put(&b"a"[..], &b"old"[..]);
        batch.flush().await.unwrap();

        let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
        let gate = Arc::new(FetchGate::default());
        gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        let paused = tokio::spawn(FETCH_GATE.scope(gate.clone(), {
            let reader = reader.clone();
            async move { reader.get(b"a").await }
        }));
        tokio::time::timeout(wait, gate.reached.notified())
            .await
            .expect("the read got the old root block");

        writer.inner.context.core.truncate(0, None).await.unwrap();
        let rewriter = Bee::open(&store, Some(writer.key())).await.unwrap();
        let mut batch = rewriter.batch();
        batch.put(&b"a"[..], &b"new"[..]);
        batch.flush().await.unwrap();
        assert_eq!(rewriter.length(), 1, "same (core, seq, offset) as before");
        // The new root gets its own pointer; the paused read's pointer is freed.
        reader.update().await.unwrap();
        reader.inner.state.lock().await.sweep();

        gate.release.notify_one();
        let entry = tokio::time::timeout(wait, paused)
            .await
            .expect("the paused read ends")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(entry.value, "new");
        assert_eq!(reader.get(b"a").await.unwrap().unwrap().value, "new");
        store.close().await;
    }

    type Model = std::collections::BTreeMap<Bytes, Bytes>;

    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    fn model_range(model: &Model, o: &RangeOptions) -> Vec<(Bytes, Bytes)> {
        let mut out: Vec<_> = model
            .iter()
            .filter(|(k, _)| {
                o.gt.as_ref().is_none_or(|b| *k > b)
                    && o.gte.as_ref().is_none_or(|b| *k >= b)
                    && o.lt.as_ref().is_none_or(|b| *k < b)
                    && o.lte.as_ref().is_none_or(|b| *k <= b)
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if o.reverse {
            out.reverse();
        }
        if let Some(limit) = o.limit {
            out.truncate(limit);
        }
        out
    }

    /// Model versions: version `i` is published before its flush starts and
    /// gets its head when the flush returns.
    type Versions = Arc<StdMutex<Vec<(Option<Head>, Arc<Model>)>>>;

    async fn check_reads(
        bee: Bee,
        versions: Versions,
        done: Arc<std::sync::atomic::AtomicBool>,
        mut seed: u64,
    ) -> usize {
        const KEYS: u64 = 5000;
        let mut ops = 0;
        while ops < 60 || !done.load(std::sync::atomic::Ordering::Acquire) {
            ops += 1;
            // Snapshot at a finished version, or the live bee with every
            // version it can be at during the call.
            let snapshot = lcg(&mut seed).is_multiple_of(2);
            let (target, before) = if snapshot {
                let (head, model) = {
                    let list = lock(&versions);
                    let finished: Vec<_> = list.iter().filter(|(h, _)| h.is_some()).collect();
                    let (head, model) = finished[lcg(&mut seed) as usize % finished.len()];
                    (head.unwrap(), model.clone())
                };
                (bee.checkout(head).await.unwrap(), Some(model))
            } else {
                (bee.clone(), None)
            };
            let low = lock(&versions).len().saturating_sub(2);
            let models = |before: &Option<Arc<Model>>| -> Vec<Arc<Model>> {
                match before {
                    Some(model) => vec![model.clone()],
                    None => lock(&versions)[low..]
                        .iter()
                        .map(|(_, m)| m.clone())
                        .collect(),
                }
            };
            if lcg(&mut seed).is_multiple_of(3) {
                let key = Bytes::from(format!("k{:05}", lcg(&mut seed) % KEYS));
                let got = target.get(&key).await.unwrap().map(|e| e.value);
                let ok = models(&before).iter().any(|m| m.get(&key) == got.as_ref());
                assert!(ok, "get {key:?} = {got:?} matches no model version");
            } else {
                let a = Bytes::from(format!("k{:05}", lcg(&mut seed) % KEYS));
                let b = Bytes::from(format!("k{:05}", lcg(&mut seed) % KEYS));
                let (a, b) = if a <= b { (a, b) } else { (b, a) };
                let mut o = RangeOptions {
                    reverse: lcg(&mut seed).is_multiple_of(2),
                    limit: lcg(&mut seed).is_multiple_of(3).then_some(150),
                    ..Default::default()
                };
                match lcg(&mut seed) % 3 {
                    0 => (o.gte, o.lt) = (Some(a), Some(b)),
                    1 => (o.gt, o.lte) = (Some(a), Some(b)),
                    _ => o.gte = Some(a),
                }
                let mut stream = target.range(o.clone());
                let mut got = Vec::new();
                while let Some(e) = stream.next().await.unwrap() {
                    got.push((e.key, e.value));
                    tokio::task::yield_now().await;
                }
                let ok = models(&before).iter().any(|m| model_range(m, &o) == got);
                assert!(
                    ok,
                    "range {o:?} ({} entries) matches no model version",
                    got.len()
                );
            }
        }
        ops
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_and_writer_match_the_model() {
        const KEYS: u64 = 5000;
        let value = |tag: String, seed: u64| {
            let pad = if seed.is_multiple_of(7) {
                1100
            } else {
                seed as usize % 40
            };
            Bytes::from(format!("{tag}{}", "x".repeat(pad)))
        };
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let writer = Bee::open(&store, None).await.unwrap();
        let mut seed = 42u64;
        let mut model = Model::new();
        let mut batch = writer.batch();
        for i in 0..4000u64 {
            let (k, v) = (
                Bytes::from(format!("k{:05}", i)),
                value(format!("v0-{i}"), lcg(&mut seed)),
            );
            model.insert(k.clone(), v.clone());
            batch.put(k, v);
        }
        batch.flush().await.unwrap();

        // A cold bee with a tiny cache, so every task evicts and sweeps.
        let bee = Bee::open(&store, Some(writer.key())).await.unwrap();
        drop(writer);
        // Two nodes: even a batch's root path is evicted by readers while the
        // batch waits for a block, unless it is retained.
        bee.inner.state.lock().await.max_nodes = 2;
        let versions: Versions = Arc::new(StdMutex::new(vec![(bee.head(), Arc::new(model))]));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let readers: Vec<_> = (0..4u64)
            .map(|i| {
                tokio::spawn(check_reads(
                    bee.clone(),
                    versions.clone(),
                    done.clone(),
                    1000 + i,
                ))
            })
            .collect();
        let write = tokio::spawn({
            let (bee, versions, done) = (bee.clone(), versions.clone(), done.clone());
            async move {
                for round in 1..=25u64 {
                    let mut next = (*lock(&versions).last().unwrap().1).clone();
                    let mut batch = bee.batch();
                    for i in 0..150 {
                        let r = lcg(&mut seed);
                        let key = Bytes::from(format!("k{:05}", r % KEYS));
                        if r.is_multiple_of(3) {
                            next.remove(&key);
                            batch.del(key);
                        } else {
                            let v = value(format!("v{round}-{i}"), lcg(&mut seed));
                            next.insert(key.clone(), v.clone());
                            batch.put(key, v);
                        }
                    }
                    lock(&versions).push((None, Arc::new(next)));
                    batch.flush().await.unwrap();
                    lock(&versions).last_mut().unwrap().0 = bee.head();
                    tokio::task::yield_now().await;
                }
                done.store(true, std::sync::atomic::Ordering::Release);
            }
        });

        let all = async {
            write.await.unwrap();
            let mut ops = 0;
            for reader in readers {
                ops += reader.await.unwrap();
            }
            ops
        };
        let ops = tokio::time::timeout(std::time::Duration::from_secs(300), all)
            .await
            .expect("stress test finishes");
        let st = bee.inner.state.lock().await;
        eprintln!(
            "STRESS reads={ops} nodes={} ptrs={} keys={}",
            st.nodes.len(),
            st.ptrs.len(),
            st.keys.len()
        );
        drop(st);
        store.close().await;
    }
}
