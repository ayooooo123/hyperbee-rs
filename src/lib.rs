//! Hyperbee on hypercore: a B-tree compatible with npm hyperbee2 2.21.1.

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};

use bytes::Bytes;
use corestore::{Corestore, GetOptions as StoreGetOptions};
use tokio::sync::Mutex as AsyncMutex;

mod compat;
pub(crate) mod context;
pub(crate) mod encoding;
mod inflate;
mod ranges;
mod tree;
mod write;

pub use ranges::{RangeOptions, RangeStream};
pub use write::Batch;

use context::{Context, Mappings, Shared};
use tree::{KeyId, Node, PtrId, TreeState};

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

#[derive(Default)]
pub(crate) struct RootState {
    pub loaded: bool,
    pub ptr: Option<PtrId>,
    pub loc: Option<(u64, u64, u64)>,
    pub length: u64,
    pub key: [u8; 32],
}

pub(crate) struct BeeInner {
    pub shared: Arc<Shared>,
    pub context: Arc<Context>,
    pub state: Arc<AsyncMutex<TreeState>>,
    pub root: StdMutex<RootState>,
    pub writable: bool,
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
        let session = Arc::new(store.get(opts)?);
        let shared = Arc::new(Shared {
            store: store.clone(),
            mappings: StdMutex::new(Mappings::default()),
            other: StdMutex::new(HashMap::new()),
        });
        let context = Context::new(shared.clone(), session.clone(), *session.key());
        let state = Arc::new(AsyncMutex::new(TreeState::new()));
        let inner = Arc::new(BeeInner {
            shared,
            context: context.clone(),
            state,
            root: StdMutex::new(RootState::default()),
            writable: session.writable(),
        });
        let bee = Bee { inner };
        bee.set_root_from_core().await?;
        Ok(bee)
    }

    async fn set_root_from_core(&self) -> Result<()> {
        let length = self.inner.context.core.length();
        let mut st = self.inner.state.lock().await;
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
        self.inner.context.core.length()
    }

    /// The root core's key.
    pub fn key(&self) -> [u8; 32] {
        *self.inner.context.core.key()
    }

    pub(crate) fn root_loc(&self) -> (bool, Option<(u64, u64, u64)>) {
        let root = lock(&self.inner.root);
        (root.loaded, root.loc)
    }

    pub(crate) async fn root_ptr(&self, st: &mut TreeState) -> Result<Option<PtrId>> {
        let (loaded, loc) = self.root_loc();
        if !loaded {
            return Ok(None);
        }
        if let Some(ptr) = lock(&self.inner.root).ptr {
            return Ok(Some(ptr));
        }
        let Some((core, seq, offset)) = loc else {
            return Ok(None);
        };
        let ptr = st.create_ptr(self.inner.context.clone(), core, seq, offset, false, None)?;
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
        root.length = ptr.map_or(0, |_| length);
        root.key = key;
    }

    /// JS `get`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Entry>> {
        let mut st = self.inner.state.lock().await;
        let Some(mut ptr) = self.root_ptr(&mut st).await? else {
            return Ok(None);
        };

        loop {
            let node = inflate::inflate(&mut st, ptr).await?;
            let (found, s, e, cmp) = {
                let n = &st.nodes[node];
                search_node(&st, n, key)?
            };
            let has_children = st.nodes[node].children.length() > 0;

            if let Some(kid) = found {
                let (seq, key, value) = {
                    let k = &st.keys[kid];
                    let value = match &k.value {
                        Some(v) => Some(v.clone()),
                        None => inflate::inflate_value(&st, kid).await?,
                    };
                    (k.seq, k.key.clone(), value)
                };
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
    }

    /// JS `createReadStream`.
    pub fn range(&self, opts: RangeOptions) -> RangeStream {
        RangeStream::new(self.clone(), opts)
    }

    /// JS `write()`.
    pub fn batch(&self) -> Batch {
        Batch::new(self.clone())
    }

    /// Read-only snapshot at `length`.
    pub fn checkout(&self, length: u64) -> Bee {
        let inner = Arc::new(BeeInner {
            shared: self.inner.shared.clone(),
            context: self.inner.context.clone(),
            state: self.inner.state.clone(),
            root: StdMutex::new(RootState {
                loaded: true,
                ptr: None,
                loc: (length > 0).then(|| (0, length - 1, 0)),
                length,
                key: *self.inner.context.core.key(),
            }),
            writable: false,
        });
        Bee { inner }
    }

    /// Move to the root core's latest length.
    pub async fn update(&self) -> Result<bool> {
        let length = self.inner.context.core.length();
        let mut st = self.inner.state.lock().await;
        let ptr = if length == 0 {
            None
        } else {
            Some(st.create_ptr(self.inner.context.clone(), 0, length - 1, 0, false, None)?)
        };
        let changed = lock(&self.inner.root).ptr != ptr;
        self.set_root_ptr(&st, ptr, length, *self.inner.context.core.key());
        Ok(changed)
    }

    pub(crate) fn context(&self) -> &Arc<Context> {
        &self.inner.context
    }

    pub(crate) fn writable(&self) -> bool {
        self.inner.writable
    }
}
