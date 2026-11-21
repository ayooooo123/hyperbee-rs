//! In-memory tree: compressed arrays, pointers and the node/pointer/key arena.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

use crate::context::Context;
use crate::encoding::{EncDelta, Loc, OP_COHORT, OP_DEL, OP_INSERT, OP_SET};

/// The tree lock, released while a caller waits for blocks.
pub(crate) struct Locked {
    mutex: Arc<AsyncMutex<TreeState>>,
    guard: Option<OwnedMutexGuard<TreeState>>,
}

impl Locked {
    pub async fn new(mutex: Arc<AsyncMutex<TreeState>>) -> Self {
        let guard = mutex.clone().lock_owned().await;
        Self {
            mutex,
            guard: Some(guard),
        }
    }

    /// Run `io` without the lock, then take it again. Ids held across this
    /// call may be freed meanwhile unless the caller retained them.
    pub async fn unlocked<F: Future>(&mut self, io: F) -> F::Output {
        self.guard = None;
        let out = io.await;
        self.guard = Some(self.mutex.clone().lock_owned().await);
        out
    }
}

impl Deref for Locked {
    type Target = TreeState;
    fn deref(&self) -> &TreeState {
        self.guard.as_ref().expect("tree locked")
    }
}

impl DerefMut for Locked {
    fn deref_mut(&mut self) -> &mut TreeState {
        self.guard.as_mut().expect("tree locked")
    }
}

/// An arena id: slot index plus the slot's generation. A freed slot is reused
/// under the next generation, so an id kept past its value's lifetime fails
/// `contains` and `get` instead of naming a different value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Id {
    index: u32,
    generation: u32,
}

pub(crate) type PtrId = Id;
pub(crate) type NodeId = Id;
pub(crate) type KeyId = Id;

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

/// Slot storage for nodes, pointers and keys; freed slots are reused.
pub(crate) struct Arena<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
    len: usize,
}

impl<T> Arena<T> {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }

    /// Number of live values.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.len
    }

    fn insert(&mut self, value: T) -> Id {
        self.len += 1;
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            slot.value = Some(value);
            return Id {
                index,
                generation: slot.generation,
            };
        }
        let index = u32::try_from(self.slots.len()).expect("fewer than 2^32 live tree values");
        self.slots.push(Slot {
            generation: 0,
            value: Some(value),
        });
        Id {
            index,
            generation: 0,
        }
    }

    fn free_slot(&mut self, index: usize) {
        let slot = &mut self.slots[index];
        slot.value = None;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(index as u32);
        self.len -= 1;
    }

    fn remove(&mut self, id: Id) {
        if self.contains(id) {
            self.free_slot(id.index as usize);
        }
    }

    pub fn get(&self, id: Id) -> Option<&T> {
        self.slots
            .get(id.index as usize)
            .filter(|slot| slot.generation == id.generation)
            .and_then(|slot| slot.value.as_ref())
    }

    pub fn contains(&self, id: Id) -> bool {
        self.get(id).is_some()
    }

    /// Free every value `keep` rejects.
    fn retain(&mut self, mut keep: impl FnMut(usize, &T) -> bool) {
        for index in 0..self.slots.len() {
            if let Some(value) = &self.slots[index].value
                && !keep(index, value)
            {
                self.free_slot(index);
            }
        }
    }

    fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|slot| slot.value.as_ref())
    }
}

impl<T> std::ops::Index<Id> for Arena<T> {
    type Output = T;
    fn index(&self, id: Id) -> &T {
        self.get(id).expect("live tree id")
    }
}

impl<T> std::ops::IndexMut<Id> for Arena<T> {
    fn index_mut(&mut self, id: Id) -> &mut T {
        self.slots
            .get_mut(id.index as usize)
            .filter(|slot| slot.generation == id.generation)
            .and_then(|slot| slot.value.as_mut())
            .expect("live tree id")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValuePointer {
    pub context: Arc<Context>,
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
    pub split: u64,
}

#[derive(Debug)]
pub(crate) struct Key {
    pub context: Arc<Context>,
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
    pub changed: bool,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub value_ptr: Option<ValuePointer>,
}

#[derive(Debug)]
pub(crate) struct Ptr {
    pub context: Arc<Context>,
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
    pub changed: bool,
    pub value: Option<NodeId>,
    pub dirty: Option<usize>,
    pub changed_by: Option<u64>,
    /// Position in the node cache's use order; 0 when not cached.
    used: u64,
    /// JS `Pointer.retained`: pinned while above `TreeState::retained`.
    retained: u64,
}

pub(crate) struct Node {
    pub t: u64,
    pub keys: CompressedArray<KeyId>,
    pub children: CompressedArray<PtrId>,
}

#[derive(Clone, Debug)]
pub(crate) enum DeltaRef<T> {
    /// A location decoded from, or written into, the block of `context`; the
    /// core index is only meaningful against that context (JS `Pointer.context`).
    Loc(Arc<Context>, Loc),
    Id(T),
}

#[derive(Clone, Debug)]
pub(crate) struct Delta<T> {
    pub changed: bool,
    pub kind: u8,
    pub index: usize,
    pub ptr: Option<DeltaRef<T>>,
    pub cohort: Vec<Delta<T>>,
}

impl<T> Delta<T> {
    pub fn new(changed: bool, kind: u8, index: usize, ptr: Option<DeltaRef<T>>) -> Self {
        Self {
            changed,
            kind,
            index,
            ptr,
            cohort: Vec::new(),
        }
    }

    pub fn cohort(changed: bool, ptr: Option<DeltaRef<T>>, cohort: Vec<Delta<T>>) -> Self {
        Self {
            changed,
            kind: OP_COHORT,
            index: 0,
            ptr,
            cohort,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompressedArray<T> {
    pub id: u64,
    pub entries: Vec<Option<T>>,
    pub delta: Vec<Delta<T>>,
    pub uentries: Option<Vec<Option<T>>>,
    pub updates: usize,
}

impl<T: Copy> CompressedArray<T> {
    pub fn empty(id: u64) -> Self {
        Self {
            id,
            entries: Vec::new(),
            delta: Vec::new(),
            uentries: None,
            updates: 0,
        }
    }

    pub fn from_delta(delta: Vec<Delta<T>>, id: u64) -> Self {
        let mut entries = Vec::new();
        for d in &delta {
            apply(&mut entries, d);
        }
        Self {
            id,
            entries,
            delta,
            uentries: None,
            updates: 0,
        }
    }

    pub fn length(&self) -> usize {
        self.entries.len()
    }

    pub fn ulength(&self) -> usize {
        self.uentries.as_ref().map_or(self.entries.len(), Vec::len)
    }

    pub fn get(&self, index: usize) -> Option<T> {
        self.entries.get(index).copied().flatten()
    }

    pub fn uget(&self, index: usize) -> Option<T> {
        match &self.uentries {
            Some(u) => u.get(index).copied().flatten(),
            None => self.get(index),
        }
    }

    pub fn reset(&mut self, new_id: u64) {
        if self.updates > 0 || self.uentries.is_some() {
            self.commit(new_id);
        }
    }

    /// Freeze the pending updates into a new array; the old array is left at
    /// its base state, as `CompressedArray.commit()` does.
    pub fn commit(&mut self, new_id: u64) -> CompressedArray<T> {
        let mut c = CompressedArray::empty(new_id);
        c.delta = std::mem::take(&mut self.delta);
        c.entries = self.uentries.take().unwrap_or_else(|| self.entries.clone());
        self.delta = if self.updates == 0 {
            c.delta.clone()
        } else {
            c.delta[..c.delta.len() - self.updates].to_vec()
        };
        self.updates = 0;
        c
    }

    fn update(&mut self) {
        if self.uentries.is_none() {
            self.uentries = Some(self.entries.clone());
        }
    }

    fn touch(&mut self) {
        self.updates += 1;
    }

    pub fn insert(&mut self, index: usize, pointer: T) {
        self.update();
        let u = self.uentries.as_mut().expect("updated");
        if insert_at(u, index, Some(pointer)) {
            self.touch();
            self.delta.push(Delta::new(
                true,
                OP_INSERT,
                index,
                Some(DeltaRef::Id(pointer)),
            ));
        }
    }

    pub fn delete(&mut self, index: usize) {
        self.update();
        let u = self.uentries.as_mut().expect("updated");
        if del_at(u, index) {
            self.touch();
            self.delta.push(Delta::new(true, OP_DEL, index, None));
        }
    }

    pub fn set(&mut self, index: usize, pointer: T) {
        self.update();
        let u = self.uentries.as_mut().expect("updated");
        if set_at(u, index, Some(pointer)) {
            self.touch();
            self.delta
                .push(Delta::new(true, OP_SET, index, Some(DeltaRef::Id(pointer))));
        }
    }

    pub fn push(&mut self, pointer: T) {
        self.update();
        let index = self.uentries.as_ref().expect("updated").len();
        self.insert(index, pointer);
    }

    pub fn unshift(&mut self, pointer: T) {
        self.insert(0, pointer);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.update();
        let index = self.uentries.as_ref().expect("updated").len();
        if index == 0 {
            return None;
        }
        let head = self.uget(index - 1);
        self.delete(index - 1);
        head
    }

    pub fn shift(&mut self) -> Option<T> {
        self.update();
        if self.uentries.as_ref().expect("updated").is_empty() {
            return None;
        }
        let head = self.uget(0);
        self.delete(0);
        head
    }

    /// Port of `CompressedArray.flush`.
    pub fn flush(&mut self, max: usize, min: usize) -> Vec<Delta<T>> {
        self.updates = 0;
        self.uentries = None;

        if self.entries.len() < self.delta.len() {
            self.delta = vec![rebuild_cohort(&self.entries)];
            return self.delta.clone();
        }

        let overflow = self.delta.iter().any(|d| d.index >= 256);
        if self.delta.len() <= max && !overflow {
            return self.delta.clone();
        }

        let mut direct = Vec::new();
        while let Some(last) = self.delta.last() {
            if last.kind == OP_COHORT {
                break;
            }
            direct.push(self.delta.pop().expect("checked"));
        }
        direct.reverse();
        let l = direct.len();

        if l > min && l < self.entries.len() && !overflow && self.delta.len() < max {
            self.delta.push(Delta::cohort(true, None, direct));
        } else {
            self.delta = vec![rebuild_cohort(&self.entries)];
        }

        self.delta.clone()
    }
}

fn rebuild_cohort<T: Copy>(entries: &[Option<T>]) -> Delta<T> {
    Delta::cohort(
        true,
        None,
        entries
            .iter()
            .enumerate()
            .map(|(i, p)| Delta::new(true, OP_INSERT, i, p.map(DeltaRef::Id)))
            .collect(),
    )
}

fn apply<T: Copy>(entries: &mut Vec<Option<T>>, d: &Delta<T>) {
    match d.kind {
        OP_COHORT => {
            for dd in &d.cohort {
                apply(entries, dd);
            }
        }
        OP_INSERT => {
            insert_at(entries, d.index, ptr_of(d));
        }
        OP_DEL => {
            del_at(entries, d.index);
        }
        OP_SET => {
            set_at(entries, d.index, ptr_of(d));
        }
        _ => {}
    }
}

fn ptr_of<T: Copy>(d: &Delta<T>) -> Option<T> {
    match &d.ptr {
        Some(DeltaRef::Id(v)) => Some(*v),
        _ => None,
    }
}

fn insert_at<T: Copy>(entries: &mut Vec<Option<T>>, index: usize, pointer: Option<T>) -> bool {
    if index > entries.len() {
        return false;
    }
    entries.insert(index, pointer);
    true
}

fn del_at<T: Copy>(entries: &mut Vec<Option<T>>, index: usize) -> bool {
    if index >= entries.len() {
        return false;
    }
    entries.remove(index);
    true
}

fn set_at<T: Copy>(entries: &mut [Option<T>], index: usize, pointer: Option<T>) -> bool {
    if index >= entries.len() {
        return false;
    }
    entries[index] = pointer;
    true
}

/// A pointer id held across lock releases, with the location it names.
#[derive(Clone)]
pub(crate) struct PtrRef {
    pub id: PtrId,
    pub context: Arc<Context>,
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
}

/// Deepest descent any read or write follows. A T=128 tree with 2^64 keys is
/// 10 levels deep; anything deeper is a pointer cycle or a hostile tree.
pub(crate) const MAX_DEPTH: usize = 64;

/// JS `maxCacheSize`: inflated nodes kept before the least recently used go.
pub(crate) const NODE_CACHE_SIZE: usize = 4096;

/// Arena state shared by a tree and its views.
///
/// Nodes, pointers and keys live in generational arenas. The node cache
/// (`lib/cache.js`) drops the nodes of the least recently used unchanged
/// pointers; a sweep then frees the pointers and keys no live node references.
/// Holders outside the arena (roots, range frames, readers across a lock
/// release) keep a location and re-create a freed pointer from it; writers
/// retain what they touch until the batch flushes.
pub(crate) struct TreeState {
    pub nodes: Arena<Node>,
    pub keys: Arena<Key>,
    pub ptrs: Arena<Ptr>,
    pub cache: HashMap<([u8; 32], u64, u64), PtrId>,
    pub dirty: Vec<PtrId>,
    /// Fork of each core when its nodes entered `cache`.
    forks: HashMap<[u8; 32], u64>,
    next_array_id: u64,
    /// Inflated, unchanged pointers by last use, oldest first.
    lru: BTreeMap<u64, PtrId>,
    clock: u64,
    /// JS `cache.retained`; advanced by every batch flush.
    retained: u64,
    pub max_nodes: usize,
    /// Nodes dropped since the last sweep.
    evicted: usize,
}

impl TreeState {
    pub fn new() -> Self {
        Self {
            nodes: Arena::new(),
            keys: Arena::new(),
            ptrs: Arena::new(),
            cache: HashMap::new(),
            dirty: Vec::new(),
            forks: HashMap::new(),
            next_array_id: 0,
            lru: BTreeMap::new(),
            clock: 0,
            retained: 0,
            max_nodes: NODE_CACHE_SIZE,
            evicted: 0,
        }
    }

    pub fn array_id(&mut self) -> u64 {
        let id = self.next_array_id;
        self.next_array_id += 1;
        id
    }

    pub fn add_node(&mut self, node: Node) -> NodeId {
        self.nodes.insert(node)
    }

    /// The location of `ptr`, to find it again after a lock release.
    pub fn ptr_ref(&self, ptr: PtrId) -> PtrRef {
        let p = &self.ptrs[ptr];
        PtrRef {
            id: ptr,
            context: p.context.clone(),
            core: p.core,
            seq: p.seq,
            offset: p.offset,
        }
    }

    pub fn has_ptr(&self, ptr: PtrId) -> bool {
        self.ptrs.contains(ptr)
    }

    /// The live pointer for `at`, re-created from its location if freed.
    pub fn resolve(&mut self, at: &mut PtrRef) -> crate::Result<PtrId> {
        if !self.ptrs.contains(at.id) {
            at.id = self.create_ptr(at.context.clone(), at.core, at.seq, at.offset, false, None)?;
        }
        Ok(at.id)
    }

    /// JS `NodeCache.bump`: mark an inflated, unchanged pointer as just used.
    pub fn touch(&mut self, ptr: PtrId) {
        let p = &mut self.ptrs[ptr];
        if p.changed || p.value.is_none() {
            return;
        }
        if p.used != 0 {
            self.lru.remove(&p.used);
        }
        self.clock += 1;
        p.used = self.clock;
        self.lru.insert(self.clock, ptr);
    }

    /// JS `Pointer.retain`: keep `ptr` and its node until the next flush.
    pub fn retain(&mut self, ptr: PtrId) {
        self.ptrs[ptr].retained = self.retained + 1;
    }

    /// JS `NodeCache.gc`: drop the least recently used nodes above the limit.
    /// Retained pointers count as used instead.
    pub fn gc(&mut self) {
        let mut budget = self.lru.len();
        while self.lru.len() > self.max_nodes && budget > 0 {
            budget -= 1;
            let (_, ptr) = self.lru.pop_first().expect("over the limit");
            if self.ptrs[ptr].retained > self.retained {
                self.ptrs[ptr].used = 0;
                self.touch(ptr);
                continue;
            }
            let p = &mut self.ptrs[ptr];
            p.used = 0;
            if let Some(node) = p.value.take() {
                self.nodes.remove(node);
                self.evicted += 1;
            }
        }
        if self.evicted >= self.max_nodes.max(1) {
            self.sweep();
        }
    }

    /// Free the pointers and keys no live node references. A pointer with a
    /// node, a change or a retain stays. Runs once per `max_nodes` evictions,
    /// so its cost per evicted node is about one node's entries.
    pub(crate) fn sweep(&mut self) {
        let mut ptrs = vec![false; self.ptrs.slots.len()];
        let mut keys = vec![false; self.keys.slots.len()];
        for node in self.nodes.values() {
            mark_array(&node.keys, &mut keys);
            mark_array(&node.children, &mut ptrs);
        }
        let retained = self.retained;
        self.ptrs.retain(|index, p| {
            ptrs[index] || p.value.is_some() || p.changed || p.retained > retained
        });
        self.keys.retain(|index, _| keys[index]);
        let live = &self.ptrs;
        self.cache.retain(|_, id| live.contains(*id));
        self.evicted = 0;
    }

    /// Forget cached nodes of `core` after it was truncated to a new fork, so
    /// equal `(seq, offset)` locations resolve to the new blocks.
    pub fn check_fork(&mut self, core: &hypercore::Session) {
        let key = *core.key();
        let fork = core.fork();
        if self.forks.insert(key, fork).is_some_and(|old| old != fork) {
            self.cache.retain(|(k, _, _), _| *k != key);
        }
    }

    pub fn add_ptr(
        &mut self,
        context: Arc<Context>,
        core: u64,
        seq: u64,
        offset: u64,
        changed: bool,
        value: Option<NodeId>,
    ) -> PtrId {
        let id = self.ptrs.insert(Ptr {
            context: context.clone(),
            core,
            seq,
            offset,
            changed: false,
            value,
            dirty: None,
            changed_by: None,
            used: 0,
            retained: 0,
        });
        if changed {
            self.mark(id);
        } else if value.is_none()
            && let Ok(key) = context.core_key(core)
        {
            self.cache.insert((key, seq, offset), id);
        }
        id
    }

    pub fn create_ptr(
        &mut self,
        context: Arc<Context>,
        core: u64,
        seq: u64,
        offset: u64,
        changed: bool,
        value: Option<NodeId>,
    ) -> crate::Result<PtrId> {
        if value.is_some() || changed {
            return Ok(self.add_ptr(context, core, seq, offset, changed, value));
        }
        let key = context.core_key(core)?;
        if let Some(id) = self.cache.get(&(key, seq, offset)) {
            return Ok(*id);
        }
        Ok(self.add_ptr(context, core, seq, offset, false, None))
    }

    pub fn mark(&mut self, id: PtrId) {
        if self.ptrs[id].changed {
            return;
        }
        self.ptrs[id].changed = true;
        let used = std::mem::take(&mut self.ptrs[id].used);
        if used != 0 {
            self.lru.remove(&used);
        }
        let at = self.dirty.len();
        self.dirty.push(id);
        self.ptrs[id].dirty = Some(at);
    }

    pub fn unmark(&mut self, id: PtrId) {
        if !self.ptrs[id].changed {
            return;
        }
        self.ptrs[id].changed = false;
        if let Some(n) = self.ptrs[id].value {
            let a = self.array_id();
            self.nodes[n].keys.reset(a);
            let b = self.array_id();
            self.nodes[n].children.reset(b);
        }
        let head = self.dirty.pop().expect("marked pointer is in dirty");
        if head != id {
            let at = self.ptrs[id].dirty.expect("dirty index");
            self.dirty[at] = head;
            self.ptrs[head].dirty = Some(at);
        }
        self.ptrs[id].dirty = None;
        self.touch(id);
    }

    /// End of a batch: undo pending changes and release the batch's retains
    /// (JS `NodeCache.flush`).
    pub fn flush_cache(&mut self) {
        while !self.dirty.is_empty() {
            let id = *self.dirty.last().expect("non-empty");
            self.unmark(id);
        }
        self.retained += 1;
        self.gc();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_key(
        &mut self,
        context: Arc<Context>,
        core: u64,
        seq: u64,
        offset: u64,
        changed: bool,
        key: Bytes,
        value: Option<Bytes>,
        value_ptr: Option<ValuePointer>,
    ) -> KeyId {
        self.keys.insert(Key {
            context,
            core,
            seq,
            offset,
            changed,
            key,
            value,
            value_ptr,
        })
    }
}

fn mark_array(array: &CompressedArray<Id>, marks: &mut [bool]) {
    let pending = array.uentries.iter().flatten();
    for id in array.entries.iter().chain(pending).flatten() {
        marks[id.index as usize] = true;
    }
    for d in &array.delta {
        mark_delta(d, marks);
    }
}

fn mark_delta(d: &Delta<Id>, marks: &mut [bool]) {
    if let Some(DeltaRef::Id(id)) = &d.ptr {
        marks[id.index as usize] = true;
    }
    for dd in &d.cohort {
        mark_delta(dd, marks);
    }
}

impl EncDelta {
    pub(crate) fn from_delta<T: Copy>(d: &Delta<T>, loc: impl Fn(&T) -> Loc) -> Self {
        Self {
            kind: d.kind,
            index: d.index,
            ptr: d.ptr.as_ref().map(|p| match p {
                DeltaRef::Loc(_, l) => *l,
                DeltaRef::Id(id) => loc(id),
            }),
        }
    }
}
