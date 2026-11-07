//! In-memory tree: compressed arrays, pointers and the node/pointer/key arena.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::context::Context;
use crate::encoding::{EncDelta, Loc, OP_COHORT, OP_DEL, OP_INSERT, OP_SET};

pub(crate) type PtrId = usize;
pub(crate) type NodeId = usize;
pub(crate) type KeyId = usize;

#[derive(Debug)]
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
}

pub(crate) struct Node {
    pub t: u64,
    pub keys: CompressedArray<KeyId>,
    pub children: CompressedArray<PtrId>,
}

#[derive(Clone, Debug)]
pub(crate) enum DeltaRef<T> {
    Loc(Loc),
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

        if l > min && l < self.entries.len() && !overflow && self.delta.len() + l <= max {
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

/// Arena state shared by a tree and its views.
pub(crate) struct TreeState {
    pub nodes: Vec<Node>,
    pub keys: Vec<Key>,
    pub ptrs: Vec<Ptr>,
    pub cache: HashMap<([u8; 32], u64, u64), PtrId>,
    pub dirty: Vec<PtrId>,
    next_array_id: u64,
}

impl TreeState {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            keys: Vec::new(),
            ptrs: Vec::new(),
            cache: HashMap::new(),
            dirty: Vec::new(),
            next_array_id: 0,
        }
    }

    pub fn array_id(&mut self) -> u64 {
        let id = self.next_array_id;
        self.next_array_id += 1;
        id
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
        let id = self.ptrs.len();
        self.ptrs.push(Ptr {
            context: context.clone(),
            core,
            seq,
            offset,
            changed: false,
            value,
            dirty: None,
            changed_by: None,
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
    }

    pub fn flush_cache(&mut self) {
        while !self.dirty.is_empty() {
            let id = *self.dirty.last().expect("non-empty");
            self.unmark(id);
        }
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
        let id = self.keys.len();
        self.keys.push(Key {
            context,
            core,
            seq,
            offset,
            changed,
            key,
            value,
            value_ptr,
        });
        id
    }
}

impl EncDelta {
    pub(crate) fn from_delta<T: Copy>(d: &Delta<T>, loc: impl Fn(&T) -> Loc) -> Self {
        Self {
            kind: d.kind,
            index: d.index,
            ptr: d.ptr.as_ref().map(|p| match p {
                DeltaRef::Loc(l) => *l,
                DeltaRef::Id(id) => loc(id),
            }),
        }
    }
}
