//! Block codecs generated from `spec/hyperschema` in npm hyperbee2 2.21.1.

use bytes::Bytes;
use compact_encoding::{Codec, Reader, Result as EncResult, Writer, uint_len};

use crate::Error;
use crate::compat;

/// Default B-tree degree. Stored as 0 in blocks.
pub(crate) const T: u64 = 128;
pub(crate) const TYPE_BLOCK0: u64 = 0;
pub(crate) const TYPE_LATEST: u64 = 1;
/// bee1 blocks.
pub(crate) const TYPE_COMPAT: u64 = 10;

pub(crate) const OP_SET: u8 = 0;
pub(crate) const OP_INSERT: u8 = 1;
pub(crate) const OP_DEL: u8 = 2;
pub(crate) const OP_COHORT: u8 = 3;

/// A block position inside one core.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Loc {
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct BatchPtr {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ValuePtr {
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
    pub split: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct KeyData {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub value_ptr: Option<ValuePtr>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CoreInfo {
    pub key: [u8; 32],
    pub fork: u64,
    pub length: u64,
    pub tree_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Metadata {
    pub cores: Vec<CoreInfo>,
    pub mappings: Option<Vec<([u8; 32], [u8; 32])>>,
}

/// A serialized tree delta. Cohort entries carry the position of their payload
/// in `Block::cohorts`.
#[derive(Clone, Debug, Default)]
pub(crate) struct EncDelta {
    pub kind: u8,
    pub index: usize,
    pub ptr: Option<Loc>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RawTree {
    pub keys: Vec<EncDelta>,
    pub children: Vec<EncDelta>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Block {
    pub type_: u64,
    pub seq: u64,
    pub checkpoint: u64,
    pub batch: BatchPtr,
    pub previous: Option<Loc>,
    pub metadata: Option<Metadata>,
    pub tree: Vec<RawTree>,
    pub keys: Vec<KeyData>,
    pub values: Vec<Bytes>,
    pub cohorts: Vec<Vec<EncDelta>>,
    pub t: u64,
    pub invalidation: u64,
}

pub(crate) fn encode_block(block: &Block) -> Bytes {
    let mut w = Writer::new();
    match block.type_ {
        TYPE_LATEST => encode_block1(&mut w, block),
        other => panic!("cannot encode block type {other}"),
    }
    w.finish()
}

fn encode_block1(w: &mut Writer, b: &Block) {
    let flags = u64::from(b.previous.is_some())
        | (u64::from(b.metadata.is_some()) << 1)
        | (u64::from(!b.tree.is_empty()) << 2)
        | (u64::from(!b.keys.is_empty()) << 3)
        | (u64::from(!b.values.is_empty()) << 4)
        | (u64::from(!b.cohorts.is_empty()) << 5)
        | (u64::from(b.t != 0 && b.t != T) << 6)
        | (u64::from(b.invalidation != 0) << 7);

    w.uint(b.type_);
    w.uint(b.checkpoint);
    w.uint(b.batch.start);
    w.uint(b.batch.end);
    w.uint(flags);

    if let Some(p) = b.previous {
        w.uint(p.core);
        w.uint(p.seq);
    }
    if let Some(m) = &b.metadata {
        w.frame(m);
    }
    if !b.tree.is_empty() {
        w.uint(b.tree.len() as u64);
        for t in &b.tree {
            encode_deltas(w, &t.keys);
            encode_deltas(w, &t.children);
        }
    }
    if !b.keys.is_empty() {
        w.uint(b.keys.len() as u64);
        for k in &b.keys {
            w.buffer(&k.key);
            let mut kf = u64::from(k.value.is_some()) | (u64::from(k.value_ptr.is_some()) << 1);
            if let Some(vp) = &k.value_ptr {
                kf |= (u64::from(vp.core != 0) << 2)
                    | (u64::from(vp.seq != 0) << 3)
                    | (u64::from(vp.offset != 0) << 4)
                    | (u64::from(vp.split != 0) << 5);
            }
            w.uint8(kf as u8);
            if let Some(v) = &k.value {
                w.buffer(v);
            }
            if let Some(vp) = &k.value_ptr {
                if vp.core != 0 {
                    w.uint(vp.core);
                }
                if vp.seq != 0 {
                    w.uint(vp.seq);
                }
                if vp.offset != 0 {
                    w.uint(vp.offset);
                }
                if vp.split != 0 {
                    w.uint(vp.split);
                }
            }
        }
    }
    if !b.values.is_empty() {
        w.uint(b.values.len() as u64);
        for v in &b.values {
            w.buffer(v);
        }
    }
    if !b.cohorts.is_empty() {
        w.uint(b.cohorts.len() as u64);
        for c in &b.cohorts {
            encode_deltas(w, c);
        }
    }
    if b.t != 0 && b.t != T {
        w.uint(b.t);
    }
    if b.invalidation != 0 {
        w.uint(b.invalidation);
    }
}

fn encode_deltas(w: &mut Writer, deltas: &[EncDelta]) {
    w.uint(deltas.len() as u64);
    for d in deltas {
        encode_delta(w, d);
    }
}

fn encode_delta(w: &mut Writer, d: &EncDelta) {
    let mut flags =
        u64::from(d.kind & 7) | (u64::from(d.index != 0) << 3) | (u64::from(d.ptr.is_some()) << 4);
    if let Some(p) = d.ptr {
        flags |= (u64::from(p.core != 0) << 5)
            | (u64::from(p.seq != 0) << 6)
            | (u64::from(p.offset != 0) << 7);
    }
    w.uint8(flags as u8);
    if d.index != 0 {
        w.uint8(d.index as u8);
    }
    if let Some(p) = d.ptr {
        if p.core != 0 {
            w.uint(p.core);
        }
        if p.seq != 0 {
            w.uint(p.seq);
        }
        if p.offset != 0 {
            w.uint(p.offset);
        }
    }
}

impl Codec for CoreInfo {
    fn encoded_len(&self) -> usize {
        32 + 1
            + uint_len(self.fork) * usize::from(self.fork != 0)
            + uint_len(self.length) * usize::from(self.length != 0)
            + 32 * usize::from(self.tree_hash.is_some())
    }

    fn encode(&self, w: &mut Writer) {
        w.raw(&self.key);
        let flags = u64::from(self.fork != 0)
            | (u64::from(self.length != 0) << 1)
            | (u64::from(self.tree_hash.is_some()) << 2);
        w.uint8(flags as u8);
        if self.fork != 0 {
            w.uint(self.fork);
        }
        if self.length != 0 {
            w.uint(self.length);
        }
        if let Some(h) = &self.tree_hash {
            w.raw(h);
        }
    }

    fn decode(r: &mut Reader) -> EncResult<Self> {
        let key = r.fixed()?;
        let flags = r.uint8()?;
        Ok(Self {
            key,
            fork: if flags & 1 != 0 { r.uint()? } else { 0 },
            length: if flags & 2 != 0 { r.uint()? } else { 0 },
            tree_hash: if flags & 4 != 0 {
                Some(r.fixed()?)
            } else {
                None
            },
        })
    }
}

impl Codec for Metadata {
    fn encoded_len(&self) -> usize {
        let cores: usize = uint_len(self.cores.len() as u64)
            + self.cores.iter().map(Codec::encoded_len).sum::<usize>();
        let mut n = cores + 1;
        if let Some(m) = &self.mappings {
            n += uint_len(m.len() as u64) + m.len() * 64;
        }
        n
    }

    fn encode(&self, w: &mut Writer) {
        w.uint(self.cores.len() as u64);
        for c in &self.cores {
            c.encode(w);
        }
        w.uint(u64::from(self.mappings.is_some()));
        if let Some(m) = &self.mappings {
            w.uint(m.len() as u64);
            for (from, to) in m {
                w.raw(from);
                w.raw(to);
            }
        }
    }

    fn decode(r: &mut Reader) -> EncResult<Self> {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n));
        }
        let mut cores = Vec::with_capacity((n as usize).min(r.remaining()));
        for _ in 0..n {
            cores.push(CoreInfo::decode(r)?);
        }
        // Older encodings end after the cores array.
        let flags = if r.remaining() > 0 { r.uint()? } else { 0 };
        let mappings = if flags & 1 != 0 {
            let n = r.uint()?;
            if n > compact_encoding::MAX_ARRAY_DECODE {
                return Err(compact_encoding::Error::ArrayTooBig(n));
            }
            let mut out = Vec::with_capacity((n as usize).min(r.remaining() / 64));
            for _ in 0..n {
                out.push((r.fixed()?, r.fixed()?));
            }
            Some(out)
        } else {
            None
        };
        Ok(Self { cores, mappings })
    }
}

fn decode_deltas(r: &mut Reader) -> EncResult<Vec<EncDelta>> {
    let n = r.uint()?;
    if n > compact_encoding::MAX_ARRAY_DECODE {
        return Err(compact_encoding::Error::ArrayTooBig(n));
    }
    let mut out = Vec::with_capacity((n as usize).min(r.remaining()));
    for _ in 0..n {
        out.push(decode_delta(r)?);
    }
    Ok(out)
}

fn decode_delta(r: &mut Reader) -> EncResult<EncDelta> {
    let flags = r.uint8()?;
    let index = if flags & 8 != 0 {
        r.uint8()? as usize
    } else {
        0
    };
    let ptr = if flags & 16 != 0 {
        Some(Loc {
            core: if flags & 32 != 0 { r.uint()? } else { 0 },
            seq: if flags & 64 != 0 { r.uint()? } else { 0 },
            offset: if flags & 128 != 0 { r.uint()? } else { 0 },
        })
    } else {
        None
    };
    Ok(EncDelta {
        kind: flags & 7,
        index,
        ptr,
    })
}

impl Codec for KeyData {
    fn encoded_len(&self) -> usize {
        let mut n = uint_len(self.key.len() as u64) + self.key.len() + 1;
        if let Some(v) = &self.value {
            n += uint_len(v.len() as u64) + v.len();
        }
        if let Some(p) = &self.value_ptr {
            n += uint_len(p.core) * usize::from(p.core != 0)
                + uint_len(p.seq) * usize::from(p.seq != 0)
                + uint_len(p.offset) * usize::from(p.offset != 0)
                + uint_len(p.split) * usize::from(p.split != 0);
        }
        n
    }

    fn encode(&self, w: &mut Writer) {
        w.buffer(&self.key);
        let mut flags =
            u64::from(self.value.is_some()) | (u64::from(self.value_ptr.is_some()) << 1);
        if let Some(p) = &self.value_ptr {
            flags |= (u64::from(p.core != 0) << 2)
                | (u64::from(p.seq != 0) << 3)
                | (u64::from(p.offset != 0) << 4)
                | (u64::from(p.split != 0) << 5);
        }
        w.uint8(flags as u8);
        if let Some(v) = &self.value {
            w.buffer(v);
        }
        if let Some(p) = &self.value_ptr {
            if p.core != 0 {
                w.uint(p.core);
            }
            if p.seq != 0 {
                w.uint(p.seq);
            }
            if p.offset != 0 {
                w.uint(p.offset);
            }
            if p.split != 0 {
                w.uint(p.split);
            }
        }
    }

    fn decode(r: &mut Reader) -> EncResult<Self> {
        let key = r.buffer()?;
        let flags = r.uint8()?;
        let value = if flags & 1 != 0 {
            Some(r.buffer()?)
        } else {
            None
        };
        let value_ptr = if flags & 2 != 0 {
            Some(ValuePtr {
                core: if flags & 4 != 0 { r.uint()? } else { 0 },
                seq: if flags & 8 != 0 { r.uint()? } else { 0 },
                offset: if flags & 16 != 0 { r.uint()? } else { 0 },
                split: if flags & 32 != 0 { r.uint()? } else { 0 },
            })
        } else {
            None
        };
        Ok(Self {
            key,
            value,
            value_ptr,
        })
    }
}

/// Decode a block buffer. `seq` is the block's index in its core.
pub(crate) fn decode_block(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    if buffer.is_empty() {
        return Err(Error::UnknownBlockType(0));
    }
    let type_ = {
        let mut r = Reader::new(buffer.clone());
        r.uint().map_err(Error::from)?
    };
    match type_ {
        TYPE_LATEST => decode_block1(buffer, seq),
        TYPE_BLOCK0 => decode_block0(buffer, seq),
        TYPE_COMPAT => compat::decode(buffer, seq),
        other => Err(Error::UnknownBlockType(other)),
    }
}

fn decode_block1(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    let mut r = Reader::new(buffer.clone());
    let type_ = r.uint()?;
    let checkpoint = r.uint()?;
    let batch = BatchPtr {
        start: r.uint()?,
        end: r.uint()?,
    };
    let flags = r.uint()?;
    let previous = if flags & 1 != 0 {
        Some(Loc {
            core: r.uint()?,
            seq: r.uint()?,
            offset: 0,
        })
    } else {
        None
    };
    let metadata = if flags & 2 != 0 {
        Some(r.frame::<Metadata>()?)
    } else {
        None
    };
    let tree = if flags & 4 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        let mut out = Vec::with_capacity((n as usize).min(r.remaining()));
        for _ in 0..n {
            out.push(RawTree {
                keys: decode_deltas(&mut r)?,
                children: decode_deltas(&mut r)?,
            });
        }
        out
    } else {
        Vec::new()
    };
    let keys = if flags & 8 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        let mut out = Vec::with_capacity((n as usize).min(r.remaining()));
        for _ in 0..n {
            out.push(KeyData::decode(&mut r)?);
        }
        out
    } else {
        Vec::new()
    };
    let values = if flags & 16 != 0 {
        r.array::<Bytes>()?
    } else {
        Vec::new()
    };
    let cohorts = if flags & 32 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        let mut out = Vec::with_capacity((n as usize).min(r.remaining()));
        for _ in 0..n {
            out.push(decode_deltas(&mut r)?);
        }
        out
    } else {
        Vec::new()
    };
    let mut t = if flags & 64 != 0 { r.uint()? } else { 0 };
    if t == 0 {
        t = T;
    }
    let invalidation = if flags & 128 != 0 { r.uint()? } else { 0 };

    Ok(Block {
        type_,
        seq,
        checkpoint,
        batch,
        previous,
        metadata,
        tree,
        keys,
        values,
        cohorts,
        t,
        invalidation,
    })
}

#[derive(Default)]
struct Block0Tree {
    keys: Vec<Loc>,
    children: Vec<Loc>,
}

fn decode_block0(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    let mut r = Reader::new(buffer.clone());
    let _type = r.uint()?;
    let checkpoint = r.uint()?;
    let batch = BatchPtr {
        start: r.uint()?,
        end: r.uint()?,
    };
    let flags = r.uint()?;
    let previous = if flags & 1 != 0 {
        Some(Loc {
            core: r.uint()?,
            seq: r.uint()?,
            offset: 0,
        })
    } else {
        None
    };
    let mut tree = Vec::new();
    if flags & 2 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        for _ in 0..n {
            tree.push(Block0Tree {
                keys: decode_loc_array(&mut r)?,
                children: decode_loc_array(&mut r)?,
            });
        }
    }
    let mut data = Vec::new();
    if flags & 4 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        for _ in 0..n {
            data.push((r.buffer()?, r.buffer()?));
        }
    }
    let mut cores = Vec::new();
    if flags & 8 != 0 {
        let n = r.uint()?;
        if n > compact_encoding::MAX_ARRAY_DECODE {
            return Err(compact_encoding::Error::ArrayTooBig(n).into());
        }
        for _ in 0..n {
            cores.push(CoreInfo::decode(&mut r)?);
        }
    }

    let mut cohorts: Vec<Vec<EncDelta>> = Vec::new();
    let tree = tree
        .into_iter()
        .map(|t| RawTree {
            keys: to_cohort(seq, &t.keys, &mut cohorts),
            children: to_cohort(seq, &t.children, &mut cohorts),
        })
        .collect();

    Ok(Block {
        type_: TYPE_BLOCK0,
        seq,
        checkpoint,
        batch,
        previous,
        metadata: Some(Metadata {
            cores,
            mappings: None,
        }),
        tree,
        keys: data
            .into_iter()
            .map(|(key, value)| KeyData {
                key,
                value: Some(value),
                value_ptr: None,
            })
            .collect(),
        values: Vec::new(),
        cohorts,
        t: T,
        invalidation: 0,
    })
}

fn decode_loc_array(r: &mut Reader) -> EncResult<Vec<Loc>> {
    let n = r.uint()?;
    if n > compact_encoding::MAX_ARRAY_DECODE {
        return Err(compact_encoding::Error::ArrayTooBig(n));
    }
    let mut out = Vec::with_capacity((n as usize).min(r.remaining()));
    for _ in 0..n {
        out.push(Loc {
            core: r.uint()?,
            seq: r.uint()?,
            offset: r.uint()?,
        });
    }
    Ok(out)
}

fn to_cohort(seq: u64, pointers: &[Loc], cohorts: &mut Vec<Vec<EncDelta>>) -> Vec<EncDelta> {
    if pointers.is_empty() {
        return Vec::new();
    }
    let offset = cohorts.len() as u64;
    cohorts.push(
        pointers
            .iter()
            .enumerate()
            .map(|(i, p)| EncDelta {
                kind: OP_INSERT,
                index: i,
                ptr: Some(*p),
            })
            .collect(),
    );
    vec![EncDelta {
        kind: OP_COHORT,
        index: 0,
        ptr: Some(Loc {
            core: 0,
            seq,
            offset,
        }),
    }]
}
