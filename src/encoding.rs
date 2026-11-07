//! Block codecs generated from `spec/hyperschema` in npm hyperbee2 2.21.1.

use bytes::Bytes;
use compact_encoding::Reader;
#[cfg(test)]
use compact_encoding::Writer;

use crate::Error;
use crate::compat;

#[allow(dead_code)]
mod schema {
    include!(concat!(env!("OUT_DIR"), "/hyperschema.rs"));
}

use schema::bee;

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
pub struct Loc {
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BatchPtr {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ValuePtr {
    pub core: u64,
    pub seq: u64,
    pub offset: u64,
    pub split: u64,
}

#[derive(Clone, Debug, Default)]
pub struct KeyData {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub value_ptr: Option<ValuePtr>,
}

#[derive(Clone, Debug, Default)]
pub struct CoreInfo {
    pub key: [u8; 32],
    pub fork: u64,
    pub length: u64,
    pub tree_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Default)]
pub struct Metadata {
    pub cores: Vec<CoreInfo>,
    pub mappings: Option<Vec<([u8; 32], [u8; 32])>>,
}

/// A serialized tree delta. Cohort entries carry the position of their payload
/// in `Block::cohorts`.
#[derive(Clone, Debug, Default)]
pub struct EncDelta {
    pub kind: u8,
    pub index: usize,
    pub ptr: Option<Loc>,
}

#[derive(Clone, Debug, Default)]
pub struct RawTree {
    pub keys: Vec<EncDelta>,
    pub children: Vec<EncDelta>,
}

#[derive(Clone, Debug, Default)]
pub struct Block {
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

/// Zero means "absent" in the wire encoding.
fn optional(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn integer(value: Option<u64>) -> u64 {
    value.unwrap_or(0)
}

impl From<Loc> for bee::TreePointer0 {
    fn from(loc: Loc) -> Self {
        Self {
            core: loc.core,
            seq: loc.seq,
            offset: loc.offset,
        }
    }
}

impl From<bee::TreePointer0> for Loc {
    fn from(pointer: bee::TreePointer0) -> Self {
        Self {
            core: pointer.core,
            seq: pointer.seq,
            offset: pointer.offset,
        }
    }
}

impl From<Loc> for bee::BlockPointer {
    fn from(loc: Loc) -> Self {
        Self {
            core: loc.core,
            seq: loc.seq,
        }
    }
}

impl From<bee::BlockPointer> for Loc {
    fn from(pointer: bee::BlockPointer) -> Self {
        Self {
            core: pointer.core,
            seq: pointer.seq,
            offset: 0,
        }
    }
}

fn tree_pointer(loc: Loc) -> bee::TreePointer {
    bee::TreePointer {
        core: optional(loc.core),
        seq: optional(loc.seq),
        offset: optional(loc.offset),
    }
}

fn delta_loc(pointer: bee::TreePointer) -> Loc {
    Loc {
        core: integer(pointer.core),
        seq: integer(pointer.seq),
        offset: integer(pointer.offset),
    }
}

impl From<EncDelta> for bee::TreeDelta {
    fn from(delta: EncDelta) -> Self {
        Self {
            type_: delta.kind,
            index: (delta.index != 0).then_some(delta.index as u8),
            pointer: delta.ptr.map(tree_pointer),
        }
    }
}

impl From<bee::TreeDelta> for EncDelta {
    fn from(delta: bee::TreeDelta) -> Self {
        Self {
            kind: delta.type_,
            index: delta.index.unwrap_or(0) as usize,
            ptr: delta.pointer.map(delta_loc),
        }
    }
}

impl From<RawTree> for bee::Tree {
    fn from(tree: RawTree) -> Self {
        Self {
            keys: tree.keys.into_iter().map(Into::into).collect(),
            children: tree.children.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<bee::Tree> for RawTree {
    fn from(tree: bee::Tree) -> Self {
        Self {
            keys: tree.keys.into_iter().map(Into::into).collect(),
            children: tree.children.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<KeyData> for bee::Key {
    fn from(data: KeyData) -> Self {
        Self {
            key: data.key,
            value: data.value,
            value_pointer: data.value_ptr.map(|pointer| bee::ValuePointer {
                core: optional(pointer.core),
                seq: optional(pointer.seq),
                offset: optional(pointer.offset),
                split: optional(pointer.split),
            }),
        }
    }
}

impl From<bee::Key> for KeyData {
    fn from(data: bee::Key) -> Self {
        Self {
            key: data.key,
            value: data.value,
            value_ptr: data.value_pointer.map(|pointer| ValuePtr {
                core: integer(pointer.core),
                seq: integer(pointer.seq),
                offset: integer(pointer.offset),
                split: integer(pointer.split),
            }),
        }
    }
}

impl From<CoreInfo> for bee::Core {
    fn from(core: CoreInfo) -> Self {
        Self {
            key: core.key,
            fork: optional(core.fork),
            length: optional(core.length),
            tree_hash: core.tree_hash,
        }
    }
}

impl From<bee::Core> for CoreInfo {
    fn from(core: bee::Core) -> Self {
        Self {
            key: core.key,
            fork: integer(core.fork),
            length: integer(core.length),
            tree_hash: core.tree_hash,
        }
    }
}

impl From<Metadata> for bee::Metadata {
    fn from(metadata: Metadata) -> Self {
        Self {
            cores: metadata.cores.into_iter().map(Into::into).collect(),
            mappings: metadata.mappings.map(|mappings| {
                mappings
                    .into_iter()
                    .map(|(from, to)| bee::Mapping { from, to })
                    .collect()
            }),
        }
    }
}

impl From<bee::Metadata> for Metadata {
    fn from(metadata: bee::Metadata) -> Self {
        Self {
            cores: metadata.cores.into_iter().map(Into::into).collect(),
            mappings: metadata
                .mappings
                .map(|mappings| mappings.into_iter().map(|m| (m.from, m.to)).collect()),
        }
    }
}

impl From<Block> for bee::Block1 {
    fn from(block: Block) -> Self {
        Self {
            type_: block.type_,
            seq: block.seq,
            checkpoint: block.checkpoint,
            batch: bee::BatchPointer {
                start: block.batch.start,
                end: block.batch.end,
            },
            previous: block.previous.map(Into::into),
            metadata: block.metadata.map(Into::into),
            tree: (!block.tree.is_empty())
                .then(|| block.tree.into_iter().map(Into::into).collect()),
            keys: (!block.keys.is_empty())
                .then(|| block.keys.into_iter().map(Into::into).collect()),
            values: (!block.values.is_empty()).then_some(block.values),
            cohorts: (!block.cohorts.is_empty()).then(|| {
                block
                    .cohorts
                    .into_iter()
                    .map(|cohort| cohort.into_iter().map(Into::into).collect())
                    .collect()
            }),
            t: (block.t != 0 && block.t != T).then_some(block.t),
            invalidation: optional(block.invalidation),
        }
    }
}

pub(crate) fn encode_block(block: Block) -> Bytes {
    match block.type_ {
        TYPE_LATEST => compact_encoding::encode(&bee::Block1::from(block)),
        other => panic!("cannot encode block type {other}"),
    }
}

/// Decode a block buffer. `seq` is the block's index in its core.
pub(crate) fn decode_block(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    if buffer.is_empty() {
        return Err(Error::UnknownBlockType(0));
    }
    let type_ = Reader::new(buffer.clone()).uint()?;
    match type_ {
        TYPE_LATEST => decode_block1(buffer, seq),
        TYPE_BLOCK0 => decode_block0(buffer, seq),
        TYPE_COMPAT => compat::decode(buffer, seq),
        other => Err(Error::UnknownBlockType(other)),
    }
}

fn decode_block1(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    let block = compact_encoding::decode::<bee::Block1>(buffer)?;
    Ok(Block {
        type_: block.type_,
        seq,
        checkpoint: block.checkpoint,
        batch: BatchPtr {
            start: block.batch.start,
            end: block.batch.end,
        },
        previous: block.previous.map(Into::into),
        metadata: block.metadata.map(Into::into),
        tree: block
            .tree
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect(),
        keys: block
            .keys
            .unwrap_or_default()
            .into_iter()
            .map(Into::into)
            .collect(),
        values: block.values.unwrap_or_default(),
        cohorts: block
            .cohorts
            .unwrap_or_default()
            .into_iter()
            .map(|cohort| cohort.into_iter().map(Into::into).collect())
            .collect(),
        t: block.t.filter(|&t| t != 0).unwrap_or(T),
        invalidation: block.invalidation.unwrap_or(0),
    })
}

fn decode_block0(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    let block = compact_encoding::decode::<bee::Block0>(buffer)?;
    let mut cohorts: Vec<Vec<EncDelta>> = Vec::new();
    let tree = block
        .tree
        .unwrap_or_default()
        .into_iter()
        .map(|tree| {
            let keys: Vec<Loc> = tree.keys.into_iter().map(Into::into).collect();
            let children: Vec<Loc> = tree.children.into_iter().map(Into::into).collect();
            RawTree {
                keys: to_cohort(seq, &keys, &mut cohorts),
                children: to_cohort(seq, &children, &mut cohorts),
            }
        })
        .collect();
    Ok(Block {
        type_: TYPE_BLOCK0,
        seq,
        checkpoint: block.checkpoint,
        batch: BatchPtr {
            start: block.batch.start,
            end: block.batch.end,
        },
        previous: block.previous.map(Into::into),
        metadata: Some(Metadata {
            cores: block
                .cores
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            mappings: None,
        }),
        tree,
        keys: block
            .data
            .unwrap_or_default()
            .into_iter()
            .map(|data| KeyData {
                key: data.key,
                value: Some(data.value),
                value_ptr: None,
            })
            .collect(),
        values: Vec::new(),
        cohorts,
        t: T,
        invalidation: 0,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reencodes_js_block_with_metadata_mappings() {
        let text = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/mappings.json"
        ))
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let root = json["cores"]
            .as_array()
            .unwrap()
            .iter()
            .find(|core| core["name"] == "root")
            .unwrap();
        let hex = root["blocks"][0].as_str().unwrap();

        let block = decode_block(Bytes::from(hex_decode(hex)), 0).unwrap();
        assert_eq!(encode_block(block), Bytes::from(hex_decode(hex)));
    }

    fn hex_decode(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn decodes_metadata_frame_from_schema_version_1() {
        let mut metadata = Writer::new();
        metadata.uint(0); // cores
        let mut w = Writer::new();
        w.uint(TYPE_LATEST);
        w.uint(0); // checkpoint
        w.uint(0); // batch.start
        w.uint(0); // batch.end
        w.uint(2 | 4); // metadata and tree present
        w.uint(metadata.len() as u64);
        w.raw(&metadata.finish());
        w.uint(0); // tree

        let block = decode_block(w.finish(), 7).unwrap();
        let metadata = block.metadata.expect("metadata");
        assert!(metadata.cores.is_empty());
        assert!(metadata.mappings.is_none());
        assert!(block.tree.is_empty());
        assert_eq!(block.seq, 7);
        assert_eq!(block.t, T);
    }
}
