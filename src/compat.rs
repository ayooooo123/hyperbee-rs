//! Read-only support for bee1 blocks (type 10), ported from `lib/compat.js`.

use bytes::Bytes;

use crate::encoding::{BatchPtr, Block, EncDelta, KeyData, Loc, OP_COHORT, OP_INSERT, RawTree};

pub(crate) const COMPAT_T: u64 = 5;

pub(crate) fn decode(buffer: Bytes, seq: u64) -> crate::Result<Block> {
    let node = if buffer.len() > 1 {
        Some(decode_node(&buffer)?)
    } else {
        None
    };
    let index = match &node {
        Some(n) => decode_index(n.index.as_deref().unwrap_or(&[]))?,
        None => Index { levels: Vec::new() },
    };

    let mut cohorts: Vec<Vec<EncDelta>> = Vec::new();
    let mut tree = Vec::with_capacity(index.levels.len());
    for level in &index.levels {
        let mut keys = Vec::new();
        if !level.keys.is_empty() {
            let offset = cohorts.len() as u64;
            cohorts.push(
                level
                    .keys
                    .iter()
                    .enumerate()
                    .map(|(i, s)| EncDelta {
                        kind: OP_INSERT,
                        index: i,
                        ptr: Some(Loc {
                            core: 0,
                            seq: *s,
                            offset: 0,
                        }),
                    })
                    .collect(),
            );
            keys.push(EncDelta {
                kind: OP_COHORT,
                index: 0,
                ptr: Some(Loc {
                    core: 0,
                    seq,
                    offset,
                }),
            });
        }

        let mut children = Vec::new();
        if !level.children.is_empty() {
            let offset = cohorts.len() as u64;
            let cohort: Vec<EncDelta> = level
                .children
                .chunks_exact(2)
                .enumerate()
                .map(|(i, pair)| EncDelta {
                    kind: OP_INSERT,
                    index: i,
                    ptr: Some(Loc {
                        core: 0,
                        seq: pair[0],
                        offset: pair[1],
                    }),
                })
                .collect();
            cohorts.push(cohort);
            children.push(EncDelta {
                kind: OP_COHORT,
                index: 0,
                ptr: Some(Loc {
                    core: 0,
                    seq,
                    offset,
                }),
            });
        }

        tree.push(RawTree { keys, children });
    }

    let keys = match &node {
        Some(n) => vec![KeyData {
            key: n.key.clone().unwrap_or_default(),
            value: n.value.clone(),
            value_ptr: None,
        }],
        None => Vec::new(),
    };

    Ok(Block {
        type_: 10,
        seq,
        checkpoint: 0,
        batch: BatchPtr::default(),
        previous: (seq > 1).then_some(Loc {
            core: 0,
            seq: seq - 1,
            offset: 0,
        }),
        metadata: None,
        tree,
        keys,
        values: Vec::new(),
        cohorts,
        t: COMPAT_T,
        invalidation: 0,
    })
}

struct Node {
    index: Option<Bytes>,
    key: Option<Bytes>,
    value: Option<Bytes>,
}

#[derive(Default)]
struct Index {
    levels: Vec<Level>,
}

#[derive(Default)]
struct Level {
    keys: Vec<u64>,
    children: Vec<u64>,
}

fn decode_node(buf: &[u8]) -> crate::Result<Node> {
    let mut node = Node {
        index: None,
        key: None,
        value: None,
    };
    let mut found_index = false;
    let mut found_key = false;
    let mut offset = 0;
    while offset < buf.len() {
        let (prefix, n) = varint(buf, offset)?;
        offset = n;
        match prefix >> 3 {
            1 => {
                let (b, n) = bytes(buf, offset)?;
                offset = n;
                node.index = Some(b);
                found_index = true;
            }
            2 => {
                let (b, n) = bytes(buf, offset)?;
                offset = n;
                node.key = Some(b);
                found_key = true;
            }
            3 => {
                let (b, n) = bytes(buf, offset)?;
                offset = n;
                node.value = Some(b);
            }
            _ => offset = skip(prefix & 7, buf, offset)?,
        }
    }
    if !found_index || !found_key {
        return Err(crate::Error::Message("invalid compat block"));
    }
    Ok(node)
}

fn decode_index(buf: &[u8]) -> crate::Result<Index> {
    let mut out = Index::default();
    let mut offset = 0;
    while offset < buf.len() {
        let (prefix, n) = varint(buf, offset)?;
        offset = n;
        match prefix >> 3 {
            1 => {
                let (len, n) = varint(buf, offset)?;
                offset = n;
                let end = offset + len as usize;
                if end > buf.len() {
                    return Err(crate::Error::Message("invalid compat index"));
                }
                out.levels.push(decode_level(&buf[offset..end])?);
                offset = end;
            }
            _ => offset = skip(prefix & 7, buf, offset)?,
        }
    }
    Ok(out)
}

fn decode_level(buf: &[u8]) -> crate::Result<Level> {
    let mut out = Level::default();
    let mut offset = 0;
    while offset < buf.len() {
        let (prefix, n) = varint(buf, offset)?;
        offset = n;
        match prefix >> 3 {
            1 => {
                let (len, n) = varint(buf, offset)?;
                offset = n;
                let end = offset + len as usize;
                if end > buf.len() {
                    return Err(crate::Error::Message("invalid compat level"));
                }
                while offset < end {
                    let (v, n) = varint(buf, offset)?;
                    offset = n;
                    out.keys.push(v);
                }
            }
            2 => {
                let (len, n) = varint(buf, offset)?;
                offset = n;
                let end = offset + len as usize;
                if end > buf.len() {
                    return Err(crate::Error::Message("invalid compat level"));
                }
                while offset < end {
                    let (v, n) = varint(buf, offset)?;
                    offset = n;
                    out.children.push(v);
                }
            }
            _ => offset = skip(prefix & 7, buf, offset)?,
        }
    }
    Ok(out)
}

fn varint(buf: &[u8], mut offset: usize) -> crate::Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let b = *buf
            .get(offset)
            .ok_or(crate::Error::Message("truncated varint"))?;
        offset += 1;
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok((value, offset));
        }
        shift += 7;
        if shift >= 64 {
            return Err(crate::Error::Message("varint overflow"));
        }
    }
}

fn bytes(buf: &[u8], offset: usize) -> crate::Result<(Bytes, usize)> {
    let (len, offset) = varint(buf, offset)?;
    let end = offset
        .checked_add(len as usize)
        .filter(|end| *end <= buf.len())
        .ok_or(crate::Error::Message("truncated bytes"))?;
    Ok((Bytes::copy_from_slice(&buf[offset..end]), end))
}

fn skip(wire: u64, buf: &[u8], offset: usize) -> crate::Result<usize> {
    match wire {
        0 => Ok(varint(buf, offset)?.1),
        1 => checked(buf, offset, 8),
        2 => {
            let (len, offset) = varint(buf, offset)?;
            checked(buf, offset, len as usize)
        }
        5 => checked(buf, offset, 4),
        _ => Err(crate::Error::Message("unsupported protobuf wire type")),
    }
}

fn checked(buf: &[u8], offset: usize, n: usize) -> crate::Result<usize> {
    offset
        .checked_add(n)
        .filter(|end| *end <= buf.len())
        .ok_or(crate::Error::Message("truncated compat block"))
}
