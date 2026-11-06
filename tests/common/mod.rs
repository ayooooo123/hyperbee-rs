#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;

impl Fixture {
    pub fn blocks_len(&self) -> u64 {
        self.cores
            .iter()
            .find(|c| c.key == self.root_key)
            .map_or(0, |c| c.blocks.len() as u64)
    }
}

pub struct Fixture {
    pub name: String,
    pub primary_key: [u8; 32],
    pub root_key: [u8; 32],
    pub replayable: bool,
    pub batches: Vec<Vec<Op>>,
    pub cores: Vec<CoreFixture>,
    pub entries: Vec<EntryData>,
}

pub struct CoreFixture {
    pub name: String,
    pub key: [u8; 32],
    pub blocks: Vec<Bytes>,
}

#[derive(Clone)]
pub enum Op {
    Put(Bytes, Bytes),
    Del(Bytes),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryData {
    pub seq: u64,
    pub key: Bytes,
    pub value: Bytes,
}

pub fn fixtures() -> Vec<Fixture> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("fixtures directory")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    paths.sort();
    paths.iter().map(|p| load(p)).collect()
}

fn load(path: &Path) -> Fixture {
    let text = fs::read_to_string(path).expect("read fixture");
    let json: serde_json::Value = serde_json::from_str(&text).expect("parse fixture");
    let name = json["name"].as_str().expect("name").to_string();
    let primary_key = key32(json["primaryKey"].as_str().expect("primaryKey"));
    let base_key = json["coreKey"]
        .as_str()
        .map(key32)
        .or_else(|| json["rootKey"].as_str().map(key32))
        .or_else(|| json["cores"][0]["key"].as_str().map(key32))
        .expect("coreKey");
    let root_key = json["rootKey"].as_str().map(key32).unwrap_or(base_key);
    let replayable = json["replayable"].as_bool().unwrap_or(true);

    let batches = json["batches"]
        .as_array()
        .expect("batches")
        .iter()
        .map(|batch| {
            batch
                .as_array()
                .expect("batch")
                .iter()
                .map(|op| {
                    let parts = op.as_array().expect("op");
                    match parts[0].as_str().expect("op name") {
                        "put" => Op::Put(
                            bytes(parts[1].as_str().expect("key")),
                            bytes(parts[2].as_str().expect("value")),
                        ),
                        "del" => Op::Del(bytes(parts[1].as_str().expect("key"))),
                        other => panic!("unknown op {other}"),
                    }
                })
                .collect()
        })
        .collect();

    let blocks: Vec<Bytes> = json["blocks"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .map(|b| bytes(b.as_str().expect("block")))
                .collect()
        })
        .unwrap_or_default();
    let cores = match json["cores"].as_array() {
        Some(cores) => cores
            .iter()
            .map(|c| CoreFixture {
                name: c["name"].as_str().expect("core name").to_string(),
                key: key32(c["key"].as_str().expect("core key")),
                blocks: c["blocks"]
                    .as_array()
                    .expect("core blocks")
                    .iter()
                    .map(|b| bytes(b.as_str().expect("block")))
                    .collect(),
            })
            .collect(),
        None => vec![CoreFixture {
            name: "bee".to_string(),
            key: base_key,
            blocks: blocks.clone(),
        }],
    };

    let entries = json["entries"]
        .as_array()
        .expect("entries")
        .iter()
        .map(|e| EntryData {
            seq: e["seq"].as_u64().expect("seq"),
            key: bytes(e["key"].as_str().expect("key")),
            value: bytes(e["value"].as_str().expect("value")),
        })
        .collect();

    Fixture {
        name,
        primary_key,
        root_key,
        replayable,
        batches,
        cores,
        entries,
    }
}

pub fn bytes(hex: &str) -> Bytes {
    let mut out = vec![0u8; hex.len() / 2];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("hex");
    }
    Bytes::from(out)
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}

fn key32(hex: &str) -> [u8; 32] {
    bytes(hex).as_ref().try_into().expect("32-byte key")
}
