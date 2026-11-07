mod common;

use std::path::Path;
use std::process::Command;

use bytes::Bytes;
use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, RangeOptions};

use common::{bytes, hex};

/// Rust writes a bee the fixtures do not contain; npm hyperbee2 must read the
/// same entries back from the raw blocks.
#[ignore]
#[tokio::test]
async fn js_reads_rust_written_bee() {
    let dir = tempfile::tempdir().unwrap();
    let primary_key = [0x5a; 32];
    let store = Corestore::open_with_primary_key(dir.path(), primary_key).unwrap();
    let bee = Bee::open(&store, None).await.unwrap();

    let mut rng = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as u32
    };

    let mut expected_keys = std::collections::BTreeSet::new();
    let mut ops = 0;
    while ops < 2000 {
        let mut batch = bee.batch();
        let count = 137.min(2000 - ops);
        for _ in 0..count {
            let n = next() % 1200;
            let key = Bytes::from(format!("k/{n:06}"));
            if next() % 5 == 0 && expected_keys.remove(&key) {
                batch.del(key);
            } else {
                let len = if next() % 97 == 0 {
                    3000 + (next() % 500) as usize
                } else {
                    8 + (next() % 40) as usize
                };
                let mut value = vec![0u8; len];
                value[0] = (n % 251) as u8;
                value[1] = (ops % 251) as u8;
                for (i, b) in value.iter_mut().enumerate().skip(2) {
                    *b = ((i * 31 + ops) % 251) as u8;
                }
                expected_keys.insert(key.clone());
                batch.put(key, Bytes::from(value));
            }
            ops += 1;
        }
        batch.flush().await.unwrap();
    }

    let mut rust_entries = Vec::new();
    let mut stream = bee.range(RangeOptions::default());
    while let Some(entry) = stream.next().await.unwrap() {
        rust_entries.push(entry);
    }
    assert!(!rust_entries.is_empty());

    let core = store.get(GetOptions::key(bee.key())).await.unwrap();
    let mut blocks = Vec::new();
    for i in 0..core.length() {
        let block = core.get(i, Default::default()).await.unwrap().unwrap();
        blocks.push(hex(&block));
    }

    let input = serde_json::json!({
        "primaryKey": hex(&primary_key),
        "blocks": blocks,
    });
    let input_path = dir.path().join("blocks.json");
    std::fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();

    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("js")
        .join("read.js");
    let output = Command::new("node")
        .arg(&script)
        .arg(&input_path)
        .output()
        .expect("run node; install tests/js/package.json dependencies first");
    assert!(
        output.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("node printed JSON");
    let js_entries: Vec<(u64, Bytes, Bytes)> = json
        .as_array()
        .expect("entries array")
        .iter()
        .map(|e| {
            (
                e["seq"].as_u64().expect("seq"),
                bytes(e["key"].as_str().expect("key")),
                bytes(e["value"].as_str().expect("value")),
            )
        })
        .collect();

    assert_eq!(js_entries.len(), rust_entries.len(), "entry count");
    for (js, rust) in js_entries.iter().zip(&rust_entries) {
        assert_eq!(js.0, rust.seq, "seq");
        assert_eq!(js.1, rust.key, "key");
        assert_eq!(js.2, rust.value, "value");
    }
}
