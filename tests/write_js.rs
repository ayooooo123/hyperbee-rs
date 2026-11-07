mod common;

use corestore::{Corestore, GetOptions};
use hyperbee::Bee;

use common::{Op, bytes, hex};

#[tokio::test]
async fn writes_the_same_blocks_as_hyperbee2() {
    for fixture in common::fixtures().into_iter().filter(|f| f.replayable) {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open_with_primary_key(dir.path(), fixture.primary_key).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        assert_eq!(bee.key(), fixture.root_key, "{}: core key", fixture.name);

        for (index, ops) in fixture.batches.iter().enumerate() {
            let mut batch = bee.batch();
            for op in ops {
                match op {
                    Op::Put(k, v) => batch.put(k.clone(), v.clone()),
                    Op::Del(k) => batch.del(k.clone()),
                }
            }
            batch
                .flush()
                .await
                .unwrap_or_else(|e| panic!("{}: batch {index}: {e}", fixture.name));
        }

        assert_eq!(
            bee.head().unwrap().length,
            fixture.blocks_len(),
            "{}: head",
            fixture.name
        );
        assert_eq!(
            bee.head().unwrap().key,
            fixture.root_key,
            "{}: head key",
            fixture.name
        );
        let blocks = &fixture.cores[0].blocks;
        let core = store.get(GetOptions::key(fixture.root_key)).await.unwrap();
        assert_eq!(
            core.length(),
            blocks.len() as u64,
            "{}: block count",
            fixture.name
        );
        for (i, expected) in blocks.iter().enumerate() {
            let actual = core
                .get(i as u64, Default::default())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{}: missing block {i}", fixture.name));
            if actual != *expected {
                let a = hex(&actual);
                let b = hex(expected);
                let at = a
                    .bytes()
                    .zip(b.bytes())
                    .position(|(x, y)| x != y)
                    .unwrap_or(a.len().min(b.len()));
                panic!(
                    "{}: block {i} differs at byte {} (len {} vs {}): actual {} expected {}",
                    fixture.name,
                    at / 2,
                    actual.len(),
                    expected.len(),
                    &a[at.saturating_sub(40)..(at + 80).min(a.len())],
                    &b[at.saturating_sub(40)..(at + 80).min(b.len())]
                );
            }
        }

        let reopened = Bee::open(&store, None).await.unwrap();
        assert_eq!(
            reopened.head().unwrap().length,
            fixture.blocks_len(),
            "{}: reopened head",
            fixture.name
        );
        let mut stream = reopened.range(Default::default());
        let mut seen = Vec::new();
        while let Some(entry) = stream.next().await.unwrap() {
            seen.push(entry);
        }
        assert_eq!(
            seen.len(),
            fixture.entries.len(),
            "{}: reopened entries",
            fixture.name
        );
        for (actual, expected) in seen.iter().zip(&fixture.entries) {
            assert_eq!(actual.seq, expected.seq, "{}: reopened seq", fixture.name);
            assert_eq!(actual.key, expected.key, "{}: reopened key", fixture.name);
            assert_eq!(
                actual.value, expected.value,
                "{}: reopened value",
                fixture.name
            );
        }
    }
}

#[tokio::test]
async fn replays_deletes_and_overwrites_with_expected_entries() {
    for fixture in common::fixtures().into_iter().filter(|f| f.replayable) {
        if fixture.batches.is_empty() {
            continue;
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open_with_primary_key(dir.path(), fixture.primary_key).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        for ops in &fixture.batches {
            let mut batch = bee.batch();
            for op in ops {
                match op {
                    Op::Put(k, v) => batch.put(k.clone(), v.clone()),
                    Op::Del(k) => batch.del(k.clone()),
                }
            }
            batch.flush().await.unwrap();
        }
        let _ = bytes("");
        let mut seen = Vec::new();
        let mut stream = bee.range(Default::default());
        while let Some(entry) = stream.next().await.unwrap() {
            seen.push(entry);
        }
        assert_eq!(
            seen.len(),
            fixture.entries.len(),
            "{}: entry count",
            fixture.name
        );
        for (actual, expected) in seen.iter().zip(&fixture.entries) {
            assert_eq!(actual.seq, expected.seq, "{}: seq", fixture.name);
            assert_eq!(actual.key, expected.key, "{}: key", fixture.name);
            assert_eq!(actual.value, expected.value, "{}: value", fixture.name);
        }
    }
}
