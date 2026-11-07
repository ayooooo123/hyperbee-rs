mod common;

use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, Head, RangeOptions};

use common::{EntryData, Fixture};

async fn open_fixture(fixture: &Fixture) -> (Bee, Corestore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), fixture.primary_key).unwrap();
    for core in &fixture.cores {
        let session = store
            .get(GetOptions::name(core.name.as_str()))
            .await
            .unwrap();
        session.append(&core.blocks).await.unwrap();
    }
    let bee = Bee::open(&store, Some(fixture.root_key)).await.unwrap();
    (bee, store, dir)
}

#[tokio::test]
async fn reads_js_blocks() {
    for fixture in common::fixtures() {
        let (bee, _store, _dir) = open_fixture(&fixture).await;
        let root_blocks = fixture
            .cores
            .iter()
            .find(|c| c.key == fixture.root_key)
            .map_or(0, |c| c.blocks.len());
        let head = bee.head().unwrap();
        assert_eq!(head.length, root_blocks as u64, "{}: head", fixture.name);
        assert_eq!(head.key, fixture.root_key, "{}: core key", fixture.name);

        for expected in &fixture.entries {
            let actual = bee.get(&expected.key).await.unwrap().unwrap_or_else(|| {
                panic!(
                    "{}: missing key {}",
                    fixture.name,
                    common::hex(&expected.key)
                )
            });
            assert_eq!(actual.seq, expected.seq, "{}: seq", fixture.name);
            assert_eq!(actual.key, expected.key, "{}: key", fixture.name);
            assert_eq!(actual.value, expected.value, "{}: value", fixture.name);
        }

        assert_eq!(bee.get(b"not-a-key").await.unwrap(), None);
        assert_eq!(bee.get(b"key-9999").await.unwrap(), None);

        let forward = collect(bee.range(RangeOptions::default())).await;
        assert_entries(&fixture.name, &forward, &fixture.entries);

        let reverse = collect(bee.range(RangeOptions {
            reverse: true,
            ..Default::default()
        }))
        .await;
        let mut expected: Vec<EntryData> = fixture.entries.clone();
        expected.reverse();
        assert_entries(&fixture.name, &reverse, &expected);

        if fixture.entries.len() > 40 {
            let a = fixture.entries[10].key.clone();
            let b = fixture.entries[fixture.entries.len() - 10].key.clone();
            let inclusive = collect(bee.range(RangeOptions {
                gte: Some(a.clone()),
                lte: Some(b.clone()),
                ..Default::default()
            }))
            .await;
            let expected: Vec<EntryData> = fixture
                .entries
                .iter()
                .filter(|e| e.key >= a && e.key <= b)
                .cloned()
                .collect();
            assert_entries(&fixture.name, &inclusive, &expected);

            let exclusive = collect(bee.range(RangeOptions {
                gt: Some(a.clone()),
                lt: Some(b.clone()),
                ..Default::default()
            }))
            .await;
            let expected: Vec<EntryData> = fixture
                .entries
                .iter()
                .filter(|e| e.key > a && e.key < b)
                .cloned()
                .collect();
            assert_entries(&fixture.name, &exclusive, &expected);

            let rev_bounded = collect(bee.range(RangeOptions {
                gte: Some(a.clone()),
                lte: Some(b.clone()),
                reverse: true,
                ..Default::default()
            }))
            .await;
            let mut expected: Vec<EntryData> = fixture
                .entries
                .iter()
                .filter(|e| e.key >= a && e.key <= b)
                .cloned()
                .collect();
            expected.reverse();
            assert_entries(&fixture.name, &rev_bounded, &expected);

            let limited = collect(bee.range(RangeOptions {
                limit: Some(7),
                ..Default::default()
            }))
            .await;
            assert_entries(
                &fixture.name,
                &limited,
                &fixture.entries[..7.min(fixture.entries.len())],
            );

            let empty = collect(bee.range(RangeOptions {
                gt: Some(b),
                lt: Some(a),
                ..Default::default()
            }))
            .await;
            assert!(empty.is_empty(), "{}: inverted range", fixture.name);
        }

        let snapshot = bee
            .checkout(Head {
                key: bee.key(),
                length: root_blocks as u64,
            })
            .await
            .unwrap();
        let snapshot_entries = collect(snapshot.range(RangeOptions::default())).await;
        assert_entries(&fixture.name, &snapshot_entries, &fixture.entries);

        let empty = bee
            .checkout(Head {
                key: bee.key(),
                length: 0,
            })
            .await
            .unwrap();
        assert!(
            collect(empty.range(RangeOptions::default()))
                .await
                .is_empty(),
            "{}: empty checkout",
            fixture.name
        );
    }
}

async fn collect(mut stream: hyperbee::RangeStream) -> Vec<hyperbee::Entry> {
    let mut out = Vec::new();
    while let Some(entry) = stream.next().await.unwrap() {
        out.push(entry);
    }
    out
}

fn assert_entries(name: &str, actual: &[hyperbee::Entry], expected: &[EntryData]) {
    assert_eq!(actual.len(), expected.len(), "{name}: entry count");
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.seq, expected.seq, "{name}: seq");
        assert_eq!(actual.key, expected.key, "{name}: key");
        assert_eq!(actual.value, expected.value, "{name}: value");
    }
}
