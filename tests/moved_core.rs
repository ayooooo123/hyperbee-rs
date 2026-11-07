mod common;

use std::sync::Arc;

use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, RangeOptions};

use common::hex;

/// JS `bee.move({ key, length })` onto a head that lives on another core, then
/// write. The moved node still carries a delta cohort from the other core, so
/// this pins the cross-core pointer of that cohort down to the byte.
#[tokio::test]
async fn writes_after_moving_onto_another_core() {
    let fixture = common::fixtures()
        .into_iter()
        .find(|f| f.name == "moved-core")
        .expect("moved-core fixture");

    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), fixture.primary_key).unwrap();

    let base = &fixture.cores[0];
    let local = &fixture.cores[1];
    // the fixture writes one put per flush, so a block count is a key count
    let base_keys = base.blocks.len();
    let extra = local.blocks.len();

    let a = Bee::from_session(
        &store,
        Arc::new(store.get(GetOptions::name("view-a")).await.unwrap()),
    )
    .await
    .unwrap();
    for i in 0..base_keys {
        let mut batch = a.batch();
        batch.put(format!("item/{i}"), format!("value/{i}"));
        batch.flush().await.unwrap();
    }
    let head = a.head().expect("moved head");

    let bee = Bee::from_session(
        &store,
        Arc::new(store.get(GetOptions::name("view-b")).await.unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(bee.key(), fixture.root_key, "local core key");
    bee.move_to(head).await.unwrap();
    for i in base_keys..base_keys + extra {
        let mut batch = bee.batch();
        batch.put(format!("item/{i}"), format!("value/{i}"));
        batch.flush().await.unwrap();
    }

    for core in &fixture.cores {
        let session = store.get(GetOptions::key(core.key)).await.unwrap();
        assert_eq!(
            session.length(),
            core.blocks.len() as u64,
            "{}: block count",
            core.name
        );
        for (i, expected) in core.blocks.iter().enumerate() {
            let actual = session
                .get(i as u64, Default::default())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{}: missing block {i}", core.name));
            assert_eq!(
                hex(&actual),
                hex(expected),
                "{}: block {i} differs",
                core.name
            );
        }
    }

    let mut seen = Vec::new();
    let mut stream = bee.range(RangeOptions::default());
    while let Some(entry) = stream.next().await.unwrap() {
        seen.push(entry);
    }
    assert_eq!(seen.len(), fixture.entries.len(), "entries");
    for (actual, expected) in seen.iter().zip(&fixture.entries) {
        assert_eq!(actual.seq, expected.seq, "seq");
        assert_eq!(actual.key, expected.key, "key");
        assert_eq!(actual.value, expected.value, "value");
    }
}
