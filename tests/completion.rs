mod common;

use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, Head};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn key(value: &Value) -> [u8; 32] {
    common::bytes(value.as_str().unwrap())
        .as_ref()
        .try_into()
        .unwrap()
}

fn head(value: &Value) -> Head {
    Head {
        key: key(&value["key"]),
        length: value["length"].as_u64().unwrap(),
    }
}

#[tokio::test]
async fn moves_and_reindexes_like_javascript() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/gen/completion.json")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open_with_primary_key(dir.path(), key(&fixture["primaryKey"])).unwrap();
    let a = Arc::new(store.get(GetOptions::name("a")).await.unwrap());
    let b = Arc::new(store.get(GetOptions::name("b")).await.unwrap());
    assert_eq!(*a.key(), key(&fixture["a"]));
    assert_eq!(*b.key(), key(&fixture["b"]));
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let bee = Bee::from_session_with_encryption(
        &store,
        a.clone(),
        Some(Arc::new(move |_| {
            seen.fetch_add(1, Ordering::Relaxed);
            None
        })),
    )
    .await
    .unwrap();
    let other = Bee::from_session(&store, b.clone()).await.unwrap();
    let mut batch = bee.batch();
    batch.put(&b"local"[..], &b"one"[..]);
    batch.flush().await.unwrap();
    let mut batch = other.batch();
    batch.put(&b"remote"[..], &b"two"[..]);
    batch.flush().await.unwrap();
    let remote = head(&fixture["remoteHead"]);
    let checkout = bee.checkout(remote).await.unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        checkout.get(b"remote").await.unwrap().unwrap().value,
        &b"two"[..]
    );
    bee.move_to(remote).await.unwrap();
    let snapshot = bee.snapshot().await.unwrap();
    let mut batch = bee.batch();
    batch.put(&b"after"[..], &b"three"[..]);
    batch.flush().await.unwrap();
    assert!(snapshot.get(b"after").await.unwrap().is_none());
    for (i, expected) in fixture["movedBlocks"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let actual = a.get(i as u64, Default::default()).await.unwrap().unwrap();
        assert_eq!(actual, common::bytes(expected.as_str().unwrap()));
    }
    assert_eq!(
        bee.cores(true).await.unwrap(),
        fixture["cores"]
            .as_array()
            .unwrap()
            .iter()
            .map(key)
            .collect::<Vec<_>>()
    );
    assert_eq!(bee.cores(false).await.unwrap(), vec![*b.key()]);
    let mut stream = bee.changes(None, 8).await.unwrap();
    for expected in fixture["changes"].as_array().unwrap() {
        let actual = stream.next().await.unwrap().unwrap();
        assert_eq!(actual.head, head(&expected["head"]));
        assert_eq!(
            actual.tail,
            if expected["tail"].is_null() {
                None
            } else {
                Some(head(&expected["tail"]))
            }
        );
        let keys: Vec<Vec<_>> = actual
            .batch
            .iter()
            .map(|block| block.keys.iter().map(|key| common::hex(&key.key)).collect())
            .collect();
        let expected_keys: Vec<Vec<_>> = expected["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|keys| {
                keys.as_array()
                    .unwrap()
                    .iter()
                    .map(|key| key.as_str().unwrap().to_owned())
                    .collect()
            })
            .collect();
        assert_eq!(keys, expected_keys);
    }
    assert!(stream.next().await.unwrap().is_none());
    let count = bee.reindex(|_| false, 8, None).await.unwrap();
    assert_eq!(count as u64, fixture["count"].as_u64().unwrap());
    for (i, expected) in fixture["reindexedBlocks"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let actual = a.get(i as u64, Default::default()).await.unwrap().unwrap();
        assert_eq!(actual, common::bytes(expected.as_str().unwrap()));
    }
    let mut map = |_: &[u8], value: &[u8], _: &hyperbee::Change| {
        let mut next = value.to_vec();
        next.push(b'!');
        Some(next.into())
    };
    let count = bee.reindex(|_| false, 8, Some(&mut map)).await.unwrap();
    assert_eq!(count as u64, fixture["mappedCount"].as_u64().unwrap());
    for (i, expected) in fixture["mappedBlocks"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        let actual = a.get(i as u64, Default::default()).await.unwrap().unwrap();
        assert_eq!(actual, common::bytes(expected.as_str().unwrap()));
    }
    assert_eq!(
        bee.get(b"after").await.unwrap().unwrap().value,
        &b"three!"[..]
    );
    let length = a.length();
    assert_eq!(
        bee.reindex_async(|_| async { Ok(true) }, 0, None)
            .await
            .unwrap(),
        0
    );
    assert_eq!(a.length(), length);
}
