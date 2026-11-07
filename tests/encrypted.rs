use std::sync::Arc;

use corestore::{Corestore, GetOptions};
use hyperbee::{Bee, Head};

#[tokio::test]
async fn opens_an_encrypted_batch_core_with_its_provider_key() {
    let dir = tempfile::tempdir().unwrap();
    let key = {
        let store = Corestore::open(dir.path()).unwrap();
        let encrypted = Arc::new(
            store
                .get(GetOptions {
                    encryption_key: Some([23; 32]),
                    ..GetOptions::name("encrypted")
                })
                .await
                .unwrap(),
        );
        let remote = Bee::from_session(&store, encrypted.clone()).await.unwrap();
        let mut batch = remote.batch();
        batch.put(&b"secret"[..], &b"value"[..]);
        batch.flush().await.unwrap();
        let key = *encrypted.key();
        store.close().await;
        key
    };
    let store = Corestore::open(dir.path()).unwrap();
    let local = Arc::new(store.get(GetOptions::name("local")).await.unwrap());
    let bee = Bee::from_session_with_encryption(
        &store,
        local,
        Some(Arc::new(move |requested| {
            (*requested == key).then_some([23; 32])
        })),
    )
    .await
    .unwrap();
    let view = bee.checkout(Head { key, length: 1 }).await.unwrap();
    assert_eq!(
        view.get(b"secret").await.unwrap().unwrap().value,
        &b"value"[..]
    );
}
