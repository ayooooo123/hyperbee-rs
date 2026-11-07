use corestore::Corestore;
use hyperbee::{Bee, RangeOptions, ReadOptions};
use hypercore::RequestBag;

#[tokio::test]
async fn cancelled_bag_reaches_get_range_and_changes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open(dir.path()).unwrap();
    let writer = Bee::open(&store, None).await.unwrap();
    let mut batch = writer.batch();
    batch.put(&b"key"[..], &b"value"[..]);
    batch.flush().await.unwrap();
    let bag = RequestBag::new();
    bag.destroy("cancelled");
    let options = ReadOptions {
        bag: Some(bag),
        ..Default::default()
    };
    let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
    assert!(
        reader
            .get_with_options(b"key", options.clone())
            .await
            .is_err()
    );
    let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
    let mut range = reader.range_with_options(RangeOptions::default(), options.clone());
    assert!(range.next().await.is_err());
    let reader = Bee::open(&store, Some(writer.key())).await.unwrap();
    let mut changes = reader.changes_with_options(None, 0, options).await.unwrap();
    assert!(changes.next().await.is_err());
    writer.close().await;
}
