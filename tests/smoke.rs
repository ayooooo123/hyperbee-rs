use corestore::{Corestore, GetOptions};
use hyperbee::Bee;

#[tokio::test]
async fn writes_a_js_identical_single_key_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = Corestore::open(dir.path()).unwrap();
    let bee = Bee::open(&store, None).await.unwrap();

    let mut batch = bee.batch();
    batch.put(&b"a"[..], &b"1"[..]);
    batch.flush().await.unwrap();

    let core = store.get(GetOptions::key(bee.key())).await.unwrap();
    let block = core.get(0, Default::default()).await.unwrap().unwrap();
    assert_eq!(
        hex(&block),
        "010000000c01011100010161010131",
        "block bytes differ from hyperbee2"
    );

    let entry = bee.get(b"a").await.unwrap().unwrap();
    assert_eq!(entry.key, &b"a"[..]);
    assert_eq!(entry.value, &b"1"[..]);
    assert_eq!(entry.seq, 0);

    let head = bee.head().unwrap();
    assert_eq!(head.length, 1);
    assert_eq!(head.key, bee.key());
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
