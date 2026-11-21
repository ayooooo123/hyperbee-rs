//! Memory a writer's tree holds after many small batches, the follower's workload (autobee applies a channel's
//! history one batch at a time). Each flush commits the touched nodes; JS `CompressedArray.commit` shares the
//! delta objects between the committed node and the base node it leaves in the cache, so a commit costs one
//! array of references, not a copy of every delta and cohort.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::Bytes;
use corestore::Corestore;
use hyperbee::Bee;

/// Bytes allocated and freed on the test's own thread (the current-thread runtime runs the tree there); other
/// threads (RocksDB's background work) are left out.
struct Allocator;
thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static LIVE: Cell<i64> = const { Cell::new(0) };
}

fn track(delta: i64) {
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            let _ = LIVE.try_with(|live| live.set(live.get() + delta));
        }
    });
}

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        track(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        track(size as i64 - layout.size() as i64);
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

#[test]
fn many_small_batches_keep_the_tree_small() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        let value = Bytes::from(vec![b'x'; 64]);
        COUNTING.with(|on| on.set(true));
        let keys = 4_000usize;
        for start in (0..keys).step_by(4) {
            let mut batch = bee.batch();
            for i in start..keys.min(start + 4) {
                batch.put(format!("announce/{:08}", i * 7919 % keys), value.clone());
            }
            batch.flush().await.unwrap();
        }
        COUNTING.with(|on| on.set(false));
        let live = LIVE.with(Cell::get);
        eprintln!("live bytes held after {keys} keys in batches of 4: {live}");
        // Every key is still there.
        for i in (0..keys).step_by(97) {
            let entry = bee.get(format!("announce/{i:08}").as_bytes()).await.unwrap();
            assert_eq!(entry.map(|e| e.value), Some(value.clone()), "key {i}");
        }
        assert!(
            live < LIMIT,
            "the tree holds {live} bytes after {keys} keys; copying every delta on commit makes it hold more than {LIMIT}"
        );
        drop(bee);
        store.close().await;
    });
}

/// Measured on this workload (release, Apple M1 Pro): 43.5 MB held when every commit copied its deltas,
/// 17.7 MB when they are shared. The limit sits between them.
const LIMIT: i64 = 30_000_000;
