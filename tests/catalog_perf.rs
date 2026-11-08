//! Isolated catalog-sized cold list and lookup workload; no app storage is used.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use corestore::Corestore;
use hyperbee::{Bee, RangeOptions};

struct Allocator;
static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: Allocator = Allocator;

fn cpu_seconds() -> f64 {
    let output = std::process::Command::new("ps")
        .args(["-p", &std::process::id().to_string(), "-o", "time="])
        .output()
        .unwrap();
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .split(':')
        .fold(0.0, |total, part| {
            total * 60.0 + part.parse::<f64>().unwrap()
        })
}

#[test]
#[ignore = "catalog allocation and latency benchmark, run in release mode"]
fn catalog_list_and_search() {
    let entries: usize = std::env::var("CATALOG_ENTRIES")
        .unwrap_or_else(|_| "560".into())
        .parse()
        .unwrap();
    assert!(entries > 0);
    let workers = std::env::var("CATALOG_WORKERS")
        .unwrap_or_else(|_| "4".into())
        .parse()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = Corestore::open(dir.path()).unwrap();
        let bee = Bee::open(&store, None).await.unwrap();
        let value = Bytes::from(vec![b'x'; 256]);
        for start in (0..entries).step_by(32) {
            let mut batch = bee.batch();
            for i in start..entries.min(start + 32) {
                batch.put(format!("videos/{i:08}"), value.clone());
            }
            batch.flush().await.unwrap();
        }
        let key = bee.key();
        drop(bee);
        let cpu = cpu_seconds();
        let start = Instant::now();
        COUNTING.store(true, Ordering::Relaxed);
        for _ in 0..3 {
            let reader = Bee::open(&store, Some(key)).await.unwrap();
            let mut list = reader.range(RangeOptions::default());
            let mut seen = 0;
            while let Some(entry) = list.next().await.unwrap() {
                assert_eq!(entry.key, format!("videos/{seen:08}").as_bytes());
                assert_eq!(entry.value, value);
                seen += 1;
            }
            assert_eq!(seen, entries);
            for i in (0..entries).step_by((entries / 32).max(1)) {
                let entry = reader
                    .get(format!("videos/{i:08}").as_bytes())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(entry.value, value);
            }
        }
        COUNTING.store(false, Ordering::Relaxed);
        eprintln!(
            "PERF catalog entries={entries} workers={workers} passes=3 elapsed_ms={:.3} cpu_s={:.3} allocations={} allocated_bytes={}",
            start.elapsed().as_secs_f64() * 1000.0,
            cpu_seconds() - cpu,
            ALLOCATIONS.load(Ordering::Relaxed),
            ALLOCATED.load(Ordering::Relaxed),
        );
        store.close().await;
    });
}
