// tests/recovery_tests.rs
use matching_engine::application::coordinator::{EngineCoordinator, WalEntry};
use matching_engine::application::matching_service::MatchingEngineService;
use matching_engine::domain::engine::OrderBook;
use matching_engine::domain::order::{Order, OrderPrice, Side};
use matching_engine::infrastructure::memory_arena::ChainedArenaManager;
use matching_engine::infrastructure::recoveries::snapshot::SnapshotManager;
use matching_engine::infrastructure::recoveries::wal::WalManager;

use std::path::PathBuf;
use tempfile::TempDir;

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

fn make_coordinator(
    wal_dir: &PathBuf,
    snap_dir: &PathBuf,
    snapshot_interval: u64,
) -> EngineCoordinator<ChainedArenaManager<Order>> {
    let arena = ChainedArenaManager::new(256);
    let service = MatchingEngineService::new("BTC-USDT".to_string(), arena);
    let wal = WalManager::new(wal_dir, 64 * 1024 * 1024);
    let snap = SnapshotManager::new(snap_dir, "engine.snapshot");
    EngineCoordinator::new(service, wal, snap, snapshot_interval)
}

fn buy_order(id: u64, price: OrderPrice, qty: u64) -> Order {
    Order::new(
        id,
        999,
        "BTC-USDT".to_string(),
        Side::Buy,
        matching_engine::domain::order::OrderType::Limit,
        matching_engine::domain::order::OrderTimeInForce::GoodTillCancel,
        price,
        qty,
        1716475000,
    )
}

fn sell_order(id: u64, price: OrderPrice, qty: u64) -> Order {
    Order::new(
        id,
        999,
        "BTC-USDT".to_string(),
        Side::Sell,
        matching_engine::domain::order::OrderType::Limit,
        matching_engine::domain::order::OrderTimeInForce::GoodTillCancel,
        price,
        qty,
        1716475000,
    )
}

// ─────────────────────────────────────────────
// 1. HAPPY PATH
// ─────────────────────────────────────────────

#[test]
fn test_full_crash_recovery_restores_state_exactly() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");

    let mut events = Vec::new();
    let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
    engine.bootstrap().unwrap();

    engine.handle_process_order(buy_order(1, OrderPrice(100), 10), &mut events).unwrap();
    engine.handle_process_order(buy_order(2, OrderPrice(99), 5), &mut events).unwrap();
    engine.handle_process_order(buy_order(3, OrderPrice(98), 3), &mut events).unwrap();
    engine.handle_process_order(sell_order(4, OrderPrice(102), 8), &mut events).unwrap();
    engine.handle_process_order(sell_order(5, OrderPrice(103), 4), &mut events).unwrap();

    assert_eq!(engine.seq_id(), 5);

    // ── save snapshot via the dedicated method (no borrow conflict) ──
    engine.save_snapshot().unwrap();

    engine.handle_process_order(buy_order(6, OrderPrice(105), 2), &mut events).unwrap();
    engine.handle_process_order(sell_order(7, OrderPrice(99), 3), &mut events).unwrap();
    engine.handle_process_order(buy_order(8, OrderPrice(97), 7), &mut events).unwrap();

    let seq_before_crash = engine.seq_id();
    assert_eq!(seq_before_crash, 8);

    let expected_bid_levels: Vec<_> = engine
        .service.book.bid_book.keys().map(|k| k.0).collect();
    let expected_ask_levels: Vec<_> = engine
        .service.book.ask_book.keys().copied().collect();
    let expected_registry_len = engine.service.book.order_registry.len();

    drop(engine);

    let mut recovered = make_coordinator(&wal_dir, &snap_dir, 9999);
    recovered.bootstrap().unwrap();

    assert_eq!(recovered.seq_id(), seq_before_crash);

    let recovered_bid_levels: Vec<_> = recovered
        .service.book.bid_book.keys().map(|k| k.0).collect();
    let recovered_ask_levels: Vec<_> = recovered
        .service.book.ask_book.keys().copied().collect();

    assert_eq!(recovered_bid_levels, expected_bid_levels);
    assert_eq!(recovered_ask_levels, expected_ask_levels);
    assert_eq!(recovered.service.book.order_registry.len(), expected_registry_len);

    println!("✅ Happy Path: seq_id={}", recovered.seq_id());
}

// ─────────────────────────────────────────────
// 2. EDGE CASES
// ─────────────────────────────────────────────

#[test]
fn test_cold_start_replay_from_wal_only() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");

    let mut events = Vec::new();

    {
        let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
        engine.bootstrap().unwrap();

        engine.handle_process_order(buy_order(10, OrderPrice(200), 5), &mut events).unwrap();
        engine.handle_process_order(sell_order(11, OrderPrice(201), 5), &mut events).unwrap();
        engine.handle_process_order(buy_order(12, OrderPrice(199), 10), &mut events).unwrap();

        assert_eq!(engine.seq_id(), 3);
        // no snapshot — drop directly
    }

    let mut recovered = make_coordinator(&wal_dir, &snap_dir, 9999);
    recovered.bootstrap().unwrap();

    assert_eq!(recovered.seq_id(), 3);
    assert!(recovered.service.book.order_registry.contains_key(&10));
    assert!(recovered.service.book.order_registry.contains_key(&12));

    println!("✅ Cold Start: seq_id={}", recovered.seq_id());
}

#[test]
fn test_incomplete_snapshot_tmp_file_is_ignored() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");
    std::fs::create_dir_all(&snap_dir).unwrap();

    let mut events = Vec::new();

    {
        let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
        engine.bootstrap().unwrap();
        engine.handle_process_order(buy_order(20, OrderPrice(300), 5), &mut events).unwrap();
        engine.save_snapshot().unwrap();
    }

    // simulate a leftover .tmp from a mid-write crash
    let tmp_path = snap_dir.join("engine.snapshot.tmp");
    std::fs::write(&tmp_path, b"THIS IS CORRUPT GARBAGE DATA").unwrap();

    let mut recovered = make_coordinator(&wal_dir, &snap_dir, 9999);
    let result = recovered.bootstrap();

    assert!(result.is_ok(), "must not error on stale .tmp: {:?}", result.err());
    assert_eq!(recovered.seq_id(), 1);
    assert!(recovered.service.book.order_registry.contains_key(&20));

    println!("✅ Incomplete Snapshot: .tmp file ignored");
}

// ─────────────────────────────────────────────
// 3. ERROR HANDLING / DATA CORRUPTION
// ─────────────────────────────────────────────

#[test]
fn test_corrupt_snapshot_returns_error_not_garbage_state() {
    let tmp = TempDir::new().unwrap();
    let snap_dir = tmp.path().join("snap");
    std::fs::create_dir_all(&snap_dir).unwrap();

    let snap_path = snap_dir.join("engine.snapshot");
    std::fs::write(
        &snap_path,
        b"\x00\xFF\xDE\xAD\xBE\xEF garbage bytes that are not valid bincode",
    )
    .unwrap();

    let snap_manager = SnapshotManager::new(&snap_dir, "engine.snapshot");
    let result = snap_manager.load_snapshot::<OrderBook, ChainedArenaManager<Order>>();

    assert!(result.is_err(), "corrupt snapshot must return Err");

    println!("✅ Corrupt Snapshot: {:?}", result.err());
}

#[test]
fn test_corrupt_wal_trailing_bytes_partial_read_succeeds() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let mut events = Vec::new();

    {
        let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
        engine.bootstrap().unwrap();
        engine.handle_process_order(buy_order(30, OrderPrice(400), 3), &mut events).unwrap();
        engine.handle_process_order(buy_order(31, OrderPrice(401), 2), &mut events).unwrap();
    }

    let wal_file = wal_dir.join("0000000000000001.wal");
    let mut file = std::fs::OpenOptions::new().append(true).open(&wal_file).unwrap();
    use std::io::Write;
    file.write_all(b"\xDE\xAD\xBE\xEF\x00\x00\x00\xFF corrupted tail").unwrap();
    drop(file);

    let wal_manager = WalManager::new(&wal_dir, 64 * 1024 * 1024);
    let entries: Vec<WalEntry> = wal_manager.read_all_entries().unwrap();

    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].seq_id, 1);
    assert_eq!(entries[1].seq_id, 2);

    println!("✅ Corrupt WAL: read {} good entries", entries.len());
}

#[test]
fn test_bootstrap_with_empty_dirs_succeeds() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");

    let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
    let result = engine.bootstrap();

    assert!(result.is_ok(), "{:?}", result.err());
    assert_eq!(engine.seq_id(), 0);
    assert!(engine.service.book.order_registry.is_empty());

    println!("✅ Empty Bootstrap: seq_id=0");
}

#[test]
fn test_wal_replay_skips_entries_already_in_snapshot() {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let snap_dir = tmp.path().join("snap");

    let mut events = Vec::new();

    {
        let mut engine = make_coordinator(&wal_dir, &snap_dir, 9999);
        engine.bootstrap().unwrap();

        engine.handle_process_order(sell_order(40, OrderPrice(500), 5), &mut events).unwrap();
        engine.handle_process_order(buy_order(41, OrderPrice(500), 5), &mut events).unwrap();
        engine.handle_process_order(buy_order(42, OrderPrice(490), 3), &mut events).unwrap();
        engine.handle_process_order(sell_order(43, OrderPrice(510), 3), &mut events).unwrap();

        assert_eq!(engine.seq_id(), 4);
        assert_eq!(engine.service.book.order_registry.len(), 2);

        engine.save_snapshot().unwrap();

        engine.handle_process_order(buy_order(44, OrderPrice(488), 1), &mut events).unwrap();
        assert_eq!(engine.seq_id(), 5);
    }

    let mut recovered = make_coordinator(&wal_dir, &snap_dir, 9999);
    recovered.bootstrap().unwrap();

    assert_eq!(recovered.seq_id(), 5);
    assert!(!recovered.service.book.order_registry.contains_key(&40));
    assert!(!recovered.service.book.order_registry.contains_key(&41));
    assert!(recovered.service.book.order_registry.contains_key(&42));
    assert!(recovered.service.book.order_registry.contains_key(&43));
    assert!(recovered.service.book.order_registry.contains_key(&44));

    println!(
        "✅ Replay Skip: book_size={}",
        recovered.service.book.order_registry.len()
    );
}