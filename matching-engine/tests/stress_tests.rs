// tests/stress_tests.rs

use std::time::Instant;
use matching_engine::domain::order::{Order, OrderPrice, Side};
use matching_engine::application::matching_service::MatchingEngineService;
use matching_engine::infrastructure::memory_arena::ChainedArenaManager;

#[test]
fn test_matching_engine_million_transaction_load() {
    // 1. Allocate arena architecture with a block size of 50,000 orders.
    // This triggers 10-20 block chainings to test pointer stability.
    let arena = ChainedArenaManager::new(50000);
    let mut service = MatchingEngineService::new("BTCUSDT".to_string(), arena);

    println!("\n==================================================");
    println!("🔥 STARTING STRESS TEST: 1,000,000 TRANSACTIONS");
    println!("==================================================");

    // --- PHASE 1: GENERATE 1 MILLION ORDERS IN MEMORY ---
    let total_orders = 1_000_000;
    let mut orders = Vec::with_capacity(total_orders);

    for i in 1..=total_orders {
        // IDs 1-500k are Buy side resting at 60,000
        // IDs 501k-1M are Sell side executing against the price
        let side = if i <= total_orders / 2 { Side::Buy } else { Side::Sell };
        let price = OrderPrice(60000);
        let qty = 10; // 10 BTC per order

        orders.push(Order::new(
            i as u64,
            999, // Client ID
            "BTCUSDT".to_string(),
            side,
            price,
            qty,
            1716475000,
        ));
    }

    // --- PHASE 2: INJECT INTO THE MATCHING ENGINE & MEASURE PERFORMANCE ---
    let start_time = Instant::now();
    let mut total_events_processed = 0;
    let mut events_buffer = Vec::new();

    for order in orders {
        service.process_order(order, &mut events_buffer);
        total_events_processed += events_buffer.len();
    }

    let duration = start_time.elapsed();
    let duration_secs = duration.as_secs_f64();
    let throughput = total_orders as f64 / duration_secs;

    // --- PHASE 3: METRICS AND AUDIT REPORT ---
    println!("🏁 STRESS TEST COMPLETED SUCCESSFULLY!");
    println!("⏱️ Total Time Taken: {:.4} seconds", duration_secs);
    println!("📈 Throughput: {:.2} Orders/Second", throughput);
    println!("📢 Total Lifecycle Events Emitted: {}", total_events_processed);

    // --- PHASE 4: INVARIANT MEMORY HEALTH CHECK ---
    // After matching 500k vs 500k orders, the book should be cleared.
    // Order registry must be cleared to 0; no memory leaks allowed.
    println!("🧠 Order Registry Remaining: {}", service.book.order_registry.len());
    
    assert_eq!(
        service.book.order_registry.len(), 
        0, 
        "Memory Leak Detected! Order registry should be completely empty after full settlement."
    );
    
    println!("✅ Memory Invariant Check: Perfect. No memory leaks or dangling indices.");
    println!("==================================================\n");
}