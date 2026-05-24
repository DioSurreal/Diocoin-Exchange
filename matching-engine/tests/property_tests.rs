// tests/property_tests.rs

use proptest::prelude::*;
use matching_engine::domain::order::{Order, OrderPrice, Side, OrderType, OrderTimeInForce};
use matching_engine::domain::traits::ArenaStore;
use matching_engine::application::matching_service::MatchingEngineService;
use matching_engine::infrastructure::memory_arena::ChainedArenaManager;

// Invariants to check:
// 1. The engine must NEVER panic under any random sequence of orders.
// 2. Memory tracking (order_registry) must perfectly align with active remaining quantities.
fn run_fuzz_matching_simulation(orders_data: Vec<(u64, u64, bool, u64)>) {
    let arena = ChainedArenaManager::new(5);
    let mut service = MatchingEngineService::new("BTCUSDT".to_string(), arena);
    let mut events = Vec::new();

    // Use .enumerate() to create Sequential IDs to prevent collisions
    for (index, (_, price_raw, is_buy, qty)) in orders_data.into_iter().enumerate() {
        if qty == 0 || price_raw == 0 { continue; } 

        let unique_id = (index + 1) as u64; // Guaranteed unique ID per loop iteration
        let side = if is_buy { Side::Buy } else { Side::Sell };
        let price = OrderPrice(price_raw);
        
        let order = Order::new(unique_id, 999, "BTCUSDT".to_string(), side, OrderType::Limit, OrderTimeInForce::GoodTillCancel, price, qty, 1716475000);
        
        service.process_order(order, &mut events);

        // Check memory safety invariants
        for (&order_id, &arena_index) in service.book.order_registry.iter() {
            let fetched_order = service.arena.get(arena_index);
            assert!(fetched_order.is_some(), "Registry points to a dead arena reference!");
            
            let active_order = fetched_order.unwrap();
            assert!(active_order.qty > 0, "Order with 0 remaining quantity left lingering in memory!");
            assert_eq!(active_order.order_id, order_id, "Registry index mismatch with stored entity!");
        }
    }
}

// Define the random data generators using proptest
proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))] // Run 500 completely different chaotic scenarios
    
    #[test]
    fn fuzz_matching_engine_integrity(
        // Generate a vector of up to 50 orders per test case
        // Tuple: (order_id, price between 50k-60k, is_buy, qty between 1-10)
        orders in prop::collection::vec(
            (1..1000u64, 50000..60000u64, prop::bool::ANY, 1..10u64), 
            1..50
        )
    ) {
        run_fuzz_matching_simulation(orders);
    }
}