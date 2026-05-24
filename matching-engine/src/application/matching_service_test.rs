#[cfg(test)]
mod tests {
    use crate::application::MatchingEvent;
    use crate::application::matching_service::MatchingEngineService;
    use crate::domain::traits::ArenaStore;
    use crate::infrastructure::memory_arena::ChainedArenaManager;
    use crate::domain::order::{Order, OrderPrice, Side};

    // Helper function to create a Service with a small Arena (to test block growth)
    fn setup_test_engine() -> MatchingEngineService<ChainedArenaManager<Order>> {
        // Set Block Size to only 2 items to test cross-block memory allocation (Chained Arena Scaling)
        let arena = ChainedArenaManager::new(2); 
        MatchingEngineService::new("BTCUSDT".to_string(), arena)
    }

    /// 1. Test placing a resting Buy Order (Resting Limit Order)
    #[test]
    fn test_resting_order_placement() {
        let mut service = setup_test_engine();
        let mut events = Vec::new(); // 🚀 Create buffer for events

        let buy_order = Order::new(1, 101, "BTCUSDT".to_string(), Side::Buy, OrderPrice(60000), 5, 1716475000);

        // 🚀 Pass buffer as a mutable reference
        service.process_order(buy_order, &mut events);

        // Verify correct event emission
        assert_eq!(events.len(), 1);
        if let MatchingEvent::OrderPlaced { order_id, qty } = events[0] {
            assert_eq!(order_id, 1);
            assert_eq!(qty, 5);
        } else {
            panic!("Expected OrderPlaced event");
        }

        // Verify the order is recorded in the registry
        assert!(service.book.order_registry.contains_key(&1));
    }

    /// 2. Test Exact Full Match
    #[test]
    fn test_exact_full_match() {
        let mut service = setup_test_engine();
        let mut events = Vec::new(); // 🚀 Use a shared buffer at the function level
        
        // Send Maker Order to sell 2 BTC at 60,000
        let maker_sell = Order::new(1, 101, "BTCUSDT".to_string(), Side::Sell, OrderPrice(60000), 2, 1716475000);
        service.process_order(maker_sell, &mut events);

        // Send Taker Order to buy 2 BTC at 60,000 (Exact match)
        let taker_buy = Order::new(2, 102, "BTCUSDT".to_string(), Side::Buy, OrderPrice(60000), 2, 1716475005);
        
        // 🚀 Pass the same buffer; the system clears it internally before adding new events
        service.process_order(taker_buy, &mut events);

        // Should emit 3 Events: TradeExecuted, OrderCompleted (Maker), and OrderCompleted (Taker)
        assert_eq!(events.len(), 3);
        
        // Verify trade execution
        if let MatchingEvent::TradeExecuted { maker_id, taker_id, price, match_qty } = events[0] {
            assert_eq!(maker_id, 1);
            assert_eq!(taker_id, 2);
            assert_eq!(price, OrderPrice(60000));
            assert_eq!(match_qty, 2);
        } else {
            panic!("Expected TradeExecuted event");
        }

        // Verify Maker is removed from memory (Reclaim Arena Memory)
        assert!(!service.book.order_registry.contains_key(&1));
        assert!(!service.book.order_registry.contains_key(&2));
    }

    /// 3. Test Partial Fill and Memory Recycling
    #[test]
    fn test_partial_fill_scenarios() {
        let mut service = setup_test_engine();
        let mut events = Vec::new();

        // 1. Maker sells 5 BTC at 60,000
        let maker_sell = Order::new(1, 101, "BTCUSDT".to_string(), Side::Sell, OrderPrice(60000), 5, 1716475000);
        service.process_order(maker_sell, &mut events);

        // 2. Taker buys 2 BTC (Maker should have 3 BTC remaining)
        let taker_buy = Order::new(2, 102, "BTCUSDT".to_string(), Side::Buy, OrderPrice(60000), 2, 1716475005);
        service.process_order(taker_buy, &mut events);

        // Should emit 2 Events: TradeExecuted and OrderCompleted (for the Taker)
        assert_eq!(events.len(), 2);
        
        // Check Arena: Maker (ID: 1) should remain with Qty = 3
        let arena_index = service.book.order_registry.get(&1).unwrap();
        let updated_maker = service.arena.get(*arena_index).unwrap();
        assert_eq!(updated_maker.qty, 3);
    }

    /// 4. Test Order Cancellation Logic & Memory Deallocation
    #[test]
    fn test_cancel_order_successfully() {
        let mut service = setup_test_engine();
        let mut events = Vec::new();

        let order = Order::new(1, 101, "BTCUSDT".to_string(), Side::Buy, OrderPrice(59000), 10, 1716475000);
        service.process_order(order, &mut events);

        // Cancel the order
        service.cancel_order(1, &mut events);

        assert_eq!(events.len(), 1);
        if let MatchingEvent::OrderCanceled { order_id } = events[0] {
            assert_eq!(order_id, 1);
        } else {
            panic!("Expected OrderCanceled event");
        }

        // Registry must be clean
        assert!(!service.book.order_registry.contains_key(&1));
        
        // 🚀 Note: assert!(service.book.bid_book.is_empty()) is removed
        // because we use Lazy Deletion; Ghost Orders remain in bid_book 
        // until the Engine processes and removes them in O(1) time.
    }

    /// 5. Test Chained Arena Dynamic Allocation
    #[test]
    fn test_arena_block_chaining() {
        let mut service = setup_test_engine();
        let mut events = Vec::new();

        // Insert 3 orders (Block Size is 2)
        // The 3rd order should force the ChainedArena to grow a second block automatically
        let o1 = Order::new(1, 101, "BTCUSDT".to_string(), Side::Buy, OrderPrice(50000), 1, 1716475000);
        let o2 = Order::new(2, 102, "BTCUSDT".to_string(), Side::Buy, OrderPrice(51000), 1, 1716475001);
        let o3 = Order::new(3, 103, "BTCUSDT".to_string(), Side::Buy, OrderPrice(52000), 1, 1716475002);

        // Use a single buffer to save memory
        service.process_order(o1, &mut events);
        service.process_order(o2, &mut events);
        service.process_order(o3, &mut events);

        // Verify the registry correctly locates the 3rd order in the new block
        assert!(service.book.order_registry.contains_key(&3));
        
        let index_o3 = service.book.order_registry.get(&3).unwrap();
        let retrieved_o3 = service.arena.get(*index_o3).unwrap();
        assert_eq!(retrieved_o3.order_id, 3);
    }
}