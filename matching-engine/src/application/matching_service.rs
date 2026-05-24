// src/application/matching_service.rs

use crate::application::MatchingEvent;
use crate::domain::engine::OrderBook;
use crate::domain::order::{Order, OrderPrice, Side};
use crate::domain::traits::ArenaStore;

pub struct MatchingEngineService<A: ArenaStore<Order>> {
    pub book: OrderBook,
    pub arena: A, 
}

impl<A: ArenaStore<Order>> MatchingEngineService<A> {
    pub fn new(symbol: String, arena: A) -> Self {
        Self {
            book: OrderBook::new(symbol),
            arena,
        }
    }

    // 🚀 1. Switch to &mut Vec (Buffer Reuse) to reduce heap allocation overhead
    pub fn process_order(&mut self, mut taker_order: Order, events: &mut Vec<MatchingEvent>) {
        events.clear(); // Clear existing data, reuse memory

        if taker_order.qty == 0 {
            return; 
        }

        while taker_order.qty > 0 {
            let best_match = match taker_order.side {
                Side::Buy => self.get_best_ask(taker_order.price),
                Side::Sell => self.get_best_bid(taker_order.price),
            };

            let maker_index = match best_match {
                Some(idx) => idx,
                None => break,
            };

            let maker_order = self.arena.get_mut(maker_index).unwrap();

            let match_qty = std::cmp::min(taker_order.qty, maker_order.qty);
            let match_price = maker_order.price;

            taker_order.fill(match_qty);
            maker_order.fill(match_qty);

            events.push(MatchingEvent::TradeExecuted {
                maker_id: maker_order.order_id,
                taker_id: taker_order.order_id,
                price: match_price,
                match_qty,
            });

            if maker_order.is_filled() {
                let maker_id = maker_order.order_id;
                
                // 🚀 2. O(1) Removal: Remove only index from Registry.
                // Do not search OrderBook queues; leave as a Ghost Order.
                self.book.order_registry.remove(&maker_id);

                events.push(MatchingEvent::OrderCompleted { order_id: maker_id });
            }
        }

        if taker_order.qty > 0 {
            let order_id = taker_order.order_id;
            let price = taker_order.price;
            let side = taker_order.side;
            let is_partial = taker_order.qty < taker_order.original_qty;
            let qty = taker_order.qty;

            if let Ok(new_index) = self.arena.allocate(taker_order) {
                self.book.insert_to_book(order_id, price, side, new_index);

                if is_partial {
                    events.push(MatchingEvent::OrderPartiallyFilled { 
                        order_id, 
                        remaining_qty: qty 
                    });
                } else {
                    events.push(MatchingEvent::OrderPlaced { 
                        order_id, 
                        qty: qty 
                    });
                }
            }
        } else {
            events.push(MatchingEvent::OrderCompleted { order_id: taker_order.order_id });
        }
    }

    pub fn cancel_order(&mut self, order_id: u64, events: &mut Vec<MatchingEvent>) {
        events.clear();

        // 🚀 3. O(1) Cancellation: Pull and remove from Registry immediately
        let arena_index = match self.book.order_registry.remove(&order_id) {
            Some(idx) => idx,
            None => {
                events.push(MatchingEvent::CancelRejected {
                    order_id,
                    reason: "Order not found or already executed".to_string(),
                });
                return;
            }
        };

        if let Some(order) = self.arena.get_mut(arena_index) {
            order.qty = 0; // Transform into a Ghost Order 
            
            // Note: Memory is not reclaimed immediately to maintain O(1) complexity.
            events.push(MatchingEvent::OrderCanceled { order_id });
        }
    }

    // 🚀 4. Lazy Garbage Collection: Updated function to take &mut self
    fn get_best_ask(&mut self, max_price: OrderPrice) -> Option<crate::domain::traits::OrderIndex> {
        loop {
            let (price, is_empty) = {
                let mut iter = self.book.ask_book.iter_mut();
                if let Some((&price, queue)) = iter.next() {
                    if price > max_price { return None; }

                    // Check the front of the queue
                    while let Some(&idx) = queue.front() {
                        if let Some(order) = self.arena.get(idx) {
                            if order.qty > 0 {
                                return Some(idx); // Found valid order ready for matching
                            }
                        }
                        // Found Ghost Order (canceled or filled) -> remove in O(1)
                        queue.pop_front();
                        let _ = self.arena.deallocate(idx); // Reclaim memory in Arena
                    }
                    (price, queue.is_empty())
                } else {
                    return None; // Order book is empty
                }
            };

            // If price level is empty, remove it
            if is_empty {
                self.book.ask_book.remove(&price);
            }
        }
    }

    // Similar garbage collection logic for the Bid side
    fn get_best_bid(&mut self, min_price: OrderPrice) -> Option<crate::domain::traits::OrderIndex> {
        loop {
            let (price_rev, is_empty) = {
                let mut iter = self.book.bid_book.iter_mut();
                if let Some((&price_rev, queue)) = iter.next() {
                    if price_rev.0 < min_price { return None; }

                    while let Some(&idx) = queue.front() {
                        if let Some(order) = self.arena.get(idx) {
                            if order.qty > 0 {
                                return Some(idx);
                            }
                        }
                        queue.pop_front();
                        let _ = self.arena.deallocate(idx);
                    }
                    (price_rev, queue.is_empty())
                } else {
                    return None;
                }
            };

            if is_empty {
                self.book.bid_book.remove(&price_rev);
            }
        }
    }
}