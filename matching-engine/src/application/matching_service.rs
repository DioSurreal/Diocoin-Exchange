// src/application/matching_service.rs

use crate::application::MatchingEvent;
use crate::domain::engine::OrderBook;
use crate::domain::order::{Order, OrderPrice, Side, OrderType, OrderTimeInForce};
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

    pub fn process_order(&mut self, mut taker_order: Order, events: &mut Vec<MatchingEvent>) {
        events.clear(); // Clear existing data, reuse memory

        if taker_order.qty == 0 {
            return; 
        }

        // ==========================================
        // 🛡️ STEP 3: Post-Only Guard
        // ==========================================
        if taker_order.time_in_force == OrderTimeInForce::PostOnly {
            let would_cross = match taker_order.side {
                Side::Buy => self.get_best_ask(taker_order.price).is_some(),
                Side::Sell => self.get_best_bid(taker_order.price).is_some(),
            };

            if would_cross {
                events.push(MatchingEvent::CancelRejected {
                    order_id: taker_order.order_id,
                    reason: "Post-Only order rejected: would take liquidity".to_string(),
                });
                return;
            }
        }

        // ==========================================
        // 🔄 Core Matching Loop (Limit / Market / IOC)
        // ==========================================
        while taker_order.qty > 0 {
            let best_match = match taker_order.side {
                Side::Buy => {
                    let max_price = match taker_order.order_type {
                        OrderType::Market => OrderPrice(u64::MAX),
                        OrderType::Limit => taker_order.price,
                    };
                    self.get_best_ask(max_price)
                }
                Side::Sell => {
                    let min_price = match taker_order.order_type {
                        OrderType::Market => OrderPrice(0),
                        OrderType::Limit => taker_order.price,
                    };
                    self.get_best_bid(min_price)
                }
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
                self.book.order_registry.remove(&maker_id);
                events.push(MatchingEvent::OrderCompleted { order_id: maker_id });
            }
        }

        // ==========================================
        // 📥 STEP 4: Remainder Placement Logic (With IOC Support)
        // ==========================================
        if taker_order.qty > 0 {
            let order_id = taker_order.order_id;
            
            // Checking both TimeInForce and OrderType to ensure proper memory allocation strategy
            if taker_order.time_in_force == OrderTimeInForce::ImmediateOrCancel || taker_order.order_type == OrderType::Market {
                // IOC or Market remainder is immediately killed -> Zero-allocation, bypassing the book queue entirely
                events.push(MatchingEvent::OrderCompleted { order_id });
            } else {
                // Limit order (GoodTillCancel / PostOnly) survives -> Allocates and persists inside order book
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
                            qty 
                        });
                    }
                }
            }
        } else {
            events.push(MatchingEvent::OrderCompleted { order_id: taker_order.order_id });
        }
    }

    pub fn cancel_order(&mut self, order_id: u64, events: &mut Vec<MatchingEvent>) {
        events.clear();

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
            order.qty = 0; 
            events.push(MatchingEvent::OrderCanceled { order_id });
        }
    }

    fn get_best_ask(&mut self, max_price: OrderPrice) -> Option<crate::domain::traits::OrderIndex> {
        loop {
            let (price, is_empty) = {
                let mut iter = self.book.ask_book.iter_mut();
                if let Some((&price, queue)) = iter.next() {
                    if price > max_price { return None; }

                    while let Some(&idx) = queue.front() {
                        if let Some(order) = self.arena.get(idx) {
                            if order.qty > 0 {
                                return Some(idx);
                            }
                        }
                        queue.pop_front();
                        let _ = self.arena.deallocate(idx);
                    }
                    (price, queue.is_empty())
                } else {
                    return None;
                }
            };

            if is_empty {
                self.book.ask_book.remove(&price);
            }
        }
    }

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