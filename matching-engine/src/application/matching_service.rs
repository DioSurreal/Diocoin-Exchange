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

    pub fn process_order(&mut self, mut taker_order: Order) -> Vec<MatchingEvent> {
        let mut events = Vec::new();
        
        if taker_order.qty == 0 {
            return events; 
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
            let match_price = maker_order.price; // ราคาผู้สร้างสภาพคล่อง (Maker) เป็นหลัก

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
                let maker_side = maker_order.side;
                let maker_price = maker_order.price;

                self.book.remove_from_book(maker_id, maker_price, maker_side);
                let _ = self.arena.deallocate(maker_index);

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

        events
    }

    pub fn cancel_order(&mut self, order_id: u64) -> Vec<MatchingEvent> {
        let mut events = Vec::new();

        let arena_index = match self.book.order_registry.get(&order_id) {
            Some(&idx) => idx,
            None => {
                events.push(MatchingEvent::CancelRejected {
                    order_id,
                    reason: "Order not found or already executed".to_string(),
                });
                return events;
            }
        };

        if let Some(order) = self.arena.get(arena_index) {
            let price = order.price;
            let side = order.side;

            self.book.remove_from_book(order_id, price, side);
            let _ = self.arena.deallocate(arena_index);

            events.push(MatchingEvent::OrderCanceled { order_id });
        }

        events
    }

    
    fn get_best_ask(&self, max_price: OrderPrice) -> Option<crate::domain::traits::OrderIndex> {
        if let Some((&price, queue)) = self.book.ask_book.iter().next() {
            if price <= max_price {
                return queue.front().copied();
            }
        }
        None
    }

    fn get_best_bid(&self, min_price: OrderPrice) -> Option<crate::domain::traits::OrderIndex> {
        if let Some((rev_price, queue)) = self.book.bid_book.iter().next() {
            let price = rev_price.0; 
            if price >= min_price {
                return queue.front().copied();
            }
        }
        None
    }
}