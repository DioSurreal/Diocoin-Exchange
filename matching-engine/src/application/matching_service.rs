// src/application/matching_service.rs

use crate::application::MatchingEvent;
use crate::domain::engine::OrderBook;
use crate::domain::order::{Order, OrderPrice, Side, OrderType, OrderTimeInForce};
use crate::domain::traits::ArenaStore;
use tokio::sync::mpsc::UnboundedSender;

pub mod proto_events {
    include!(concat!(env!("OUT_DIR"), "/diocoin.exchange.matching.rs"));
}

pub struct MatchingEngineService<A: ArenaStore<Order>> {
    pub book: OrderBook,
    pub arena: A, 
    pub event_sender: UnboundedSender<proto_events::OutboundEvent>,
}

impl<A: ArenaStore<Order>> MatchingEngineService<A> {
    // Constructor accepts event_sender for the outbound async pipeline.
    pub fn new(symbol: String, arena: A, event_sender: UnboundedSender<proto_events::OutboundEvent>) -> Self {
        Self {
            book: OrderBook::new(symbol),
            arena,
            event_sender,
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
                self.emit_proto_events(events); // Emit events before returning.
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

            // ===================================================================
            // [PRECISION AUDIT] Calculate the total quote value.
            // ===================================================================
            let total_value = maker_order.calculate_execution_value(match_qty);

            taker_order.fill(match_qty);
            maker_order.fill(match_qty);

            // Safely emit the calculated notional value to downstream consumers.
            events.push(MatchingEvent::TradeExecuted {
                maker_id: maker_order.order_id,
                taker_id: taker_order.order_id,
                price: match_price,
                match_qty,
                total_value,
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
            
            if taker_order.time_in_force == OrderTimeInForce::ImmediateOrCancel || taker_order.order_type == OrderType::Market {
                events.push(MatchingEvent::OrderCompleted { order_id });
            } else {
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

        // 5. Stream data into the non-blocking pipeline before exiting.
        self.emit_proto_events(events);
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
                self.emit_proto_events(events); // Emit events before returning from the failure case.
                return;
            }
        };

        if let Some(order) = self.arena.get_mut(arena_index) {
            order.qty = 0; 
            events.push(MatchingEvent::OrderCanceled { order_id });
        }

        // Stream data into the pipeline after successful processing.
        self.emit_proto_events(events);
    }

    // ===================================================================
    // Private helper: convert events to Protobuf and send them fire-and-forget.
    // ===================================================================
    fn emit_proto_events(&self, events: &[MatchingEvent]) {
        for event in events {
            if let MatchingEvent::TradeExecuted { maker_id, taker_id, price, match_qty, total_value } = event {
                
                let proto_trade = proto_events::TradeExecutedEvent {
                    maker_id: *maker_id,
                    taker_id: *taker_id,
                    price: price.0,
                    match_qty: *match_qty,
                    total_value: *total_value,
                    timestamp: 1716475000, // Can be replaced with a real timestamp later.
                };

                let outbound = proto_events::OutboundEvent {
                    event: Some(proto_events::outbound_event::Event::Trade(proto_trade)),
                };

                // Use an unbounded channel to send data in O(1) without blocking the matching path.
                let _ = self.event_sender.send(outbound);
            }
            // Note: expand this mapping for other events such as OrderPlaced or Canceled.
            // Add more mappings here as the team's `.proto` schema evolves.
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
