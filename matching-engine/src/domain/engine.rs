// src/domain/engine.rs

use std::collections::{BTreeMap, VecDeque};
use fxhash::FxHashMap; 
use crate::domain::order::{Order, OrderPrice, Side};
use crate::domain::traits::{ArenaStore, OrderIndex};

pub struct OrderBook {
    pub symbol: String,
    
    pub bid_book: BTreeMap<std::cmp::Reverse<OrderPrice>, VecDeque<OrderIndex>>,
    
    pub ask_book: BTreeMap<OrderPrice, VecDeque<OrderIndex>>,
    
    pub order_registry: FxHashMap<u64, OrderIndex>,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            bid_book: BTreeMap::new(),
            ask_book: BTreeMap::new(),
            order_registry: FxHashMap::default(),
        }
    }

    pub fn insert_to_book(&mut self, order_id: u64, price: OrderPrice, side: Side, index: OrderIndex) {
        self.order_registry.insert(order_id, index);

        match side {
            Side::Buy => {
                self.bid_book
                    .entry(std::cmp::Reverse(price))
                    .or_insert_with(VecDeque::new)
                    .push_back(index);
            }
            Side::Sell => {
                self.ask_book
                    .entry(price)
                    .or_insert_with(VecDeque::new)
                    .push_back(index);
            }
        }
    }

    pub fn remove_from_book(&mut self, order_id: u64, price: OrderPrice, side: Side) -> Option<OrderIndex> {
        let arena_index = self.order_registry.remove(&order_id)?;

        match side {
            Side::Buy => {
                let rev_price = std::cmp::Reverse(price);
                if let Some(queue) = self.bid_book.get_mut(&rev_price) {
                    queue.retain(|&idx| idx != arena_index);
                    if queue.is_empty() {
                        self.bid_book.remove(&rev_price);
                    }
                }
            }
            Side::Sell => {
                if let Some(queue) = self.ask_book.get_mut(&price) {
                    queue.retain(|&idx| idx != arena_index);
                    if queue.is_empty() {
                        self.ask_book.remove(&price);
                    }
                }
            }
        }
        
        Some(arena_index)
    }
}