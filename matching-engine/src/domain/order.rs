// src/domain/order.rs

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderTimeInForce {
    GoodTillCancel,
    PostOnly,
    ImmediateOrCancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderPrice(pub u64); 

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: u64,
    pub client_id: u64,
    pub symbol: String,       
    pub side: Side,
    pub order_type: OrderType,       // Added OrderType
    pub time_in_force: OrderTimeInForce, // Added TimeInForce
    pub price: OrderPrice,
    pub qty: u64,              
    pub original_qty: u64,     
    pub timestamp: u64,
}

impl Order {
    pub fn new(
        order_id: u64,
        client_id: u64,
        symbol: String,
        side: Side,
        order_type: OrderType,
        time_in_force: OrderTimeInForce,
        price: OrderPrice,
        qty: u64,
        timestamp: u64,
    ) -> Self {
        Self {
            order_id,
            client_id,
            symbol,
            side,
            order_type,
            time_in_force,
            price,
            qty,
            original_qty: qty,
            timestamp,
        }
    }

    #[inline(always)]
    pub fn is_filled(&self) -> bool {
        self.qty == 0
    }

    #[inline(always)]
    pub fn fill(&mut self, fill_qty: u64) {
        self.qty = self.qty.saturating_sub(fill_qty);
    }
}