// src/application/mod.rs

pub mod matching_service;

use serde::{Serialize, Deserialize};
use crate::domain::order::OrderPrice;

/// ประกาศ Event ที่จะพ่นออกไปให้ Kafka (เพื่อนำไปลง DB และ Wallet)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MatchingEvent {
    OrderPlaced { order_id: u64, qty: u64 },
    OrderPartiallyFilled { order_id: u64, remaining_qty: u64 },
    TradeExecuted { maker_id: u64, taker_id: u64, price: OrderPrice, match_qty: u64 },
    OrderCompleted { order_id: u64 },
    OrderCanceled { order_id: u64 },
    CancelRejected { order_id: u64, reason: String },
}