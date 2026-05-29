// src/domain/order.rs

use serde::{Serialize, Deserialize};


pub const SCALE_FACTOR: u128 = 100_000_000;



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

    pub fn calculate_execution_value(&self, executed_qty: u64) -> u64 {
        // ดึงราคาข้างใน Tuple Struct (OrderPrice) ออกมาแปลงเป็น u128
        let price_128 = self.price.0 as u128;
        let qty_128 = executed_qty as u128;
        
        // คูณขยายถัง ขจัดปัญหา Integer Overflow 
        let total_inflated_value = price_128 * qty_128;
        
        // หารตบสเกลกลับมาให้อยู่ในระดับทศนิยม 8 ตำแหน่งเท่าเดิม
        let final_value_128 = total_inflated_value / SCALE_FACTOR;
        
        // Downcast กลับเป็น u64 เพื่อนำไปใช้ทำ Clearing ขาออก
        final_value_128 as u64
    }
}