// src/domain/router.rs
use crate::domain::order::Order;

// Business intent, resides purely in the Domain.
pub enum EngineCommand {
    Submit(Order),
    Cancel(u64),
}
