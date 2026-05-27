// src/infrastructure/grpc_gateway/router.rs
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender; 
use crate::domain::order::Order;
use crate::domain::router::EngineCommand;

// Internal system command definitions

pub struct EngineRouter {
    // Use RwLock to wrap HashMap, allowing gRPC threads to read channels concurrently with high priority
    channels: RwLock<HashMap<String, UnboundedSender<EngineCommand>>>,
}

impl EngineRouter {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    // Function to register data transmission channels for tenants
    pub fn register_tenant(&self, symbol: String, sender: UnboundedSender<EngineCommand>) -> Result<(), String> {
        let mut lock = self.channels.write().map_err(|_| "Lock poisoned")?;
        lock.insert(symbol, sender);
        Ok(())
    }

    pub fn route_order(&self, symbol: &str, order: Order) -> Result<(), String> {
        let lock = self.channels.read().map_err(|_| "Lock poisoned")?;
        if let Some(sender) = lock.get(symbol) {
            sender.send(EngineCommand::Submit(order)).map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err(format!("Pair {} not found", symbol))
        }
    }

    pub fn route_cancel(&self, symbol: &str, order_id: u64) -> Result<(), String> {
        let lock = self.channels.read().map_err(|_| "Lock poisoned")?;
        if let Some(sender) = lock.get(symbol) {
            sender.send(EngineCommand::Cancel(order_id)).map_err(|e| e.to_string())?;
            Ok(())
        } else {
            Err(format!("Pair {} not found", symbol))
        }
    }
}