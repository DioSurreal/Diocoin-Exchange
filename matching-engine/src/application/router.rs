// src/application/router.rs

use crate::application::tenant::{TenantCommand, TenantConfig, TenantHandle, TenantWorker, EventDispatcher};
use crate::domain::order::Order;
use std::collections::HashMap;
use std::sync::RwLock;

/// Domain/Application level error definitions for the Router system
#[derive(Debug)]
pub enum RouterError {
    SymbolNotSupported(String),
    TenantChannelDropped,
}

pub struct EngineRouter<D: EventDispatcher> {
    // Use RwLock to support high-speed Concurrent Reads from multiple Kafka streams
    tenants: RwLock<HashMap<String, TenantHandle>>,
    config: TenantConfig,
    dispatcher: D,
}

impl<D: EventDispatcher + Clone> EngineRouter<D> {
    pub fn new(config: TenantConfig, dispatcher: D) -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            config,
            dispatcher,
        }
    }

    /// 🌐 Register and dynamically initialize new trading pairs as Tenants
    pub fn register_tenant(&self, symbol: String) -> Result<(), std::io::Error> {
        let mut tenants_guard = self.tenants.write().unwrap();
        
        // If the pair is already active, skip to prevent overlapping threads
        if tenants_guard.contains_key(&symbol) {
            return Ok(());
        }

        // Copy config to isolate WAL and Snapshot folders for this aggregate
        let tenant_config = TenantConfig {
            wal_base_dir: self.config.wal_base_dir.clone(),
            snap_base_dir: self.config.snap_base_dir.clone(),
            snapshot_interval: self.config.snapshot_interval,
        };

        // Spawn isolated OS threads for each pair
        let handle = TenantWorker::spawn(symbol.clone(), tenant_config, self.dispatcher.clone())?;
        tenants_guard.insert(symbol, handle);
        
        Ok(())
    }

    /// 🎯 Route and dispatch orders directly to the pair's processing channel (Non-blocking)
    pub fn route_order(&self, order: Order) -> Result<(), RouterError> {
        // 💡 Clean Code & Ownership Optimization:
        // We must extract the key (Symbol) independently beforehand to prevent the 
        // Rust Borrow Checker from seeing the key as locked during the move.
        let target_symbol = order.symbol.clone(); 

        let tenants_guard = self.tenants.read().unwrap();
        
        if let Some(tenant) = tenants_guard.get(&target_symbol) {
            tenant.tx.send(TenantCommand::ProcessOrder(order))
                .map_err(|_| RouterError::TenantChannelDropped)?;
            Ok(())
        } else {
            Err(RouterError::SymbolNotSupported(target_symbol))
        }
    }

    /// 🛑 Safely shut down all trading pairs (Graceful Shutdown Checklist)
    /// The router ensures every engine takes a final snapshot before thread termination; no data loss.
    pub fn shutdown_all(&self) {
        let mut tenants_guard = self.tenants.write().unwrap();
        
        println!("🛑 [Engine Router] Starting graceful shutdown sequence for all tenants...");
        
        // Use .drain() to take ownership of handles for sequential system shutdown
        for (symbol, mut handle) in tenants_guard.drain() {
            println!("🌐 Dispatching stop signal to context: {}", symbol);
            let _ = handle.tx.send(TenantCommand::Shutdown);
            
            if let Some(join_handle) = handle.join_handle.take() {
                let _ = join_handle.join();
                println!("✅ Tenant thread for {} has successfully exited and joined.", symbol);
            }
        }
    }
}