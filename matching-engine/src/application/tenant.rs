// src/application/tenant.rs

use crate::application::coordinator::EngineCoordinator;
use crate::application::matching_service::MatchingEngineService;
use crate::application::MatchingEvent;
use crate::domain::order::Order;
use crate::infrastructure::memory_arena::ChainedArenaManager;
use crate::infrastructure::recoveries::snapshot::SnapshotManager;
use crate::infrastructure::recoveries::wal::WalManager;

// Use standard library channels to avoid external dependency issues 
// or ensure 'crossbeam-channel' is added to Cargo.toml
use std::sync::mpsc::{channel as unbounded, Receiver, Sender};

use std::path::PathBuf;
use std::thread::{self, JoinHandle};

/// Application port (DDD Interface) for broadcasting domain events to the outside world (e.g., Kafka).
pub trait EventDispatcher: Send + Sync + 'static {
    fn dispatch(&self, symbol: &str, events: &[MatchingEvent]);
}

/// Configuration required to instantiate a isolated Tenant Bounded Context.
#[derive(Clone)]
pub struct TenantConfig {
    pub wal_base_dir: PathBuf,
    pub snap_base_dir: PathBuf,
    pub snapshot_interval: u64,
}

pub enum TenantCommand {
    ProcessOrder(Order),
    TriggerSnapshot,
    Shutdown,
}

pub struct TenantHandle {
    pub symbol: String,
    pub tx: Sender<TenantCommand>,
    pub join_handle: Option<JoinHandle<()>>,
}

pub struct TenantWorker<D: EventDispatcher> {
    pub symbol: String,
    pub coordinator: EngineCoordinator<ChainedArenaManager<Order>>,
    pub rx: Receiver<TenantCommand>,
    pub dispatcher: D,
}

impl<D: EventDispatcher> TenantWorker<D> {
    /// Spawns a sovereign domain engine thread, isolating both its memory arena and storage paths.
    pub fn spawn(
        symbol: String,
        config: TenantConfig,
        dispatcher: D,
    ) -> Result<TenantHandle, std::io::Error> {
        let (tx, rx) = unbounded::<TenantCommand>();

        // Clean Code / DDD: Strict isolation of infrastructure per symbol aggregate
        let tenant_wal_dir = config.wal_base_dir.join(&symbol);
        let tenant_snap_dir = config.snap_base_dir.join(&symbol);
        let snapshot_filename = format!("{}.snapshot", symbol.to_lowercase());

        std::fs::create_dir_all(&tenant_wal_dir)?;
        std::fs::create_dir_all(&tenant_snap_dir)?;

        // Re-using our exact domain structures from Phase 1-3
        let arena = ChainedArenaManager::new(256);
        let service = MatchingEngineService::new(symbol.clone(), arena);
        let wal = WalManager::new(&tenant_wal_dir, 64 * 1024 * 1024);
        let snap = SnapshotManager::new(&tenant_snap_dir, &snapshot_filename);
        
        let coordinator = EngineCoordinator::new(service, wal, snap, config.snapshot_interval);

        let worker = TenantWorker {
            symbol: symbol.clone(),
            coordinator,
            rx,
            dispatcher,
        };

        let join_handle = thread::spawn(move || {
            worker.run_loop();
        });

        Ok(TenantHandle {
            symbol,
            tx,
            join_handle: Some(join_handle),
        })
    }

    fn run_loop(mut self) {
        // Crash Recovery Pipeline execution on startup
        if let Err(e) = self.coordinator.bootstrap() {
            eprintln!("❌ [Tenant Context {}] Recovery failed critical abort: {:?}", self.symbol, e);
            return;
        }

        let mut events: Vec<MatchingEvent> = Vec::new();

        while let Ok(command) = self.rx.recv() {
            match command {
                TenantCommand::ProcessOrder(order) => {
                    events.clear();
                    
                    // Route directly into our Phase 3 execution core
                    match self.coordinator.handle_process_order(order, &mut events) {
                        Ok(_) => {
                            if !events.is_empty() {
                                // Dispatch domain events cleanly via our application port
                                self.dispatcher.dispatch(&self.symbol, &events);
                            }
                        }
                        Err(e) => {
                            eprintln!("⚠️ [Tenant Context {}] Order processing failed: {:?}", self.symbol, e);
                        }
                    }
                }
                TenantCommand::TriggerSnapshot => {
                    let _ = self.coordinator.save_snapshot();
                }
                TenantCommand::Shutdown => {
                    let _ = self.coordinator.save_snapshot(); // Graceful checkpointing
                    break;
                }
            }
        }
    }
}