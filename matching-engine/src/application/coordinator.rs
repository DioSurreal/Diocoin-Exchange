// src/application/coordinator.rs

use crate::application::matching_service::MatchingEngineService;
use crate::application::MatchingEvent;
use crate::domain::order::Order;
use crate::domain::traits::ArenaStore;
use crate::infrastructure::recoveries::snapshot::SnapshotManager;
use crate::infrastructure::recoveries::wal::WalManager;
use serde::{Deserialize, Serialize};

/// Command block to be recorded in the Write-Ahead Log
#[derive(Serialize, Deserialize, Clone)]
pub enum WalCommand {
    ProcessOrder(Order),
    CancelOrder { order_id: u64 },
}

#[derive(Serialize, Deserialize, Clone)]
pub struct WalEntry {
    pub seq_id: u64,
    pub command: WalCommand,
}

pub struct EngineCoordinator<A: ArenaStore<Order>> {
    pub service: MatchingEngineService<A>,
    wal_manager: WalManager,
    snapshot_manager: SnapshotManager,
    global_seq_id: u64,
    snapshot_interval: u64, // Defines how many orders occur before a snapshot is taken
}

impl<A: ArenaStore<Order> + Serialize + for<'de> Deserialize<'de>> EngineCoordinator<A> {
    pub fn new(
        service: MatchingEngineService<A>,
        wal_manager: WalManager,
        snapshot_manager: SnapshotManager,
        snapshot_interval: u64,
    ) -> Self {
        Self {
            service,
            wal_manager,
            snapshot_manager,
            global_seq_id: 0,
            snapshot_interval,
        }
    }
    /// Returns the current global sequence ID (for tests and external inspection)
    pub fn seq_id(&self) -> u64 {
        self.global_seq_id
    }

    /// Exposes the snapshot manager for test-side snapshot calls
    pub fn snapshot_manager(&mut self) -> &mut SnapshotManager {
        &mut self.snapshot_manager
    }

    /// Manually triggers a snapshot of the current engine state.
    /// This is primarily for testing and explicit snapshot requests.
    pub fn save_snapshot(&mut self) -> std::io::Result<()> {
        self.snapshot_manager.save_snapshot(
            self.global_seq_id,
            &self.service.book,
            &self.service.arena,
        )?;
        Ok(())
    }

    /// 🧭 System Recovery Function (Bootstrap & Fast Replay)
    /// Called once during startup to restore the system state from disk
    pub fn bootstrap(&mut self) -> std::io::Result<()> {
        // 1. Check and sync existing WAL files first
        self.wal_manager.initialize()?;

        // 2. Attempt to load the latest snapshot
        // Explicitly specify the Type for the Rust compiler to identify the target structure
        if let Some(state) = self
            .snapshot_manager
            .load_snapshot::<crate::domain::engine::OrderBook, A>()?
        {
            // Restore snapshot data directly into engine memory
            self.service.book = state.book;
            self.service.arena = state.arena;
            self.global_seq_id = state.last_included_wal_index;
            println!(
                "🔄 Snapshot loaded successfully. Resuming from Sequence ID: {}",
                self.global_seq_id
            );
        } else {
            println!("🆕 No snapshot found. Starting with a fresh clean state.");
        }

        // 3. Read all WAL files to prepare for Fast Replay
        let all_entries: Vec<WalEntry> = self.wal_manager.read_all_entries()?;
        let mut replay_count = 0;
        let mut dummy_events = Vec::new();

        for entry in all_entries {
            // 🌟 Important: Skip entries older than or equal to the last sequence ID in the snapshot
            if entry.seq_id <= self.global_seq_id {
                continue;
            }

            // Replay historical events by feeding them to matching logic (without re-writing to WAL)
            match entry.command {
                WalCommand::ProcessOrder(order) => {
                    self.service.process_order(order, &mut dummy_events);
                }
                WalCommand::CancelOrder { order_id } => {
                    self.service.cancel_order(order_id, &mut dummy_events);
                }
            }
            self.global_seq_id = entry.seq_id;
            replay_count += 1;
        }

        if replay_count > 0 {
            println!(
                "⚡ Fast Replay completed! Replayed {} events from WAL. Current Seq ID: {}",
                replay_count, self.global_seq_id
            );
        }

        Ok(())
    }

    /// 📥 Process a new order (Write to WAL first -> then send to Core matching)
    pub fn handle_process_order(
        &mut self,
        order: Order,
        events: &mut Vec<MatchingEvent>,
    ) -> std::io::Result<()> {
        self.global_seq_id += 1;

        let entry = WalEntry {
            seq_id: self.global_seq_id,
            command: WalCommand::ProcessOrder(order.clone()),
        };

        // Guarantee safety: Ensure disk persistence before matching
        self.wal_manager.append_entry(&entry)?;
        self.service.process_order(order, events);

        // Check interval for automatic Periodic Snapshot
        self.check_and_trigger_snapshot()?;

        Ok(())
    }

    /// 🚫 Handle order cancellation (Write to WAL first -> then send to Core)
    pub fn handle_cancel_order(
        &mut self,
        order_id: u64,
        events: &mut Vec<MatchingEvent>,
    ) -> std::io::Result<()> {
        self.global_seq_id += 1;

        let entry = WalEntry {
            seq_id: self.global_seq_id,
            command: WalCommand::CancelOrder { order_id },
        };

        self.wal_manager.append_entry(&entry)?;
        self.service.cancel_order(order_id, events);

        self.check_and_trigger_snapshot()?;

        Ok(())
    }

    /// 📸 Check interval for periodic snapshot triggering
    fn check_and_trigger_snapshot(&mut self) -> std::io::Result<()> {
        if self.global_seq_id % self.snapshot_interval == 0 {
            println!(
                "💾 Triggering automatic snapshot at Sequence ID: {}...",
                self.global_seq_id
            );
            self.snapshot_manager.save_snapshot(
                self.global_seq_id,
                &self.service.book,
                &self.service.arena,
            )?;
            println!("✅ Snapshot saved successfully!");
        }
        Ok(())
    }
}
