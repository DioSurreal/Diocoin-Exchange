// src/application/tenant.rs

use crate::application::coordinator::EngineCoordinator;
use crate::application::matching_service::MatchingEngineService;
use crate::application::MatchingEvent;
use crate::domain::order::Order;
use crate::infrastructure::memory_arena::ChainedArenaManager;
use crate::infrastructure::recoveries::snapshot::SnapshotManager;
use crate::infrastructure::recoveries::wal::WalManager;
use crate::application::matching_service::proto_events;

// Use standard library channels to avoid external dependency issues 
// or ensure 'crossbeam-channel' is added to Cargo.toml
use std::sync::mpsc::{channel as unbounded, Receiver, Sender};

use std::path::PathBuf;
use std::thread::{self, JoinHandle};

// 📦 Dependencies สำหรับการทำงานร่วมกับ Kafka และ Protobuf Serialization
use prost::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};

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

        // 🚀 สร้าง Async Channel ขาออกมารองรับโครงสร้าง Protobuf OutboundEvent (แก้ไข Syntax เรียบร้อย)
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<proto_events::OutboundEvent>();

        // 🎬 เตรียม Kafka Client Configuration สู่การทำงานจริง
        let kafka_brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.ms", "0") // ⚡ Set เป็น 0 สำหรับระบบ Ultra-low Latency 
            .create()
            .expect("❌ ไม่สามารถสร้าง Kafka Producer ได้");

        let symbol_topic = format!("market-data.{}", symbol.to_lowercase());
        let symbol_for_kafka = symbol.clone();

        // 🧠 รันลูปดักฟังสแตนด์บายบน OS Thread ขาออก เพื่อแปลงและส่งข้อมูลลง Kafka Topic
        std::thread::spawn(move || {
            while let Some(proto_event) = event_rx.blocking_recv() {
                let mut buffer = Vec::new();
                if proto_event.encode(&mut buffer).is_ok() {
                    let record = BaseRecord::to(&symbol_topic)
                        .payload(&buffer)
                        .key(&symbol_for_kafka);
                    
                    if let Err((e, _)) = producer.send(record) {
                        eprintln!("❌ [Kafka Export Error] พ่นอีเวนต์ไม่สำเร็จ: {:?}", e);
                    }
                }
            }
        });

        // 🎯 ส่ง event_tx เข้าไปเป็นอาร์กิวเมนต์ตัวที่ 3 ให้แก่โมดูลเรียบร้อยครับ
        let service = MatchingEngineService::new(symbol.clone(), arena, event_tx);
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
                    
                    // ⏱️ เริ่มสตาร์ทนาฬิกาจับเวลาความเร็วระดับไมโครวินาที
                    let start_time = std::time::Instant::now();
                    
                    // Route directly into our Phase 3 execution core
                    match self.coordinator.handle_process_order(order, &mut events) {
                        Ok(_) => {
                            // 📊 Fluent API: แยก .record() และ .increment() ออกมาต่อท้ายตามกฏของเวอร์ชันใหม่
                            let duration = start_time.elapsed().as_secs_f64();
                            metrics::histogram!(
                                "diocoin_matching_engine_process_duration_seconds", 
                                "symbol" => self.symbol.clone()
                            ).record(duration);

                            metrics::counter!(
                                "diocoin_matching_engine_orders_processed_total", 
                                "symbol" => self.symbol.clone()
                            ).increment(1);

                            if !events.is_empty() {
                                // Dispatch domain events cleanly via our application port
                                self.dispatcher.dispatch(&self.symbol, &events);
                            }
                        }
                        Err(e) => {
                            // 📊 แทร็กเคสออร์เดอร์ที่เกิด Error ด้วยระเบียบวิธีสากลแบบ Fluent API
                            metrics::counter!(
                                "diocoin_matching_engine_order_errors_total", 
                                "symbol" => self.symbol.clone()
                            ).increment(1);
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