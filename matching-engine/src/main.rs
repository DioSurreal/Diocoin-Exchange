// src/main.rs

pub mod domain;
pub mod application;
pub mod infrastructure;
pub mod interface;

use std::sync::Arc;
use serde::Deserialize;
use crate::application::matching_service::MatchingEngineService;
use crate::infrastructure::memory_arena::ChainedArenaManager;
use crate::infrastructure::observability::governor::MemoryGovernor;
use crate::interface::kafka_consumer::PairOrderConsumer;

/// Defines resource demand tiers for each trading pair.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum LoadTier {
    High,   // Very high volume -> dedicated thread + large memory blocks
    Medium, // Mid volume -> shared pool + medium memory blocks
    Low,    // Low volume -> shared pool + small memory blocks
}

/// Configuration model for dynamic pair setup.
#[derive(Debug, Clone, Deserialize)]
pub struct PairConfig {
    pub symbol: String,
    pub tier: LoadTier,
    pub kafka_topic: String,
}

#[tokio::main]
async fn main() {
    let kafka_brokers = "localhost:9092";

    // 1. Start the main memory monitor with a warning threshold of 85%.
    let memory_governor = Arc::new(MemoryGovernor::new(85.0));
    memory_governor.start_monitoring();

    // 2. Load pair config (in production this may come from JSON, env, or database).
    let market_pairs = load_market_configuration();

    println!("Initializing Matching Engine Cluster with {} active pairs...", market_pairs.len());

    // 3. Dynamically assign thread and memory resources by load tier.
    for pair in market_pairs {
        let governor_clone = memory_governor.clone();
        let brokers = kafka_brokers.to_string();

        // Tune chained-arena block size per tier (dynamic memory tuning).
        let arena_block_size = match pair.tier {
            LoadTier::High => 500_000,   // Max performance, fewer OS allocations
            LoadTier::Medium => 100_000, // Balanced default size
            LoadTier::Low => 10_000,     // Memory-efficient for low-activity pairs
        };

        // Create pair-specific engine and memory arena.
        let arena = ChainedArenaManager::new(arena_block_size);
        let engine_service = MatchingEngineService::new(pair.symbol.clone(), arena);

        // 4. Apply dynamic thread partitioning.
        match pair.tier {
            LoadTier::High => {
                // [VIP Thread] Spin up an isolated dedicated worker.
                println!("🧬 [Dedicated Worker] Deploying High-Performance Thread for: {}", pair.symbol);
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(&brokers, &format!("group-{}", pair.symbol), &pair.kafka_topic);
                    consumer.start_worker_loop(engine_service, governor_clone).await;
                });
            }
            LoadTier::Medium | LoadTier::Low => {
                // [Shared Worker Pool] Run regular pairs on Tokio's async task pool.
                // This keeps CPU usage efficient under bursty order flow.
                println!("🌐 [Shared Pool Worker] Registering Pair into Global Thread Pool: {}", pair.symbol);
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(&brokers, &format!("group-shared-{}", pair.symbol), &pair.kafka_topic);
                    consumer.start_worker_loop(engine_service, governor_clone).await;
                });
            }
        }
    }

    println!("🚀 All Dynamic Matching Engine Workers are successfully deployed!");

    // Keep the main thread alive so the service does not exit.
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

/// Mock function for loading dynamic market configuration.
fn load_market_configuration() -> Vec<PairConfig> {
    // In production this would come from a database, so pairs can be added or
    // removed by updating data and restarting the service (no code recompilation).
    vec![
        PairConfig { symbol: "BTCUSDT".to_string(), tier: LoadTier::High, kafka_topic: "inbound.orders.btc".to_string() },
        PairConfig { symbol: "ETHUSDT".to_string(), tier: LoadTier::High, kafka_topic: "inbound.orders.eth".to_string() },
        PairConfig { symbol: "SOLUSDT".to_string(), tier: LoadTier::Medium, kafka_topic: "inbound.orders.sol".to_string() },
        PairConfig { symbol: "DOGEUSDT".to_string(), tier: LoadTier::Medium, kafka_topic: "inbound.orders.doge".to_string() },
        PairConfig { symbol: "SHIBUSDT".to_string(), tier: LoadTier::Low, kafka_topic: "inbound.orders.shib".to_string() },
        PairConfig { symbol: "GALAUSDT".to_string(), tier: LoadTier::Low, kafka_topic: "inbound.orders.gala".to_string() },
    ]
}
