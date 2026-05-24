// src/main.rs

use std::sync::Arc;
use serde::Deserialize;

// Import from our own Library Crate
use matching_engine::application::matching_service::MatchingEngineService;
use matching_engine::infrastructure::memory_arena::ChainedArenaManager;
use matching_engine::infrastructure::observability::governor::MemoryGovernor;
use matching_engine::interface::kafka_consumer::PairOrderConsumer;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub enum LoadTier {
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PairConfig {
    pub symbol: String,
    pub tier: LoadTier,
    pub kafka_topic: String,
}

impl PairConfig {
    pub fn new(symbol: &str, tier: LoadTier, kafka_topic: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
            tier,
            kafka_topic: kafka_topic.to_string(),
        }
    }
}

#[tokio::main]
async fn main() {
    let kafka_brokers = "localhost:9092";
    
    let memory_governor = Arc::new(MemoryGovernor::new(85.0));
    memory_governor.start_monitoring();

    let market_pairs = load_market_configuration();

    println!("Initializing Matching Engine Cluster with {} active pairs...", market_pairs.len());

    for pair in market_pairs {
        let governor_clone = memory_governor.clone();
        let brokers = kafka_brokers.to_string();
        
        let arena_block_size = match pair.tier {
            LoadTier::High => 500_000,
            LoadTier::Medium => 100_000,
            LoadTier::Low => 10_000,
        };

        let arena = ChainedArenaManager::new(arena_block_size);
        let engine_service = MatchingEngineService::new(pair.symbol.clone(), arena);

        match pair.tier {
            LoadTier::High => {
                println!("🧬 [Dedicated Worker] Deploying High-Performance Thread for: {}", pair.symbol);
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(&brokers, &format!("group-{}", pair.symbol), &pair.kafka_topic);
                    consumer.start_worker_loop(engine_service, governor_clone).await;
                });
            }
            LoadTier::Medium | LoadTier::Low => {
                println!("🌐 [Shared Pool Worker] Registering Pair into Global Thread Pool: {}", pair.symbol);
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(&brokers, &format!("group-shared-{}", pair.symbol), &pair.kafka_topic);
                    consumer.start_worker_loop(engine_service, governor_clone).await;
                });
            }
        }
    }

    println!("🚀 All Dynamic Matching Engine Workers are successfully deployed!");
    
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

fn load_market_configuration() -> Vec<PairConfig> {
    vec![
        PairConfig::new("BTCUSDT", LoadTier::High, "inbound.orders.btc"),
        PairConfig::new("ETHUSDT", LoadTier::High, "inbound.orders.eth"),
        PairConfig::new("SOLUSDT", LoadTier::Medium, "inbound.orders.sol"),
        PairConfig::new("DOGEUSDT", LoadTier::Medium, "inbound.orders.doge"),
        PairConfig::new("SHIBUSDT", LoadTier::Low, "inbound.orders.shib"),
        PairConfig::new("GALAUSDT", LoadTier::Low, "inbound.orders.gala"),
    ]
}