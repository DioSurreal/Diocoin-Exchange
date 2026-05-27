// src/main.rs

use serde::Deserialize;
use std::sync::Arc;
use tonic::transport::Server;

// Imports from your Library Crate
use matching_engine::application::matching_service::MatchingEngineService;
use matching_engine::infrastructure::memory_arena::ChainedArenaManager;
use matching_engine::infrastructure::observability::governor::MemoryGovernor;
use matching_engine::interface::kafka_consumer::PairOrderConsumer;

// Imports for gRPC Gateway and Router
use matching_engine::infrastructure::grpc_gateway::grpc_gateway::GrpcOrderGateway;
use matching_engine::infrastructure::grpc_gateway::router::EngineRouter;
use matching_engine::infrastructure::pb::order_execution_service_server::OrderExecutionServiceServer;

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 Starting Diocoin Production-Grade Matching Engine Cluster...");

    let kafka_brokers = "localhost:9092";
    let grpc_addr = "[::1]:50051".parse()?;

    // 1. Boot memory monitoring system (Memory Governor)
    let memory_governor = Arc::new(MemoryGovernor::new(85.0));
    memory_governor.start_monitoring();

    // 2. Boot Core Engine Router (for routing gRPC orders)
    let router = Arc::new(EngineRouter::new());

    // 3. Load market configuration
    let market_pairs = load_market_configuration();
    println!(
        "🧬 Initializing Cluster Components with {} active pairs...",
        market_pairs.len()
    );

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

        // 💡 [NEW] 1. Create high-visibility command channel for this pair
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::unbounded_channel();

        // 2. Register sender (tx) with Router to allow gRPC inbound orders
        router.register_tenant(pair.symbol.clone(), cmd_tx)?;

        // 3. Pass receiver (cmd_rx) to the worker thread
        match pair.tier {
            LoadTier::High => {
                println!(
                    "🚀 [Dedicated High-Path] Deploying Thread for: {}",
                    pair.symbol
                );
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(
                        &brokers,
                        &format!("group-{}", pair.symbol),
                        &pair.kafka_topic,
                    );
                    // 👈 Pass cmd_rx to worker loop here
                    consumer
                        .start_worker_loop(engine_service, governor_clone, cmd_rx)
                        .await;
                });
            }
            LoadTier::Medium | LoadTier::Low => {
                println!(
                    "🌐 [Shared-Pool Path] Registering Pair into Global Workers: {}",
                    pair.symbol
                );
                tokio::spawn(async move {
                    let consumer = PairOrderConsumer::new(
                        &brokers,
                        &format!("group-shared-{}", pair.symbol),
                        &pair.kafka_topic,
                    );
                    // 👈 Pass cmd_rx to worker loop here
                    consumer
                        .start_worker_loop(engine_service, governor_clone, cmd_rx)
                        .await;
                });
            }
        }
    }

    println!("✅ All Dynamic Kafka Ingress Loops are actively polling.");

    // 5. Assemble gRPC Inbound Gateway network interface
    // Pass Router Arc to distribute orders directly to internal pipelines
    let gateway = GrpcOrderGateway::new(router.clone());

    println!(
        "📡 High-Speed gRPC Inbound Gateway is spinning up on {}",
        grpc_addr
    );

    // 6. Start Tokio Server instead of the original Infinite Sleep
    // Block main execution here to handle external network traffic
    Server::builder()
        .add_service(OrderExecutionServiceServer::new(gateway))
        .serve(grpc_addr)
        .await?;

    Ok(())
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
