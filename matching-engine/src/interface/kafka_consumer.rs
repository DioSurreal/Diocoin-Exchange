// src/interface/kafka_consumer.rs

use super::kafka_producer::EngineEventProducer;
use crate::application::matching_service::MatchingEngineService;
use crate::domain::order::Order;
use crate::domain::traits::ArenaStore;
use crate::domain::router::EngineCommand; // 💡 Include gRPC command Enums for implementation
use crate::infrastructure::observability::governor::MemoryGovernor;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver; // 💡 Added for receiving streams from In-Memory gRPC pipes

pub struct PairOrderConsumer {
    consumer: StreamConsumer,
    brokers: String,
    topic: String,
}

impl PairOrderConsumer {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Consumer creation failed");

        Self {
            consumer,
            brokers: brokers.to_string(),
            topic: topic.to_string(),
        }
    }

    pub async fn start_worker_loop<A: ArenaStore<Order> + Send + 'static>(
        self,
        mut engine_service: MatchingEngineService<A>, // 💡 Remove underscore to utilize the engine service
        governor: Arc<MemoryGovernor>,
        mut grpc_rx: UnboundedReceiver<EngineCommand>, // 💡 Open high-speed signaling channel for gRPC
    ) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic");

        let event_producer = EngineEventProducer::new(&self.brokers);

        println!("Matching Engine Worker started for topic: {}", self.topic);

        // ✅ [Z-ALLOC] 1. Create one large Buffer "outside the loop"
        let mut event_buffer = Vec::with_capacity(1000);

        loop {
            // 🛑 [BACKPRESSURE] Detect memory pressure at the loop head to protect the engine from all ingress channels
            if governor.is_under_pressure() {
                eprintln!(
                    "CRITICAL: Memory threshold exceeded! Triggering backpressure for {}",
                    self.topic
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            // ⚡ [DUAL-INGRESS EVENT LOOP] Organize switching queues to prevent data collisions
            tokio::select! {
                // 🟢 gRPC Ingress channel (direct local commands)
                Some(command) = grpc_rx.recv() => {
                    // ✅ [Z-ALLOC] 2. Clear existing buffer (does not return memory to OS)
                    event_buffer.clear();

                    match command {
                        EngineCommand::Submit(order) => {
                            engine_service.process_order(order, &mut event_buffer);
                        }
                        EngineCommand::Cancel(order_id) => {
                            // Use the actual Cancel method name in your MatchingEngineService
                            engine_service.cancel_order(order_id, &mut event_buffer); 
                        }
                    }

                    // ✅ [Z-ALLOC] 4. Emit via Borrowed Slice for atomic event update
                    event_producer.emit_events(&event_buffer).await;
                }

                // 🔵 Kafka Ingress channel (main streaming queue)
                kafka_msg = self.consumer.recv() => {
                    match kafka_msg {
                        Err(e) => eprintln!("Kafka error: {}", e),
                        Ok(borrowed_message) => {
                            let payload = match borrowed_message.payload_view::<str>() {
                                Some(Ok(s)) => s,
                                _ => continue,
                            };

                            let order: Order = match serde_json::from_str(payload) {
                                Ok(o) => o,
                                Err(_) => continue,
                            };

                            // ✅ [Z-ALLOC] 2. Clear existing buffer
                            event_buffer.clear();

                            // ✅ [Z-ALLOC] 3. Pass buffer pointer to core engine to reuse memory
                            engine_service.process_order(order, &mut event_buffer);

                            // ✅ [Z-ALLOC] 4. Emit events to destination without new allocations
                            event_producer.emit_events(&event_buffer).await;
                        }
                    }
                }
            }
        }
    }
}