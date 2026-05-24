// src/interface/kafka_consumer.rs

use super::kafka_producer::EngineEventProducer;
use crate::application::matching_service::MatchingEngineService;
use crate::domain::order::Order;
use crate::domain::traits::ArenaStore;
use crate::infrastructure::observability::governor::MemoryGovernor;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::Arc;

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
            consumer: consumer,
            brokers: brokers.to_string(),
            topic: topic.to_string(),
        }
    }

    pub async fn start_worker_loop<A: ArenaStore<Order> + Send + 'static>(
        self,
        mut engine_service: MatchingEngineService<A>,
        governor: Arc<MemoryGovernor>,
    ) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic");

        let event_producer = EngineEventProducer::new(&self.brokers);

        println!("Matching Engine Worker started for topic: {}", self.topic);

        loop {
            match self.consumer.recv().await {
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

                    if governor.is_under_pressure() {
                        eprintln!(
                            "CRITICAL: Memory threshold exceeded! Triggering backpressure for {}",
                            order.symbol
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                    // 1. Create buffer first
                    let mut events = Vec::new();

                    // 2. Pass buffer to the Engine (events will be updated)
                    engine_service.process_order(order, &mut events);

                    // 3. Send events to Producer for Kafka emission
                    event_producer.emit_events(events).await;
                }
            }
        }
    }
}
