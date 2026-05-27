// src/interface/kafka_producer.rs

use crate::application::MatchingEvent;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub struct EngineEventProducer {
    producer: FutureProducer,
}

impl EngineEventProducer {
    pub fn new(brokers: &str) -> Self {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("acks", "1")
            .create()
            .expect("Producer creation failed");

        Self { producer }
    }

    pub async fn emit_events(&self, events: &[MatchingEvent]) {
        if events.is_empty() {
            return;
        }
        for event in events {
            let payload = serde_json::to_string(&event).unwrap();

            let topic = match &event {
                MatchingEvent::TradeExecuted { .. } => "market.trading-history", 
                MatchingEvent::OrderCompleted { .. } | MatchingEvent::OrderCanceled { .. } => {
                    "order.transactions-update"
                } 
                _ => "order.audit-log",
            };

            let record = FutureRecord::to(topic).payload(&payload).key("");

            // ⚡ Emit to Kafka queue immediately
            if let Err((err, _)) = self.producer.send(record, Duration::from_secs(0)).await {
                eprintln!("Failed to stream event to Kafka topic {}: {:?}", topic, err);
            }
        }
    }
}