// src/interface/kafka_producer.rs

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use crate::application::MatchingEvent;

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

    pub async fn emit_events(&self, events: Vec<MatchingEvent>) {
        for event in events {
            let payload = serde_json::to_string(&event).unwrap();
            
            let topic = match &event {
                MatchingEvent::TradeExecuted { .. } => "market.trading-history", // ส่งไปวาดกราฟ/เก็บบันทึก
                MatchingEvent::OrderCompleted { .. } | MatchingEvent::OrderCanceled { .. } => "order.transactions-update", // ส่งไปแก้ Status ใน DB
                _ => "order.audit-log", 
            };

            let record = FutureRecord::to(topic)
                .payload(&payload)
                .key(""); 

            if let Err((err, _)) = self.producer.send(record, Duration::from_secs(0)).await {
                eprintln!("Failed to stream event to Kafka topic {}: {:?}", topic, err);
            }
        }
    }
}