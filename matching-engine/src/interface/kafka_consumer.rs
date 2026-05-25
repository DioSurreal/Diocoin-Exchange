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
        _engine_service: MatchingEngineService<A>,
        governor: Arc<MemoryGovernor>,
    ) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic");

        let event_producer = EngineEventProducer::new(&self.brokers);

        println!("Matching Engine Worker started for topic: {}", self.topic);

        // ✅ [Z-ALLOC] 1. Create one large Buffer "outside the loop"
        // only once when the worker starts.
        // Pre-allocate around 1000 slots in advance
        // (adjust depending on the expected maximum matches per order)
        let mut event_buffer = Vec::with_capacity(1000);

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
                    // ✅ [Z-ALLOC] 2. Clear old data from the previous order
                    // without returning the memory back to the OS
                    event_buffer.clear();

                    // ✅ [Z-ALLOC] 3. Pass a reference of the external Buffer
                    // to the Engine so it can fill new data into it instead                    engine_service.process_order(order, &mut event_buffer);

                    // ✅ [Z-ALLOC] 4. Send as a Borrowed Slice (add `&`)
                    // so values can continue being passed without allocation
                    // (Note: don't forget to update
                    // src/interface/kafka_producer.rs
                    // to accept &[MatchingEvent] as well)
                    event_producer.emit_events(&event_buffer).await;
                }
            }
        }
    }
}
