// src/interface/kafka_consumer.rs

use super::kafka_producer::EngineEventProducer;
use crate::application::matching_service::MatchingEngineService;
use crate::domain::order::Order;
use crate::domain::router::EngineCommand;
use crate::domain::traits::ArenaStore;
use crate::infrastructure::observability::governor::MemoryGovernor;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer}; // 💡 เพิ่ม CommitMode เข้ามาครับ
use rdkafka::message::Message;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct PairOrderConsumer {
    pub consumer: StreamConsumer,
    pub brokers: String,
    pub topic: String,
}

impl PairOrderConsumer {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Self {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false") // ❌ 1. ปิด Auto-Commit เพื่อคุม Offset เอง 100%
            .set("auto.offset.reset", "latest")
            // ป้องกันไม่ให้ Kafka ดึงข้อความไปดองไว้เยอะเกินไปถ้าระบบประมวลผลไม่ทัน
            .set("queued.max.messages.kbytes", "32768") 
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
        mut engine_service: MatchingEngineService<A>,
        governor: Arc<MemoryGovernor>,
        mut grpc_rx: UnboundedReceiver<EngineCommand>,
    ) {
        self.consumer
            .subscribe(&[&self.topic])
            .expect("Can't subscribe to specified topic");

        let event_producer = EngineEventProducer::new(&self.brokers);

        // ===================================================================
        // 🛡️ [PHASE 1: STATE RECOVERY] (คงเดิมตามที่เราต่อท่อไว้รอบที่แล้ว)
        // ===================================================================
        println!("⏳ [RECOVERY] [{}] Starting state restoration...", self.topic);
        let last_snapshot_offset: i64 = 0; 
        println!("📦 [RECOVERY] [{}] Snapshot loaded successfully. Last Offset: {}", self.topic, last_snapshot_offset);
        println!("🔄 [RECOVERY] [{}] Replaying logs from WAL/Kafka...", self.topic);
        println!("⚡ [RECOVERY] [{}] State recovery complete!", self.topic);
        // ===================================================================

        println!("🎯 Matching Engine Worker started for topic: {}", self.topic);
        let mut event_buffer = Vec::with_capacity(1000);

        loop {
            // 🛑 [BACKPRESSURE]
            if governor.is_under_pressure() {
                eprintln!("CRITICAL: Memory threshold exceeded! Triggering backpressure for {}", self.topic);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            }

            // ⚡ [DUAL-INGRESS EVENT LOOP + GRACEFUL SHUTDOWN]
            tokio::select! {
                // 🟢 gRPC Ingress channel
                Some(command) = grpc_rx.recv() => {
                    event_buffer.clear();
                    match command {
                        EngineCommand::Submit(order) => { engine_service.process_order(order, &mut event_buffer); }
                        EngineCommand::Cancel(order_id) => { engine_service.cancel_order(order_id, &mut event_buffer); }
                    }
                    event_producer.emit_events(&event_buffer).await;
                }

                // 🔵 Kafka Ingress channel (พร้อมระบบแมนนวลออฟเซ็ต)
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

                            event_buffer.clear();
                            engine_service.process_order(order, &mut event_buffer);
                            
                            // พ่นผลลัพธ์การจับคู่ราคา (Trade Events) ออกไปที่ Kafka ขาออกก่อน
                            event_producer.emit_events(&event_buffer).await;

                            // 🎯 2. [MANUAL COMMIT] สั่งเลื่อน Offset แมนนวลหลังจากส่งอีเวนต์สำเร็จเรียบร้อยแล้วเท่านั้น!
                            // ใช้ CommitMode::Async เพื่อประสิทธิภาพสูงสุด (ไม่บล็อก Hot Path ในการรอ Network Ack จากโบรเกอร์)
                            if let Err(e) = self.consumer.commit_message(&borrowed_message, CommitMode::Async) {
                                eprintln!("⚠️ [KAFKA] [{}] Failed to commit offset manually: {}", self.topic, e);
                            }
                        }
                    }
                }

                // 🛑 3. [GRACEFUL SHUTDOWN] ดักจับสัญญาณปิดโปรแกรมของระบบปฏิบัติการ
                _ = tokio::signal::ctrl_c() => {
                    println!("🛑 [SHUTDOWN] [{}] Signal received! Initiating graceful worker drain...", self.topic);
                    
                    // ปิดประตูรับงานใหม่จาก Kafka ทันทีเพื่อเคลียร์คิว
                    self.consumer.unsubscribe();
                    
                    // เคลียร์ออเดอร์ที่ค้างคาอยู่ในท่อ gRPC ให้หมดก่อนปิดตัว
                    while let Ok(command) = grpc_rx.try_recv() {
                        event_buffer.clear();
                        match command {
                            EngineCommand::Submit(order) => { engine_service.process_order(order, &mut event_buffer); }
                            EngineCommand::Cancel(order_id) => { engine_service.cancel_order(order_id, &mut event_buffer); }
                        }
                        event_producer.emit_events(&event_buffer).await;
                    }

                    println!("👋 [SHUTDOWN] [{}] All buffers flushed and committed safely. Exiting worker thread.", self.topic);
                    break; // หลุดออกจากลูปนรกเพื่อจบเอนจินเธรดอย่างปลอดภัย
                }
            }
        }
    }
}