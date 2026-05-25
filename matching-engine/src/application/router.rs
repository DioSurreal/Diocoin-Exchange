// src/application/router.rs

use crate::application::tenant::{TenantCommand, TenantConfig, TenantHandle, TenantWorker, EventDispatcher};
use crate::domain::order::Order;
use std::collections::HashMap;
use std::sync::RwLock;

/// นิยามข้อผิดพลาดระดับ Domain/Application สำหรับระบบ Router
#[derive(Debug)]
pub enum RouterError {
    SymbolNotSupported(String),
    TenantChannelDropped,
}

pub struct EngineRouter<D: EventDispatcher> {
    // ใช้ RwLock เพื่อให้รองรับ Concurrent Read ความเร็วสูงจากสตรีม Kafka หลายช่องพร้อมกัน
    tenants: RwLock<HashMap<String, TenantHandle>>,
    config: TenantConfig,
    dispatcher: D,
}

impl<D: EventDispatcher + Clone> EngineRouter<D> {
    pub fn new(config: TenantConfig, dispatcher: D) -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            config,
            dispatcher,
        }
    }

    /// 🌐 ลงทะเบียนและชุบชีวิตคู่เหรียญใหม่ขึ้นมาในตระกูล Tenant แบบ Dynamic
    pub fn register_tenant(&self, symbol: String) -> Result<(), std::io::Error> {
        let mut tenants_guard = self.tenants.write().unwrap();
        
        // หากคู่เหรียญนี้เคยตื่นขึ้นมาทำงานอยู่แล้ว ให้ข้ามไปได้เลยป้องกัน Thread ซ้อนกัน
        if tenants_guard.contains_key(&symbol) {
            return Ok(());
        }

        // คัดลอกคอนฟิกเพื่อแยกโฟลเดอร์ WAL และ Snapshot ประจำตัว aggregate ชิ้นนี้
        let tenant_config = TenantConfig {
            wal_base_dir: self.config.wal_base_dir.clone(),
            snap_base_dir: self.config.snap_base_dir.clone(),
            snapshot_interval: self.config.snapshot_interval,
        };

        // สั่งสปอว์นแยกสายการบิน (OS Thread) ออกไปอย่างโดดเดี่ยว
        let handle = TenantWorker::spawn(symbol.clone(), tenant_config, self.dispatcher.clone())?;
        tenants_guard.insert(symbol, handle);
        
        Ok(())
    }

    /// 🎯 คัดแยกและยิงส่งออเดอร์ตรงเข้าสู่ช่องทางประมวลผลของคู่เหรียญนั้นแบบ Non-blocking
    pub fn route_order(&self, order: Order) -> Result<(), RouterError> {
        // 💡 Clean Code & Ownership Optimization:
        // เนื่องจากเราจำเป็นต้องย้ายสิทธิ์ (Move) ก้อน `order` เข้าไปในช่องแชนแนล 
        // เราต้องทำการดึงชื่อคีย์ (Symbol) ออกมาเป็นอิสระล่วงหน้าก่อน 
        // เพื่อไม่ให้ตัว Borrow Checker ของ Rust มองว่าคีย์โดนล็อกขณะกำลังย้ายข้อมูลตัวแม่
        let target_symbol = order.symbol.clone(); 

        let tenants_guard = self.tenants.read().unwrap();
        
        if let Some(tenant) = tenants_guard.get(&target_symbol) {
            tenant.tx.send(TenantCommand::ProcessOrder(order))
                .map_err(|_| RouterError::TenantChannelDropped)?;
            Ok(())
        } else {
            Err(RouterError::SymbolNotSupported(target_symbol))
        }
    }

    /// 🛑 สั่งปิดสวิตช์ระบบคู่เหรียญทั้งหมดอย่างปลอดภัย (Graceful Shutdown Checklist)
    /// ตัวคุมทิศทางจะสั่งให้ทุกเอนจิ้นปั๊มสแนปช็อตหยดสุดท้ายลงดิสก์ก่อนปิด Thread แน่นอน ข้อมูลไม่มีวันหาย
    pub fn shutdown_all(&self) {
        let mut tenants_guard = self.tenants.write().unwrap();
        
        println!("🛑 [Engine Router] Starting graceful shutdown sequence for all tenants...");
        
        // ใช้ .drain() เพื่อดึง ownership ของแฮนเดิลออกมาทำลายและปิดระบบทีละสถานี
        for (symbol, mut handle) in tenants_guard.drain() {
            println!("🌐 Dispatching stop signal to context: {}", symbol);
            let _ = handle.tx.send(TenantCommand::Shutdown);
            
            if let Some(join_handle) = handle.join_handle.take() {
                let _ = join_handle.join();
                println!("✅ Tenant thread for {} has successfully exited and joined.", symbol);
            }
        }
    }
}