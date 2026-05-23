// src/infrastructure/observability/governor.rs

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

pub struct MemoryGovernor {
    max_threshold_pct: f64,
    is_under_pressure: Arc<AtomicBool>,
}

impl MemoryGovernor {
    pub fn new(max_threshold_pct: f64) -> Self {
        Self {
            max_threshold_pct,
            is_under_pressure: Arc::new(AtomicBool::new(false)),
        }
    }

    #[inline(always)]
    pub fn is_under_pressure(&self) -> bool {
        self.is_under_pressure.load(Ordering::Relaxed)
    }

    pub fn start_monitoring(&self) {
        let is_under_pressure_clone = self.is_under_pressure.clone();
        let threshold = self.max_threshold_pct;

        tokio::spawn(async move {
            loop {
                let current_mem_pct = Self::get_system_memory_usage();

                if current_mem_pct >= threshold {
                    is_under_pressure_clone.store(true, Ordering::SeqCst);
                } else {
                    is_under_pressure_clone.store(false, Ordering::SeqCst);
                }

                sleep(Duration::from_millis(500)).await;
            }
        });
    }

    fn get_system_memory_usage() -> f64 {
        70.0 
    }
}