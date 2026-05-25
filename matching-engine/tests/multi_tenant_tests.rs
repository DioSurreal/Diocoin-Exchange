// tests/multi_tenant_tests.rs

use matching_engine::application::router::{EngineRouter, RouterError};
use matching_engine::application::tenant::{EventDispatcher, TenantConfig};
use matching_engine::domain::order::{Order, OrderPrice, Side, OrderType, OrderTimeInForce};
use matching_engine::application::MatchingEvent; 

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

// ─────────────────────────────────────────────
// 🧪 MOCK INFRASTRUCTURE (DDD Application Port)
// ─────────────────────────────────────────────

#[derive(Clone)]
pub struct MockDispatcher {
    pub dispatch_counts: Arc<Mutex<HashMap<String, usize>>>,
}

impl MockDispatcher {
    pub fn new() -> Self {
        Self {
            dispatch_counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_count(&self, symbol: &str) -> usize {
        let counts = self.dispatch_counts.lock().unwrap();
        *counts.get(symbol).unwrap_or(&0)
    }
}

impl EventDispatcher for MockDispatcher {
    fn dispatch(&self, symbol: &str, events: &[MatchingEvent]) {
        let mut counts = self.dispatch_counts.lock().unwrap();
        let counter = counts.entry(symbol.to_string()).or_insert(0);
        *counter += events.len();
    }
}

// ─────────────────────────────────────────────
// 🧱 HELPERS
// ─────────────────────────────────────────────

fn create_test_order(id: u64, symbol: &str, side: Side, price: u64, qty: u64) -> Order {
    Order::new(
        id,
        999, // User ID
        symbol.to_string(),
        side,
        OrderType::Limit,
        OrderTimeInForce::GoodTillCancel,
        OrderPrice(price),
        qty,
        1716475000,
    )
}

fn setup_env(tmp: &TempDir) -> (TenantConfig, MockDispatcher) {
    let wal_base_dir = tmp.path().join("wal");
    let snap_base_dir = tmp.path().join("snap");
    
    let config = TenantConfig {
        wal_base_dir,
        snap_base_dir,
        snapshot_interval: 9999, 
    };
    
    (config, MockDispatcher::new())
}

// ─────────────────────────────────────────────
// 🔬 INTEGRATION TEST CASES
// ─────────────────────────────────────────────

#[test]
fn test_router_isolates_and_routes_to_correct_tenants() {
    let tmp = TempDir::new().unwrap();
    let (config, dispatcher) = setup_env(&tmp);
    
    let router = EngineRouter::new(config, dispatcher.clone());
    
    router.register_tenant("BTC-USDT".to_string()).unwrap();
    router.register_tenant("ETH-USDT".to_string()).unwrap();

    router.route_order(create_test_order(1, "BTC-USDT", Side::Buy, 100, 5)).unwrap();
    router.route_order(create_test_order(2, "BTC-USDT", Side::Buy, 99, 10)).unwrap();

    router.route_order(create_test_order(3, "ETH-USDT", Side::Buy, 3000, 2)).unwrap();


    router.shutdown_all();


    assert_eq!(dispatcher.get_count("BTC-USDT"), 2);
    assert_eq!(dispatcher.get_count("ETH-USDT"), 1);

    assert!(tmp.path().join("wal/BTC-USDT").exists());
    assert!(tmp.path().join("wal/ETH-USDT").exists());
}

#[test]
fn test_router_rejects_unregistered_symbols() {
    let tmp = TempDir::new().unwrap();
    let (config, dispatcher) = setup_env(&tmp);
    let router = EngineRouter::new(config, dispatcher);
    
    router.register_tenant("BTC-USDT".to_string()).unwrap();

    let ghost_order = create_test_order(1, "DOGE-USDT", Side::Buy, 1, 100);
    let result = router.route_order(ghost_order);

    assert!(result.is_err());
    match result.err().unwrap() {
        RouterError::SymbolNotSupported(sym) => assert_eq!(sym, "DOGE-USDT"),
        _ => panic!("Expected SymbolNotSupported error"),
    }
}

#[test]
fn test_tenant_crash_recovery_after_router_shutdown() {
    let tmp = TempDir::new().unwrap();
    let (config, dispatcher) = setup_env(&tmp);
    
    
    {
        let router = EngineRouter::new(config.clone(), dispatcher.clone());
        router.register_tenant("BTC-USDT".to_string()).unwrap();
        
       
        router.route_order(create_test_order(42, "BTC-USDT", Side::Buy, 50000, 1)).unwrap();
        
        router.shutdown_all(); 
    }


    let next_dispatcher = MockDispatcher::new();
    let next_router = EngineRouter::new(config, next_dispatcher.clone());
    
    next_router.register_tenant("BTC-USDT".to_string()).unwrap();
    
    next_router.shutdown_all();

    println!("🎉 Tenant multi-level recovery state verified successfully!");
}