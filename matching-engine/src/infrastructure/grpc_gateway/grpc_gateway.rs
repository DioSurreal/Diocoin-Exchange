// src/infrastructure/grpc_gateway/grpc_gateway.rs
use std::sync::Arc;
use tonic::{Request, Response, Status};

// 1. Pull actual Domain Models (resolving field and Enum collisions)
use crate::domain::order::{Order, Side as DomainSide, OrderType as DomainOrderType, OrderPrice, OrderTimeInForce};

// 2. Pull Protobuf Types and alias with "Pb" to avoid internal conflicts
use crate::infrastructure::pb::order_execution_service_server::OrderExecutionService;
use crate::infrastructure::pb::{
    SubmitOrderRequest, SubmitOrderResponse, 
    CancelOrderRequest, CancelOrderResponse, 
    Side as PbSide, OrderType as PbOrderType
};

// 3. Utilize Router from internal infrastructure modules
use crate::infrastructure::grpc_gateway::router::EngineRouter; 

pub struct GrpcOrderGateway {
    router: Arc<EngineRouter>,
}

impl GrpcOrderGateway {
    pub fn new(router: Arc<EngineRouter>) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl OrderExecutionService for GrpcOrderGateway {
    
    // 📥 Direct trading command submission to core engine (Submit Order Path)
    async fn submit_order(
        &self,
        request: Request<SubmitOrderRequest>,
    ) -> Result<Response<SubmitOrderResponse>, Status> {
        let req = request.into_inner();

        // Convert Side using gateway PbSide and internal DomainSide
        let domain_side = match PbSide::try_from(req.side) {
            Ok(PbSide::Buy) => DomainSide::Buy,
            Ok(PbSide::Sell) => DomainSide::Sell,
            _ => return Err(Status::invalid_argument("Invalid order side")),
        };

        // Convert OrderType by stripping prefixes per Prost compiler mechanics
        let domain_type = match PbOrderType::try_from(req.order_type) {
            Ok(PbOrderType::Limit) => DomainOrderType::Limit,
            Ok(PbOrderType::Market) => DomainOrderType::Market,
            _ => return Err(Status::invalid_argument("Invalid order type")),
        };

        // Construct order entity to match actual domain fields
        let order = Order {
            order_id: req.order_id,
            client_id: req.user_id,             // Match user_id to actual client_id field
            symbol: req.symbol.clone(),
            qty: req.quantity,                  // Rename quantity to qty per domain module
            original_qty: req.quantity,
            price: OrderPrice(req.price),       // Wrap for Type Safety to prevent precision errors
            side: domain_side,
            order_type: domain_type,
            time_in_force: OrderTimeInForce::GoodTillCancel,
            timestamp: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64,
        };

        let target_symbol = req.symbol.clone();

        // Pass data through switching board to pair-specific threads
        match self.router.route_order(&target_symbol, order) {
            Ok(_) => {
                Ok(Response::new(SubmitOrderResponse {
                    success: true,
                    message: format!("Order {} accepted for {}", req.order_id, target_symbol),
                }))
            }
            Err(e) => {
                Err(Status::not_found(format!("Trading pair {} not active: {}", target_symbol, e)))
            }
        }
    }

    // 📤 High-speed order cancellation transport (Cancel Order Path)
    async fn cancel_order(
        &self,
        request: Request<CancelOrderRequest>,
    ) -> Result<Response<CancelOrderResponse>, Status> {
        let req = request.into_inner();
        
        match self.router.route_cancel(&req.symbol, req.order_id) {
            Ok(_) => {
                Ok(Response::new(CancelOrderResponse {
                    success: true,
                    message: format!("Cancel request for order {} dispatched", req.order_id),
                }))
            }
            Err(e) => {
                Err(Status::not_found(format!("Failed to cancel order: {}", e)))
            }
        }
    }
}