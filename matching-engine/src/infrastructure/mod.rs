// src/infrastructure/mod.rs

pub mod grpc_gateway;
pub mod memory_arena;
pub mod observability;
pub mod recoveries;

pub mod pb {
    tonic::include_proto!("diocoin.exchange.matching");
}
