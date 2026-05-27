// src/infrastructure/mod.rs

pub mod observability;   
pub mod memory_arena;
pub mod recoveries;    
pub mod grpc_gateway;

pub mod pb {
    tonic::include_proto!("matching_engine.v1");
}