// src/domain/traits.rs

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize,Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderIndex(pub usize);

pub trait ArenaStore<T> {
    fn allocate(&mut self, item: T) -> Result<OrderIndex, String>;
    
    fn get(&self, index: OrderIndex) -> Option<&T>;
    
    fn get_mut(&mut self, index: OrderIndex) -> Option<&mut T>;
    
    fn deallocate(&mut self, index: OrderIndex) -> Result<(), String>;
}