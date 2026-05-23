// src/infrastructure/memory_arena/manager.rs

use super::block::ArenaBlock;
use crate::domain::traits::{ArenaStore, OrderIndex};

pub struct ChainedArenaManager<T> {
    pub blocks: Vec<ArenaBlock<T>>,
    pub block_size: usize,
    pub current_block_idx: usize,
}

impl<T> ChainedArenaManager<T> {
    pub fn new(block_size: usize) -> Self {
        let first_block = ArenaBlock::new(block_size);
        Self {
            blocks: vec![first_block],
            block_size,
            current_block_idx: 0,
        }
    }

    #[inline(always)]
    fn decode_index(&self, global_idx: usize) -> (usize, usize) {
        let block_idx = global_idx / self.block_size;
        let local_idx = global_idx % self.block_size;
        (block_idx, local_idx)
    }

    #[inline(always)]
    fn encode_index(&self, block_idx: usize, local_idx: usize) -> usize {
        (block_idx * self.block_size) + local_idx
    }
}

impl<T> ArenaStore<T> for ChainedArenaManager<T> {
    fn allocate(&mut self, item: T) -> Result<OrderIndex, String> {
        // 1. ลอง block ปัจจุบันก่อน (เช็ค is_full ก่อน move)
        if !self.blocks[self.current_block_idx].is_full() {
            let local_idx = self.blocks[self.current_block_idx].allocate(item).unwrap();
            let global_idx = self.encode_index(self.current_block_idx, local_idx);
            return Ok(OrderIndex(global_idx));
        }

        // 2. หา block ที่ยังว่างอยู่
        for b_idx in 0..self.blocks.len() {
            if !self.blocks[b_idx].is_full() {
                self.current_block_idx = b_idx;
                let local_idx = self.blocks[b_idx].allocate(item).unwrap();
                let global_idx = self.encode_index(b_idx, local_idx);
                return Ok(OrderIndex(global_idx));
            }
        }

        // 3. ทุก block เต็ม — สร้างใหม่
        let mut new_block = ArenaBlock::new(self.block_size);
        let local_idx = new_block.allocate(item).unwrap();
        self.blocks.push(new_block);
        self.current_block_idx = self.blocks.len() - 1;

        let global_idx = self.encode_index(self.current_block_idx, local_idx);
        Ok(OrderIndex(global_idx))
    }

    #[inline(always)]
    fn get(&self, index: OrderIndex) -> Option<&T> {
        let (block_idx, local_idx) = self.decode_index(index.0);
        self.blocks.get(block_idx)?.storage.get(local_idx)?.as_ref()
    }

    #[inline(always)]
    fn get_mut(&mut self, index: OrderIndex) -> Option<&mut T> {
        let (block_idx, local_idx) = self.decode_index(index.0);
        self.blocks
            .get_mut(block_idx)?
            .storage
            .get_mut(local_idx)?
            .as_mut()
    }

    fn deallocate(&mut self, index: OrderIndex) -> Result<(), String> {
        let (block_idx, local_idx) = self.decode_index(index.0);

        if block_idx >= self.blocks.len() {
            return Err("Global index out of bounds".to_string());
        }

        self.blocks[block_idx].deallocate(local_idx)?;

        if block_idx > 0 && self.blocks[block_idx].free_slots.len() == self.block_size {}

        Ok(())
    }
}
