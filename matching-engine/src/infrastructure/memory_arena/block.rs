// src/infrastructure/memory_arena/block.rs

pub struct ArenaBlock<T> {
    
    pub storage: Vec<Option<T>>,
    pub free_slots: Vec<usize>,
    pub capacity: usize,
}

impl<T> ArenaBlock<T> {
    pub fn new(capacity: usize) -> Self {
        let mut storage = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            storage.push(None);
        }
        
        let free_slots = (0..capacity).rev().collect();

        Self {
            storage,
            free_slots,
            capacity,
        }
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.free_slots.is_empty()
    }

    #[inline(always)]
    pub fn allocate(&mut self, item: T) -> Option<usize> {
        let local_idx = self.free_slots.pop()?;
        self.storage[local_idx] = Some(item);
        Some(local_idx)
    }

    #[inline(always)]
    pub fn deallocate(&mut self, local_idx: usize) -> Result<(), String> {
        if local_idx >= self.capacity {
            return Err("Local index out of bounds".to_string());
        }
        if self.storage[local_idx].is_none() {
            return Err("Slot already empty".to_string());
        }
        
        self.storage[local_idx] = None;
        self.free_slots.push(local_idx);
        Ok(())
    }
}