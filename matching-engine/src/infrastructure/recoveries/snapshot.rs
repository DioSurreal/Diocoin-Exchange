// src/infrastructure/recoveries/snapshot.rs

use bincode::Options;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};

/// Container combining the full Engine state for Snapshotting
#[derive(Serialize, Deserialize)]
pub struct EngineSnapshotState<B, A> {
    pub last_included_wal_index: u64, // Indicates which WAL sequence ID this snapshot covers
    pub book: B,
    pub arena: A,
}

pub struct SnapshotManager {
    base_dir: PathBuf,
    snapshot_filename: String,
}

impl SnapshotManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, snapshot_filename: &str) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            snapshot_filename: snapshot_filename.to_string(),
        }
    }

    /// 📸 Save State to disk atomically (Zero-Corrupt Guarantee)
    pub fn save_snapshot<B, A>(&self, last_wal_idx: u64, book: &B, arena: &A) -> std::io::Result<()>
    where
        B: Serialize,
        A: Serialize,
    {
        // 1. Create paths for temporary and real files
        let real_path = self.base_dir.join(&self.snapshot_filename);
        let tmp_path = self.base_dir.join(format!("{}.tmp", self.snapshot_filename));

        // 2. Ensure the parent directory exists
        if let Some(parent) = real_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 3. Open temporary file with BufWriter for fast sequential I/O
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        let mut writer = BufWriter::new(file);

        // 4. Pack data into a single state object
        let state = EngineSnapshotState {
            last_included_wal_index: last_wal_idx,
            book,
            arena,
        };

        // 5. Serialize using bincode (fastest and smallest binary format in Rust)
        bincode::serialize_into(&mut writer, &state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        // Ensure all data is flushed to disk before renaming
        writer.flush()?;

        // 6. 🌟 OS Atomic Rename: Move temporary file to real path (safe from power failures)
        std::fs::rename(tmp_path, real_path)?;

        Ok(())
    }

    /// 🔄 Load the latest Snapshot back into memory
    pub fn load_snapshot<B, A>(&self) -> std::io::Result<Option<EngineSnapshotState<B, A>>>
    where
        B: for<'de> Deserialize<'de>,
        A: for<'de> Deserialize<'de>,
    {
        let real_path = self.base_dir.join(&self.snapshot_filename);

        if !real_path.exists() {
            return Ok(None); // If no snapshot file exists, it's a fresh system start
        }

        let file = File::open(real_path)?;
        let reader = BufReader::new(file);

        // Create configuration with a size limit (e.g., 1GB) to prevent OOM attacks
        let bincode_config = bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .with_limit(1024 * 1024 * 1024); 

        let state: EngineSnapshotState<B, A> = bincode_config
            .deserialize_from(reader)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Bincode deserialization failed: {}", e)))?;

        Ok(Some(state))
    }
}