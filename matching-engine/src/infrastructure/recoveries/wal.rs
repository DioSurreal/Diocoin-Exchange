// src/infrastructure/recoveries/wal.rs

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use serde::{Serialize, Deserialize};

pub struct WalManager {
    base_dir: PathBuf,
    max_file_size: u64,
    current_file_idx: u64,
    current_writer: Option<BufWriter<File>>,
    current_file_size: u64,
}

impl WalManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, max_file_size: u64) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            max_file_size,
            current_file_idx: 1,
            current_writer: None,
            current_file_size: 0,
        }
    }

    /// 🛠️ Scan the directory for the latest WAL files to resume from the previous state
    pub fn initialize(&mut self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.base_dir)?;
        
        let mut max_idx = 0;
        for entry in std::fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "wal") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(idx) = stem.parse::<u64>() {
                        if idx > max_idx { max_idx = idx; }
                    }
                }
            }
        }

        // If existing files are found, start at the latest; otherwise, start at 1
        self.current_file_idx = if max_idx == 0 { 1 } else { max_idx };
        self.open_current_file()?;
        Ok(())
    }

    /// 📝 Record trading commands to WAL (Append-Only Speed)
    pub fn append_entry<T: Serialize>(&mut self, entry: &T) -> std::io::Result<(u64, u64)> {
        // Check if current file is full; if so, rotate to a new one
        if self.current_file_size >= self.max_file_size {
            self.rotate_file()?;
        }

        let writer = self.current_writer.as_mut().expect("WAL writer not initialized");

        // Serialize directly into file using bincode
        let start_pos = self.current_file_size;
        bincode::serialize_into(&mut *writer, entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        
        // Intercept current position to update the actual file size
        let current_pos = writer.stream_position()?;
        self.current_file_size = current_pos;

        // Force data flush to Disk Buffer immediately for transactional integrity
        writer.flush()?;

        // Return the file Index and Offset (useful for advanced indexing)
        Ok((self.current_file_idx, start_pos))
    }

    /// 🔄 Perform log rotation
    fn rotate_file(&mut self) -> std::io::Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            writer.flush()?;
        }
        self.current_file_idx += 1;
        self.open_current_file()?;
        Ok(())
    }

    fn open_current_file(&mut self) -> std::io::Result<()> {
        let filename = format!("{:016}.wal", self.current_file_idx); // Result: 0000000000000001.wal
        let path = self.base_dir.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        self.current_file_size = metadata.len();
        
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::End(0))?; // Always move pointer to the end of the file
        
        self.current_writer = Some(writer);
        Ok(())
    }

    /// 🔍 Read all entries from WAL files sequentially for system replay during startup
    pub fn read_all_entries<T: for<'de> Deserialize<'de>>(&self) -> std::io::Result<Vec<T>> {
        let mut entries = Vec::new();
        let mut files = Vec::new();

        // 1. Scan for all .wal files in the directory
        if self.base_dir.exists() {
            for entry in std::fs::read_dir(&self.base_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "wal") {
                    files.push(path);
                }
            }
        }

        // 2. 🌟 Sort files alphabetically (ensuring correct chronological order)
        files.sort();

        // 3. Read data from each file stream into a vector
        for file_path in files {
            let file = File::open(file_path)?;
            let mut reader = BufReader::new(file);
            // Loop until EOF of the specific file is reached
            while let Ok(entry) = bincode::deserialize_from(&mut reader) {
                entries.push(entry);
            }
        }

        Ok(entries)
    }
}