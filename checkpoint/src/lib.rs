use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use wal::wal::{DEFAULT_WAL_FILE_PREFIX, DEFAULT_WAL_FOLDER};

pub enum CheckPoint {
    SequenceWindowMemtableFlushed,
}

struct CheckPointManager {}

impl CheckPointManager {
    fn sequence_window_memtable_flushed(seq_beginning: u64, seq_end: u64) -> anyhow::Result<()> {
        let filename = format!(
            "{}-{:020}-{:020}.log",
            DEFAULT_WAL_FILE_PREFIX, seq_beginning, seq_end
        );
        let wal_path = Path::new(DEFAULT_WAL_FOLDER).join(&filename);

        if wal_path.exists() {
            fs::remove_file(&wal_path)
                .context(format!("Failed to delete WAL file: {:?}", wal_path))?;
            println!(
                "Deleted WAL file for sequence range [{}, {}]: {}",
                seq_beginning, seq_end, filename
            );
        } else {
            println!(
                "WAL file not found for sequence range [{}, {}]: {}",
                seq_beginning, seq_end, filename
            );
        }

        Ok(())
    }
}
