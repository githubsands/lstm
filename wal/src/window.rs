use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use transaction::PendingTransaction;

use anyhow::Result;
use bytes::BufMut;
use std::hash::Hasher;
use std::io::Write;

use crate::wal::WalFile;

const DEFAULT_MIN_BATCH_SIZE: usize = 300;
const DEFAULT_WAL_FOLDER: &'static str = "/test";

type PendingTxSkipMap = Arc<SkipMap<u64, PendingTransaction>>;

struct WindowFormation {
    max_batch_size: usize,
    seq_beginning: u64,
    seq_end: Option<u64>,
    pending: PendingTxSkipMap,
    closed: AtomicBool,
    wal_writer: WalWriter,
}

struct WalWriter {
    initial: bool,
    folder: String,
}

impl WalWriter {
    pub fn put_batch(&self, file: &mut WalFile, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let mut batch_buf = Vec::new();
        for (key, value) in entries {
            let key_len = key.len() as u16;
            let value_len = value.len() as u16;
            let mut entry_buf: Vec<u8> = Vec::with_capacity(
                key.len()
                    + value.len()
                    + std::mem::size_of::<u16>() * 2
                    + std::mem::size_of::<u32>(),
            );
            entry_buf.put_u16(key_len);
            entry_buf.put_slice(key);
            entry_buf.put_u16(value_len);
            entry_buf.put_slice(value);
            let mut hasher = crc32fast::Hasher::new();
            hasher.write(&entry_buf);
            let crc = hasher.finalize();
            entry_buf.put_u32(crc);
            batch_buf.extend_from_slice(&entry_buf);
        }
        file.writer.write_all(&batch_buf)?;
        file.writer.flush()?;
        file.writer.get_mut().sync_all()?;
        Ok(())
    }
}

pub enum WindowState {
    Open,
    Closed,
}

impl WindowFormation {
    fn new(
        seq_beginning: u64,
        pending: PendingTxSkipMap, // Use the alias here too
    ) -> Self {
        Self {
            seq_beginning,
            seq_end: None,
            pending,
            max_batch_size: DEFAULT_MIN_BATCH_SIZE,
            closed: AtomicBool::new(false),
            wal_writer: WalWriter {
                initial: false,
                folder: "".to_string(),
            },
        }
    }

    fn add_transaction(&mut self, tx: PendingTransaction) -> WindowState {
        if self.pending.len() >= self.max_batch_size {
            self.seq_end = Some(tx.seq_num);
            return WindowState::Closed;
        }
        self.pending.insert(tx.seq_num, tx);
        WindowState::Open
    }

    // todo: optimize write and ack_batch
    async fn write(self) -> anyhow::Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut wf = WalFile::new(DEFAULT_WAL_FOLDER, 0, 0)?;
        let mut batch = vec![];
        for entry in self.pending.range(0..(self.max_batch_size as u64)) {
            let key = entry.key().to_le_bytes().to_vec();
            let values = entry.key().to_le_bytes().to_vec();
            batch.push((key, values))
        }
        self.wal_writer.put_batch(&mut wf, &batch)?;
        Ok(())
    }

    async fn ack_batch(self) -> anyhow::Result<()> {
        let ceiling = self.max_batch_size as u64;
        let keys_to_remove: Vec<_> = self
            .pending
            .range(0..ceiling)
            .map(|entry| *entry.key())
            .collect();
        for seq_num in keys_to_remove {
            if let Some(pending_tx) = self.pending.remove(&seq_num) {
                pending_tx.value().ack().await?;
                drop(pending_tx)
            }
        }
        Ok(())
    }
}
