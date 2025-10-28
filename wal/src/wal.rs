use anyhow::{Context, Result};
use bytes::BufMut;
use std::fs::{self, File};
use std::hash::Hasher;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use transaction::Transaction;

const DEFAULT_WAL_FILE_PREFIX: &str = "wal";
const DEFAULT_MIN_BATCH_SIZE: u64 = 3000;

pub struct WriteAheadLog {
    initial: bool,
    folder: String,
}

pub struct RecoveredWindow {
    seq_beginning: u64,
    seq_end: u64,
    transactions: Vec<Transaction>,
}

pub struct WalFile<'a> {
    pub seq_start: u64,
    pub seq_end: Option<u64>,
    pub filename: &'a str,
    pub writer: BufWriter<File>,
}

impl<'a> WalFile<'a> {
    pub fn new(folder: &'a str, seq_start: u64, seq_end: u64) -> anyhow::Result<Self> {
        let filename = format!("wal-{:020}-{:020}.log", seq_start, seq_end);
        let filepath = Path::new(folder).join(&filename);
        let file = File::create(&filepath)?;

        Ok(WalFile {
            seq_start,
            seq_end: Some(seq_end),
            filename: Box::leak(filename.into_boxed_str()), // Convert to &'static str
            writer: BufWriter::new(file),
        })
    }
}

impl WriteAheadLog {
    fn recover_from_file(path: &Path) -> Result<Vec<Transaction>> {
        let file =
            File::open(path).with_context(|| format!("Failed to open WAL file: {:?}", path))?;
        let mut reader = BufReader::new(file);
        let mut transactions = Vec::new();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;
        let mut pos = 0;
        while pos < buffer.len() {
            if pos + 2 > buffer.len() {
                break;
            }
            let key_len = u16::from_be_bytes([buffer[pos], buffer[pos + 1]]) as usize;
            pos += 2;
            if pos + key_len > buffer.len() {
                eprintln!("Warning: Incomplete key at position {}", pos);
                break;
            }
            pos += key_len;
            if pos + 2 > buffer.len() {
                eprintln!("Warning: Incomplete value length at position {}", pos);
                break;
            }
            let value_len = u16::from_be_bytes([buffer[pos], buffer[pos + 1]]) as usize;
            pos += 2;
            if pos + value_len > buffer.len() {
                eprintln!("Warning: Incomplete value at position {}", pos);
                break;
            }
            let value = buffer[pos..pos + value_len].to_vec();
            pos += value_len;
            if pos + 4 > buffer.len() {
                eprintln!("Warning: Incomplete CRC at position {}", pos);
                break;
            }
            let stored_crc = u32::from_be_bytes([
                buffer[pos],
                buffer[pos + 1],
                buffer[pos + 2],
                buffer[pos + 3],
            ]);
            pos += 4;
            let data_end = pos - 4;
            let data_start = data_end - (2 + key_len + 2 + value_len);
            let mut hasher = crc32fast::Hasher::new();
            hasher.write(&buffer[data_start..data_end]);
            let computed_crc = hasher.finalize();
            if stored_crc != computed_crc {
                eprintln!(
                    "Warning: CRC mismatch at position {}. Expected: {}, Got: {}",
                    pos, computed_crc, stored_crc
                );
                continue;
            }
            match Transaction::from_bytes(&value) {
                Ok(transaction) => transactions.push(transaction),
                Err(e) => {
                    eprintln!("Warning: Failed to deserialize transaction: {}", e);
                    continue;
                }
            }
        }
        Ok(transactions)
    }

    fn initial_wal(folder: impl AsRef<Path>) -> anyhow::Result<Option<Vec<PathBuf>>> {
        let folder = folder.as_ref();
        fs::create_dir_all(folder)?;
        match Self::find_wal_files(folder)? {
            Some(f) => return Ok(Some(f)),
            None => return Ok(None),
        };
    }

    pub fn recover(folder: impl AsRef<Path>) -> Result<(Self, Option<Vec<RecoveredWindow>>, u64)> {
        let folder = folder.as_ref();
        let mut max_seq_end = 0u64;
        let mut recovered_windows = Vec::new();
        let wal_files = Self::initial_wal(folder)?;
        if let Some(ref wal_files) = wal_files {
            for wal_path in wal_files {
                if let Some((seq_beginning, seq_end)) = Self::extract_seq_range_from_path(wal_path)
                {
                    let transactions = Self::recover_from_file(wal_path)?;
                    if !transactions.is_empty() {
                        let window = RecoveredWindow {
                            seq_beginning,
                            seq_end,
                            transactions,
                        };
                        recovered_windows.push(window);
                    }
                    if seq_end > max_seq_end {
                        max_seq_end = seq_end;
                    }
                }
            }
        }
        let next_seq_num = if max_seq_end == 0 { 0 } else { max_seq_end + 1 };

        let wal = Self {
            initial: wal_files.is_some() && !recovered_windows.is_empty(),
            folder: folder.to_string_lossy().to_string(),
        };

        let windows_result = if recovered_windows.is_empty() {
            println!("No windows recovered from WAL");
            None
        } else {
            println!("Recovered {} windows from WAL", recovered_windows.len());
            for window in &recovered_windows {
                println!(
                    "  Window [{}-{}]: {} transactions",
                    window.seq_beginning,
                    window.seq_end,
                    window.transactions.len()
                );
            }
            println!("Continuing from sequence number {}", next_seq_num);
            Some(recovered_windows)
        };

        Ok((wal, windows_result, next_seq_num))
    }

    fn find_wal_files(folder: &Path) -> Result<Option<Vec<PathBuf>>> {
        let mut wal_files: Vec<PathBuf> = fs::read_dir(folder)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file()
                    && path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .map(|filename| {
                            filename.starts_with(DEFAULT_WAL_FILE_PREFIX)
                                && filename.ends_with(".log")
                                && Self::extract_seq_range_from_path(path).is_some()
                        })
                        .unwrap_or(false)
            })
            .collect();
        if wal_files.is_empty() {
            return Ok(None);
        }
        wal_files.sort_by_key(|path| {
            Self::extract_seq_range_from_path(path)
                .map(|(start, _)| start)
                .unwrap_or(0)
        });
        Ok(Some(wal_files))
    }

    fn extract_seq_range_from_path(path: &PathBuf) -> Option<(u64, u64)> {
        let filename = path.file_name()?.to_str()?;
        let without_prefix = filename.strip_prefix("wal-")?;
        let without_suffix = without_prefix.strip_suffix(".log")?;
        let mut parts = without_suffix.split('-');
        let start = parts.next()?.parse::<u64>().ok()?;
        let end = parts.next()?.parse::<u64>().ok()?;
        Some((start, end))
    }

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

    pub fn delete_wal_file_by_seq(&self, seq_start: u64, seq_end: u64) -> Result<bool> {
        let filename = format!("wal-{:020}-{:020}.log", seq_start, seq_end);
        let wal_path = Path::new(&self.folder).join(&filename);
        if !wal_path.exists() {
            return Ok(false);
        }
        fs::remove_file(&wal_path).context(format!("failed to delete WAL file: {:?}", wal_path))?;
        Ok(true)
    }
}
