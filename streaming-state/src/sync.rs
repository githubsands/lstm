use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use std::sync::Arc;

const MAX_MESSAGE_SIZE_BYTES: u64 = 4_000_000; // pulled from grpc docs

// todo: by memtable live
enum SyncMode {
    ByMemtableFrozen,
}

struct Memtable {}

struct Account<'a> {
    key: &'a [u8; 32],
    data: &'a [u8],
}

const END_FRAME: u8 = 0x01;
const ONGOING_FRAME: u8 = 0x02;
const BEGINNING_FRAME: u8 = 0x03;

pub struct StreamedAccountsChunk<'a> {
    frame: u8,
    data: &'a [u8],
}

impl<'a> From<&Memtable> for StreamedAccountsChunk<'a> {
    fn from(memtable: &Memtable) -> Self {
        StreamedAccountsChunk {
            frame: BEGINNING_FRAME,
            data: &[0],
        }
    }
}

impl<'a> Default for StreamedAccountsChunk<'a> {
    fn default() -> Self {
        StreamedAccountsChunk {
            frame: BEGINNING_FRAME,
            data: &[0],
        }
    }
}

pub struct Synchronous {
    sync_mode: SyncMode,
    post_execution_cache: std::sync::Arc<Memtable>,
}

impl Synchronous {
    fn new(memtable: Arc<Memtable>) -> Self {
        Synchronous {
            sync_mode: SyncMode::ByMemtableFrozen,
            post_execution_cache: memtable,
        }
    }
    async fn broadcast_recent_account_updates(&mut self) {
        let listener = TcpListener::bind("127.0.0.1:50002").unwrap();
        loop {
            let incoming = listener.accept().await;
            match incoming {
                Ok((stream, addr)) => {
                    println!("accepted a connection from {}", addr);
                    monoio::spawn(Self::stream_updated_accounts_post_frozen_memtable(stream));
                }
                Err(e) => {
                    println!("accepted connection failed: {}", e);
                    return;
                }
            }
        }
    }
    async fn stream_updated_accounts_post_frozen_memtable(
        mut stream: TcpStream,
    ) -> anyhow::Result<()> {
        let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
        let mut res;
        loop {
            (res, buf) = stream.read(buf).await;
            if res? == 0 {
                return Ok(());
            }
            (res, buf) = stream.write_all(buf).await;
            res?;
            buf.clear();
        }
    }
}
