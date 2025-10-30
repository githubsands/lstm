use crossbeam_skiplist::SkipMap;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::{TcpListener, TcpStream};
use std::sync::Arc;

struct Transactions {}

pub struct Asynchronous {
    preconfirmed_txs: Arc<SkipMap<u64, Transactions>>,
}

impl Asynchronous {
    fn new(txs: Arc<SkipMap<u64, Transactions>>) -> Self {
        Asynchronous {
            preconfirmed_txs: Arc::new(SkipMap::new()),
        }
    }
    async fn broadcast_preconfirmations(&mut self) {
        let listener = TcpListener::bind("127.0.0.1:50002").unwrap();
        loop {
            let incoming = listener.accept().await;
            match incoming {
                Ok((stream, addr)) => {
                    println!("accepted a connection from {}", addr);
                    monoio::spawn(Self::stream_preconfirmed_ordered_transactions(stream));
                }
                Err(e) => {
                    println!("accepted connection failed: {}", e);
                    return;
                }
            }
        }
    }
    async fn stream_preconfirmed_ordered_transactions(mut stream: TcpStream) -> anyhow::Result<()> {
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
