use anyhow::Result;
use kanal::AsyncSender;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_keccak::{Hasher, Sha3};

const MAX_DATA_TRANSACTION_BYTES: usize = 1600;
const MAX_INSTRUCTION_TRANSACTION: usize = 5;

pub struct PendingTransaction {
    pub tx: Transaction,
    // bytes: Option<&'a [u8]>,
    pub seq_num: u64,
    pub response_tx: AsyncSender<()>,
    closed: bool,
}

impl PendingTransaction {
    pub fn new(
        tx: Transaction,
        seq_num: u64,
        // bytes: Option<&'a [u8]>,
        response_tx: AsyncSender<()>,
    ) -> PendingTransaction {
        PendingTransaction {
            seq_num,
            //  bytes,
            tx,
            response_tx,
            closed: false,
        }
    }

    pub async fn ack(&self) -> anyhow::Result<()> {
        self.response_tx.send(()).await?;
        Ok(())
    }
}

#[derive(Copy, Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct Transaction {
    pub signer: Signer,
    pub itxs: [Instruction; MAX_INSTRUCTION_TRANSACTION],
    pub timestamp: u64,
}

/// Transaction body without timestamp (for deserialization from network)
#[derive(Copy, Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct TransactionBody {
    pub signer: Signer,
    pub itxs: [Instruction; MAX_INSTRUCTION_TRANSACTION],
}

#[derive(Copy, Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct Signer([u8; 32]);

#[derive(Copy, Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct StorageDelta([u8; 32]);

impl Signer {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Copy, Clone, Deserialize, Serialize, PartialEq, Debug)]
pub struct Instruction {
    pub contract: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub storage_account: [u8; 32],
    #[serde(with = "serde_bytes")]
    pub data: [u8; MAX_DATA_TRANSACTION_BYTES],
}

impl Transaction {
    pub fn new(signer: Signer, itxs: [Instruction; MAX_INSTRUCTION_TRANSACTION]) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            signer,
            itxs,
            timestamp,
        }
    }

    pub fn new_with_timestamp(
        signer: Signer,
        itxs: [Instruction; MAX_INSTRUCTION_TRANSACTION],
        timestamp: u64,
    ) -> Self {
        Self {
            signer,
            itxs,
            timestamp,
        }
    }

    pub fn from_body(body: TransactionBody) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            signer: body.signer,
            itxs: body.itxs,
            timestamp,
        }
    }

    /// Create transaction from body with explicit timestamp
    pub fn from_body_with_timestamp(body: TransactionBody, timestamp: u64) -> Self {
        Self {
            signer: body.signer,
            itxs: body.itxs,
            timestamp,
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let body: TransactionBody = bincode::deserialize(bytes)?;
        Ok(Self::from_body(body))
    }

    pub fn from_slice_with_timestamp(bytes: &[u8], timestamp: u64) -> Result<Self> {
        let body: TransactionBody = bincode::deserialize(bytes)?;
        Ok(Self::from_body_with_timestamp(body, timestamp))
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }

    pub fn serialized_size() -> usize {
        bincode::serialized_size(&Transaction::default()).unwrap() as usize
    }

    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn is_older_than(&self, seconds: u64) -> bool {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        current_time.saturating_sub(self.timestamp) > seconds
    }

    pub fn storage_delta(&self) -> StorageDelta {
        let timestamp_bytes = self.timestamp.to_le_bytes();
        let mut sha3 = Sha3::v256();
        sha3.update(&self.itxs[0].data[..]);
        sha3.update(&timestamp_bytes[..]);
        let mut output = [0u8; 32];
        sha3.finalize(&mut output);
        StorageDelta(output)
    }
}

impl TransactionBody {
    /// Serialize transaction body to bytes using bincode
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize transaction body from bytes using bincode
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }

    /// Get the size of serialized transaction body in bytes
    pub fn serialized_size() -> usize {
        bincode::serialized_size(&TransactionBody::default()).unwrap() as usize
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            signer: Signer::default(),
            itxs: [Instruction::default(); MAX_INSTRUCTION_TRANSACTION],
            timestamp: 0,
        }
    }
}

impl Default for TransactionBody {
    fn default() -> Self {
        Self {
            signer: Signer::default(),
            itxs: [Instruction::default(); MAX_INSTRUCTION_TRANSACTION],
        }
    }
}

impl Default for Signer {
    fn default() -> Self {
        Self([0u8; 32])
    }
}

impl Default for Instruction {
    fn default() -> Self {
        Self {
            contract: [0u8; 32],
            storage_account: [0u8; 32],
            data: [0u8; MAX_DATA_TRANSACTION_BYTES],
        }
    }
}
