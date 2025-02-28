use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{hash, mem};

use log::{debug, error, info};
use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use solana_accounts_db::accounts_file::ALIGN_BOUNDARY_OFFSET;
use solana_accounts_db::accounts_hash::AccountHash;
use solana_accounts_db::{account_storage::meta::StoredMetaWriteVersion, u64_align};
use solana_sdk::{clock::Epoch, pubkey::Pubkey};
use thiserror::Error;

use crate::postgres_actor::{PostgresActor, PostgresMessage};

// The amount of account files to process in a single chunk
pub const CHUNK_SIZE: usize = 1000;
pub const CHANNEL_BUFFER_SIZE: usize = 10_000;

#[derive(Error, Debug)]
pub enum ScanAccountsError {
    #[error("IO Error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Folder account is missing")]
    FolderAccountMissing,
    #[error("Failed to open file: {0}")]
    FailedToOpenFile(std::io::Error),
    #[error("Failed to memory-map file: {0}")]
    FailedToMemoryMapFile(String),
    #[error("SQLx Error: {0}")]
    SqlxError(#[from] sqlx::Error),
    #[error("Join error while processing files: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Postgres Actor Error: {0}")]
    PostgresActorError(#[from] crate::postgres_actor::ActorError),
}

pub type Result<T> = std::result::Result<T, ScanAccountsError>;

/// Meta contains enough context to recover the index from storage itself
/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[derive(Clone, PartialEq, Eq, Debug)]
#[repr(C)]
pub struct StoredMeta {
    /// global write version
    /// This will be made completely obsolete such that we stop storing it.
    /// We will not support multiple append vecs per slot anymore, so this concept is no longer necessary.
    /// Order of stores of an account to an append vec will determine 'latest' account data per pubkey.
    pub write_version_obsolete: StoredMetaWriteVersion,
    pub data_len: u64,
    /// key for the account
    pub public_key: Pubkey,
}

/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct AccountMeta {
    /// lamports in the account
    pub lamports: u64,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountWithBalance {
    pub public_key: String,
    pub lamports: i64,
}

// We only need to hash the pubkey
impl hash::Hash for AccountWithBalance {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.public_key.hash(state);
    }
}

impl AccountWithBalance {
    pub fn new(public_key: Pubkey, lamports: u64) -> Self {
        let public_key = public_key.to_string();
        let lamports = lamports.try_into().unwrap_or(-1);
        Self {
            public_key,
            lamports,
        }
    }

    pub fn get_pubkey(&self) -> String {
        self.public_key.clone()
    }

    /// Convert lamports to i64
    /// If overflow occurs, return -1
    pub fn get_lamports(&self) -> i64 {
        self.lamports.try_into().unwrap_or(-1)
    }
}

pub async fn scan_accounts(db_url: &str, unarchived_snapshot_path: PathBuf) -> Result<()> {
    let accounts_path = unarchived_snapshot_path.join("accounts");

    if !accounts_path.exists() {
        return Err(ScanAccountsError::FolderAccountMissing);
    }

    let account_files: Vec<Arc<_>> = fs::read_dir(&accounts_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .map(|entry| Arc::new(entry.path()))
        .collect();

    let (postgres_message_tx, postgres_messages_rx) =
        tokio::sync::mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel(); // Completion channel

    PostgresActor::new(db_url, postgres_messages_rx, exit_rx, completion_tx).await?;

    info!(
        "Starting to scan accounts (total files: {})...",
        account_files.len()
    );

    let processed_count = Arc::new(AtomicUsize::new(0));

    let num_threads = num_cpus::get();
    info!("Number of threads: {}", num_threads);

    // Create a custom Rayon thread pool with the specified number of CPUs
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .expect("Failed to create Rayon thread pool");

    for chunk in account_files.chunks(CHUNK_SIZE) {
        // Use the custom thread pool to process the chunk
        thread_pool.install(|| {
            chunk.par_iter().for_each(|file_path| {
                // Clone the file_path for thread safety
                let file_path = file_path.clone();
                // Call the function to scan accounts and balances
                let result = scan_accounts_and_balances(file_path.as_ref());

                match result {
                    Ok(inner_result) => {
                        let vec = inner_result.into_iter().collect::<Vec<_>>();
                        postgres_message_tx
                            .blocking_send(PostgresMessage::InsertAccountsButch(vec))
                            .expect("Failed to send accounts, receiver dropped");
                    }
                    Err(e) => panic!("Error: {:?}", e),
                }
            });
        });

        // Update progress
        processed_count.fetch_add(CHUNK_SIZE, Ordering::Relaxed);
        let processed = processed_count.load(Ordering::Relaxed);
        info!("Processed {} / {} files", processed, account_files.len());
    }

    // Signal the actor to exit
    exit_tx
        .send(())
        .expect("Failed to send exit signal, receiver dropped");

    // Wait for the actor to confirm completion
    completion_rx
        .await
        .expect("Failed to receive completion signal");

    info!("Finished scanning all accounts.");

    Ok(())
}

fn scan_accounts_and_balances(file_path: &PathBuf) -> Result<HashSet<AccountWithBalance>> {
    debug!("Opening file: {:?}", file_path);

    // Open the file in read-only mode
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)
        .map_err(ScanAccountsError::FailedToOpenFile)?;

    // Memory-map the file
    let mmap = unsafe { Mmap::map(&file) }
        .map_err(|e| ScanAccountsError::FailedToMemoryMapFile(e.to_string()))?;

    // Start scanning the file for accounts
    let mut offset = 0;

    let mut accounts_with_balances = HashSet::new();

    while offset < mmap.len() {
        // Read StoredMeta
        let (stored_meta, next_offset) = match get_type::<StoredMeta>(&mmap, offset) {
            Some((meta, next)) => (meta, next),
            None => panic!("Cannot read StoredMeta"),
        };

        // Read AccountMeta
        let (account_meta, next_offset) = match get_type::<AccountMeta>(&mmap, next_offset) {
            Some((meta, next)) => (meta, next),
            None => panic!("Cannot read AccountMeta"),
        };

        // Read AccountHash
        let (_, next_offset) = match get_type::<AccountHash>(&mmap, next_offset) {
            Some((hash, next)) => (hash, next),
            None => panic!("Cannot read AccountHash"),
        };

        // Read account data
        let account_data_size = stored_meta.data_len as usize;
        let (_, next_offset) = match get_slice(&mmap, next_offset, account_data_size) {
            Some((data, next)) => (data, next),
            None => panic!("Cannot read account data"),
        };

        // Check whether pubkey is on curve and insert the account into the set
        if stored_meta.public_key.is_on_curve() {
            accounts_with_balances.insert(AccountWithBalance::new(
                stored_meta.public_key,
                account_meta.lamports,
            ));
        }

        // Update the offset to continue reading the next account
        offset = next_offset;
    }

    debug!("Finished scanning accounts for path {:?}.", file_path);

    Ok(accounts_with_balances)
}

/// Return a reference to the type at `offset` if its data doesn't overrun the internal buffer.
/// Otherwise return None. Also return the offset of the first byte after the requested data
/// that falls on a 64-byte boundary.
fn get_type<T>(slice: &[u8], offset: usize) -> Option<(&T, usize)> {
    let (data, next) = get_slice(slice, offset, mem::size_of::<T>())?;
    let ptr = data.as_ptr().cast();
    //UNSAFE: The cast is safe because the slice is aligned and fits into the memory
    //and the lifetime of the &T is tied to self, which holds the underlying memory map
    Some((unsafe { &*ptr }, next))
}

/// Get a reference to the data at `offset` of `size` bytes if that slice
/// doesn't overrun the internal buffer. Otherwise return None.
/// Also return the offset of the first byte after the requested data that
/// falls on a 64-byte boundary.
fn get_slice(slice: &[u8], offset: usize, size: usize) -> Option<(&[u8], usize)> {
    // SAFETY: Wrapping math is safe here because if `end` does wrap, the Range
    // parameter to `.get()` will be invalid, and `.get()` will correctly return None.
    let end = offset.wrapping_add(size);
    slice
        .get(offset..end)
        .map(|subslice| (subslice, u64_align!(end)))
}
