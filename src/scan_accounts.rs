use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{hash, mem};

use futures::future::join_all;
use futures::{pin_mut, SinkExt};
use log::{debug, error, info};
use memmap2::Mmap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use solana_accounts_db::accounts_file::ALIGN_BOUNDARY_OFFSET;
use solana_accounts_db::accounts_hash::AccountHash;
use solana_accounts_db::{account_storage::meta::StoredMetaWriteVersion, u64_align};
use solana_runtime::accounts_background_service;
use solana_sdk::blake3::Hash;
use solana_sdk::lamports;
use solana_sdk::signer::Signer;
use solana_sdk::{clock::Epoch, pubkey::Pubkey};
use sqlx::{query, Pool, Postgres};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, NoTls};

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
    pub pubkey: Pubkey,
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
    pub pubkey: String,
    pub lamports: i64,
}

// We only need to hash the pubkey
impl hash::Hash for AccountWithBalance {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.pubkey.hash(state);
    }
}

impl AccountWithBalance {
    pub fn new(pubkey: Pubkey, lamports: u64) -> Self {
        let pubkey = pubkey.to_string();
        let lamports = lamports.try_into().unwrap_or(-1);
        Self { pubkey, lamports }
    }

    pub fn get_pubkey(&self) -> String {
        self.pubkey.clone()
    }

    /// Convert lamports to i64
    /// If overflow occurs, return -1
    pub fn get_lamports(&self) -> i64 {
        self.lamports.try_into().unwrap_or(-1)
    }
}

pub async fn scan_accounts(unarchived_snapshot_path: PathBuf) -> Result<()> {
    let accounts_path = unarchived_snapshot_path.join("accounts");

    // Check if the `accounts` directory exist
    if !accounts_path.exists() {
        return Err(ScanAccountsError::FolderAccountMissing);
    }

    // Collect all files in the `accounts` directory
    let account_files: Vec<Arc<_>> = fs::read_dir(&accounts_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .map(|entry| Arc::new(entry.path()))
        .collect();

    // Create a connection pool
    let pool = Arc::new(Mutex::new(Pool::<Postgres>::connect_lazy(
        "postgres://postgres:password@localhost:5432/bruteforce",
    )?));

    info!(
        "Starting to scan accounts (total files: {})...",
        account_files.len()
    );

    let chunk_size = 500; // Number of files to process concurrently
    let processed_count = Arc::new(AtomicUsize::new(0)); // Counter for processed files

    // Process files in chunks
    for chunk in account_files.chunks(chunk_size) {
        let tasks: Vec<_> = chunk
            .iter()
            .map(|file_path| {
                // let pool = pool.clone();
                let file_path = file_path.clone();
                tokio::spawn(async move {
                    let result = scan_accounts_and_balances(file_path.as_ref());
                    result
                })
            })
            .collect();

        let chunk_results = join_all(tasks).await;

        let mut accounts_and_balances: HashSet<AccountWithBalance> = HashSet::new();

        for result in chunk_results {
            // let result: HashSet<AccountWithBalance> = result??;
            match result {
                Ok(inner_result) => {
                    accounts_and_balances.extend(inner_result?);
                }
                Err(e) => {
                    return Err(ScanAccountsError::JoinError(e));
                }
            }
        }

        // Insert the accounts into the database
        insert_accounts_into_db(accounts_and_balances, pool.clone()).await?;

        // Log progress update
        processed_count.fetch_add(chunk_size, Ordering::Relaxed);
        let processed = processed_count.load(Ordering::Relaxed);
        info!("Processed {} / {} files", processed, account_files.len());
    }

    info!("Finished scanning all accounts.");

    Ok(())
}

async fn insert_accounts_into_db(
    accounts_with_balances: HashSet<AccountWithBalance>,
    pool: Arc<Mutex<Pool<Postgres>>>,
) -> Result<()> {
    // Batch insert accounts into the database
    let pool = pool.lock().await;

    // Prepare a batch insert query

    let accounts = accounts_with_balances.into_iter().collect::<Vec<_>>();

    for chunk in accounts.chunks(10000) {
        let mut query_builder =
            String::from("INSERT INTO existing_accounts (account_pubkey, balance) VALUES ");
        // let mut params: Vec<(String, i64)> = Vec::new();

        // for account_with_balance in accounts_with_balances {
        //     // Collect parameters for batch insertion
        //     params.push((
        //         account_with_balance.get_pubkey(),
        //         account_with_balance.get_lamports(),
        //     ));
        // }

        // Create a string with placeholders for each row
        for (i, _) in chunk.iter().enumerate() {
            if i > 0 {
                query_builder.push(',');
            }
            query_builder.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
        }

        query_builder.push_str(" ON CONFLICT (account_pubkey) DO NOTHING");

        // Create a SQLx query and bind all parameters
        let mut query = sqlx::query(&query_builder);

        for AccountWithBalance { pubkey, lamports } in chunk {
            query = query.bind(pubkey).bind(lamports);
        }

        // Execute the batch query
        query
            .execute(&*pool)
            .await
            .map_err(ScanAccountsError::SqlxError)?;
    }

    error!("Batch insert completed!");

    Ok(())
}

fn scan_accounts_and_balances(file_path: &PathBuf) -> Result<HashSet<AccountWithBalance>> {
    debug!("Opening file: {:?}", file_path);

    // Open the file in read-only mode
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)
        .map_err(|e| ScanAccountsError::FailedToOpenFile(e))?;

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
        if stored_meta.pubkey.is_on_curve() {
            accounts_with_balances.insert(AccountWithBalance::new(
                stored_meta.pubkey,
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
