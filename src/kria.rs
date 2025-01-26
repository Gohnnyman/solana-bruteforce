use std::fs::{self, OpenOptions};
use std::mem;
use std::path::PathBuf;

use anyhow::Context;
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use solana_accounts_db::accounts_file::ALIGN_BOUNDARY_OFFSET;
use solana_accounts_db::accounts_hash::AccountHash;
use solana_accounts_db::{account_storage::meta::StoredMetaWriteVersion, u64_align};
use solana_sdk::{clock::Epoch, pubkey::Pubkey};

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

pub fn scan_accounts(unarchived_snapshot_path: PathBuf) -> anyhow::Result<()> {
    // Check if the `accounts` directory exist
    let accounts_path = unarchived_snapshot_path.join("accounts");

    if !accounts_path.exists() {
        anyhow::bail!("The required folder `accounts` are missing");
    }

    // Find the first file inside the `accounts` directory
    let first_file_path = fs::read_dir(&accounts_path)
        .context("Failed to read the `accounts` directory")?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().is_file())
        .map(|entry| entry.path())
        .next();

    let first_file_path = first_file_path.context("No files found in the `accounts` directory")?;

    scan_file(&first_file_path)?;

    Ok(())
}

fn scan_file(file_path: &PathBuf) -> anyhow::Result<()> {
    println!("Opening file: {:?}", file_path);
    // Open the file in read-only mode
    let file = OpenOptions::new()
        .read(true)
        .open(file_path)
        .with_context(|| format!("Failed to open file {:?}", file_path))?;

    // Memory-map the file
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("Failed to memory-map file {:?}", file_path))?;
    // Start scanning the file for accounts
    let mut offset = 0;
    while offset < mmap.len() {
        // Read StoredMeta
        let (stored_meta, next_offset) = match get_type::<StoredMeta>(&mmap, offset) {
            Some((meta, next)) => (meta, next),
            None => panic!("Cannot read StoredMeta"),
        };
        println!("StoredMeta: {:?}", stored_meta);

        // Read AccountMeta
        let (account_meta, next_offset) = match get_type::<AccountMeta>(&mmap, next_offset) {
            Some((meta, next)) => (meta, next),
            None => panic!("Cannot read AccountMeta"),
        };
        println!("AccountMeta: {:?}", account_meta);

        // Read AccountHash
        let (account_hash, next_offset) = match get_type::<AccountHash>(&mmap, next_offset) {
            Some((hash, next)) => (hash, next),
            None => panic!("Cannot read AccountHash"),
        };
        println!("AccountHash: {:?}", account_hash);

        // Read account data
        let account_data_size = stored_meta.data_len as usize;
        let (account_data, next_offset) = match get_slice(&mmap, next_offset, account_data_size) {
            Some((data, next)) => (data, next),
            None => panic!("Cannot read account data"),
        };

        println!(
            "Account Data Length: {}, First Bytes: {:?}",
            account_data.len(),
            &account_data[0..account_data.len().min(10)] // Show first few bytes
        );

        // Update the offset to continue reading the next account
        offset = next_offset;

        // Align the offset to the nearest 64-byte boundary
        offset = u64_align!(offset);
    }

    println!("Finished scanning accounts.");

    Ok(())
}
