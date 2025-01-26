use {
    crate::{bank_forks_utils::StorageAndNextAccountsFileId, serde_snapshot::bank_from_streams},
    log::*,
    serde::de::value,
    solana_accounts_db::{
        account_storage::AccountStorageMap,
        accounts_db::{AccountsDbConfig, AtomicAccountsFileId, DuplicatesLtHash},
        accounts_update_notifier_interface::AccountsUpdateNotifier,
        utils::delete_contents_of_path,
    },
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_processor::{BlockstoreProcessorError, CacheBlockMetaSender, ProcessOptions},
        entry_notifier_service::EntryNotifierSender,
        leader_schedule_cache::LeaderScheduleCache,
        use_snapshot_archives_at_startup::{self, UseSnapshotArchivesAtStartup},
    },
    solana_measure::{measure::Measure, measure_time},
    solana_runtime::{
        bank::{builtins::BuiltinPrototype, Bank},
        bank_forks::BankForks,
        runtime_config::RuntimeConfig,
        snapshot_archive_info::{
            FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
        },
        snapshot_config::SnapshotConfig,
        snapshot_hash::StartingSnapshotHashes,
        snapshot_utils::{
            self, deserialize_snapshot_data_files, rebuild_storages_from_snapshot_dir,
            verify_and_unarchive_snapshots, verify_unpacked_snapshots_dir_and_version,
            SnapshotRootPaths, SnapshotVersion, UnpackedSnapshotsDirAndVersion,
        },
    },
    solana_sdk::{genesis_config::GenesisConfig, pubkey::Pubkey},
    std::{
        collections::HashSet,
        path::PathBuf,
        result,
        sync::{atomic::AtomicBool, Arc, RwLock},
    },
    thiserror::Error,
};

/// This struct contains side-info from rebuilding the bank
#[derive(Debug)]
pub struct RebuiltBankInfo {
    duplicates_lt_hash: Option<Box<DuplicatesLtHash>>,
}

#[allow(clippy::too_many_arguments)]
pub fn rebuild_bank_from_unarchived_snapshots(
    full_snapshot_unpacked_snapshots_dir_and_version: &UnpackedSnapshotsDirAndVersion,
    incremental_snapshot_unpacked_snapshots_dir_and_version: Option<
        &UnpackedSnapshotsDirAndVersion,
    >,
    account_paths: &[PathBuf],
    storage_and_next_append_vec_id: StorageAndNextAccountsFileId,
    genesis_config: &GenesisConfig,
    runtime_config: &RuntimeConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&[BuiltinPrototype]>,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> snapshot_utils::Result<(Bank, RebuiltBankInfo)> {
    let (full_snapshot_version, full_snapshot_root_paths) =
        verify_unpacked_snapshots_dir_and_version(
            full_snapshot_unpacked_snapshots_dir_and_version,
        )?;
    let (incremental_snapshot_version, incremental_snapshot_root_paths) =
        if let Some(snapshot_unpacked_snapshots_dir_and_version) =
            incremental_snapshot_unpacked_snapshots_dir_and_version
        {
            Some(verify_unpacked_snapshots_dir_and_version(
                snapshot_unpacked_snapshots_dir_and_version,
            )?)
        } else {
            None
        }
        .unzip();
    info!(
        "Rebuilding bank from full snapshot {} and incremental snapshot {:?}",
        full_snapshot_root_paths.snapshot_path().display(),
        incremental_snapshot_root_paths
            .as_ref()
            .map(|paths| paths.snapshot_path()),
    );

    error!(
        "ACCOUNTS ABOBA1: {}",
        storage_and_next_append_vec_id.storage.len()
    );
    let mut accounts = HashSet::new();
    let mut accounts_on_curve = HashSet::new();
    // Find the smallest and largest keys
    let (min_key, max_key) = storage_and_next_append_vec_id.storage.iter().fold(
        (None::<u64>, None::<u64>),
        |(min, max), entry| {
            let key = *entry.key();
            (
                Some(min.map_or(key, |m| m.min(key))),
                Some(max.map_or(key, |m| m.max(key))),
            )
        },
    );
    for (_, storage) in storage_and_next_append_vec_id.storage.clone() {
        storage.storage.accounts.scan_accounts(|acc| {
            let key = acc.pubkey();
            accounts.insert(key.clone());
            if key.is_on_curve() {
                accounts_on_curve.insert(key.clone());
            }
        });

        storage.storage.accounts.scan_accounts(|acc| {
            let key = acc.pubkey();
            accounts.insert(key.clone());
            if key.is_on_curve() {
                accounts_on_curve.insert(key.clone());
            }
        });
    }

    error!(
        "Got min_key: {:?}, max_key: {:?}, total: {:?}, on_curve: {:?}",
        min_key,
        max_key,
        accounts.len(),
        accounts_on_curve.len()
    );

    let snapshot_root_paths = SnapshotRootPaths {
        full_snapshot_root_file_path: full_snapshot_root_paths.snapshot_path(),
        incremental_snapshot_root_file_path: incremental_snapshot_root_paths
            .map(|root_paths| root_paths.snapshot_path()),
    };

    let (bank, info) = deserialize_snapshot_data_files(&snapshot_root_paths, |snapshot_streams| {
        Ok(
            match incremental_snapshot_version.unwrap_or(full_snapshot_version) {
                SnapshotVersion::V1_2_0 => bank_from_streams(
                    snapshot_streams,
                    account_paths,
                    storage_and_next_append_vec_id,
                    genesis_config,
                    runtime_config,
                    debug_keys,
                    additional_builtins,
                    limit_load_slot_count_from_snapshot,
                    verify_index,
                    accounts_db_config,
                    accounts_update_notifier,
                    exit,
                ),
            }?,
        )
    })?;

    todo!("Rebuild bank from unarchived snapshots");

    // verify_epoch_stakes(&bank)?;

    // // The status cache is rebuilt from the latest snapshot.  So, if there's an incremental
    // // snapshot, use that.  Otherwise use the full snapshot.
    // let status_cache_path = incremental_snapshot_unpacked_snapshots_dir_and_version
    //     .map_or_else(
    //         || {
    //             full_snapshot_unpacked_snapshots_dir_and_version
    //                 .unpacked_snapshots_dir
    //                 .as_path()
    //         },
    //         |unpacked_snapshots_dir_and_version| {
    //             unpacked_snapshots_dir_and_version
    //                 .unpacked_snapshots_dir
    //                 .as_path()
    //         },
    //     )
    //     .join(snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME);
    // let slot_deltas = deserialize_status_cache(&status_cache_path)?;

    // verify_slot_deltas(slot_deltas.as_slice(), &bank)?;

    // bank.status_cache.write().unwrap().append(&slot_deltas);

    // info!("Rebuilt bank for slot: {}", bank.slot());
    // Ok((
    //     bank,
    //     RebuiltBankInfo {
    //         duplicates_lt_hash: info.duplicates_lt_hash,
    //     },
    // ))
}
