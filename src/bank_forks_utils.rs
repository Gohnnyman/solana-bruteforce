use {
    log::*,
    solana_accounts_db::{
        accounts_db::AtomicAccountsFileId,
        accounts_update_notifier_interface::AccountsUpdateNotifier, utils::delete_contents_of_path,
    },
    solana_ledger::{
        blockstore::Blockstore,
        blockstore_processor::{BlockstoreProcessorError, CacheBlockMetaSender, ProcessOptions},
        entry_notifier_service::EntryNotifierSender,
        leader_schedule_cache::LeaderScheduleCache,
        use_snapshot_archives_at_startup::{self, UseSnapshotArchivesAtStartup},
    },
    solana_measure::measure_time,
    solana_runtime::{
        bank_forks::BankForks,
        snapshot_archive_info::{
            FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
        },
        snapshot_config::SnapshotConfig,
        snapshot_hash::StartingSnapshotHashes,
        snapshot_utils::{self, rebuild_storages_from_snapshot_dir},
    },
    solana_sdk::genesis_config::GenesisConfig,
    std::{
        path::PathBuf,
        result,
        sync::{atomic::AtomicBool, Arc, RwLock},
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum BankForksUtilsError {
    #[error("accounts path(s) not present when booting from snapshot")]
    AccountPathsNotPresent,

    #[error(
        "failed to load bank: {source}, full snapshot archive: {full_snapshot_archive}, \
         incremental snapshot archive: {incremental_snapshot_archive}"
    )]
    BankFromSnapshotsArchive {
        source: snapshot_utils::SnapshotError,
        full_snapshot_archive: String,
        incremental_snapshot_archive: String,
    },

    #[error(
        "there is no local state to startup from. Ensure --{flag} is NOT set to \"{value}\" and \
         restart"
    )]
    NoBankSnapshotDirectory { flag: String, value: String },

    #[error("failed to load bank: {source}, snapshot: {path}")]
    BankFromSnapshotsDirectory {
        source: snapshot_utils::SnapshotError,
        path: PathBuf,
    },

    #[error("failed to process blockstore from root: {0}")]
    ProcessBlockstoreFromRoot(#[source] BlockstoreProcessorError),
}

pub type LoadResult = result::Result<
    (
        Arc<RwLock<BankForks>>,
        LeaderScheduleCache,
        Option<StartingSnapshotHashes>,
    ),
    BankForksUtilsError,
>;

#[allow(clippy::too_many_arguments)]
pub fn load_bank_forks(
    genesis_config: &GenesisConfig,
    blockstore: &Blockstore,
    account_paths: Vec<PathBuf>,
    snapshot_config: Option<&SnapshotConfig>,
    process_options: &ProcessOptions,
    cache_block_meta_sender: Option<&CacheBlockMetaSender>,
    entry_notification_sender: Option<&EntryNotifierSender>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    fn get_snapshots_to_load(
        snapshot_config: Option<&SnapshotConfig>,
    ) -> Option<(
        FullSnapshotArchiveInfo,
        Option<IncrementalSnapshotArchiveInfo>,
    )> {
        let Some(snapshot_config) = snapshot_config else {
            info!("Snapshots disabled; will load from genesis");
            return None;
        };

        let Some(full_snapshot_archive_info) =
            snapshot_utils::get_highest_full_snapshot_archive_info(
                &snapshot_config.full_snapshot_archives_dir,
            )
        else {
            warn!(
                "No snapshot package found in directory: {}; will load from genesis",
                snapshot_config.full_snapshot_archives_dir.display()
            );
            return None;
        };

        let incremental_snapshot_archive_info =
            snapshot_utils::get_highest_incremental_snapshot_archive_info(
                &snapshot_config.incremental_snapshot_archives_dir,
                full_snapshot_archive_info.slot(),
            );

        Some((
            full_snapshot_archive_info,
            incremental_snapshot_archive_info,
        ))
    }

    let Some((full_snapshot_archive_info, incremental_snapshot_archive_info)) =
        get_snapshots_to_load(snapshot_config)
    else {
        panic!("No snapshots to load");
    };

    // SAFETY: Having snapshots to load ensures a snapshot config
    let snapshot_config = snapshot_config.unwrap();
    info!(
        "Initializing bank snapshots dir: {}",
        snapshot_config.bank_snapshots_dir.display()
    );
    std::fs::create_dir_all(&snapshot_config.bank_snapshots_dir)
        .expect("create bank snapshots dir");

    // let (bank_forks, starting_snapshot_hashes) = bank_forks_from_snapshot(
    bank_forks_from_snapshot(
        full_snapshot_archive_info,
        incremental_snapshot_archive_info,
        genesis_config,
        account_paths,
        snapshot_config,
        process_options,
        accounts_update_notifier,
        exit,
    )?;

    // let mut leader_schedule_cache =
    //     LeaderScheduleCache::new_from_bank(&bank_forks.read().unwrap().root_bank());
    // if process_options.full_leader_cache {
    //     leader_schedule_cache.set_max_schedules(usize::MAX);
    // }

    // if let Some(ref new_hard_forks) = process_options.new_hard_forks {
    //     let root_bank = bank_forks.read().unwrap().root_bank();
    //     new_hard_forks
    //         .iter()
    //         .for_each(|hard_fork_slot| root_bank.register_hard_fork(*hard_fork_slot));
    // }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bank_forks_from_snapshot(
    full_snapshot_archive_info: FullSnapshotArchiveInfo,
    incremental_snapshot_archive_info: Option<IncrementalSnapshotArchiveInfo>,
    genesis_config: &GenesisConfig,
    account_paths: Vec<PathBuf>,
    snapshot_config: &SnapshotConfig,
    process_options: &ProcessOptions,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
    // ) -> Result<(Arc<RwLock<BankForks>>, StartingSnapshotHashes), BankForksUtilsError> {
) -> anyhow::Result<()> {
    // Fail hard here if snapshot fails to load, don't silently continue
    if account_paths.is_empty() {
        Err(BankForksUtilsError::AccountPathsNotPresent)?;
    }

    let latest_snapshot_archive_slot = std::cmp::max(
        full_snapshot_archive_info.slot(),
        incremental_snapshot_archive_info
            .as_ref()
            .map(SnapshotArchiveInfoGetter::slot)
            .unwrap_or(0),
    );

    let fastboot_snapshot = match process_options.use_snapshot_archives_at_startup {
        UseSnapshotArchivesAtStartup::Always => None,
        UseSnapshotArchivesAtStartup::Never => {
            let Some(bank_snapshot) =
                snapshot_utils::get_highest_loadable_bank_snapshot(snapshot_config)
            else {
                return Err(anyhow::anyhow!(
                    BankForksUtilsError::NoBankSnapshotDirectory {
                        flag: use_snapshot_archives_at_startup::cli::LONG_ARG.to_string(),
                        value: UseSnapshotArchivesAtStartup::Never.to_string(),
                    }
                ));
            };
            // If a newer snapshot archive was downloaded, it is possible that its slot is
            // higher than the local state we will load.  Did the user intend for this?
            if bank_snapshot.slot < latest_snapshot_archive_slot {
                warn!(
                    "Starting up from local state at slot {}, which is *older* than the latest \
                     snapshot archive at slot {}. If this is not desired, change the --{} CLI \
                     option to *not* \"{}\" and restart.",
                    bank_snapshot.slot,
                    latest_snapshot_archive_slot,
                    use_snapshot_archives_at_startup::cli::LONG_ARG,
                    UseSnapshotArchivesAtStartup::Never.to_string(),
                );
            }
            Some(bank_snapshot)
        }
        UseSnapshotArchivesAtStartup::WhenNewest => {
            snapshot_utils::get_highest_loadable_bank_snapshot(snapshot_config)
                .filter(|bank_snapshot| bank_snapshot.slot >= latest_snapshot_archive_slot)
        }
    };

    let Some(bank_snapshot) = fastboot_snapshot else {
        panic!("No snapshots to load");
    };

    ///////////
    info!(
        "Loading bank from snapshot dir: {}",
        bank_snapshot.snapshot_dir.display()
    );

    // Clear the contents of the account paths run directories.  When constructing the bank, the appendvec
    // files will be extracted from the snapshot hardlink directories into these run/ directories.
    for path in &account_paths {
        delete_contents_of_path(path);
    }

    let next_append_vec_id = Arc::new(AtomicAccountsFileId::new(0));
    let storage_access = process_options
        .accounts_db_config
        .as_ref()
        .map(|config| config.storage_access)
        .unwrap_or_default();

    let (storages, measure_rebuild_storages) = measure_time!(
        rebuild_storages_from_snapshot_dir(
            &bank_snapshot,
            &account_paths,
            next_append_vec_id.clone(),
            storage_access,
        )?,
        "rebuild storages from snapshot dir"
    );

    info!("{}", measure_rebuild_storages);

    for (slot, storage) in storages {
        error!(
            "ACCOUNT AMOUNT FOR SLOT {}: {}",
            slot,
            storage.storage.accounts.len()
        );
    }

    /////////////
    // let bank = if let Some(fastboot_snapshot) = fastboot_snapshot {
    //     let (bank, _) = bank_from_snapshot_dir(
    //         &account_paths,
    //         &fastboot_snapshot,
    //         genesis_config,
    //         &process_options.runtime_config,
    //         process_options.debug_keys.clone(),
    //         None,
    //         process_options.account_indexes.clone(),
    //         process_options.limit_load_slot_count_from_snapshot,
    //         process_options.shrink_ratio,
    //         process_options.verify_index,
    //         process_options.accounts_db_config.clone(),
    //         accounts_update_notifier,
    //         exit,
    //     )
    //     .map_err(|err| BankForksUtilsError::BankFromSnapshotsDirectory {
    //         source: err,
    //         path: fastboot_snapshot.snapshot_path(),
    //     })?;
    //     bank
    // } else {
    //     // Given that we are going to boot from an archive, the append vecs held in the snapshot dirs for fast-boot should
    //     // be released.  They will be released by the account_background_service anyway.  But in the case of the account_paths
    //     // using memory-mounted file system, they are not released early enough to give space for the new append-vecs from
    //     // the archives, causing the out-of-memory problem.  So, purge the snapshot dirs upfront before loading from the archive.
    //     snapshot_utils::purge_all_bank_snapshots(&snapshot_config.bank_snapshots_dir);

    //     let (bank, _) = snapshot_bank_utils::bank_from_snapshot_archives(
    //         &account_paths,
    //         &snapshot_config.bank_snapshots_dir,
    //         &full_snapshot_archive_info,
    //         incremental_snapshot_archive_info.as_ref(),
    //         genesis_config,
    //         &process_options.runtime_config,
    //         process_options.debug_keys.clone(),
    //         None,
    //         process_options.account_indexes.clone(),
    //         process_options.limit_load_slot_count_from_snapshot,
    //         process_options.shrink_ratio,
    //         process_options.accounts_db_test_hash_calculation,
    //         process_options.accounts_db_skip_shrink,
    //         process_options.accounts_db_force_initial_clean,
    //         process_options.verify_index,
    //         process_options.accounts_db_config.clone(),
    //         accounts_update_notifier,
    //         exit,
    //     )
    //     .map_err(|err| BankForksUtilsError::BankFromSnapshotsArchive {
    //         source: err,
    //         full_snapshot_archive: full_snapshot_archive_info.path().display().to_string(),
    //         incremental_snapshot_archive: incremental_snapshot_archive_info
    //             .as_ref()
    //             .map(|archive| archive.path().display().to_string())
    //             .unwrap_or("none".to_string()),
    //     })?;
    //     bank
    // };

    // let full_snapshot_hash = FullSnapshotHash((
    //     full_snapshot_archive_info.slot(),
    //     *full_snapshot_archive_info.hash(),
    // ));
    // let incremental_snapshot_hash =
    //     incremental_snapshot_archive_info.map(|incremental_snapshot_archive_info| {
    //         IncrementalSnapshotHash((
    //             incremental_snapshot_archive_info.slot(),
    //             *incremental_snapshot_archive_info.hash(),
    //         ))
    //     });
    // let starting_snapshot_hashes = StartingSnapshotHashes {
    //     full: full_snapshot_hash,
    //     incremental: incremental_snapshot_hash,
    // };
    // Ok((BankForks::new_rw_arc(bank), starting_snapshot_hashes))
    Ok(())
}

// /// Build bank from a snapshot (a snapshot directory, not a snapshot archive)
// #[allow(clippy::too_many_arguments)]
// pub fn bank_from_snapshot_dir(
//     account_paths: &[PathBuf],
//     bank_snapshot: &BankSnapshotInfo,
//     genesis_config: &GenesisConfig,
//     runtime_config: &RuntimeConfig,
//     debug_keys: Option<Arc<HashSet<Pubkey>>>,
//     additional_builtins: Option<&[BuiltinPrototype]>,
//     limit_load_slot_count_from_snapshot: Option<usize>,
//     verify_index: bool,
//     accounts_db_config: Option<AccountsDbConfig>,
//     accounts_update_notifier: Option<AccountsUpdateNotifier>,
//     exit: Arc<AtomicBool>,
// ) -> snapshot_utils::Result<(Bank, BankFromDirTimings)> {
//     info!(
//         "Loading bank from snapshot dir: {}",
//         bank_snapshot.snapshot_dir.display()
//     );

//     // Clear the contents of the account paths run directories.  When constructing the bank, the appendvec
//     // files will be extracted from the snapshot hardlink directories into these run/ directories.
//     for path in account_paths {
//         delete_contents_of_path(path);
//     }

//     let next_append_vec_id = Arc::new(AtomicAccountsFileId::new(0));
//     let storage_access = accounts_db_config
//         .as_ref()
//         .map(|config| config.storage_access)
//         .unwrap_or_default();

//     let (storage, measure_rebuild_storages) = measure_time!(
//         rebuild_storages_from_snapshot_dir(
//             bank_snapshot,
//             account_paths,
//             next_append_vec_id.clone(),
//             storage_access,
//         )?,
//         "rebuild storages from snapshot dir"
//     );
//     info!("{}", measure_rebuild_storages);

//     let next_append_vec_id =
//         Arc::try_unwrap(next_append_vec_id).expect("this is the only strong reference");
//     let storage_and_next_append_vec_id = StorageAndNextAccountsFileId {
//         storage,
//         next_append_vec_id,
//     };
//     let ((bank, _info), measure_rebuild_bank) = measure_time!(
//         rebuild_bank_from_snapshot(
//             bank_snapshot,
//             account_paths,
//             storage_and_next_append_vec_id,
//             genesis_config,
//             runtime_config,
//             debug_keys,
//             additional_builtins,
//             limit_load_slot_count_from_snapshot,
//             verify_index,
//             accounts_db_config,
//             accounts_update_notifier,
//             exit,
//         )?,
//         "rebuild bank from snapshot"
//     );
//     info!("{}", measure_rebuild_bank);

//     // Skip bank.verify_snapshot_bank.  Subsequent snapshot requests/accounts hash verification requests
//     // will calculate and check the accounts hash, so we will still have safety/correctness there.
//     bank.set_initial_accounts_hash_verification_completed();

//     let timings = BankFromDirTimings {
//         rebuild_storages_us: measure_rebuild_storages.as_us(),
//         rebuild_bank_us: measure_rebuild_bank.as_us(),
//     };
//     datapoint_info!(
//         "bank_from_snapshot_dir",
//         ("rebuild_storages_us", timings.rebuild_storages_us, i64),
//         ("rebuild_bank_us", timings.rebuild_bank_us, i64),
//     );
//     Ok((bank, timings))
// }
