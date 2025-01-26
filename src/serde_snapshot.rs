use bincode::{Error, Options};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use solana_accounts_db::accounts_hash::{SerdeAccountsDeltaHash, SerdeAccountsHash};
use solana_lattice_hash::lt_hash::LtHash;
use solana_runtime::{
    bank::{builtins::BuiltinPrototype, Bank},
    epoch_stakes::{EpochStakes, VersionedEpochStakes},
    runtime_config::RuntimeConfig,
    serde_snapshot::{
        BankFromStreamsInfo, BankIncrementalSnapshotPersistence, ReconstructedAccountsDbInfo,
        SnapshotStreams,
    },
    stakes::Stakes,
};
use solana_sdk::deserialize_utils::default_on_eof;

const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;
use crate::bank_forks_utils::StorageAndNextAccountsFileId;

use {
    log::*,
    solana_accounts_db::{
        account_storage::meta::StoredMetaWriteVersion,
        accounts::Accounts,
        accounts_db::{
            stats::BankHashStats, AccountStorageEntry, AccountsDb, AccountsDbConfig,
            AccountsFileId, AtomicAccountsFileId, DuplicatesLtHash, IndexGenerationInfo,
        },
        accounts_file::{AccountsFile, StorageAccess},
        accounts_hash::{AccountsDeltaHash, AccountsHash},
        accounts_update_notifier_interface::AccountsUpdateNotifier,
        ancestors::AncestorsForSerialization,
        blockhash_queue::BlockhashQueue,
        epoch_accounts_hash::EpochAccountsHash,
    },
    solana_measure::measure::Measure,
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        epoch_schedule::EpochSchedule,
        fee_calculator::{FeeCalculator, FeeRateGovernor},
        genesis_config::GenesisConfig,
        hard_forks::HardForks,
        hash::Hash,
        inflation::Inflation,
        pubkey::Pubkey,
        rent_collector::RentCollector,
        stake::state::Delegation,
    },
    std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        io::{self, BufReader, BufWriter, Read, Write},
        path::{Path, PathBuf},
        result::Result,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::Builder,
    },
};

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Default, Clone, PartialEq, Eq, Debug, Deserialize)]
struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

/// Bank's common fields shared by all supported snapshot versions for deserialization.
/// Sync fields with BankFieldsToSerialize! This is paired with it.
/// All members are made public to remain Bank's members private and to make versioned deserializer workable on this
/// Note that some fields are missing from the serializer struct. This is because of fields added later.
/// Since it is difficult to insert fields to serialize/deserialize against existing code already deployed,
/// new fields can be optionally serialized and optionally deserialized. At some point, the serialization and
/// deserialization will use a new mechanism or otherwise be in sync more clearly.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "dev-context-only-utils", derive(PartialEq))]
pub struct BankFieldsToDeserialize {
    pub(crate) blockhash_queue: BlockhashQueue,
    pub(crate) ancestors: AncestorsForSerialization,
    pub(crate) hash: Hash,
    pub(crate) parent_hash: Hash,
    pub(crate) parent_slot: Slot,
    pub(crate) hard_forks: HardForks,
    pub(crate) transaction_count: u64,
    pub(crate) tick_height: u64,
    pub(crate) signature_count: u64,
    pub(crate) capitalization: u64,
    pub(crate) max_tick_height: u64,
    pub(crate) hashes_per_tick: Option<u64>,
    pub(crate) ticks_per_slot: u64,
    pub(crate) ns_per_slot: u128,
    pub(crate) genesis_creation_time: UnixTimestamp,
    pub(crate) slots_per_year: f64,
    pub(crate) slot: Slot,
    pub(crate) epoch: Epoch,
    pub(crate) block_height: u64,
    pub(crate) collector_id: Pubkey,
    pub(crate) collector_fees: u64,
    pub(crate) fee_rate_governor: FeeRateGovernor,
    pub(crate) collected_rent: u64,
    pub(crate) rent_collector: RentCollector,
    pub(crate) epoch_schedule: EpochSchedule,
    pub(crate) inflation: Inflation,
    pub(crate) stakes: Stakes<Delegation>,
    pub(crate) epoch_stakes: HashMap<Epoch, EpochStakes>,
    pub(crate) is_delta: bool,
    pub(crate) accounts_data_len: u64,
    pub(crate) incremental_snapshot_persistence: Option<BankIncrementalSnapshotPersistence>,
    pub(crate) epoch_accounts_hash: Option<Hash>,
}

impl From<DeserializableVersionedBank> for BankFieldsToDeserialize {
    fn from(dvb: DeserializableVersionedBank) -> Self {
        BankFieldsToDeserialize {
            blockhash_queue: dvb.blockhash_queue,
            ancestors: dvb.ancestors,
            hash: dvb.hash,
            parent_hash: dvb.parent_hash,
            parent_slot: dvb.parent_slot,
            hard_forks: dvb.hard_forks,
            transaction_count: dvb.transaction_count,
            tick_height: dvb.tick_height,
            signature_count: dvb.signature_count,
            capitalization: dvb.capitalization,
            max_tick_height: dvb.max_tick_height,
            hashes_per_tick: dvb.hashes_per_tick,
            ticks_per_slot: dvb.ticks_per_slot,
            ns_per_slot: dvb.ns_per_slot,
            genesis_creation_time: dvb.genesis_creation_time,
            slots_per_year: dvb.slots_per_year,
            accounts_data_len: dvb.accounts_data_len,
            slot: dvb.slot,
            epoch: dvb.epoch,
            block_height: dvb.block_height,
            collector_id: dvb.collector_id,
            collector_fees: dvb.collector_fees,
            fee_rate_governor: dvb.fee_rate_governor,
            collected_rent: dvb.collected_rent,
            rent_collector: dvb.rent_collector,
            epoch_schedule: dvb.epoch_schedule,
            inflation: dvb.inflation,
            stakes: dvb.stakes,
            epoch_stakes: dvb.epoch_stakes,
            is_delta: dvb.is_delta,
            incremental_snapshot_persistence: None,
            epoch_accounts_hash: None,
        }
    }
}

/// Helper type to wrap BankFields when reconstructing Bank from either just a full
/// snapshot, or both a full and incremental snapshot
#[derive(Debug)]
pub struct SnapshotBankFields {
    full: BankFieldsToDeserialize,
    incremental: Option<BankFieldsToDeserialize>,
}

impl SnapshotBankFields {
    /// Collapse the SnapshotBankFields into a single (the latest) BankFieldsToDeserialize.
    pub fn collapse_into(self) -> BankFieldsToDeserialize {
        self.incremental.unwrap_or(self.full)
    }
}

/// Helper type to wrap AccountsDbFields when reconstructing AccountsDb from either just a full
/// snapshot, or both a full and incremental snapshot
#[derive(Debug)]
pub struct SnapshotAccountsDbFields<T> {
    full_snapshot_accounts_db_fields: AccountsDbFields<T>,
    incremental_snapshot_accounts_db_fields: Option<AccountsDbFields<T>>,
}

impl<T> SnapshotAccountsDbFields<T> {
    /// Collapse the SnapshotAccountsDbFields into a single AccountsDbFields.  If there is no
    /// incremental snapshot, this returns the AccountsDbFields from the full snapshot.
    /// Otherwise, use the AccountsDbFields from the incremental snapshot, and a combination
    /// of the storages from both the full and incremental snapshots.
    fn collapse_into(self) -> Result<AccountsDbFields<T>, Error> {
        match self.incremental_snapshot_accounts_db_fields {
            None => Ok(self.full_snapshot_accounts_db_fields),
            Some(AccountsDbFields(
                mut incremental_snapshot_storages,
                incremental_snapshot_version,
                incremental_snapshot_slot,
                incremental_snapshot_bank_hash_info,
                incremental_snapshot_historical_roots,
                incremental_snapshot_historical_roots_with_hash,
            )) => {
                let full_snapshot_storages = self.full_snapshot_accounts_db_fields.0;
                let full_snapshot_slot = self.full_snapshot_accounts_db_fields.2;

                // filter out incremental snapshot storages with slot <= full snapshot slot
                incremental_snapshot_storages.retain(|slot, _| *slot > full_snapshot_slot);

                // There must not be any overlap in the slots of storages between the full snapshot and the incremental snapshot
                incremental_snapshot_storages
                    .iter()
                    .all(|storage_entry| !full_snapshot_storages.contains_key(storage_entry.0)).then_some(()).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "Snapshots are incompatible: There are storages for the same slot in both the full snapshot and the incremental snapshot!")
                    })?;

                let mut combined_storages = full_snapshot_storages;
                combined_storages.extend(incremental_snapshot_storages);

                Ok(AccountsDbFields(
                    combined_storages,
                    incremental_snapshot_version,
                    incremental_snapshot_slot,
                    incremental_snapshot_bank_hash_info,
                    incremental_snapshot_historical_roots,
                    incremental_snapshot_historical_roots_with_hash,
                ))
            }
        }
    }
}

// Deserializable version of Bank which need not be serializable,
// because it's handled by SerializableVersionedBank.
// So, sync fields with it!
#[derive(Clone, Deserialize)]
struct DeserializableVersionedBank {
    blockhash_queue: BlockhashQueue,
    ancestors: AncestorsForSerialization,
    hash: Hash,
    parent_hash: Hash,
    parent_slot: Slot,
    hard_forks: HardForks,
    transaction_count: u64,
    tick_height: u64,
    signature_count: u64,
    capitalization: u64,
    max_tick_height: u64,
    hashes_per_tick: Option<u64>,
    ticks_per_slot: u64,
    ns_per_slot: u128,
    genesis_creation_time: UnixTimestamp,
    slots_per_year: f64,
    accounts_data_len: u64,
    slot: Slot,
    epoch: Epoch,
    block_height: u64,
    collector_id: Pubkey,
    collector_fees: u64,
    _fee_calculator: FeeCalculator,
    fee_rate_governor: FeeRateGovernor,
    collected_rent: u64,
    rent_collector: RentCollector,
    epoch_schedule: EpochSchedule,
    inflation: Inflation,
    stakes: Stakes<Delegation>,
    #[allow(dead_code)]
    unused_accounts: UnusedAccounts,
    epoch_stakes: HashMap<Epoch, EpochStakes>,
    is_delta: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn bank_from_streams<R>(
    snapshot_streams: &mut SnapshotStreams<R>,
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
) -> std::result::Result<(Bank, BankFromStreamsInfo), Error>
where
    R: Read,
{
    let (bank_fields, accounts_db_fields) = fields_from_streams(snapshot_streams)?;
    let (bank, info) = reconstruct_bank_from_fields(
        bank_fields,
        accounts_db_fields,
        genesis_config,
        runtime_config,
        account_paths,
        storage_and_next_append_vec_id,
        debug_keys,
        additional_builtins,
        limit_load_slot_count_from_snapshot,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
        exit,
    )?;
    Ok((
        bank,
        BankFromStreamsInfo {
            duplicates_lt_hash: info.duplicates_lt_hash,
        },
    ))
}

/// This struct contains side-info while reconstructing the bank from fields
#[derive(Debug)]
struct ReconstructedBankInfo {
    duplicates_lt_hash: Option<Box<DuplicatesLtHash>>,
}

pub(super) trait SerializableStorage {
    fn id(&self) -> SerializedAccountsFileId;
    fn current_len(&self) -> usize;
}

impl SerializableStorage for SerializableAccountStorageEntry {
    fn id(&self) -> SerializedAccountsFileId {
        self.id
    }
    fn current_len(&self) -> usize {
        self.accounts_current_len
    }
}

impl From<&AccountStorageEntry> for SerializableAccountStorageEntry {
    fn from(rhs: &AccountStorageEntry) -> Self {
        Self {
            id: rhs.id() as SerializedAccountsFileId,
            accounts_current_len: rhs.accounts.len(),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn reconstruct_bank_from_fields<E>(
    bank_fields: SnapshotBankFields,
    snapshot_accounts_db_fields: SnapshotAccountsDbFields<E>,
    genesis_config: &GenesisConfig,
    runtime_config: &RuntimeConfig,
    account_paths: &[PathBuf],
    storage_and_next_append_vec_id: StorageAndNextAccountsFileId,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&[BuiltinPrototype]>,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
) -> Result<(Bank, ReconstructedBankInfo), Error>
where
    E: SerializableStorage + std::marker::Sync,
{
    let capitalizations = (
        bank_fields.full.capitalization,
        bank_fields
            .incremental
            .as_ref()
            .map(|bank_fields| bank_fields.capitalization),
    );
    let bank_fields = bank_fields.collapse_into();
    let (accounts_db, reconstructed_accounts_db_info) = reconstruct_accountsdb_from_fields(
        snapshot_accounts_db_fields,
        account_paths,
        storage_and_next_append_vec_id,
        genesis_config,
        limit_load_slot_count_from_snapshot,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
        exit,
        bank_fields.epoch_accounts_hash,
        capitalizations,
        bank_fields.incremental_snapshot_persistence.as_ref(),
    )?;

    // let bank_rc = BankRc::new(Accounts::new(Arc::new(accounts_db)));
    // let runtime_config = Arc::new(runtime_config.clone());

    // // if limit_load_slot_count_from_snapshot is set, then we need to side-step some correctness checks beneath this call
    // let debug_do_not_add_builtins = limit_load_slot_count_from_snapshot.is_some();
    // let bank = Bank::new_from_fields(
    //     bank_rc,
    //     genesis_config,
    //     runtime_config,
    //     bank_fields,
    //     debug_keys,
    //     additional_builtins,
    //     debug_do_not_add_builtins,
    //     reconstructed_accounts_db_info.accounts_data_len,
    // );

    // info!("rent_collector: {:?}", bank.rent_collector());

    todo!()

    // Ok((
    //     bank,
    //     ReconstructedBankInfo {
    //         duplicates_lt_hash: reconstructed_accounts_db_info.duplicates_lt_hash,
    //     },
    // ))
}

/// The serialized AccountsFileId type is fixed as usize
pub(crate) type SerializedAccountsFileId = usize;

// Serializable version of AccountStorageEntry for snapshot format
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SerializableAccountStorageEntry {
    id: SerializedAccountsFileId,
    accounts_current_len: usize,
}

pub(crate) fn fields_from_streams(
    snapshot_streams: &mut SnapshotStreams<impl Read>,
) -> std::result::Result<
    (
        SnapshotBankFields,
        SnapshotAccountsDbFields<SerializableAccountStorageEntry>,
    ),
    Error,
> {
    let (full_snapshot_bank_fields, full_snapshot_accounts_db_fields) =
        fields_from_stream(snapshot_streams.full_snapshot_stream)?;
    let (incremental_snapshot_bank_fields, incremental_snapshot_accounts_db_fields) =
        snapshot_streams
            .incremental_snapshot_stream
            .as_mut()
            .map(|stream| fields_from_stream(stream))
            .transpose()?
            .unzip();

    let snapshot_bank_fields = SnapshotBankFields {
        full: full_snapshot_bank_fields,
        incremental: incremental_snapshot_bank_fields,
    };
    let snapshot_accounts_db_fields = SnapshotAccountsDbFields {
        full_snapshot_accounts_db_fields,
        incremental_snapshot_accounts_db_fields,
    };
    Ok((snapshot_bank_fields, snapshot_accounts_db_fields))
}

pub(crate) fn fields_from_stream<R: Read>(
    snapshot_stream: &mut BufReader<R>,
) -> std::result::Result<
    (
        BankFieldsToDeserialize,
        AccountsDbFields<SerializableAccountStorageEntry>,
    ),
    Error,
> {
    deserialize_bank_fields(snapshot_stream)
}

/// Snapshot serde-safe AccountsLtHash
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[serde_with::serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SerdeAccountsLtHash(
    // serde only has array support up to 32 elements; anything larger needs to be handled manually
    // see https://github.com/serde-rs/serde/issues/1937 for more information
    #[serde_as(as = "[_; LtHash::NUM_ELEMENTS]")] pub [u16; LtHash::NUM_ELEMENTS],
);

/// Extra fields that are deserialized from the end of snapshots.
///
/// Note that this struct's fields should stay synced with the fields in
/// ExtraFieldsToSerialize with the exception that new "extra fields" should be
/// added to this struct a minor release before they are added to the serialize
/// struct.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(feature = "dev-context-only-utils", derive(PartialEq))]
#[derive(Clone, Debug, Deserialize)]
struct ExtraFieldsToDeserialize {
    #[serde(deserialize_with = "default_on_eof")]
    lamports_per_signature: u64,
    #[serde(deserialize_with = "default_on_eof")]
    incremental_snapshot_persistence: Option<BankIncrementalSnapshotPersistence>,
    #[serde(deserialize_with = "default_on_eof")]
    epoch_accounts_hash: Option<Hash>,
    #[serde(deserialize_with = "default_on_eof")]
    versioned_epoch_stakes: HashMap<u64, VersionedEpochStakes>,
    #[serde(deserialize_with = "default_on_eof")]
    #[allow(dead_code)]
    accounts_lt_hash: Option<SerdeAccountsLtHash>,
}

fn deserialize_from<R, T>(reader: R) -> bincode::Result<T>
where
    R: Read,
    T: DeserializeOwned,
{
    bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from::<R, T>(reader)
}

fn deserialize_accounts_db_fields<R>(
    stream: &mut BufReader<R>,
) -> Result<AccountsDbFields<SerializableAccountStorageEntry>, Error>
where
    R: Read,
{
    deserialize_from::<_, _>(stream)
}

fn deserialize_bank_fields<R>(
    mut stream: &mut BufReader<R>,
) -> Result<
    (
        BankFieldsToDeserialize,
        AccountsDbFields<SerializableAccountStorageEntry>,
    ),
    Error,
>
where
    R: Read,
{
    let mut bank_fields: BankFieldsToDeserialize =
        deserialize_from::<_, DeserializableVersionedBank>(&mut stream)?.into();
    let accounts_db_fields = deserialize_accounts_db_fields(stream)?;
    let extra_fields = deserialize_from(stream)?;

    // Process extra fields
    let ExtraFieldsToDeserialize {
        lamports_per_signature,
        incremental_snapshot_persistence,
        epoch_accounts_hash,
        versioned_epoch_stakes,
        accounts_lt_hash: _,
    } = extra_fields;

    bank_fields.fee_rate_governor = bank_fields
        .fee_rate_governor
        .clone_with_lamports_per_signature(lamports_per_signature);
    bank_fields.incremental_snapshot_persistence = incremental_snapshot_persistence;
    bank_fields.epoch_accounts_hash = epoch_accounts_hash;

    // If we deserialize the new epoch stakes, add all of the entries into the
    // other deserialized map which could still have old epoch stakes entries
    bank_fields.epoch_stakes.extend(
        versioned_epoch_stakes
            .into_iter()
            .map(|(epoch, versioned_epoch_stakes)| (epoch, versioned_epoch_stakes.into())),
    );

    Ok((bank_fields, accounts_db_fields))
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BankHashInfo {
    accounts_delta_hash: SerdeAccountsDeltaHash,
    accounts_hash: SerdeAccountsHash,
    stats: BankHashStats,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AccountsDbFields<T>(
    HashMap<Slot, Vec<T>>,
    StoredMetaWriteVersion,
    Slot,
    BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    Vec<(Slot, Hash)>,
);

#[allow(clippy::too_many_arguments)]
fn reconstruct_accountsdb_from_fields<E>(
    snapshot_accounts_db_fields: SnapshotAccountsDbFields<E>,
    account_paths: &[PathBuf],
    storage_and_next_append_vec_id: StorageAndNextAccountsFileId,
    genesis_config: &GenesisConfig,
    limit_load_slot_count_from_snapshot: Option<usize>,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
    exit: Arc<AtomicBool>,
    epoch_accounts_hash: Option<Hash>,
    capitalizations: (u64, Option<u64>),
    incremental_snapshot_persistence: Option<&BankIncrementalSnapshotPersistence>,
) -> Result<(AccountsDb, ReconstructedAccountsDbInfo), Error>
where
    E: SerializableStorage + std::marker::Sync,
{
    let mut accounts_db = AccountsDb::new_with_config(
        account_paths.to_vec(),
        accounts_db_config,
        accounts_update_notifier,
        exit,
    );

    if let Some(epoch_accounts_hash) = epoch_accounts_hash {
        accounts_db
            .epoch_accounts_hash_manager
            .set_valid(EpochAccountsHash::new(epoch_accounts_hash), 0);
    }

    error!(
        "ACCOUNTS ABOBA2: {}",
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
    }

    error!(
        "Got min_key: {:?}, max_key: {:?}, total: {:?}, on_curve: {:?}",
        min_key,
        max_key,
        accounts.len(),
        accounts_on_curve.len()
    );

    // Store the accounts hash & capitalization, from the full snapshot, in the new AccountsDb
    {
        let AccountsDbFields(_, _, slot, bank_hash_info, _, _) =
            &snapshot_accounts_db_fields.full_snapshot_accounts_db_fields;

        if let Some(incremental_snapshot_persistence) = incremental_snapshot_persistence {
            // If we've booted from local state that was originally intended to be an incremental
            // snapshot, then we will use the incremental snapshot persistence field to set the
            // initial accounts hashes in accounts db.
            let old_accounts_hash = accounts_db.set_accounts_hash_from_snapshot(
                incremental_snapshot_persistence.full_slot,
                incremental_snapshot_persistence.full_hash.clone(),
                incremental_snapshot_persistence.full_capitalization,
            );
            assert!(
                old_accounts_hash.is_none(),
                "There should not already be an AccountsHash at slot {slot}: {old_accounts_hash:?}",
            );
            let old_incremental_accounts_hash = accounts_db
                .set_incremental_accounts_hash_from_snapshot(
                    *slot,
                    incremental_snapshot_persistence.incremental_hash.clone(),
                    incremental_snapshot_persistence.incremental_capitalization,
                );
            assert!(
                old_incremental_accounts_hash.is_none(),
                "There should not already be an IncrementalAccountsHash at slot {slot}: {old_incremental_accounts_hash:?}",
            );
        } else {
            // Otherwise, we've booted from a snapshot archive, or from local state that was *not*
            // intended to be an incremental snapshot.
            let old_accounts_hash = accounts_db.set_accounts_hash_from_snapshot(
                *slot,
                bank_hash_info.accounts_hash.clone(),
                capitalizations.0,
            );
            assert!(
                old_accounts_hash.is_none(),
                "There should not already be an AccountsHash at slot {slot}: {old_accounts_hash:?}",
            );
        }
    }

    // Store the accounts hash & capitalization, from the incremental snapshot, in the new AccountsDb
    {
        if let Some(AccountsDbFields(_, _, slot, bank_hash_info, _, _)) =
            snapshot_accounts_db_fields
                .incremental_snapshot_accounts_db_fields
                .as_ref()
        {
            if let Some(incremental_snapshot_persistence) = incremental_snapshot_persistence {
                // Use the presence of a BankIncrementalSnapshotPersistence to indicate the
                // Incremental Accounts Hash feature is enabled, and use its accounts hashes
                // instead of `BankHashInfo`'s.
                let AccountsDbFields(_, _, full_slot, full_bank_hash_info, _, _) =
                    &snapshot_accounts_db_fields.full_snapshot_accounts_db_fields;
                let full_accounts_hash = &full_bank_hash_info.accounts_hash;
                assert_eq!(
                    incremental_snapshot_persistence.full_slot, *full_slot,
                    "The incremental snapshot's base slot ({}) must match the full snapshot's slot ({full_slot})!",
                    incremental_snapshot_persistence.full_slot,
                );
                assert_eq!(
                    &incremental_snapshot_persistence.full_hash, full_accounts_hash,
                    "The incremental snapshot's base accounts hash ({}) must match the full snapshot's accounts hash ({})!",
                    &incremental_snapshot_persistence.full_hash.0, full_accounts_hash.0,
                );
                assert_eq!(
                    incremental_snapshot_persistence.full_capitalization, capitalizations.0,
                    "The incremental snapshot's base capitalization ({}) must match the full snapshot's capitalization ({})!",
                    incremental_snapshot_persistence.full_capitalization, capitalizations.0,
                );
                let old_incremental_accounts_hash = accounts_db
                    .set_incremental_accounts_hash_from_snapshot(
                        *slot,
                        incremental_snapshot_persistence.incremental_hash.clone(),
                        incremental_snapshot_persistence.incremental_capitalization,
                    );
                assert!(
                    old_incremental_accounts_hash.is_none(),
                    "There should not already be an IncrementalAccountsHash at slot {slot}: {old_incremental_accounts_hash:?}",
                );
            } else {
                // ..and without a BankIncrementalSnapshotPersistence then the Incremental Accounts
                // Hash feature is disabled; the accounts hash in `BankHashInfo` is valid.
                let old_accounts_hash = accounts_db.set_accounts_hash_from_snapshot(
                    *slot,
                    bank_hash_info.accounts_hash.clone(),
                    capitalizations
                        .1
                        .expect("capitalization from incremental snapshot"),
                );
                assert!(
                    old_accounts_hash.is_none(),
                    "There should not already be an AccountsHash at slot {slot}: {old_accounts_hash:?}",
                );
            };
        }
    }

    let AccountsDbFields(
        _snapshot_storages,
        snapshot_version,
        snapshot_slot,
        snapshot_bank_hash_info,
        _snapshot_historical_roots,
        _snapshot_historical_roots_with_hash,
    ) = snapshot_accounts_db_fields.collapse_into()?;

    error!(
        "ACCOUNTS ABOBA3: {}",
        storage_and_next_append_vec_id.storage.len()
    );
    let mut accounts = HashSet::new();
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
        });
    }

    error!(
        "Got min_key: {:?}, max_key: {:?}, total: {:?}",
        min_key,
        max_key,
        accounts.len()
    );

    // Ensure all account paths exist
    for path in &accounts_db.paths {
        std::fs::create_dir_all(path)
            .unwrap_or_else(|err| panic!("Failed to create directory {}: {}", path.display(), err));
    }

    let StorageAndNextAccountsFileId {
        storage,
        next_append_vec_id,
    } = storage_and_next_append_vec_id;

    error!("ACCOUNTS ABOBA4: {}", storage.len());
    let mut accounts = HashSet::new();
    // Find the smallest and largest keys
    let (min_key, max_key) =
        storage
            .iter()
            .fold((None::<u64>, None::<u64>), |(min, max), entry| {
                let key = *entry.key();
                (
                    Some(min.map_or(key, |m| m.min(key))),
                    Some(max.map_or(key, |m| m.max(key))),
                )
            });
    for (_, storage) in storage.clone() {
        storage.storage.accounts.scan_accounts(|acc| {
            let key = acc.pubkey();
            accounts.insert(key.clone());
        });
    }

    error!(
        "Got min_key: {:?}, max_key: {:?}, total: {:?}",
        min_key,
        max_key,
        accounts.len()
    );

    assert!(
        !storage.is_empty(),
        "At least one storage entry must exist from deserializing stream"
    );

    let next_append_vec_id = next_append_vec_id.load(Ordering::Acquire);
    let max_append_vec_id = next_append_vec_id - 1;
    assert!(
        max_append_vec_id <= AccountsFileId::MAX / 2,
        "Storage id {max_append_vec_id} larger than allowed max"
    );

    // Process deserialized data, set necessary fields in self
    let old_accounts_delta_hash = accounts_db.set_accounts_delta_hash_from_snapshot(
        snapshot_slot,
        snapshot_bank_hash_info.accounts_delta_hash,
    );
    assert!(
        old_accounts_delta_hash.is_none(),
        "There should not already be an AccountsDeltaHash at slot {snapshot_slot}: {old_accounts_delta_hash:?}",
        );
    let old_stats = accounts_db
        .update_bank_hash_stats_from_snapshot(snapshot_slot, snapshot_bank_hash_info.stats);
    assert!(
        old_stats.is_none(),
        "There should not already be a BankHashStats at slot {snapshot_slot}: {old_stats:?}",
    );
    accounts_db.storage.initialize(storage);
    accounts_db
        .next_id
        .store(next_append_vec_id, Ordering::Release);
    accounts_db
        .write_version
        .fetch_add(snapshot_version, Ordering::Release);

    let mut measure_notify = Measure::start("accounts_notify");

    let accounts_db = Arc::new(accounts_db);
    let accounts_db_clone = accounts_db.clone();
    let handle = Builder::new()
        .name("solNfyAccRestor".to_string())
        .spawn(move || {
            accounts_db_clone.notify_account_restore_from_snapshot();
        })
        .unwrap();

    let IndexGenerationInfo {
        accounts_data_len,
        rent_paying_accounts_by_partition,
        duplicates_lt_hash,
    } = accounts_db.generate_index(
        limit_load_slot_count_from_snapshot,
        verify_index,
        genesis_config,
    );
    accounts_db
        .accounts_index
        .rent_paying_accounts_by_partition
        .set(rent_paying_accounts_by_partition)
        .unwrap();

    handle.join().unwrap();
    measure_notify.stop();

    Ok((
        Arc::try_unwrap(accounts_db).unwrap(),
        ReconstructedAccountsDbInfo {
            accounts_data_len,
            duplicates_lt_hash,
        },
    ))
}
