use anyhow::Result;
use core::panic;
use log::info;
use std::{
    fmt::Display,
    path::{Path, PathBuf},
    process::exit,
    str::FromStr,
    sync::Arc,
};

use clap::{
    crate_description, crate_name, value_t_or_exit, App, AppSettings, Arg, ArgMatches, SubCommand,
};
use solana_accounts_db::hardened_unpack::open_genesis_config;
use solana_bruteforce::{
    args::{self, parse_process_options},
    kria,
    ledger_path::canonicalize_ledger_path,
    ledger_utils::{get_access_type, load_and_process_ledger, open_blockstore},
};
use solana_core::validator::BlockVerificationMethod;
use solana_sdk::{genesis_config::GenesisConfig, pubkey::Pubkey, signature::read_keypair_file};

fn is_parsable_generic<U, T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + Display,
    U: FromStr,
    U::Err: Display,
{
    string
        .as_ref()
        .parse::<U>()
        .map(|_| ())
        .map_err(|err| format!("error parsing '{string}': {err}"))
}

// Return an error if a pubkey cannot be parsed.
pub fn is_pubkey<T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + Display,
{
    is_parsable_generic::<Pubkey, _>(string)
}

// Return an error if a keypair file cannot be parsed.
pub fn is_keypair<T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + Display,
{
    read_keypair_file(string.as_ref())
        .map(|_| ())
        .map_err(|err| format!("{err}"))
}

// Return an error if string cannot be parsed as pubkey string or keypair file location
pub fn is_pubkey_or_keypair<T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + Display,
{
    is_pubkey(string.as_ref()).or_else(|_| is_keypair(string))
}

// For our current version of CLAP, the value passed to Arg::default_value()
// must be a &str. But, we can't convert an integer to a &str at compile time.
// So, declare this constant and enforce equality with the following unit test
// test_max_genesis_archive_unpacked_size_constant
const MAX_GENESIS_ARCHIVE_UNPACKED_SIZE_STR: &str = "10485760";

fn main() -> Result<()> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info) // Set default log level to `info`
        .init();

    let load_genesis_config_arg = args::load_genesis_arg();
    let accounts_db_config_args = args::accounts_db_args();
    let snapshot_config_args = args::snapshot_args();

    let accounts_db_test_hash_calculation_arg = Arg::with_name("accounts_db_test_hash_calculation")
        .long("accounts-db-test-hash-calculation")
        .help("Enable hash calculation test");
    // let halt_at_slot_arg = Arg::with_name("halt_at_slot")
    //     .long("halt-at-slot")
    //     .value_name("SLOT")
    //     .validator(is_slot)
    //     .takes_value(true)
    //     .help("Halt processing at the given slot");
    let os_memory_stats_reporting_arg = Arg::with_name("os_memory_stats_reporting")
        .long("os-memory-stats-reporting")
        .help("Enable reporting of OS memory statistics.");
    // let halt_at_slot_store_hash_raw_data = Arg::with_name("halt_at_slot_store_hash_raw_data")
    //     .long("halt-at-slot-store-hash-raw-data")
    //     .help(
    //         "After halting at slot, run an accounts hash calculation and store the raw hash data \
    //          for debugging.",
    //     )
    // .hidden(hidden_unless_forced());
    let verify_index_arg = Arg::with_name("verify_accounts_index")
        .long("verify-accounts-index")
        .takes_value(false)
        .help("For debugging and tests on accounts index.");
    // let limit_load_slot_count_from_snapshot_arg =
    //     Arg::with_name("limit_load_slot_count_from_snapshot")
    //         .long("limit-load-slot-count-from-snapshot")
    //         .value_name("SLOT")
    //         .validator(is_slot)
    //         .takes_value(true)
    //         .help(
    //             "For debugging and profiling with large snapshots, artificially limit how many \
    //              slots are loaded from a snapshot.",
    //         );
    // let hard_forks_arg = Arg::with_name("hard_forks")
    //     .long("hard-fork")
    //     .value_name("SLOT")
    //     .validator(is_slot)
    //     .multiple(true)
    //     .takes_value(true)
    //     .help("Add a hard fork at this slot");
    let allow_dead_slots_arg = Arg::with_name("allow_dead_slots")
        .long("allow-dead-slots")
        .takes_value(false)
        .help("Output dead slots as well");
    let hashes_per_tick = Arg::with_name("hashes_per_tick")
        .long("hashes-per-tick")
        .value_name("NUM_HASHES|\"sleep\"")
        .takes_value(true)
        .help(
            "How many PoH hashes to roll before emitting the next tick. If \"sleep\", for \
             development sleep for the target tick duration instead of hashing",
        );
    // let snapshot_version_arg = Arg::with_name("snapshot_version")
    //     .long("snapshot-version")
    //     .value_name("SNAPSHOT_VERSION")
    //     .validator(is_parsable::<SnapshotVersion>)
    //     .takes_value(true)
    //     .default_value(SnapshotVersion::default().into())
    //     .help("Output snapshot version");
    let debug_key_arg = Arg::with_name("debug_key")
        .long("debug-key")
        .validator(is_pubkey)
        .value_name("ADDRESS")
        .multiple(true)
        .takes_value(true)
        .help("Log when transactions are processed that reference the given key(s).");

    let geyser_plugin_args = Arg::with_name("geyser_plugin_config")
        .long("geyser-plugin-config")
        .value_name("FILE")
        .takes_value(true)
        .multiple(true)
        .help("Specify the configuration file for the Geyser plugin.");

    // let log_messages_bytes_limit_arg = Arg::with_name("log_messages_bytes_limit")
    //     .long("log-messages-bytes-limit")
    //     .takes_value(true)
    //     .validator(is_parsable::<usize>)
    //     .value_name("BYTES")
    //     .help("Maximum number of bytes written to the program log before truncation");

    let accounts_data_encoding_arg = Arg::with_name("encoding")
        .long("encoding")
        .takes_value(true)
        .possible_values(&["base64", "base64+zstd", "jsonParsed"])
        .default_value("base64")
        .help("Print account data in specified format when printing account contents.");

    let matches = App::new(crate_name!())
        .about(crate_description!())
        .global_setting(AppSettings::ColoredHelp)
        .global_setting(AppSettings::InferSubcommands)
        .global_setting(AppSettings::UnifiedHelpMessage)
        .global_setting(AppSettings::VersionlessSubcommands)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .global(true)
                .default_value("ledger")
                .help("Use DIR as ledger location"),
        )
        .arg(
            Arg::with_name("wal_recovery_mode")
                .long("wal-recovery-mode")
                .value_name("MODE")
                .takes_value(true)
                .global(true)
                .possible_values(&[
                    "tolerate_corrupted_tail_records",
                    "absolute_consistency",
                    "point_in_time",
                    "skip_any_corrupted_record",
                ])
                .help("Mode to recovery the ledger db write ahead log"),
        )
        .arg(
            Arg::with_name("force_update_to_open")
                .long("force-update-to-open")
                .takes_value(false)
                .global(true)
                .help(
                    "Allow commands that would otherwise not alter the blockstore to make \
                     necessary updates in order to open it",
                ),
        )
        .arg(
            Arg::with_name("ignore_ulimit_nofile_error")
                .long("ignore-ulimit-nofile-error")
                .takes_value(false)
                .global(true)
                .help(
                    "Allow opening the blockstore to succeed even if the desired open file \
                     descriptor limit cannot be configured. Use with caution as some commands may \
                     run fine with a reduced file descriptor limit while others will not",
                ),
        )
        .arg(
            Arg::with_name("block_verification_method")
                .long("block-verification-method")
                .value_name("METHOD")
                .takes_value(true)
                .possible_values(BlockVerificationMethod::cli_names())
                .global(true)
                .help(BlockVerificationMethod::cli_message()),
        )
        .arg(
            Arg::with_name("output_format")
                .long("output")
                .value_name("FORMAT")
                .global(true)
                .takes_value(true)
                .possible_values(&["json", "json-compact"])
                .help(
                    "Return information in specified output format, currently only available for \
                     bigtable and program subcommands",
                ),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .global(true)
                .multiple(true)
                .takes_value(false)
                .help("Show additional information where supported"),
        )
        .subcommand(
            SubCommand::with_name("accounts")
                .about("Print account stats and contents after processing the ledger")
                .arg(&load_genesis_config_arg)
                .args(&accounts_db_config_args)
                .args(&snapshot_config_args)
                // .arg(&halt_at_slot_arg)
                // .arg(&hard_forks_arg)
                .arg(&geyser_plugin_args)
                // .arg(&log_messages_bytes_limit_arg)
                .arg(&accounts_data_encoding_arg)
                .arg(
                    Arg::with_name("include_sysvars")
                        .long("include-sysvars")
                        .takes_value(false)
                        .help("Include sysvars too"),
                )
                .arg(
                    Arg::with_name("no_account_contents")
                        .long("no-account-contents")
                        .takes_value(false)
                        .help(
                            "Do not print contents of each account, which is very slow with lots \
                             of accounts.",
                        ),
                )
                .arg(
                    Arg::with_name("no_account_data")
                        .long("no-account-data")
                        .takes_value(false)
                        .help("Do not print account data when printing account contents."),
                )
                .arg(
                    Arg::with_name("account")
                        .long("account")
                        .takes_value(true)
                        .value_name("PUBKEY")
                        .validator(is_pubkey)
                        .multiple(true)
                        .help(
                            "Limit output to accounts corresponding to the specified pubkey(s), \
                            may be specified multiple times",
                        ),
                )
                .arg(
                    Arg::with_name("program_accounts")
                        .long("program-accounts")
                        .takes_value(true)
                        .value_name("PUBKEY")
                        .validator(is_pubkey)
                        .conflicts_with("account")
                        .help("Limit output to accounts owned by the provided program pubkey"),
                ),
        )
        .subcommand(
            SubCommand::with_name("test").arg(
                Arg::with_name("path")
                    .long("path")
                    .takes_value(true)
                    .value_name("PATH"),
            ),
        )
        .get_matches();

    let ledger_path = PathBuf::from(value_t_or_exit!(matches, "ledger_path", String));
    let ledger_path = canonicalize_ledger_path(&ledger_path);

    match matches.subcommand() {
        ("accounts", Some(arg_matches)) => {
            let process_options = parse_process_options(&ledger_path, arg_matches);
            let genesis_config = open_genesis_config_by(&ledger_path, arg_matches);
            let blockstore =
                open_blockstore(&ledger_path, arg_matches, get_access_type(&process_options));

            load_and_process_ledger(
                arg_matches,
                &genesis_config,
                Arc::new(blockstore),
                process_options,
                None,
            )?;
        }

        ("test", Some(_arg_matches)) => {
            info!("Running test");
            let path = _arg_matches.value_of("path").unwrap();
            kria::scan_accounts(path.into())?;
        }

        _ => panic!("Unrecognised subcommand"),
    };

    Ok(())
}

pub fn open_genesis_config_by(ledger_path: &Path, matches: &ArgMatches<'_>) -> GenesisConfig {
    let max_genesis_archive_unpacked_size =
        value_t_or_exit!(matches, "max_genesis_archive_unpacked_size", u64);

    open_genesis_config(ledger_path, max_genesis_archive_unpacked_size).unwrap_or_else(|err| {
        eprintln!("Exiting. Failed to open genesis config: {err}");
        exit(1);
    })
}
