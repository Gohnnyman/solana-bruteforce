use anyhow::Result;
use clap::{crate_description, crate_name, ArgMatches};
use core::panic;
use log::info;
use solana_bruteforce::{
    bruteforce::run_bruteforce,
    config::{load_config, AppConfig},
    scan_accounts,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Initialize logger
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    // Define CLI using `clap 4.5`
    let matches = clap::Command::new(crate_name!())
        .about(crate_description!())
        .arg(
            clap::Arg::new("config")
                .long("config")
                .value_name("CONFIG_PATH")
                .help("Path to the configuration file")
                .default_value("./Config.toml")
                .num_args(1),
        )
        .arg(
            clap::Arg::new("db_host")
                .long("db-host")
                .value_name("HOST")
                .help("PostgreSQL host")
                .default_value("localhost")
                .num_args(1),
        )
        .arg(
            clap::Arg::new("db_port")
                .long("db-port")
                .value_name("PORT")
                .help("PostgreSQL port")
                .default_value("5432")
                .num_args(1),
        )
        .arg(
            clap::Arg::new("db_user")
                .long("db-user")
                .value_name("USER")
                .help("PostgreSQL username")
                .default_value("postgres")
                .num_args(1),
        )
        .arg(
            clap::Arg::new("db_password")
                .long("db-password")
                .value_name("PASSWORD")
                .help("PostgreSQL password")
                .default_value("password")
                .num_args(1),
        )
        .arg(
            clap::Arg::new("db_name")
                .long("db-name")
                .value_name("NAME")
                .help("PostgreSQL database name")
                .default_value("bruteforce")
                .num_args(1),
        )
        .subcommand(
            clap::Command::new("scan_accounts")
                .about(
                    "Scan accounts from the snapshot directory and insert them into the database",
                )
                .arg(
                    clap::Arg::new("path")
                        .long("path")
                        .value_name("PATH")
                        .help("Path to the snapshot directory")
                        .num_args(1)
                        .required(true),
                ),
        )
        .subcommand(
            clap::Command::new("start").about("Start the application with default behavior"),
        )
        .arg_required_else_help(true)
        .subcommand_required(true)
        .get_matches();

    // Extract the path to the configuration file
    let config_path = matches
        .get_one::<String>("config")
        .expect("Config path should always have a default value");

    // Load configuration from the specified path
    let config = load_config(config_path);

    let db_url = construct_db_url(&matches, &config);

    // Match the subcommand
    match matches.subcommand() {
        Some(("scan_accounts", sub_matches)) => {
            info!("Running scan_accounts...");

            // Extract CLI arguments or fallback to Config.toml
            let path = sub_matches
                .get_one::<String>("path")
                .expect("Snapshot path is required")
                .to_string();

            // Run the scan_accounts function
            scan_accounts::scan_accounts(&db_url, path.into()).await?;
        }
        Some(("start", _)) => {
            info!("Running default start behavior...");
            run_bruteforce(&db_url, &config).await?;
        }
        _ => panic!("Unrecognized subcommand"), // Fallback for unknown subcommands
    };

    Ok(())
}

fn construct_db_url(matches: &ArgMatches, config: &AppConfig) -> String {
    let db_host = matches
        .get_one::<String>("db_host")
        .cloned()
        .unwrap_or(config.get_database_host());

    let db_port: u16 = matches
        .get_one::<String>("db_port")
        .cloned()
        .map(|v| v.parse().expect("Should be a valid port number"))
        .unwrap_or(config.get_database_port());

    let db_user = matches
        .get_one::<String>("db_user")
        .cloned()
        .unwrap_or(config.get_database_user());

    let db_password = matches
        .get_one::<String>("db_password")
        .cloned()
        .unwrap_or(config.get_database_password());

    let db_name = matches
        .get_one::<String>("db_name")
        .cloned()
        .unwrap_or(config.get_database_name());

    // Construct the database URL
    format!(
        "postgres://{}:{}@{}:{}/{}",
        db_user, db_password, db_host, db_port, db_name
    )
}
