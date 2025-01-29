use std::time::Duration;

use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    database: DatabaseConfig,
    bruteforce: BruteforceConfig,
}

#[derive(Debug, Deserialize)]
struct BruteforceConfig {
    num_of_threads: Option<u16>,
    private_keys_per_thread: u32,
    status_update_interval_sec: u64,
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    name: String,
}

impl AppConfig {
    // Database

    pub fn get_database_host(&self) -> String {
        self.database.host.clone()
    }

    pub fn get_database_port(&self) -> u16 {
        self.database.port
    }

    pub fn get_database_user(&self) -> String {
        self.database.user.clone()
    }

    pub fn get_database_password(&self) -> String {
        self.database.password.clone()
    }

    pub fn get_database_name(&self) -> String {
        self.database.name.clone()
    }

    // Bruteforce

    pub fn get_num_of_threads(&self) -> u16 {
        self.bruteforce
            .num_of_threads
            .unwrap_or(num_cpus::get() as u16)
    }

    pub fn get_private_keys_per_thread(&self) -> u32 {
        self.bruteforce.private_keys_per_thread
    }

    pub fn get_status_update_interval_sec(&self) -> Duration {
        Duration::from_secs(self.bruteforce.status_update_interval_sec)
    }
}

/// Load configuration from a custom path or fallback to `Config.toml` in the current directory
pub fn load_config(path: &str) -> AppConfig {
    let config = Config::builder()
        .add_source(File::with_name(path).required(false)) // Load the specified config file
        .add_source(File::with_name("Config.toml").required(false)) // Fallback to `Config.toml` if not specified
        .build()
        .expect("Failed to build configuration");

    config
        .try_deserialize::<AppConfig>()
        .expect("Failed to deserialize configuration")
}
