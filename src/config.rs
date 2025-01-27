use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    database: DatabaseConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    host: String,
    port: u16,
    user: String,
    password: String,
    name: String,
}

impl AppConfig {
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
