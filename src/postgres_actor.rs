use std::sync::Arc;

use log::{debug, error, info};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use sqlx::{ConnectOptions, Pool, Postgres};
use thiserror::Error;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Interval;

use crate::scan_accounts::AccountWithBalance;

#[derive(Error, Debug)]
pub enum ActorError {
    #[error("SQLx Error: {0}")]
    SqlxError(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, ActorError>;

pub const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const FLUSH_THRESHOLD: usize = 10_000;

// Maximum number of parallel connections to the database
// Set to 1 due to deadlock issues with parallel connections
pub const MAX_PSQL_CONNECTIONS: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresMessage {
    InsertAccountsButch(Vec<AccountWithBalance>),
    CheckAccountsButch(Vec<AccountWithPrivateKey>),
}

// TODO: move into bruteforce.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountWithPrivateKey {
    pub public_key: String,
    pub private_key: [u8; 32],
}

/// PostgresActor is responsible for inserting accounts into the database
/// and checking if any of the provided accounts exist in the database.
pub struct PostgresActor {
    pool: Arc<Pool<Postgres>>,
    accounts_with_balance_buffer: Vec<AccountWithBalance>,
    flush_interval: Interval,
}

impl PostgresActor {
    pub async fn new(
        url: &str,
        accounts_rc: mpsc::Receiver<PostgresMessage>,
        exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>,
    ) -> Result<()> {
        let pg_connection_options = url
            .parse::<sqlx::postgres::PgConnectOptions>()?
            .log_slow_statements(log::LevelFilter::Off, std::time::Duration::from_secs(10))
            .log_statements(log::LevelFilter::Off);

        let pool = PgPoolOptions::new()
            .max_connections(MAX_PSQL_CONNECTIONS)
            .connect_with(pg_connection_options)
            .await?;

        let pool = Arc::new(pool);

        let actor = PostgresActor {
            pool,
            accounts_with_balance_buffer: Vec::new(),
            flush_interval: tokio::time::interval(FLUSH_TIMEOUT),
        };

        tokio::spawn(actor.run(accounts_rc, exit_signal, completion_signal));

        Ok(())
    }

    async fn check_accounts_exist(
        pool: Arc<Pool<Postgres>>,
        pubkeys: &[AccountWithPrivateKey],
    ) -> Result<bool> {
        // Extract public keys as a vector of strings
        let pubkeys_vec: Vec<String> = pubkeys
            .iter()
            .map(|account| account.public_key.clone())
            .collect();

        // SQL query to check if any pubkeys exist in the database
        let query = "
            SELECT EXISTS (
                SELECT 1
                FROM existing_accounts
                WHERE account_pubkey = ANY($1)
            )
        ";

        // Execute the query and fetch the result
        let exists: bool = sqlx::query_scalar(query)
            .bind(&pubkeys_vec)
            .fetch_one(&*pool)
            .await?;

        Ok(exists)
    }

    async fn insert_found_accounts(
        pubkeys_with_keys: &[AccountWithPrivateKey],
        pool: Arc<Pool<Postgres>>,
    ) -> Result<()> {
        // Extract public keys for the query
        let pubkeys: Vec<String> = pubkeys_with_keys
            .iter()
            .map(|account| account.public_key.clone())
            .collect();

        // Query the database to find matching accounts
        let query = r#"
            SELECT account_pubkey
            FROM existing_accounts
            WHERE account_pubkey = ANY($1)
        "#;

        let rows = sqlx::query(query).bind(&pubkeys).fetch_all(&*pool).await?;

        if rows.is_empty() {
            info!("No matching accounts found to insert into found_accounts.");
            return Ok(());
        }

        // Collect the matching accounts and their private keys
        let found_accounts: Vec<AccountWithPrivateKey> = rows
            .iter()
            .filter_map(|row| {
                let public_key: String = row.get("account_pubkey");

                // Find the corresponding private key from the input list
                pubkeys_with_keys
                    .iter()
                    .find(|acc| acc.public_key == public_key)
                    .cloned()
            })
            .collect();

        if found_accounts.is_empty() {
            info!("No accounts with private keys to insert.");
            return Ok(());
        }

        // Insert the found accounts into the `found_accounts` table
        let mut query_builder =
            String::from("INSERT INTO found_accounts (account_pubkey, private_key) VALUES ");
        for (i, _) in found_accounts.iter().enumerate() {
            if i > 0 {
                query_builder.push(',');
            }
            query_builder.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
        }
        query_builder.push_str(" ON CONFLICT (account_pubkey) DO NOTHING");

        let mut query = sqlx::query(&query_builder);
        for account in found_accounts {
            query = query.bind(account.public_key).bind(
                account
                    .private_key
                    .into_iter()
                    .map(|v| v as i32)
                    .collect::<Vec<i32>>(),
            );
        }

        query.execute(&*pool).await.map_err(ActorError::SqlxError)?;

        info!("Inserted found accounts into the found_accounts table.");
        Ok(())
    }

    async fn handle_message(&mut self, message: PostgresMessage) {
        match message {
            PostgresMessage::InsertAccountsButch(accounts) => {
                self.accounts_with_balance_buffer.extend(accounts);

                if self.accounts_with_balance_buffer.len() >= FLUSH_THRESHOLD {
                    let to_flush = self
                        .accounts_with_balance_buffer
                        .split_off(self.accounts_with_balance_buffer.len() - FLUSH_THRESHOLD);

                    if let Err(e) = Self::flush(&to_flush, self.pool.clone()).await {
                        error!("Failed to flush accounts: {:?}", e);
                    }
                }

                // Reset the flush interval
                self.flush_interval = tokio::time::interval(FLUSH_TIMEOUT);
                self.flush_interval.tick().await; // Ensure the interval starts fresh
            }
            PostgresMessage::CheckAccountsButch(accounts_with_private_keys) => {
                // Check if any pubkeys exist in the database
                let time_before_check = std::time::Instant::now();
                match Self::check_accounts_exist(self.pool.clone(), &accounts_with_private_keys)
                    .await
                {
                    Ok(exists) => {
                        debug!(
                            "Checking accounts took: {}ms",
                            time_before_check.elapsed().as_millis()
                        );

                        if exists {
                            info!("At least one of the provided accounts exists in the database.");
                            // Fetch the accounts that exist in the database and insert them into the found_accounts table
                            if let Err(e) = Self::insert_found_accounts(
                                &accounts_with_private_keys,
                                self.pool.clone(),
                            )
                            .await
                            {
                                error!("Failed to insert found accounts into found_accounts table: {:?}, data: {:#?}", e, accounts_with_private_keys);
                            }
                        }
                    }
                    Err(e) => error!("Failed to check accounts: {:?}", e),
                }
            }
        }
    }

    async fn run(
        mut self,
        mut messages_rc: mpsc::Receiver<PostgresMessage>,
        mut exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>,
    ) {
        let mut accounts_buffer: Vec<AccountWithBalance> = Vec::new();
        let mut flush_interval = tokio::time::interval(FLUSH_TIMEOUT);

        loop {
            debug!("Messages size: {}", messages_rc.len());
            select! {
                // Handle receiving new accounts
                Some(message) = messages_rc.recv() => {
                    self.handle_message(message).await;
                }

                // Handle timeout-based flushing
                _ = flush_interval.tick() => {
                    if !accounts_buffer.is_empty() {
                        let to_flush = accounts_buffer.as_slice();

                        if let Err(e) = Self::flush(to_flush, self.pool.clone()).await {
                            error!("Failed to flush accounts on timeout: {:?}", e);
                        }

                        accounts_buffer.clear();

                        // Reset the flush interval
                        flush_interval = tokio::time::interval(FLUSH_TIMEOUT);
                        flush_interval.tick().await; // Ensure the interval starts fresh
                    }
                }

                // Handle exit signal
                _ = &mut exit_signal => {
                    info!("Exit signal received. Flushing remaining accounts and exiting...");

                    let amount_of_accounts_to_flush = accounts_buffer.len();
                    let mut flushed = 0;

                    while !accounts_buffer.is_empty() {
                        let to_flush = if accounts_buffer.len() > FLUSH_THRESHOLD {
                            // Split off the last FLUSH_THRESHOLD accounts for flushing
                            accounts_buffer.split_off(accounts_buffer.len() - FLUSH_THRESHOLD)
                        } else {
                            // Flush all remaining accounts if below the threshold
                            accounts_buffer.split_off(0)
                        };

                        flushed += to_flush.len();

                        if let Err(e) = Self::flush(&to_flush, self.pool.clone()).await {
                            error!("Failed to flush accounts during exit: {:?}", e);
                        }

                        info!("Flushed {} / {} of reamaiming accounts...", flushed, amount_of_accounts_to_flush);
                    }

                    break; // Exit the loop after flushing
                }
            }
        }

        // Notify completion of flushing
        if let Err(e) = completion_signal.send(()) {
            error!("Failed to send completion signal: {:?}", e);
        }

        info!("PostgresInserterActor has exited.");
    }

    /// Flushes the accounts buffer into the database
    async fn flush(
        accounts_buffer: &[AccountWithBalance],
        pool: Arc<Pool<Postgres>>,
    ) -> Result<()> {
        debug!(
            "Flushing {} accounts into the database...",
            accounts_buffer.len()
        );

        // Calculate the chunk size to ensure the number of chunks <= MAX_PSQL_CONNECTIONS
        let chunk_size = (accounts_buffer.len() + MAX_PSQL_CONNECTIONS as usize - 1)
            / MAX_PSQL_CONNECTIONS as usize;

        // Split the accounts buffer into chunks
        let data_chunks: Vec<Vec<AccountWithBalance>> = accounts_buffer
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Use flush_data for parallel flushing
        Self::flush_data(pool, data_chunks).await;

        Ok(())
    }

    async fn flush_data(pool: Arc<Pool<Postgres>>, data_chunks: Vec<Vec<AccountWithBalance>>) {
        let mut tasks = vec![];

        for chunk in data_chunks {
            let pool = pool.clone();
            tasks.push(tokio::task::spawn(async move {
                Self::insert_accounts_into_db(&chunk, pool.clone())
                    .await
                    .unwrap();
            }));
        }

        // Wait for all tasks to complete
        for task in tasks {
            task.await.unwrap();
        }
    }

    async fn insert_accounts_into_db(
        accounts_with_balances: &[AccountWithBalance],
        pool: Arc<Pool<Postgres>>,
    ) -> Result<()> {
        if accounts_with_balances.is_empty() {
            return Ok(());
        }

        if accounts_with_balances.len() > FLUSH_THRESHOLD {
            error!(
                "Batch insert size too large: {}",
                accounts_with_balances.len()
            );

            return Err(ActorError::SqlxError(sqlx::Error::Protocol(
                "Batch insert size too large".to_string(),
            )));
        }

        let mut query_builder =
            String::from("INSERT INTO existing_accounts (account_pubkey, balance) VALUES ");

        for (i, _) in accounts_with_balances.iter().enumerate() {
            if i > 0 {
                query_builder.push(',');
            }
            query_builder.push_str(&format!("(${}, ${})", i * 2 + 1, i * 2 + 2));
        }

        query_builder.push_str(" ON CONFLICT (account_pubkey) DO NOTHING");

        let mut query = sqlx::query(&query_builder);

        for account in accounts_with_balances {
            query = query.bind(&account.public_key).bind(account.lamports);
        }

        query.execute(&*pool).await.map_err(ActorError::SqlxError)?;

        debug!("Batch insert completed!");
        Ok(())
    }
}
