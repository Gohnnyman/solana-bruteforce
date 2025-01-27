use std::sync::Arc;

use log::{debug, error, info};
use sqlx::postgres::PgPoolOptions;
use sqlx::{ConnectOptions, Pool, Postgres};
use thiserror::Error;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

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

pub struct PostgresInserterActor;

impl PostgresInserterActor {
    pub fn new(
        url: &str,
        accounts_rc: mpsc::UnboundedReceiver<Vec<AccountWithBalance>>,
        exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>, // New completion signal
    ) -> Result<()> {
        let pg_connection_options = url
            .parse::<sqlx::postgres::PgConnectOptions>()?
            .log_slow_statements(log::LevelFilter::Off, std::time::Duration::from_secs(10))
            .log_statements(log::LevelFilter::Off);

        let pool = PgPoolOptions::new()
            .max_connections(MAX_PSQL_CONNECTIONS)
            .connect_lazy_with(pg_connection_options);

        let pool = Arc::new(pool);

        tokio::spawn(Self::run(
            accounts_rc,
            pool.clone(),
            exit_signal,
            completion_signal,
        ));

        Ok(())
    }

    async fn run(
        mut accounts_rc: mpsc::UnboundedReceiver<Vec<AccountWithBalance>>,
        pool: Arc<Pool<Postgres>>,
        mut exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>,
    ) {
        let mut accounts_buffer: Vec<AccountWithBalance> = Vec::new();
        let mut flush_interval = tokio::time::interval(FLUSH_TIMEOUT);

        loop {
            select! {
                // Handle receiving new accounts
                Some(new_accounts) = accounts_rc.recv() => {
                    accounts_buffer.extend(new_accounts);

                    if accounts_buffer.len() >= FLUSH_THRESHOLD {
                        let to_flush = accounts_buffer.split_off(accounts_buffer.len() - FLUSH_THRESHOLD);


                        if let Err(e) = Self::flush(&to_flush, pool.clone()).await {
                            error!("Failed to flush accounts: {:?}", e);
                        }


                        // Reset the flush interval
                        flush_interval = tokio::time::interval(FLUSH_TIMEOUT);
                        flush_interval.tick().await; // Ensure the interval starts fresh
                    }
                }

                // Handle timeout-based flushing
                _ = flush_interval.tick() => {
                    if !accounts_buffer.is_empty() {
                        let to_flush = accounts_buffer.as_slice();

                        if let Err(e) = Self::flush(to_flush, pool.clone()).await {
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

                        if let Err(e) = Self::flush(&to_flush, pool.clone()).await {
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
            query = query.bind(&account.pubkey).bind(account.lamports);
        }

        query.execute(&*pool).await.map_err(ActorError::SqlxError)?;

        debug!("Batch insert completed!");
        Ok(())
    }
}
