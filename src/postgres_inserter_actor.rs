use std::sync::Arc;

use futures::SinkExt;
use log::{debug, error, info};
use solana_sdk::signer::Signer;
use sqlx::{Pool, Postgres};
use thiserror::Error;
use tokio::select;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::scan_accounts::AccountWithBalance;

#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Join error while processing files: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("SQLx Error: {0}")]
    SqlxError(#[from] sqlx::Error),
}

pub type Result<T> = std::result::Result<T, ActorError>;

pub const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
pub const FLUSH_THRESHOLD: usize = 5_000;

pub struct PostgresInserterActor;

impl PostgresInserterActor {
    pub fn new(
        url: &str,
        accounts_rc: mpsc::UnboundedReceiver<Vec<AccountWithBalance>>,
        exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>, // New completion signal
    ) -> Result<()> {
        let pool = Arc::new(Mutex::new(Pool::<Postgres>::connect_lazy(url)?));

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
        pool: Arc<Mutex<Pool<Postgres>>>,
        mut exit_signal: oneshot::Receiver<()>,
        completion_signal: oneshot::Sender<()>, // New completion signal sender
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

                        flush_interval = tokio::time::interval(FLUSH_TIMEOUT); // Reset interval
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
                        flush_interval = tokio::time::interval(FLUSH_TIMEOUT); // Reset interval
                    }
                }

                // Handle exit signal
                _ = &mut exit_signal => {
                    info!("Exit signal received. Flushing remaining accounts and exiting...");

                    if !accounts_buffer.is_empty() {
                        if let Err(e) = Self::flush(accounts_buffer.as_slice(), pool.clone()).await {
                            error!("Failed to flush accounts during exit: {:?}", e);
                        }
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
        pool: Arc<Mutex<Pool<Postgres>>>,
    ) -> Result<()> {
        debug!(
            "Flushing {} accounts into the database...",
            accounts_buffer.len()
        );
        Self::insert_accounts_into_db(accounts_buffer, pool).await
    }

    async fn insert_accounts_into_db(
        accounts_with_balances: &[AccountWithBalance],
        pool: Arc<Mutex<Pool<Postgres>>>,
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

        let pool = pool.lock().await;

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
