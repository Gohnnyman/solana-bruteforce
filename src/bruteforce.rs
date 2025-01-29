use log::{error, info};
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time;

use crate::config::AppConfig;
use crate::postgres_actor::{AccountWithPrivateKey, PostgresActor, PostgresMessage};

const CHANNEL_BUFFER_SIZE: usize = 100;

#[derive(Debug, Error)]
pub enum BruteforceError {
    #[error("Failed to generate Solana Keypair")]
    FailedToGenerateKeypair,
    #[error("Postgres Actor Error: {0}")]
    PostgresActorError(#[from] crate::postgres_actor::ActorError),
}

pub type Result<T> = std::result::Result<T, BruteforceError>;

pub async fn run_bruteforce(db_url: &str, config: &AppConfig) -> Result<()> {
    let (postgres_message_tx, postgres_messages_rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
    let (_exit_tx, exit_rx) = oneshot::channel();
    let (completion_tx, completion_rx) = oneshot::channel();

    // Start the Postgres actor
    PostgresActor::new(db_url, postgres_messages_rx, exit_rx, completion_tx).await?;

    let num_threads = config.get_num_of_threads();
    let tx = Arc::new(postgres_message_tx);

    info!("Starting bruteforce using {} threads...", num_threads);

    let checked_accounts = Arc::new(AtomicUsize::new(0));

    spawn_logging_task(
        checked_accounts.clone(),
        config.get_status_update_interval_sec(),
    );

    rayon::scope(|s| {
        for _ in 0..num_threads {
            let tx = Arc::clone(&tx);
            let checked_accounts = Arc::clone(&checked_accounts);

            s.spawn(move |_| {
                loop {
                    // Generate a batch of private keys
                    let keys: Vec<AccountWithPrivateKey> = (0..config
                        .get_private_keys_per_thread())
                        .map(|_| {
                            let keypair = Keypair::new();
                            AccountWithPrivateKey {
                                private_key: keypair.secret().to_bytes(),
                                public_key: keypair.pubkey().to_string(),
                            }
                        })
                        .collect();

                    checked_accounts.fetch_add(keys.len(), Ordering::Relaxed);

                    // Wait until space is available in the bounded channel before sending
                    match tx.blocking_send(PostgresMessage::CheckAccountsButch(keys)) {
                        Ok(_) => (),
                        Err(_) => {
                            error!("Failed to send keys to PostgresActor, receiver dropped.");
                            break;
                        }
                    }
                }
            });
        }
    });

    completion_rx.await.expect("Failed to wait for completion");

    Ok(())
}

/// Spawns a separate task that logs the number of checked accounts every few seconds.
fn spawn_logging_task(checked_accounts: Arc<AtomicUsize>, status_update_interval: time::Duration) {
    tokio::spawn(async move {
        let mut interval = time::interval(status_update_interval);
        loop {
            interval.tick().await;
            let count = checked_accounts.load(Ordering::Relaxed);
            info!("Checked {} accounts so far...", count);
        }
    });
}
