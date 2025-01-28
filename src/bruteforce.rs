use log::info;
use rayon::prelude::*;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use thiserror::Error;

use crate::postgres_actor::{AccountWithPrivateKey, PostgresActor, PostgresMessage};

#[derive(Debug, Error)]
pub enum BruteforceError {
    #[error("Failed to generate Solana Keypair")]
    FailedToGenerateKeypair,
    #[error("Postgres Actor Error: {0}")]
    PostgresActorError(#[from] crate::postgres_actor::ActorError),
}

pub type Result<T> = std::result::Result<T, BruteforceError>;

pub async fn run_bruteforce(db_url: &str) -> Result<()> {
    let (postgres_message_tx, postgres_messages_rx) = tokio::sync::mpsc::unbounded_channel();
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel(); // Completion channel

    PostgresActor::new(db_url, postgres_messages_rx, exit_rx, completion_tx).await?;

    postgres_message_tx
        .send(PostgresMessage::CheckAccountsButch(vec![
            AccountWithPrivateKey {
                private_key: [0; 32],
                public_key: "DA4u1ac1x88XGKiEjZWX49WBBUJm2RevLVE2dakgNZDS".to_string(),
            },
        ]))
        .expect("Failed to send accounts, receiver dropped");

    // // Run infinite loop to generate keys
    // loop {
    //     // Generate keys in parallel using Rayon
    //     (0..1000).into_par_iter().for_each(|_| {
    //         let keypair = Keypair::new();
    //         let private_key = keypair.secret().as_bytes();
    //         let public_key = keypair.pubkey();

    //         // Print the keys
    //         info!("Private Key: {:?}", private_key);
    //         info!("Public Key: {}", public_key);
    //     });
    // }

    completion_rx.await.expect("Failed to wait for completion");

    Ok(())
}
