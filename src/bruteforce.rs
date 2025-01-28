use log::info;
use rayon::prelude::*;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BruteforceError {
    #[error("Failed to generate Solana Keypair")]
    FailedToGenerateKeypair,
}

pub type Result<T> = std::result::Result<T, BruteforceError>;

pub async fn run_bruteforce() -> Result<()> {
    // Run infinite loop to generate keys
    loop {
        // Generate keys in parallel using Rayon
        (0..1000).into_par_iter().for_each(|_| {
            let keypair = Keypair::new();
            let private_key = keypair.secret().as_bytes();
            let public_key = keypair.pubkey();

            // Print the keys
            info!("Private Key: {:?}", private_key);
            info!("Public Key: {}", public_key);
        });
    }

    Ok(())
}
