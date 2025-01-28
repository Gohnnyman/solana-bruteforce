
CREATE TABLE IF NOT EXISTS existing_accounts (
    account_pubkey VARCHAR(44) PRIMARY KEY,
    balance BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS found_accounts (
    account_pubkey VARCHAR(44) PRIMARY KEY, 
    private_key INTEGER[] NOT NULL             
);