# NOTICE
You need roughly 500GB of free space to run this project.

# How to get solana snap shot
1. Install solana cli 
```bash
sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)" 
``` 
or update it
```bash
solana-install update 
```
2. Install [solana-snapshot-finder](https://github.com/c29r3/solana-snapshot-finder/tree/main)
3. Run the following command to get the latest snapshot
```bash
python3 snapshot-finder.py --snapshot_path solana-bruteforce/validator-ledger
```
4. Whait for the snapshot to be downloaded, roughly 1 hour
5. 
```bash
tar --use-compress-program=unzstd -xf validator-ledger/snapshot-315225125-RAJGjz9paJ7cpt1AzkmtADQpKcQGNdyeg6Fbv8NF2My.tar.zst -C validator-ledger/
tar --use-compress-program=unzstd -xf validator-ledger/incremental-snapshot-315225125-315233770-9kzV32AeMZnRZEZqR1YPX6pg7pMaZ2JA7oS8XWsUp5xG.tar.zst -C validator-ledger/
wget -O validator-ledger/genesis.tar.bz2 https://api.mainnet-beta.solana.com/genesis.tar.bz2

solana-ledger-tool accounts --ledger validator-ledger/ --output json --force-update-to-open
solana-ledger-tool accounts --ledger validator-ledger/ --output json --force-update-to-open --no-account-data --no-account-contents
```