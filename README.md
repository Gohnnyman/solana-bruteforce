# Known Problems

* Though the bruteforce process is significantly faster than the standart approach with RPC calls, it could be optimized even more.
I checked the performance and it turns PostgreSQL is the bottleneck. 


# Requirements

You need roughly 500GB of free space to initialize this project and 6GB afterwards.

Init script (`make init`) will initialize the project, download the latest snapshot, unarchive it, create dockerized postgres
and inserts the accounts from the snapshot into DB.

This long initialization (1.5 hours approximately) is needed because all the accounts will be stored locally in the database,
thus it'll allow to check whether the private key leads to the public key that has (or had) any SOL on it without
any external API calls. You could even run this script without internet connection after the initialization.
This boosts the speed of the bruteforce process significantly.

# Performance increase

There is roughly 10^77 possible private keys, with the standart approach it would take a lot of time to check
each of them, via RPC calls, but with this approach it's possible to check all of them in a "reasonable time".

Under "reasonable time" I mean it will increase the speed of the bruteforce process by 1000x times, which means it would 
take not 10^66, but 10^63 years to check all the possible private keys (10^10 years is the age of the universe). Impressive, huh?

# Benchmark

Well, there is no actuall benchmarking here, but on my machine (AMD Ryzen 9 7900X 12-Core Processor, GeForce RTX 4080, 32RAM) it takes 
one minute to check 2_590_000 accounts or 43_167 accounts/second.

# Notice 
I'm not trying to steal anyone's SOL; I made this pet project just for fun (at the moment of writing this project I was unemployed). 
Not that it could actually find anything...


# How to use

1. This will do every step needed to initialize the project:
```bash
make init
```

2. Now you have dockerized postgres running with the accounts from the latest snapshot.

3. Run the bruteforce
```bash
make start
```

4. Check the logs. If the program finds something (which I doubt), you'll see a message. Then, check the found_accounts table in the database for the private key.
