# Basic Makefile to manage the project

.PHONY: all init start clean-docker clean help

all: help

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Available targets:"
	@echo "  init          Run the initialization script (sets up the database, fetches snapshots, etc.)"
	@echo "  start         Run the Rust program (cargo run -- start)"
	@echo "  clean         Clean up artifacts. Includs cleanup of Docker container, DB, snapshot, etc."
	@echo "  help          Display this help message"
	@echo ""

init: 
	@chmod +x scripts/init.sh
	@./scripts/init.sh

start: 
	@docker start postgres_bruteforce_db
	@cargo run -- start

clean:
	@cargo clean
	@./scripts/clean.sh
