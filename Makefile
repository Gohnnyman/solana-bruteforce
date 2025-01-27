# Basic Makefile to run the init.sh script

all:
	@chmod +x scripts/init.sh
	@./scripts/init.sh

clean-docker:
	@./scripts/clean.sh

clean:
	@cargo clean
