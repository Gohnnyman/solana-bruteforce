#!/bin/bash

set -e 

# Global variable for the temporary directory
TMP_DIR=""
# Variable for the ledger folder name
LEDGER_FOLDER_NAME="ledger"
# Docker container name
CONTAINER_NAME="postgres_bruteforce_db"


# Function to create a temporary directory
create_tmp_dir() {
    TMP_DIR="./tmp"
    if [ ! -d "$TMP_DIR" ]; then
        mkdir "$TMP_DIR"
        echo "Created temporary directory: $TMP_DIR"
    else
        echo "Temporary directory already exists: $TMP_DIR"
    fi
}

# Function to clean up the temporary directory
cleanup_tmp_dir() {
    if [[ -d "$TMP_DIR" ]]; then
        echo "Cleaning up temporary directory: $TMP_DIR"
        rm -rf "$TMP_DIR"
    fi
}

# Function to check if a Docker container with a specific name exists
check_docker_container() {
    if docker ps -a --format "{{.Names}}" | grep -qw "$CONTAINER_NAME"; then
        echo "Docker container '$CONTAINER_NAME' already exists. Exiting without errors."
        exit 0
    fi
}

# Function to parse Config.toml
parse_config() {
    local config_file="Config.toml"
    local venv_dir="$TMP_DIR/venv"  # Use TMP_DIR for the virtual environment

    # Check if the virtual environment exists, create it if necessary
    if [ ! -d "$venv_dir" ]; then
        echo "Creating a virtual environment for yq in $venv_dir..."
        python3 -m venv "$venv_dir"
    fi

    # Activate the virtual environment
    source "$venv_dir/bin/activate"

    # Check if yq (which includes tomlq) is installed in the virtual environment
    if ! pip show yq &> /dev/null; then
        echo "Installing yq with TOML support in the virtual environment..."
        pip install --quiet yq
    fi

    # Deactivate the virtual environment if the script exits prematurely
    trap deactivate EXIT

    # Check if the Config.toml file exists
    if [ ! -f "$config_file" ]; then
        echo "Error: Config.toml file not found!"
        deactivate
        exit 1
    fi

    # Extract values from Config.toml using `tomlq`
    POSTGRES_USER=$(tomlq -r .database.user "$config_file")
    POSTGRES_PASSWORD=$(tomlq -r .database.password "$config_file")
    POSTGRES_DB=$(tomlq -r .database.name "$config_file")
    POSTGRES_PORT=$(tomlq -r .database.port "$config_file")

    # Deactivate the virtual environment
    deactivate
    trap - EXIT
}


# Function to initialize the Docker container and setup the database
init_docker() {

    # Start the PostgreSQL container
    echo "Starting PostgreSQL Docker container..."
    docker run -d \
        --name $CONTAINER_NAME \
        -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
        -e POSTGRES_USER=$POSTGRES_USER \
        -e POSTGRES_DB=$POSTGRES_DB \
        -p $POSTGRES_PORT:5432 \
        postgres:latest

    # Wait for PostgreSQL to start
    echo "Waiting for PostgreSQL to initialize..."
    sleep 5

     # Check if init.sql exists
    local init_sql="$(dirname "$0")/init.sql"

    if [ ! -f "$init_sql" ]; then
        echo "Error: $init_sql file not found!"
        exit 1
    fi

    # Copy the init.sql file into the Docker container
    echo "Copying $init_sql to Docker container..."
    docker cp "$init_sql" "$CONTAINER_NAME:/init.sql"

    # Execute the init.sql file inside the PostgreSQL container
    echo "Running $init_sql to initialize the database..."
    docker exec -i $CONTAINER_NAME psql -U $POSTGRES_USER -d $POSTGRES_DB -f /init.sql

    echo "Database initialized successfully from $init_sql."

    # Confirm the container is running
    echo "PostgreSQL container is running with the following details:"
    echo "Container Name: $CONTAINER_NAME"
    echo "Port: $POSTGRES_PORT"
    echo "Database: $POSTGRES_DB"
    echo "User: $POSTGRES_USER"
    echo "Password: $POSTGRES_PASSWORD"
}

# Function to install and run snapshot-finder
# 20 minutes to run
install_and_run_snapshot_finder() {
    # Check if Solana is already installed
    if ! command -v solana &> /dev/null; then
        echo "Installing Solana CLI..."
        sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"
    fi

    # Use the global temporary directory for cloning
    SNAPSHOT_DIR="$TMP_DIR/solana-snapshot-finder"

    # Clean up any previous installation in the temp directory
    rm -rf "$SNAPSHOT_DIR"

    # Install Python dependencies and clone solana-snapshot-finder
    echo "Installing solana-snapshot-finder in temporary directory..."
    git clone https://github.com/c29r3/solana-snapshot-finder.git "$SNAPSHOT_DIR"
    cd "$SNAPSHOT_DIR" || exit
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

    # Run snapshot-finder
    echo "Running snapshot-finder..."
    echo python snapshot-finder.py --snapshot_path "../$TMP_DIR"
    python snapshot-finder.py --snapshot_path "../"

    # Return to the original directory
    deactivate
    cd -
}

# Function to unarchive snapshot file
# 5 minutes to run
unarchive_snapshot() {
    local snapshot_file
    snapshot_file=$(find "$TMP_DIR" -name "snapshot-*.tar.zst" | head -n 1)

    if [[ -z "$snapshot_file" ]]; then
        echo "Error: No snapshot file found in $TMP_DIR!"
        exit 1
    fi

    local ledger_dir="$TMP_DIR/$LEDGER_FOLDER_NAME"
    mkdir -p "$ledger_dir"

    echo "Unarchiving snapshot file: $snapshot_file to $ledger_dir"
    tar --use-compress-program=unzstd -xf "$snapshot_file" -C "$ledger_dir"

    echo "Snapshot has been unarchived to $ledger_dir"
    chmod +r -R "$ledger_dir"
    echo "Snapshot files have been made readable."
}



# Function to scan accounts from AccountsDB
# 20 minutes to run
scan_accounts_from_accountsdb() {
    local ledger_dir="$TMP_DIR/$LEDGER_FOLDER_NAME"
    echo "Scanning accounts from AccountsDB at $ledger_dir..."
    cargo run -- scan_accounts --path "$ledger_dir"
}

# Function to check if required dependencies are installed
check_requirements() {
    local missing_deps=()

    # Check for required dependencies
    for dep in docker git python3 pip curl cargo unzstd; do
        if ! command -v $dep &> /dev/null; then
            missing_deps+=("$dep")
        fi
    done

    # Check for Rust (cargo) installation
    if ! command -v cargo &> /dev/null; then
        missing_deps+=("cargo (Rust)")
    fi

    # Print missing dependencies
    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo "Error: The following dependencies are missing:"
        for dep in "${missing_deps[@]}"; do
            echo "  - $dep"
        done
        echo "Please install the missing dependencies and try again."
        exit 1
    fi

    echo "All required dependencies are installed."
}

# Call the check_requirements function at the beginning of the script
check_requirements


# Main script execution

# Create a temporary directory
create_tmp_dir

# Ensure cleanup is done on script exit
trap cleanup_tmp_dir EXIT 

# Call the parse_config function to load values
parse_config


# Check if the Docker container already exists
check_docker_container

# Git clone the solana-snapshot-finder repository and download snapshot
install_and_run_snapshot_finder

# Unarchive the snapshot file
unarchive_snapshot

# Initialize the Docker container and setup the database
init_docker

# Scan accounts from AccountsDB and populate the database
scan_accounts_from_accountsdb
