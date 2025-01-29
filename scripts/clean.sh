#!/bin/bash

# Global variable for the temporary directory
TMP_DIR="./tmp"

# Function to clean up the Docker container and database resources
cleanup_docker() {
    # Container name used in init_docker.sh
    CONTAINER_NAME="postgres_bruteforce_db"

    # Check if the container exists
    if docker ps -a --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
        echo "Stopping and removing Docker container '${CONTAINER_NAME}'..."
        docker stop $CONTAINER_NAME > /dev/null 2>&1
        docker rm $CONTAINER_NAME > /dev/null 2>&1
        echo "Docker container '${CONTAINER_NAME}' has been removed."
    else
        echo "No Docker container named '${CONTAINER_NAME}' found."
    fi

    # Remove Docker volumes (if any)
    echo "Cleaning up dangling Docker volumes..."
    docker volume prune -f > /dev/null 2>&1
    echo "Dangling Docker volumes have been removed."
}

# Function to clean up PostgreSQL data (if any custom paths were used)
cleanup_postgres_data() {
    echo "Checking for custom PostgreSQL data directories..."
    if [ -d "pg_data" ]; then
        echo "Removing local PostgreSQL data directory 'pg_data'..."
        rm -rf pg_data
        echo "Local PostgreSQL data directory 'pg_data' has been removed."
    else
        echo "No local PostgreSQL data directory 'pg_data' found."
    fi
}

# Function to clean up the temporary directory
cleanup_tmp_dir() {
    if [[ -d "$TMP_DIR" ]]; then
        echo "Cleaning up temporary directory: $TMP_DIR"
        rm -rf "$TMP_DIR"
        echo "Temporary directory '$TMP_DIR' has been removed."
    else
        echo "No temporary directory '$TMP_DIR' found."
    fi
}

# Main cleanup function
cleanup() {
    echo "Starting cleanup process..."
    cleanup_docker
    cleanup_postgres_data
    cleanup_tmp_dir
    echo "Cleanup process completed."
}

# Execute cleanup
cleanup
