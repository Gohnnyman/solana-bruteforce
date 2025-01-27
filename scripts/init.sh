#!/bin/bash

# Function to initialize the Docker container and setup the database
init_docker() {
    # Set PostgreSQL container details
    CONTAINER_NAME="postgres_bruteforce_db"
    POSTGRES_PASSWORD="password"
    POSTGRES_USER="postgres"
    POSTGRES_DB="bruteforce"
    POSTGRES_PORT=5432

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

    # Create the table in the database
    echo "Creating table 'existing_accounts' in the database 'bruteforce'..."
    docker exec -i $CONTAINER_NAME psql -U $POSTGRES_USER -d $POSTGRES_DB <<EOF
CREATE TABLE IF NOT EXISTS existing_accounts (
    id SERIAL PRIMARY KEY,
    account_pubkey VARCHAR(44) UNIQUE NOT NULL,
    balance BIGINT NOT NULL
);
EOF

    echo "Table 'existing_accounts' has been created successfully."

    # Confirm the container is running
    echo "PostgreSQL container is running with the following details:"
    echo "Container Name: $CONTAINER_NAME"
    echo "Port: $POSTGRES_PORT"
    echo "Database: $POSTGRES_DB"
    echo "User: $POSTGRES_USER"
    echo "Password: $POSTGRES_PASSWORD"
}

# Call the function to initialize the Docker container and database
init_docker
