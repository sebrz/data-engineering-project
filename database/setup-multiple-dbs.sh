#!/bin/bash

set -e
set -u

# Create the financial_data database
echo "Creating financial_data database"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE financial_data;
    GRANT ALL PRIVILEGES ON DATABASE financial_data TO $POSTGRES_USER;
EOSQL

echo "Created financial_data database"

# Now connect to financial_data database and run the init script
echo "Initializing financial_data tables"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d financial_data -f /docker-entrypoint-initdb.d/create_tables.sql
echo "Initialized financial_data tables"