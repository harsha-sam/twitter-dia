#!/bin/bash

# Path to the database directory
DB_DIR=/data/databases
DB_NAME=twitter

# Check if the database directory is empty
if [ -z "$(ls -A $DB_DIR/$DB_NAME)" ]; then
    echo "Database directory is empty. Importing the dump..."
    bin/neo4j-admin database load --from-path=/var/lib/neo4j/import $DB_NAME
else
    echo "Database is already initialized."
fi

bin/neo4j console