#!/bin/bash

# Check if the database directory is empty
if [ -z "$(ls -A /data/databases/$DB_NAME)" ]; then
    bin/neo4j-admin dbms set-initial-password $NEO4JDB_PASSWORD
    echo "Database directory is empty. Importing the dump..."
    bin/neo4j-admin database load --from-path=/var/lib/neo4j/import $DB_NAME
else
    echo "Database is already initialized."
fi

bin/neo4j console