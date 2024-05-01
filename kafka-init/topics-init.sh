#!/bin/bash

# Kafka Broker Address
KAFKA_BROKER=$KAFKA_BOOTSTRAP_SERVERS

# List of topics to create with their partition and replication factor settings
declare -A topics=(
    ["RAW"]="26 3"
    ["SENTIMENT"]="26 3"
)

# Function to create a Kafka topic
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3

    echo "Creating topic: $topic_name with $partitions partitions and replication factor of $replication_factor"
    
    # Command to create topic
    /usr/bin/kafka-topics --create \
        --if-not-exists \
        --bootstrap-server $KAFKA_BROKER \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication_factor

    # Check the result of topic creation
    if [ $? -eq 0 ]; then
        echo "Successfully created topic: $topic_name"
    else
        echo "Failed to create topic: $topic_name"
        exit 1
    fi
}

# Loop through all topics and create them
for topic in "${!topics[@]}"; do
    IFS=' ' read -ra ADDR <<< "${topics[$topic]}"
    create_topic $topic ${ADDR[0]} ${ADDR[1]}
done

echo "All topics created successfully."
