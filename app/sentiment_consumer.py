from confluent_kafka import Consumer, KafkaError

def create_consumer(config):
    # Create a Kafka consumer with specified configurations
    return Consumer(config)

def consume_messages(consumer, topic):
    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)  # Wait for a message for up to 1 second
            if msg is None:
                continue  # No message was available

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event is normal, not an error.
                    print('End of partition reached {0}[{1}] at offset {2}'.format(msg.topic(), msg.partition(), msg.offset()))
                else:
                    # Log any other errors that occur
                    print('Kafka error: {}'.format(msg.error()))
            else:
                # Log the message to the console
                print(f'Received message: {msg.value().decode("utf-8")}')
    finally:
        # Always close the consumer cleanly on exit
        consumer.close()

if __name__ == '__main__':
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    topic = 'SENTIMENT'

    consumer = create_consumer(kafka_config)
    consume_messages(consumer, topic)
