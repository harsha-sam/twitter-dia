import re
import json
from neo4j import GraphDatabase
from confluent_kafka import Consumer, KafkaError
import sys


# Neo4j connection details
neo4j_uri = "neo4j://neo4j_db:7687"
neo4j_user = "neo4j"
neo4j_password = "zpv@ntu7WAY6paq3nbe"
db_driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))


def update_neo4j(tweet_id, sentiment_score, hashtags):
    with db_driver.session() as session:
        # Update the tweet's sentiment score
        session.run("""
            MATCH (tweet:Tweet {id: $tweet_id})
            SET tweet.sentimentScore = toFloat($sentiment_score)
        """, tweet_id=tweet_id, sentiment_score=sentiment_score)

        
        # Update average sentiment score for user
        session.run("""
            MATCH (user:User)-[:POSTS]->(tweet:Tweet)
            WHERE tweet.sentimentScore IS NOT NULL
            WITH user, avg(tweet.sentimentScore) AS avgSentiment
            SET user.avgSentimentScore = avgSentiment
        """)

        # Update average sentiment score for hashtags
        for hashtag in hashtags:
            session.run("""
                MATCH (hashtag:Hashtag {name: $hashtag})<-[:TAGS]-(tweet:Tweet)
                WITH hashtag, avg(tweet.sentimentScore) AS avgSentiment
                SET hashtag.avgSentimentScore = avgSentiment
            """, hashtag=hashtag)


def consume_messages(consumer):
    consumer.subscribe(['SENTIMENT'])
    try:
        while True:
            msg = consumer.poll(1.0)  # Wait for a message for up to 1 second
            if msg is None:
                continue  # No message was available

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event is normal, not an error.
                    print('End of partition reached {0}[{1}] at offset {2}'.format(
                        msg.topic(), msg.partition(), msg.offset()))
                else:
                    # Log any other errors that occur
                    print('Kafka error: {}'.format(msg.error()))
            else:
                # Log the message to the console
                print(f'Received message: {msg.value().decode("utf-8")}')
                message_data = json.loads(msg.value().decode('utf-8'))
                tweet_id = message_data.get('tweet_id')
                sentiment_score = message_data.get('sentiment')
                hashtags = re.findall(r"#(\w+)", message_data.get('tweet'))

                update_neo4j(tweet_id, sentiment_score, hashtags)
                print(f"Processed tweet ID {
                    tweet_id} with sentiment {sentiment_score}")

    finally:
        # Always close the consumer cleanly on exit
        consumer.close()


kafka_config = {
    'bootstrap.servers': 'kafka-1:19092,kafka-2:19093,kafka-3:19094',
    'group.id': 'test-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(kafka_config)
consume_messages(consumer)
