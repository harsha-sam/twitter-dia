import os
import re
import uuid
import socket
from flask import Flask, request, jsonify, json
from confluent_kafka import Producer
from neo4j import GraphDatabase

app = Flask(__name__)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


# Neo4j connection details
uri = os.getenv('NEO4JDB_URI')
user = os.getenv('NEO4JDB_USER')
password = os.getenv('NEO4JDB_PASSWORD')
db_driver = GraphDatabase.driver(uri, auth=(user, password))

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'client.id': socket.gethostname()
}
producer = Producer(conf)

@app.route('/')
def hello():
    return 'Hello, World!'

def fetch_recommendations(tx, screen_name):
    query = """
    MATCH (currentUser:User {screen_name: $screen_name})
    WITH currentUser.avgSentiment AS baseSentiment

    // Find users with similar average sentiment scores
    MATCH (otherUser:User)
    WHERE otherUser.screen_name <> $screen_name
    WITH otherUser, abs(otherUser.avgSentiment - baseSentiment) AS diff
    ORDER BY diff ASC
    LIMIT 10
    WITH collect(otherUser.screen_name) AS similarUsers

    // Find hashtags with similar average sentiment scores
    MATCH (hashtag:Hashtag)
    WITH similarUsers, hashtag, abs(hashtag.avgSentiment - baseSentiment) AS hashtagDiff
    ORDER BY hashtagDiff ASC
    LIMIT 10
    RETURN similarUsers, collect(hashtag.name) AS similarHashtags
    """
    result = tx.run(query, screen_name=screen_name)
    return result.single()

@app.route('/tweet', methods=['POST'])
def post_tweet():
    data = request.json
    tweet_text = data.get('tweet')
    user_id = data.get('user_id')
    if not tweet_text or not user_id:
        return jsonify({'error': 'Tweet text or User ID is missing'}), 400
    
    # Extract hashtags using a regular expression
    hashtags = re.findall(r"#(\w+)", tweet_text)

    # Add to Neo4j database
    try:
        with db_driver.session() as session:
            result = session.write_transaction(
                add_tweet_to_neo4j, user_id, tweet_text, hashtags)
            # Extract the tweet ID from the result
            tweet_id = result['tweet_id']
            recommendations = session.read_transaction(
                fetch_recommendations, user_id)
            user_recommendations = recommendations['similarUsers']
            hashtag_recommendations = recommendations['similarHashtags']
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    message = json.dumps(
        {'tweet': tweet_text, 'user_id': user_id, 'tweet_id': tweet_id})
    # Stream the tweet to Kafka on the topic 'RAW'
    producer.produce('RAW', message.encode('utf-8'), callback=acked)
    producer.flush()

    return jsonify({
        'status': 'Tweet added to database and sent to Kafka',
        'tweet_id': tweet_id,
        'user_recommendations': user_recommendations,
        'hashtag_recommendations': hashtag_recommendations
    }), 200


def add_tweet_to_neo4j(tx, screen_name, tweet, hashtags):
    tweet_id = str(uuid.uuid4())
    query = """
    MERGE (user:User {screen_name: $screen_name})
    CREATE (tweet:Tweet {id: $tweet_id, text: $tweet, created_at: datetime()})
    MERGE (user)-[:POSTS]->(tweet)
    """
    if hashtags:
        query += """
        WITH tweet, $hashtags AS tags
        UNWIND tags AS tagName
        MERGE (hashtag:Hashtag {name: tagName})
        MERGE (tweet)-[:TAGS]->(hashtag)
        """
    query += "RETURN tweet.id AS tweet_id"
    return tx.run(query, screen_name=screen_name, tweet_id=tweet_id, tweet=tweet, hashtags=hashtags).single()


def fetch_recommendations(tx, screen_name):
    query = """
    MATCH (currentUser:User {screen_name: $screen_name})
    WITH currentUser.avgSentimentScore AS baseSentiment

    // Find users with similar average sentiment scores
    MATCH (otherUser:User)
    WHERE otherUser.screen_name <> $screen_name
    WITH otherUser, baseSentiment, abs(otherUser.avgSentimentScore - baseSentiment) AS diff
    ORDER BY diff ASC
    LIMIT 10
    WITH baseSentiment, collect(otherUser.screen_name) AS similarUsers

    // Find hashtags with similar average sentiment scores
    MATCH (hashtag:Hashtag)
    WITH similarUsers, baseSentiment, hashtag, abs(hashtag.avgSentimentScore - baseSentiment) AS hashtagDiff
    ORDER BY hashtagDiff ASC
    LIMIT 10
    RETURN similarUsers, collect(hashtag.name) AS similarHashtags
    """
    result = tx.run(query, screen_name=screen_name)
    return result.single()

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_DEBUG'), port=os.getenv('FLASK_PORT'))