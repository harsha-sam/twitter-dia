import os
import re
import uuid
import socket
import datetime
import jwt
import bcrypt
from flask import Flask, request, jsonify, json
from confluent_kafka import Producer
from bson.objectid import ObjectId
from pymongo import MongoClient
from neo4j import GraphDatabase

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
client = MongoClient(os.getenv('MONGO_URI'))
db = client['twitter']
users = db['users']
tweets = db['tweets']


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

@app.route('/signin', methods=['POST'])
def signin():
    username = request.json.get('username')
    password = request.json.get('password')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    user = users.find_one({'screen_name': username})
    if not user:
        return jsonify({'error': 'Invalid credentials'}), 401

    if bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
        # Create a token
        payload = {
            'user_id': str(user['screen_name']),
            # Token expires in 1 hour
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        }
        token = jwt.encode(
            payload, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token}), 200

    else:
        return jsonify({'error': 'Invalid credentials'}), 401

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
    token = request.headers.get('Authorization')
    if not token:
        return jsonify({'error': 'Authorization token required'}), 400

    try:
        data = request.json
        tweet_text = data.get('tweet')
        if not tweet_text:
            return jsonify({'error': 'Tweet text is missing'}), 400
        
        # Extract hashtags using a regular expression
        hashtags = re.findall(r"#(\w+)", tweet_text)

        data = jwt.decode(
            token, app.config['SECRET_KEY'], algorithms=['HS256'])

        user_id = data['user_id']

        with db_driver.session() as session:
            result = session.write_transaction(
                add_tweet_to_neo4j, user_id, tweet_text, hashtags)
            # Extract the tweet ID from the result
            tweet_id = result['tweet_id']
            recommendations = session.read_transaction(
                fetch_recommendations, user_id)
            user_recommendations = recommendations['similarUsers']
            hashtag_recommendations = recommendations['similarHashtags']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    message = json.dumps(
        {'tweet': tweet_text, 'user_id': user_id, 'tweet_id': tweet_id})
    partition_key = user_id[0].lower()
    # Stream the tweet to Kafka on the topic 'RAW'
    producer.produce('RAW', key=partition_key, value=message.encode('utf-8'), callback=acked)
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