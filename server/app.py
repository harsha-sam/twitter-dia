import os
import socket
import bcrypt
from utils import convert_neo4j_datetime, extract_hashtags, extract_mentions
from flask import Flask, request, jsonify, json, render_template, session, redirect, url_for
from confluent_kafka import Producer
from pymongo import MongoClient
from neo4j import GraphDatabase
from queries import add_tweet_to_neo4j, fetch_recommendations

app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

# Neo4j connection details
neo4jdb_uri = os.getenv('NEO4JDB_URI')
neo4jdb_user = os.getenv('NEO4JDB_USER')
neo4jdb_password = os.getenv('NEO4JDB_PASSWORD')
neo4jdb_driver = GraphDatabase.driver(neo4jdb_uri, auth=(neo4jdb_uri, neo4jdb_password))

mongo_client = MongoClient(os.getenv('MONGO_URI'))
mongo_db = mongo_client['twitter']
mongoc_users = mongo_db['users']
mongoc_tweets = mongo_db['tweets']

#kafka connection
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'client.id': socket.gethostname()
}
producer = Producer(conf)


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/')
def index():
    if 'username' in session:
        return render_template('home.html', username=session['username'])
    return redirect(url_for('login'))

@app.route('/api')
def hello():
    return 'Hello, World!'

@app.route('/api/login', methods=['POST'])
def signin():
    username = request.json.get('username')
    password = request.json.get('password')
    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    user = mongoc_users.find_one({'screen_name': username})
    if not user or not bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
        return jsonify({'error': 'Invalid credentials'}), 401

    session['username'] = user['screen_name']
    return redirect(url_for('index'))

@app.route('/api/tweet', methods=['POST'])
def post_tweet():
    if 'username' not in session:
        return jsonify({'error': 'User not logged in'}), 401

    try:
        tweet_text = request.form['tweet']
        if not tweet_text:
            return jsonify({'error': 'Tweet text is missing'}), 400

        screen_name = session['username']
        hashtags = extract_hashtags(tweet_text)
        mentions = extract_mentions(tweet_text)

        with neo4jdb_driver.session() as neo4j_session:
            result = neo4j_session.write_transaction(
                add_tweet_to_neo4j, screen_name, tweet_text, hashtags, mentions)
            # Extract the tweet ID from the result
            tweet = result['tweet']
            recommendations = neo4j_session.read_transaction(
                fetch_recommendations, screen_name)
            user_recommendations = recommendations['similarUsers']
            hashtag_recommendations = recommendations['similarHashtags']
        
        # adding tweet to mongo
        tweet["created_at"] = convert_neo4j_datetime(tweet["created_at"])
        tweet["username"] = screen_name
        mongoc_tweets.insert_one(tweet)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    message = json.dumps(
        {'tweet': tweet_text, 'username': screen_name, 'tweet_id': tweet["id"]})
    partition_key = screen_name[0].lower()
    # Stream the tweet to Kafka on the topic 'RAW'
    producer.produce('RAW', key=partition_key, value=message.encode('utf-8'), callback=acked)
    producer.flush()

    return render_template('home.html', username=session['username'], tweetId=tweet["id"], recommendations={
        "hashtags": hashtag_recommendations,
        "users": user_recommendations
    })

if __name__ == '__main__':
    app.run(debug=os.getenv('FLASK_DEBUG'), port=os.getenv('FLASK_PORT'))