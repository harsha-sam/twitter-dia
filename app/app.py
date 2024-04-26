from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json

app = Flask(__name__)

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Function to deliver messages to Kafka
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# API endpoint to receive a tweet
@app.route('/tweet', methods=['POST'])
def post_tweet():
    data = request.json
    tweet_text = data.get('tweet')
    user_id = data.get('user_id')
    if not tweet_text or not user_id:
        return jsonify({'error': 'Tweet text or User ID is missing'}), 400

    message = json.dumps({'tweet': tweet_text, 'user_id': user_id})
    # Stream the tweet to Kafka on the topic 'RAW'
    producer.produce('RAW', message.encode('utf-8'), callback=delivery_report)
    producer.flush()  # Ensure all messages are sent out

    return jsonify({'status': 'Tweet sent to Kafka'}), 200

if __name__ == '__main__':
    app.run(debug=True)
