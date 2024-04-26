from neo4j import GraphDatabase
from textblob import TextBlob

class SentimentUpdater:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def update_sentiments(self):
        with self.driver.session() as session:
            # Update the sentiment score of each Tweet
            tweets = session.read_transaction(self._get_all_tweets)
            for tweet in tweets:
                sentiment_score = self._analyze_sentiment(tweet['text'])
                session.write_transaction(self._update_tweet_sentiment, tweet['id'], sentiment_score)
            
            # Update the average sentiment score for Hashtags and Users
            session.write_transaction(self._update_hashtags_avg_sentiment)
            session.write_transaction(self._update_users_avg_sentiment)

    @staticmethod
    def _get_all_tweets(tx):
        # Retrieve all tweets that do not have a sentiment score
        query = (
            "MATCH (t:Tweet) "
            "WHERE t.text IS NOT NULL "
            "RETURN t.id AS id, t.text AS text"
        )
        return list(tx.run(query))

    def _analyze_sentiment(self, text):
        # Simple sentiment analysis using TextBlob
        return TextBlob(text).sentiment.polarity

    @staticmethod
    def _update_tweet_sentiment(tx, tweet_id, sentiment_score):
        query = (
            "MATCH (t:Tweet {id: $tweet_id}) "
            "SET t.sentimentScore = $sentiment_score"
        )
        tx.run(query, tweet_id=tweet_id, sentiment_score=sentiment_score)

    @staticmethod
    def _update_hashtags_avg_sentiment(tx):
        query = (
            "MATCH (h:Hashtag)<-[:TAGS]-(t:Tweet) "
            "WITH h, AVG(t.sentimentScore) AS avgScore "
            "SET h.avgSentimentScore = avgScore"
        )
        tx.run(query)

    @staticmethod
    def _update_users_avg_sentiment(tx):
        query = (
            "MATCH (u:User)-[:POSTS]->(t:Tweet) "
            "WITH u, AVG(t.sentimentScore) AS avgScore "
            "SET u.avgSentimentScore = avgScore"
        )
        tx.run(query)

# Neo4j connection details
uri = "neo4j://localhost:7687"
user = "neo4j"
password = "zpv@ntu7WAY6paq3nbe"

# Create a new instance of the sentiment updater and run it
sentiment_updater = SentimentUpdater(uri, user, password)
try:
    sentiment_updater.update_sentiments()
finally:
    sentiment_updater.close()
