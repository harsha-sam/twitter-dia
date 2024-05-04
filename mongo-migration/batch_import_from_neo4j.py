import os
import pytz
import bcrypt
from pymongo import MongoClient
from neo4j import GraphDatabase
from datetime import datetime

# Configuration for Neo4j
neo4j_uri = os.getenv('NEO4JDB_URI')
neo4j_user = os.getenv('NEO4JDB_USER')
neo4j_password = os.getenv('NEO4JDB_PASSWORD')

# Configuration for MongoDB
mongo_uri = os.getenv('MONGO_URI')
mongo_db_name = os.getenv('DB_NAME')
mongo_user_collection = "users"
mongo_tweet_collection = "tweets"

# Connect to Neo4j
neo4j_driver = GraphDatabase.driver(
    neo4j_uri, auth=(neo4j_user, neo4j_password))

# Connect to MongoDB
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
mongo_users = mongo_db[mongo_user_collection]
mongo_tweets = mongo_db[mongo_tweet_collection]


def convert_neo4j_datetime(neo4j_datetime):
    """Convert a neo4j.time.DateTime object to a Python datetime.datetime object."""
    # Extract components from the Neo4j DateTime
    year = neo4j_datetime.year
    month = neo4j_datetime.month
    day = neo4j_datetime.day
    hour = neo4j_datetime.hour
    minute = neo4j_datetime.minute
    second = neo4j_datetime.second
    # Convert nanoseconds to microseconds
    microsecond = neo4j_datetime.nanosecond // 1000

    # Create a naive datetime object (without timezone)
    dt_naive = datetime(year, month, day, hour, minute, second, microsecond)

    # Use the timezone info from the Neo4j DateTime object to make the datetime aware
    # Ensure the tzinfo is converted to a string if necessary
    timezone = pytz.timezone(str(neo4j_datetime.tzinfo))
    dt_aware = timezone.localize(dt_naive)

    return dt_aware


def is_mongodb_empty():
    user_count = mongo_users.count_documents({})
    tweet_count = mongo_tweets.count_documents({})
    return user_count == 0 and tweet_count == 0


def hash_password(password):
    """Hash a password for storing."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def fetch_and_update_users():
    default_password = "defaultPassword123"  # Default password for all users
    hashed_password = hash_password(default_password)

    with neo4j_driver.session() as session:
        user_query = "MATCH (user:User) RETURN user"
        result = session.run(user_query)
        for record in result:
            record = record.data()
            user = record["user"]
            # Add hashed password to user data
            user['password'] = hashed_password
            mongo_users.insert_one(user)


def fetch_and_update_tweets():
    with neo4j_driver.session() as session:
        tweet_query = "MATCH (tweet:Tweet)<-[:POSTS]-(user:User) RETURN tweet, user.screen_name AS username"
        result = session.run(tweet_query)
        for record in result:
            record = record.data()
            tweet = record["tweet"]
            tweet["created_at"] = convert_neo4j_datetime(tweet["created_at"])
            tweet["username"] = record["username"]
            mongo_tweets.insert_one(tweet)

def main():
    if is_mongodb_empty():
       fetch_and_update_users()
       fetch_and_update_tweets()
       print("Data migration from Neo4j to MongoDB completed successfully.")
    else:
        print("MongoDB is not empty. Data migration skipped.")


if __name__ == '__main__':
    main()
