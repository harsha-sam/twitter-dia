import re
from datetime import datetime
import pytz
import bcrypt

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


def extract_mentions(tweet_text):
    """
    Extract user mentions from a string using a regular expression.
    Usernames are assumed to start with an alphabetic character and can contain
    numbers, letters, and underscores.
    """
    mentions = re.findall(r"@([a-zA-Z]\w+)", tweet_text)
    return mentions


def extract_hashtags(tweet_text):
    """
    Extract hashtags from a string using a regular expression.
    Hashtags start with '#' and can include letters, numbers, and underscores.
    """
    hashtags = re.findall(r"#(\w+)", tweet_text)
    return hashtags


def hash_password(password):
    """Hash a password for storing."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
