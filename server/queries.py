import uuid

def add_tweet_to_neo4j(tx, screen_name, tweet, hashtags, mentions):
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
    if mentions:
        query += """
        WITH tweet, $mentions AS usertags
        UNWIND usertags AS userTag
        OPTIONAL MATCH (mentionedUser:User {screen_name: userTag}) 
        WITH tweet, collect(mentionedUser) AS mentionedUsers
        UNWIND mentionedUsers AS validUser
        WITH tweet, validUser
        WHERE validUser IS NOT NULL
        MERGE (tweet)-[:MENTIONS]->(validUser)
        """
    query += "RETURN tweet AS tweet"
    return tx.run(query, screen_name=screen_name, tweet_id=tweet_id, tweet=tweet, hashtags=hashtags, mentions=mentions).single()


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

