"""
FanSense - Database Writer Module
---------------------------------
This module handles writing processed tweets to the PostgreSQL database.
Updated with hashtag extraction functionality.
"""

import psycopg2
import json
import re
import logging
from psycopg2.extras import Json
from typing import List, Dict, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "dbname": "fansense",
    "user": "dee",
    "password": "",
    "host": "localhost",
    "port": 5432,
}

def extract_hashtags(text: str) -> List[str]:
    """
    Extract hashtags from tweet text.
    
    Args:
        text: Tweet text
        
    Returns:
        List of hashtags (without # symbol)
    """
    if not text:
        return []
    
    # Use regex to find all hashtags
    hashtag_pattern = re.compile(r'#(\w+)')
    hashtags = hashtag_pattern.findall(text)
    
    # Convert to lowercase and remove duplicates
    hashtags = [tag.lower() for tag in hashtags]
    return list(set(hashtags))

def extract_mentions(text: str) -> List[str]:
    """
    Extract mentions from tweet text.
    
    Args:
        text: Tweet text
        
    Returns:
        List of mentions (without @ symbol)
    """
    if not text:
        return []
    
    # Use regex to find all mentions
    mention_pattern = re.compile(r'@(\w+)')
    mentions = mention_pattern.findall(text)
    
    # Remove duplicates
    return list(set(mentions))

def insert_tweet(tweet, user, sentiment_data=None, location_data=None):
    """
    Insert a single tweet with sentiment and location data into the database.
    
    Args:
        tweet: Raw tweet data
        user: User data
        sentiment_data: Optional sentiment analysis results
        location_data: Optional location parsing results
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Extract basic tweet data
    tweet_id = tweet["id"]
    author_id = tweet["author_id"]
    username = user.get("username", "")
    text = tweet["text"]
    created_at = tweet["created_at"]
    lang = tweet["lang"]
    geo_place_id = tweet.get("geo", {}).get("place_id")
    raw_data = tweet
    
    # Extract hashtags and mentions
    hashtags = extract_hashtags(text)
    mentions = extract_mentions(text)
    
    # Extract sentiment data if available
    sentiment_negative = None
    sentiment_neutral = None
    sentiment_positive = None
    sentiment_compound = None
    emotion = None
    
    if sentiment_data:
        sentiment_scores = sentiment_data.get('scores', {})
        sentiment_negative = sentiment_scores.get('negative')
        sentiment_neutral = sentiment_scores.get('neutral')
        sentiment_positive = sentiment_scores.get('positive')
        sentiment_compound = sentiment_scores.get('compound')
        emotion = sentiment_data.get('emotion')
    
    # Extract location data if available
    location_raw = None
    location_address = None
    latitude = None
    longitude = None
    country = None
    state_province = None
    city = None
    
    if location_data:
        location_raw = location_data.get('raw_location')
        geocoded = location_data.get('geocoded', {})
        
        if geocoded:
            location_address = geocoded.get('address')
            latitude = geocoded.get('latitude')
            longitude = geocoded.get('longitude')
            
            # Extract location components if available
            address_parts = location_address.split(', ') if location_address else []
            if len(address_parts) >= 3:
                city = address_parts[-3]
                state_province = address_parts[-2]
                country = address_parts[-1]
            elif len(address_parts) == 2:
                state_province = address_parts[0]
                country = address_parts[1]
            elif len(address_parts) == 1:
                country = address_parts[0]
    
    # Construct query with additional fields
    query = """
    INSERT INTO tweets (
        tweet_id, author_id, username, text, created_at, lang, geo_place_id, 
        sentiment_negative, sentiment_neutral, sentiment_positive, sentiment_compound, emotion,
        location_raw, location_address, latitude, longitude, country, state_province, city,
        hashtags, mentions, raw_data
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (tweet_id) DO UPDATE
    SET hashtags = EXCLUDED.hashtags,
        mentions = EXCLUDED.mentions,
        sentiment_negative = COALESCE(EXCLUDED.sentiment_negative, tweets.sentiment_negative),
        sentiment_neutral = COALESCE(EXCLUDED.sentiment_neutral, tweets.sentiment_neutral),
        sentiment_positive = COALESCE(EXCLUDED.sentiment_positive, tweets.sentiment_positive),
        sentiment_compound = COALESCE(EXCLUDED.sentiment_compound, tweets.sentiment_compound),
        emotion = COALESCE(EXCLUDED.emotion, tweets.emotion),
        location_raw = COALESCE(EXCLUDED.location_raw, tweets.location_raw),
        location_address = COALESCE(EXCLUDED.location_address, tweets.location_address),
        latitude = COALESCE(EXCLUDED.latitude, tweets.latitude),
        longitude = COALESCE(EXCLUDED.longitude, tweets.longitude),
        country = COALESCE(EXCLUDED.country, tweets.country),
        state_province = COALESCE(EXCLUDED.state_province, tweets.state_province),
        city = COALESCE(EXCLUDED.city, tweets.city);
    """
    
    try:
        cur.execute(query, (
            tweet_id, author_id, username, text, created_at, lang, geo_place_id,
            sentiment_negative, sentiment_neutral, sentiment_positive, sentiment_compound, emotion,
            location_raw, location_address, latitude, longitude, country, state_province, city,
            hashtags, mentions, Json(raw_data)
        ))
        conn.commit()
        logger.info(f"Inserted tweet {tweet_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting tweet {tweet_id}: {str(e)}")
    finally:
        cur.close()
        conn.close()

def insert_tweets(tweets, users, sentiment_data_list=None, location_data_list=None):
    """
    Insert multiple tweets with their sentiment and location data into the database.
    
    Args:
        tweets: List of raw tweet data
        users: Dictionary mapping user IDs to user data
        sentiment_data_list: Optional list of sentiment analysis results
        location_data_list: Optional list of location parsing results
        
    Returns:
        Number of tweets inserted
    """
    if not tweets:
        return 0
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    values = []
    
    for i, tweet in enumerate(tweets):
        # Extract basic tweet data
        tweet_id = tweet["id"]
        author_id = tweet["author_id"]
        user = users.get(author_id, {})
        username = user.get("username", "")
        text = tweet["text"]
        created_at = tweet["created_at"]
        lang = tweet["lang"]
        geo_place_id = tweet.get("geo", {}).get("place_id")
        raw_data = tweet
        
        # Extract hashtags and mentions
        hashtags = extract_hashtags(text)
        mentions = extract_mentions(text)
        
        # Extract sentiment data if available
        sentiment_negative = None
        sentiment_neutral = None
        sentiment_positive = None
        sentiment_compound = None
        emotion = None
        
        if sentiment_data_list and i < len(sentiment_data_list):
            sentiment_data = sentiment_data_list[i]
            sentiment_scores = sentiment_data.get('scores', {})
            sentiment_negative = sentiment_scores.get('negative')
            sentiment_neutral = sentiment_scores.get('neutral')
            sentiment_positive = sentiment_scores.get('positive')
            sentiment_compound = sentiment_scores.get('compound')
            emotion = sentiment_data.get('emotion')
        
        # Extract location data if available
        location_raw = None
        location_address = None
        latitude = None
        longitude = None
        country = None
        state_province = None
        city = None
        
        if location_data_list and i < len(location_data_list):
            location_data = location_data_list[i]
            location_raw = location_data.get('raw_location')
            geocoded = location_data.get('geocoded', {})
            
            if geocoded:
                location_address = geocoded.get('address')
                latitude = geocoded.get('latitude')
                longitude = geocoded.get('longitude')
                
                # Extract location components if available
                address_parts = location_address.split(', ') if location_address else []
                if len(address_parts) >= 3:
                    city = address_parts[-3]
                    state_province = address_parts[-2]
                    country = address_parts[-1]
                elif len(address_parts) == 2:
                    state_province = address_parts[0]
                    country = address_parts[1]
                elif len(address_parts) == 1:
                    country = address_parts[0]
        
        # Add to values list
        values.append((
            tweet_id, author_id, username, text, created_at, lang, geo_place_id,
            sentiment_negative, sentiment_neutral, sentiment_positive, sentiment_compound, emotion,
            location_raw, location_address, latitude, longitude, country, state_province, city,
            hashtags, mentions, Json(raw_data)
        ))
    
    # Construct query with additional fields and upsert logic
    query = """
    INSERT INTO tweets (
        tweet_id, author_id, username, text, created_at, lang, geo_place_id, 
        sentiment_negative, sentiment_neutral, sentiment_positive, sentiment_compound, emotion,
        location_raw, location_address, latitude, longitude, country, state_province, city,
        hashtags, mentions, raw_data
    )
    VALUES %s
    ON CONFLICT (tweet_id) DO UPDATE
    SET hashtags = EXCLUDED.hashtags,
        mentions = EXCLUDED.mentions,
        sentiment_negative = COALESCE(EXCLUDED.sentiment_negative, tweets.sentiment_negative),
        sentiment_neutral = COALESCE(EXCLUDED.sentiment_neutral, tweets.sentiment_neutral),
        sentiment_positive = COALESCE(EXCLUDED.sentiment_positive, tweets.sentiment_positive),
        sentiment_compound = COALESCE(EXCLUDED.sentiment_compound, tweets.sentiment_compound),
        emotion = COALESCE(EXCLUDED.emotion, tweets.emotion),
        location_raw = COALESCE(EXCLUDED.location_raw, tweets.location_raw),
        location_address = COALESCE(EXCLUDED.location_address, tweets.location_address),
        latitude = COALESCE(EXCLUDED.latitude, tweets.latitude),
        longitude = COALESCE(EXCLUDED.longitude, tweets.longitude),
        country = COALESCE(EXCLUDED.country, tweets.country),
        state_province = COALESCE(EXCLUDED.state_province, tweets.state_province),
        city = COALESCE(EXCLUDED.city, tweets.city);
    """
    
    try:
        # Use psycopg2's execute_values for better performance
        from psycopg2.extras import execute_values
        execute_values(cur, query, values)
        conn.commit()
        inserted_count = cur.rowcount
        logger.info(f"Inserted {inserted_count} tweets")
        return inserted_count
    except Exception as e:
        conn.rollback()
        logger.error(f"Error batch inserting tweets: {str(e)}")
        return 0
    finally:
        cur.close()
        conn.close()

def update_hashtags_for_all_tweets():
    """
    Process and update hashtags for all tweets in the database that don't have hashtags.
    
    Returns:
        Number of tweets updated
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Get tweets without hashtags
        cur.execute("""
        SELECT tweet_id, text
        FROM tweets
        WHERE hashtags IS NULL OR array_length(hashtags, 1) IS NULL;
        """)
        
        tweets = cur.fetchall()
        logger.info(f"Found {len(tweets)} tweets without hashtags")
        
        updated_count = 0
        
        for tweet_id, text in tweets:
            hashtags = extract_hashtags(text)
            
            if hashtags:
                update_query = """
                UPDATE tweets
                SET hashtags = %s
                WHERE tweet_id = %s;
                """
                
                cur.execute(update_query, (hashtags, tweet_id))
                updated_count += 1
        
        conn.commit()
        logger.info(f"Updated hashtags for {updated_count} tweets")
        return updated_count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating hashtags: {str(e)}")
        return 0
    finally:
        cur.close()
        conn.close()

# For testing
if __name__ == "__main__":
    # Update hashtags for all tweets
    update_hashtags_for_all_tweets()
