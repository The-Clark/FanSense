"""
FanSense - Tweet Processing Script
---------------------------------
This script processes tweets from the database to add sentiment and location data.
It can be run as a standalone script or scheduled as a batch job.
"""

import os
import sys
import argparse
import logging
import psycopg2
import psycopg2.extras
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Add the project root to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our modules
from data_processing.sentiment_model import SentimentAnalyzer
from data_processing.location_parser import LocationParser
from db.db_writer import update_schema

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process_tweets.log"),
        logging.StreamHandler()
    ]
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

def get_unprocessed_tweets(batch_size=100):
    """
    Get tweets from the database that haven't been processed for sentiment and location.
    
    Args:
        batch_size: Number of tweets to process in a batch
        
    Returns:
        List of tweets and users
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        # Get tweets without sentiment or location data
        query = """
        SELECT * FROM tweets 
        WHERE (sentiment_compound IS NULL OR latitude IS NULL)
        ORDER BY created_at DESC
        LIMIT %s;
        """
        
        cur.execute(query, (batch_size,))
        tweets = cur.fetchall()
        
        # Convert to list of dictionaries
        tweet_list = []
        user_dict = {}
        
        for tweet in tweets:
            # Extract the raw_data JSON field from each tweet
            raw_data = tweet['raw_data'] if 'raw_data' in tweet else {}
            
            # If raw_data is a string, parse it to JSON
            if isinstance(raw_data, str):
                try:
                    raw_data = json.loads(raw_data)
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse raw_data for tweet {tweet['tweet_id']}")
                    raw_data = {}
            
            # Add to tweet list
            tweet_list.append({
                'id': tweet['tweet_id'],
                'author_id': tweet['author_id'],
                'text': tweet['text'],
                'created_at': tweet['created_at'].isoformat() if isinstance(tweet['created_at'], datetime) else tweet['created_at'],
                'lang': tweet.get('lang', ''),
                'geo': {'place_id': tweet.get('geo_place_id')} if tweet.get('geo_place_id') else {},
                'raw_data': raw_data
            })
            
            # Add user data
            if tweet['author_id'] not in user_dict:
                user_dict[tweet['author_id']] = {
                    'username': tweet['username'],
                    'location': tweet.get('user_location', '')
                }
        
        return tweet_list, user_dict
    except Exception as e:
        logger.error(f"Error getting unprocessed tweets: {str(e)}")
        return [], {}
    finally:
        cur.close()
        conn.close()

def update_tweet_sentiment_location(tweet_id, sentiment_data, location_data):
    """
    Update a tweet with processed sentiment and location data.
    
    Args:
        tweet_id: ID of the tweet to update
        sentiment_data: Sentiment analysis results
        location_data: Location parsing results
        
    Returns:
        Success status
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Extract sentiment data
        sentiment_scores = sentiment_data.get('scores', {})
        sentiment_negative = sentiment_scores.get('negative')
        sentiment_neutral = sentiment_scores.get('neutral')
        sentiment_positive = sentiment_scores.get('positive')
        sentiment_compound = sentiment_scores.get('compound')
        emotion = sentiment_data.get('emotion')
        
        # Extract location data
        location_raw = location_data.get('raw_location')
        geocoded = location_data.get('geocoded', {})
        location_address = None
        latitude = None
        longitude = None
        country = None
        state_province = None
        city = None
        
        if geocoded:
            location_address = geocoded.get('address')
            latitude = geocoded.get('latitude')
            longitude = geocoded.get('longitude')
            
            # Extract location components
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
        
        # Build update query
        query = """
        UPDATE tweets SET
            sentiment_negative = %s,
            sentiment_neutral = %s,
            sentiment_positive = %s,
            sentiment_compound = %s,
            emotion = %s,
            location_raw = %s,
            location_address = %s,
            latitude = %s,
            longitude = %s,
            country = %s,
            state_province = %s,
            city = %s
        WHERE tweet_id = %s;
        """
        
        cur.execute(query, (
            sentiment_negative, sentiment_neutral, sentiment_positive, sentiment_compound, emotion,
            location_raw, location_address, latitude, longitude, country, state_province, city,
            tweet_id
        ))
        
        conn.commit()
        logger.info(f"Updated tweet {tweet_id} with sentiment and location data")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating tweet {tweet_id}: {str(e)}")
        return False
    finally:
        cur.close()
        conn.close()

def main():
    """Main function to process tweets."""
    parser = argparse.ArgumentParser(description='Process tweets to add sentiment and location data.')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of tweets to process in a batch')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode, processing tweets as they arrive')
    parser.add_argument('--delay', type=int, default=60, help='Delay in seconds between batches when running in continuous mode')
    args = parser.parse_args()
    
    # Initialize analyzers
    sentiment_analyzer = SentimentAnalyzer()
    location_parser = LocationParser(user_agent="FanSense_App")
    
    # Ensure database schema is updated
    update_schema()
    
    logger.info(f"Starting tweet processing with batch size {args.batch_size}")
    
    # Process tweets
    def process_batch():
        # Get unprocessed tweets
        tweets, users = get_unprocessed_tweets(args.batch_size)
        
        if not tweets:
            logger.info("No unprocessed tweets found")
            return 0
        
        count = 0
        
        for tweet in tweets:
            # Process sentiment
            sentiment_result = sentiment_analyzer.analyze_tweet(tweet)
            sentiment_data = sentiment_result.get('sentiment', {})
            
            # Process location
            location_result = location_parser.parse_tweet_location(tweet)
            location_data = location_result.get('location', {})
            
            # Update the database
            success = update_tweet_sentiment_location(
                tweet['id'],
                sentiment_data,
                location_data
            )
            
            if success:
                count += 1
        
        logger.info(f"Processed {count}/{len(tweets)} tweets successfully")
        return len(tweets)
    
    # Main processing loop
    if args.continuous:
        logger.info(f"Running in continuous mode with {args.delay} second delay between batches")
        
        import time
        try:
            while True:
                processed = process_batch()
                if processed == 0:
                    logger.info(f"No tweets to process, waiting {args.delay} seconds")
                time.sleep(args.delay)
        except KeyboardInterrupt:
            logger.info("Processing stopped by user")
    else:
        process_batch()
        logger.info("Processing complete")

if __name__ == "__main__":
    main()
