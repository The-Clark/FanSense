"""
Script to update hashtags for all tweets in the database.
Run this script directly to extract and store hashtags.
"""

import psycopg2
import re
import logging
from typing import List

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

def update_hashtags_for_all_tweets():
    """
    Process and update hashtags for all tweets in the database.
    
    Returns:
        Number of tweets updated
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Get all tweets
        cur.execute("SELECT tweet_id, text FROM tweets")
        
        tweets = cur.fetchall()
        logger.info(f"Found {len(tweets)} tweets to process")
        
        updated_count = 0
        
        for tweet_id, text in tweets:
            hashtags = extract_hashtags(text)
            
            # Update the hashtags column
            update_query = """
            UPDATE tweets
            SET hashtags = %s
            WHERE tweet_id = %s;
            """
            
            cur.execute(update_query, (hashtags, tweet_id))
            updated_count += 1
            
            # Log progress for every 100 tweets
            if updated_count % 100 == 0:
                logger.info(f"Processed {updated_count}/{len(tweets)} tweets")
        
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

if __name__ == "__main__":
    logger.info("Starting hashtag update process")
    updated = update_hashtags_for_all_tweets()
    logger.info(f"Completed hashtag update. Updated {updated} tweets.")
