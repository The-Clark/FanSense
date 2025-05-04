"""
FanSense - Twitter Stream Module
--------------------------------
This module handles the connection to Twitter API v2 to stream tweets based on keywords or hashtags.
"""

import os
import json
import time
import logging
import requests
from typing import List, Dict, Any, Optional, Generator, Callable
import sys
from datetime import datetime

# Add the project root to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import other modules
from data_processing.sentiment_model import SentimentAnalyzer
from data_processing.location_parser import LocationParser
from db.db_writer import insert_tweet, insert_tweets

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TwitterStream:
    """Class for connecting to Twitter API and streaming tweets."""
    
    def __init__(self, bearer_token: Optional[str] = None):
        """
        Initialize the Twitter stream connection.
    
        Args:
            bearer_token: Twitter API Bearer Token (will use env var TWITTER_BEARER_TOKEN if None)
        """
        self.bearer_token = bearer_token or os.environ.get("TWITTER_BEARER_TOKEN", "your_bearer_token")
        if not self.bearer_token:
            raise ValueError("Twitter Bearer Token is required. Set it as an argument or as TWITTER_BEARER_TOKEN environment variable.")
        
        self.base_url = "https://api.twitter.com/2"
        self.sentiment_analyzer = SentimentAnalyzer()
        self.location_parser = LocationParser()
        logger.info("TwitterStream initialized")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Twitter API requests."""
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json"
        }
    
    def search_recent_tweets(self, query: str, max_results: int = 100, next_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Search for recent tweets based on a query.
        
        Args:
            query: Search query (hashtags, keywords, etc.)
            max_results: Maximum number of results to return (10-100)
            next_token: Pagination token for subsequent requests
            
        Returns:
            Response from Twitter API
        """
        # Define tweet fields to retrieve
        tweet_fields = [
            "id", "text", "author_id", "created_at", "geo", "lang",
            "public_metrics", "entities"
        ]
        
        # Define user fields to retrieve
        user_fields = ["id", "name", "username", "location", "description"]
        
        # Construct URL
        url = f"{self.base_url}/tweets/search/recent"
        
        # Construct params
        params = {
            "query": query,
            "max_results": max_results,
            "tweet.fields": ",".join(tweet_fields),
            "user.fields": ",".join(user_fields),
            "expansions": "author_id,geo.place_id"
        }
        
        # Add pagination token if provided
        if next_token:
            params["next_token"] = next_token
        
        # Make request
        try:
            response = requests.get(
                url,
                params=params,
                headers=self._get_headers()
            )
            
            # Check if request was successful
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limit exceeded
                reset_time = int(response.headers.get("x-rate-limit-reset", 0))
                wait_time = max(reset_time - int(time.time()), 1)
                logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds.")
                time.sleep(wait_time)
                return self.search_recent_tweets(query, max_results, next_token)
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Error searching tweets: {str(e)}")
            return {}
    
    def create_filtered_stream_rule(self, value: str, tag: Optional[str] = None) -> bool:
        """
        Create a rule for the filtered stream.
        
        Args:
            value: Rule value (e.g., hashtags, keywords)
            tag: Optional tag for the rule
            
        Returns:
            Success status
        """
        url = f"{self.base_url}/tweets/search/stream/rules"
        
        # Construct payload
        payload = {
            "add": [
                {
                    "value": value,
                    "tag": tag or value
                }
            ]
        }
        
        # Make request
        try:
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload
            )
            
            # Check if request was successful
            if response.status_code == 201:
                logger.info(f"Created stream rule: {value}")
                return True
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error creating stream rule: {str(e)}")
            return False
    
    def get_stream_rules(self) -> List[Dict[str, Any]]:
        """
        Get current filtered stream rules.
        
        Returns:
            List of rules
        """
        url = f"{self.base_url}/tweets/search/stream/rules"
        
        # Make request
        try:
            response = requests.get(
                url,
                headers=self._get_headers()
            )
            
            # Check if request was successful
            if response.status_code == 200:
                return response.json().get("data", [])
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error getting stream rules: {str(e)}")
            return []
    
    def delete_stream_rules(self, rule_ids: List[str]) -> bool:
        """
        Delete filtered stream rules.
        
        Args:
            rule_ids: List of rule IDs to delete
            
        Returns:
            Success status
        """
        if not rule_ids:
            return True
            
        url = f"{self.base_url}/tweets/search/stream/rules"
        
        # Construct payload
        payload = {
            "delete": {
                "ids": rule_ids
            }
        }
        
        # Make request
        try:
            response = requests.post(
                url,
                headers=self._get_headers(),
                json=payload
            )
            
            # Check if request was successful
            if response.status_code == 200:
                logger.info(f"Deleted {len(rule_ids)} stream rules")
                return True
            else:
                logger.error(f"Error {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error deleting stream rules: {str(e)}")
            return False
    
    def reset_stream_rules(self) -> bool:
        """
        Reset all filtered stream rules.
        
        Returns:
            Success status
        """
        # Get current rules
        rules = self.get_stream_rules()
        
        # If there are rules, delete them
        if rules:
            rule_ids = [rule["id"] for rule in rules]
            return self.delete_stream_rules(rule_ids)
        
        return True
    
    def connect_to_filtered_stream(
        self,
        on_data: Optional[Callable[[Dict[str, Any], Dict[str, Any]], None]] = None,
        process_data: bool = True,
        save_to_db: bool = True,
        expansions: str = "author_id,geo.place_id",
        tweet_fields: str = "id,text,author_id,created_at,geo,lang,public_metrics,entities",
        user_fields: str = "id,name,username,location,description",
    ) -> None:
        """
        Connect to the filtered stream and process incoming tweets.
        
        Args:
            on_data: Optional callback to call for each tweet
            process_data: Whether to process sentiment and location
            save_to_db: Whether to save tweets to the database
            expansions: Fields to expand in the API response
            tweet_fields: Tweet fields to include
            user_fields: User fields to include
        """
        url = f"{self.base_url}/tweets/search/stream"
        
        # Construct params
        params = {
            "expansions": expansions,
            "tweet.fields": tweet_fields,
            "user.fields": user_fields
        }
        
        # Make request
        try:
            with requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                stream=True
            ) as response:
                if response.status_code != 200:
                    logger.error(f"Error {response.status_code}: {response.text}")
                    return
                
                logger.info("Connected to Twitter filtered stream")
                
                for line in response.iter_lines():
                    if not line:
                        continue
                    
                    try:
                        tweet_data = json.loads(line)
                        
                        # Extract tweet and user data
                        tweet = tweet_data.get("data", {})
                        users = {user["id"]: user for user in tweet_data.get("includes", {}).get("users", [])}
                        user = users.get(tweet.get("author_id", ""), {})
                        
                        if process_data:
                            # Process sentiment
                            tweet_with_sentiment = self.sentiment_analyzer.analyze_tweet(tweet)
                            sentiment_data = tweet_with_sentiment.get("sentiment", {})
                            
                            # Process location
                            tweet_with_location = self.location_parser.parse_tweet_location(tweet)
                            location_data = tweet_with_location.get("location", {})
                        else:
                            sentiment_data = None
                            location_data = None
                        
                        # Save to database if requested
                        if save_to_db:
                            insert_tweet(tweet, user, sentiment_data, location_data)
                        
                        # Call callback if provided
                        if on_data:
                            processed_tweet = {
                                **tweet,
                                "sentiment": sentiment_data,
                                "location": location_data
                            }
                            on_data(processed_tweet, user)
                    
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse tweet: {line}")
                    except Exception as e:
                        logger.error(f"Error processing tweet: {str(e)}")
        
        except requests.exceptions.ChunkedEncodingError:
            logger.error("Connection broken, attempting to reconnect...")
            time.sleep(5)
            self.connect_to_filtered_stream(on_data, process_data, save_to_db)
        except Exception as e:
            logger.error(f"Error connecting to stream: {str(e)}")
    
    def collect_tweets_by_search(
        self,
        query: str,
        max_tweets: int = 1000,
        process_data: bool = True,
        save_to_db: bool = True,
        batch_size: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Collect tweets using the search API.
        
        Args:
            query: Search query (hashtags, keywords, etc.)
            max_tweets: Maximum number of tweets to collect
            process_data: Whether to process sentiment and location
            save_to_db: Whether to save tweets to the database
            batch_size: Number of tweets to process in a batch
            
        Returns:
            List of collected tweets
        """
        collected_tweets = []
        next_token = None
        max_requests = (max_tweets // batch_size) + 1
        request_count = 0
        
        logger.info(f"Collecting up to {max_tweets} tweets for query: {query}")
        
        while request_count < max_requests and len(collected_tweets) < max_tweets:
            # Get batch of tweets
            current_batch_size = min(batch_size, max_tweets - len(collected_tweets))
            result = self.search_recent_tweets(query, current_batch_size, next_token)
            
            # Check if we got data
            if not result or "data" not in result:
                logger.warning("No more data available or API error")
                break
            
            # Extract tweets and users
            tweets = result.get("data", [])
            users = {user["id"]: user for user in result.get("includes", {}).get("users", [])}
            
            # Process tweets
            processed_tweets = []
            processed_users = {}
            
            for tweet in tweets:
                user = users.get(tweet.get("author_id", ""), {})
                processed_users[user.get("id", "")] = user
                
                if process_data:
                    # Process sentiment
                    tweet_with_sentiment = self.sentiment_analyzer.analyze_tweet(tweet)
                    
                    # Process location
                    tweet_with_location = self.location_parser.parse_tweet_location(tweet_with_sentiment)
                    
                    processed_tweets.append(tweet_with_location)
                else:
                    processed_tweets.append(tweet)
            
            # Save to database if requested
            if save_to_db and processed_tweets:
                # Get sentiment and location data from processed tweets
                sentiment_data_list = [
                    tweet.get("sentiment", {})
                    for tweet in processed_tweets
                ]
                
                location_data_list = [
                    tweet.get("location", {})
                    for tweet in processed_tweets
                ]
                
                # Insert tweets
                insert_tweets(tweets, processed_users, sentiment_data_list, location_data_list)
            
            # Add to collected tweets
            collected_tweets.extend(processed_tweets)
            
            # Check if there's a next token
            next_token = result.get("meta", {}).get("next_token")
            request_count += 1
            
            # If no next token, we've reached the end
            if not next_token:
                break
            
            # Log progress
            logger.info(f"Collected {len(collected_tweets)} tweets so far")
            
            # Sleep a little to avoid rate limits
            time.sleep(2)
        
        logger.info(f"Collected a total of {len(collected_tweets)} tweets")
        return collected_tweets


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect tweets from Twitter API.')
    parser.add_argument('--token', help='Twitter API Bearer Token')
    parser.add_argument('--query', required=True, help='Search query (hashtags, keywords, etc.)')
    parser.add_argument('--mode', choices=['search', 'stream'], default='search', help='Collection mode')
    parser.add_argument('--max-tweets', type=int, default=1000, help='Maximum number of tweets to collect (search mode)')
    parser.add_argument('--process', action='store_true', help='Process sentiment and location')
    parser.add_argument('--save', action='store_true', help='Save tweets to database')
    parser.add_argument('--output', help='Output file path (JSON)')
    args = parser.parse_args()
    
    # Initialize TwitterStream
    twitter_stream = TwitterStream(args.token)
    
    # Collect tweets based on mode
    if args.mode == 'search':
        tweets = twitter_stream.collect_tweets_by_search(
            args.query,
            max_tweets=args.max_tweets,
            process_data=args.process,
            save_to_db=args.save
        )
        
        # Save to output file if specified
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(tweets, f, indent=2)
            logger.info(f"Saved {len(tweets)} tweets to {args.output}")
    else:
        # Stream mode
        if args.save:
            logger.info("Tweets will be saved to database")
        
        # Reset stream rules
        twitter_stream.reset_stream_rules()
        
        # Add stream rule
        twitter_stream.create_filtered_stream_rule(args.query)
        
        # Connect to stream
        def on_data(tweet, user):
            logger.info(f"Received tweet from @{user.get('username')}: {tweet.get('text')[:50]}...")
            
            # Save to output file if specified
            if args.output:
                with open(args.output, 'a') as f:
                    f.write(json.dumps({"tweet": tweet, "user": user}) + "\n")
        
        twitter_stream.connect_to_filtered_stream(
            on_data=on_data if args.output else None,
            process_data=args.process,
            save_to_db=args.save
        )
