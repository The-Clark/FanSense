"""
FanSense - Sentiment Analysis Module
---------------------------------------
This module provides sentiment analysis functionality for tweets.
Uses VADER (Valence Aware Dictionary and sEntiment Reasoner) for sentiment scoring,
which is well-suited for social media text.
"""

import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from typing import Dict, Any, Union, List, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Ensure VADER lexicon is downloaded
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    logger.info("Downloading VADER lexicon...")
    nltk.download('vader_lexicon')


class SentimentAnalyzer:
    """Class for analyzing sentiment in tweets."""
    
    def __init__(self):
        """Initialize the sentiment analyzer with VADER."""
        self.analyzer = SentimentIntensityAnalyzer()
        logger.info("Sentiment analyzer initialized with VADER")

    def clean_text(self, text: str) -> str:
        """
        Clean tweet text for better sentiment analysis.
        
        Args:
            text: The tweet text to clean
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove URLs
        text = re.sub(r'https?://\S+', '', text)
        
        # Remove user mentions
        text = re.sub(r'@\w+', '', text)
        
        # Remove hashtag symbol but keep the text
        text = re.sub(r'#(\w+)', r'\1', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def get_sentiment_scores(self, text: str) -> Dict[str, float]:
        """
        Get sentiment scores for the given text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with negative, neutral, positive, and compound scores
        """
        cleaned_text = self.clean_text(text)
        if not cleaned_text:
            return {
                'negative': 0.0,
                'neutral': 0.0,
                'positive': 0.0,
                'compound': 0.0
            }
            
        return self.analyzer.polarity_scores(cleaned_text)
    
    def get_emotion_label(self, compound_score: float) -> str:
        """
        Convert compound score to a human-readable emotion label.
        
        Args:
            compound_score: The compound sentiment score from VADER
            
        Returns:
            Emotion label (very_negative, negative, neutral, positive, very_positive)
        """
        if compound_score <= -0.75:
            return "very_negative"
        elif compound_score <= -0.25:
            return "negative"
        elif compound_score <= 0.25:
            return "neutral"
        elif compound_score <= 0.75:
            return "positive"
        else:
            return "very_positive"
    
    def analyze_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze sentiment of a tweet and add sentiment data.
        
        Args:
            tweet: Dictionary containing tweet data
            
        Returns:
            Tweet dictionary with added sentiment analysis
        """
        text = tweet.get('text', '')
        sentiment_scores = self.get_sentiment_scores(text)
        emotion_label = self.get_emotion_label(sentiment_scores['compound'])
        
        # Add sentiment data to tweet
        tweet['sentiment'] = {
            'scores': sentiment_scores,
            'emotion': emotion_label
        }
        
        return tweet
    
    def analyze_tweets(self, tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze sentiment for a list of tweets.
        
        Args:
            tweets: List of tweet dictionaries
            
        Returns:
            List of tweet dictionaries with added sentiment analysis
        """
        logger.info(f"Analyzing sentiment for {len(tweets)} tweets")
        return [self.analyze_tweet(tweet) for tweet in tweets]


# If you want to use BERTweet or RoBERTa instead of VADER, 
# you can implement these classes:

"""
class BERTweetSentimentAnalyzer:
    def __init__(self):
        # Load model using transformers
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained("vinai/bertweet-base")
        self.model = AutoModelForSequenceClassification.from_pretrained("vinai/bertweet-base")
        
    def analyze_tweet(self, tweet):
        # Implement BERTweet sentiment analysis
        pass


class RoBERTaSentimentAnalyzer:
    def __init__(self):
        # Load model using transformers
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment")
        self.model = AutoModelForSequenceClassification.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment")
        
    def analyze_tweet(self, tweet):
        # Implement RoBERTa sentiment analysis
        pass
"""


# Example usage
if __name__ == "__main__":
    # Example tweet
    example_tweet = {
        "id": "1234567890",
        "text": "I absolutely love how our team played tonight! Best game of the season! #GoTeam",
        "created_at": "2025-05-01T12:34:56Z",
        "user": {
            "id": "9876543210",
            "screen_name": "fan123",
            "location": "New York, NY"
        }
    }
    
    # Initialize analyzer
    analyzer = SentimentAnalyzer()
    
    # Analyze tweet
    result = analyzer.analyze_tweet(example_tweet)
    
    # Print results
    print(f"Text: {result['text']}")
    print(f"Sentiment scores: {result['sentiment']['scores']}")
    print(f"Emotion: {result['sentiment']['emotion']}")
