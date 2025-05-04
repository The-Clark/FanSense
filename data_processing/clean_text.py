"""
FanSense - Text Cleaning Module
-------------------------------
This module provides functions for cleaning and preprocessing tweet text
before sentiment analysis and location parsing.
"""

import re
import html
import string
from typing import List, Dict, Any, Optional

def remove_urls(text: str) -> str:
    """
    Remove URLs from text.
    
    Args:
        text: Input text
        
    Returns:
        Text with URLs removed
    """
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    return url_pattern.sub('', text)

def remove_mentions(text: str) -> str:
    """
    Remove @mentions from text.
    
    Args:
        text: Input text
        
    Returns:
        Text with mentions removed
    """
    mention_pattern = re.compile(r'@\w+')
    return mention_pattern.sub('', text)

def extract_hashtags(text: str) -> List[str]:
    """
    Extract hashtags from text.
    
    Args:
        text: Input text
        
    Returns:
        List of hashtags (without the # symbol)
    """
    hashtag_pattern = re.compile(r'#(\w+)')
    return hashtag_pattern.findall(text)

def clean_hashtags(text: str, keep_text: bool = True) -> str:
    """
    Clean hashtags from text or keep the text without the # symbol.
    
    Args:
        text: Input text
        keep_text: Whether to keep the hashtag text (without #)
        
    Returns:
        Cleaned text
    """
    if keep_text:
        # Replace #word with word
        return re.sub(r'#(\w+)', r'\1', text)
    else:
        # Remove hashtags entirely
        return re.sub(r'#\w+', '', text)

def remove_rt_prefix(text: str) -> str:
    """
    Remove 'RT @user:' prefix from retweets.
    
    Args:
        text: Input text
        
    Returns:
        Text without RT prefix
    """
    rt_pattern = re.compile(r'^RT @\w+: ')
    return rt_pattern.sub('', text)

def unescape_html(text: str) -> str:
    """
    Unescape HTML entities.
    
    Args:
        text: Input text with HTML entities
        
    Returns:
        Text with HTML entities unescaped
    """
    return html.unescape(text)

def remove_punctuation(text: str) -> str:
    """
    Remove punctuation from text.
    
    Args:
        text: Input text
        
    Returns:
        Text without punctuation
    """
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)

def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace (remove extra spaces, tabs, newlines).
    
    Args:
        text: Input text
        
    Returns:
        Text with normalized whitespace
    """
    return ' '.join(text.split())

def remove_emojis(text: str) -> str:
    """
    Remove emojis from text.
    
    Args:
        text: Input text
        
    Returns:
        Text without emojis
    """
    # Unicode ranges for emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0000257F"  # Enclosed characters
        "]", 
        flags=re.UNICODE
    )
    return emoji_pattern.sub('', text)

def extract_cashtags(text: str) -> List[str]:
    """
    Extract cashtags ($symbol) from text.
    
    Args:
        text: Input text
        
    Returns:
        List of cashtags (without the $ symbol)
    """
    cashtag_pattern = re.compile(r'\$(\w+)')
    return cashtag_pattern.findall(text)

def clean_cashtags(text: str, keep_text: bool = True) -> str:
    """
    Clean cashtags from text or keep the text without the $ symbol.
    
    Args:
        text: Input text
        keep_text: Whether to keep the cashtag text (without $)
        
    Returns:
        Cleaned text
    """
    if keep_text:
        # Replace $word with word
        return re.sub(r'\$(\w+)', r'\1', text)
    else:
        # Remove cashtags entirely
        return re.sub(r'\$\w+', '', text)

def extract_mentions(text: str) -> List[str]:
    """
    Extract @mentions from text.
    
    Args:
        text: Input text
        
    Returns:
        List of mentions (without the @ symbol)
    """
    mention_pattern = re.compile(r'@(\w+)')
    return mention_pattern.findall(text)

def remove_numbers(text: str) -> str:
    """
    Remove numbers from text.
    
    Args:
        text: Input text
        
    Returns:
        Text without numbers
    """
    return re.sub(r'\d+', '', text)

def preprocess_for_sentiment(text: str) -> str:
    """
    Preprocess text for sentiment analysis.
    
    Args:
        text: Raw tweet text
        
    Returns:
        Preprocessed text ready for sentiment analysis
    """
    if not text:
        return ""
        
    # Apply preprocessing steps
    text = unescape_html(text)
    text = remove_rt_prefix(text)
    text = remove_urls(text)
    text = remove_mentions(text)
    text = clean_hashtags(text, keep_text=True)
    text = clean_cashtags(text, keep_text=True)
    text = remove_emojis(text)
    text = normalize_whitespace(text)
    
    return text.strip()

def preprocess_for_location(text: str) -> str:
    """
    Preprocess text for location extraction.
    
    Args:
        text: Raw tweet text
        
    Returns:
        Preprocessed text ready for location extraction
    """
    if not text:
        return ""
        
    # Apply preprocessing steps
    text = unescape_html(text)
    text = remove_rt_prefix(text)
    text = remove_urls(text)
    text = remove_mentions(text)
    text = clean_hashtags(text, keep_text=True)
    
    # For location extraction, we keep emojis and punctuation
    # as they might be useful for location context
    text = normalize_whitespace(text)
    
    return text.strip()

def extract_entities(text: str) -> Dict[str, List[str]]:
    """
    Extract all entities (hashtags, mentions, cashtags) from text.
    
    Args:
        text: Input text
        
    Returns:
        Dictionary with lists of entities
    """
    return {
        'hashtags': extract_hashtags(text),
        'mentions': extract_mentions(text),
        'cashtags': extract_cashtags(text)
    }

def clean_text(text: str, for_sentiment: bool = True) -> str:
    """
    General purpose text cleaning function.
    
    Args:
        text: Input text
        for_sentiment: Whether to optimize for sentiment analysis (True) or location extraction (False)
        
    Returns:
        Cleaned text
    """
    if for_sentiment:
        return preprocess_for_sentiment(text)
    else:
        return preprocess_for_location(text)
