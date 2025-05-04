"""
FanSense - Improved Location Parser Module
-----------------------------------------
This module provides enhanced functionality to extract and parse location information from tweets.
It handles:
1. Explicit location tags in tweets
2. Location mentions in tweet text using a dictionary of known locations
3. User profile location data
"""

import re
import json
import os
import logging
from typing import Dict, Any, List, Tuple, Optional, Set
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Common location patterns in text - improved with more patterns
LOCATION_PATTERNS = [
    r'in ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',          # "in New York", "in Los Angeles"
    r'from ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',        # "from Chicago", "from San Francisco"
    r'at ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',          # "at Miami", "at Washington"
    r'near ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',        # "near Boston", "near Seattle"
    r'to ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',          # "to London", "to Paris"
    r'visiting ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',    # "visiting Tokyo", "visiting Berlin"
    r'live in ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',     # "live in Austin", "live in Toronto"
    r'based in ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',    # "based in NYC", "based in LA"
    r'located in ([A-Z][a-z]+ ?[A-Z]?[a-z]*)',  # "located in Dallas", "located in Seoul"
]

# List of major cities and countries for better location detection
MAJOR_LOCATIONS = {
    # Major cities
    "new york", "los angeles", "chicago", "houston", "phoenix", "philadelphia", 
    "san antonio", "san diego", "dallas", "san jose", "austin", "san francisco", 
    "boston", "seattle", "miami", "atlanta", "tokyo", "delhi", "shanghai", "sao paulo", 
    "mexico city", "cairo", "mumbai", "beijing", "dhaka", "osaka", "london", "paris", 
    "istanbul", "moscow", "karachi", "lagos", "manila", "berlin", "rome", "madrid", 
    "toronto", "sydney", "melbourne", "singapore", "dubai", "bangkok", "hong kong", 
    "kuala lumpur", "jakarta", "seoul", "tehran", "brussels", "johannesburg", "kiev",
    
    # Countries
    "usa", "united states", "america", "canada", "mexico", "brazil", "argentina", 
    "uk", "united kingdom", "england", "france", "germany", "spain", "italy", 
    "russia", "china", "japan", "india", "australia", "south korea", "north korea",
    "egypt", "south africa", "nigeria", "kenya", "pakistan", "bangladesh", "thailand",
    "vietnam", "malaysia", "indonesia", "philippines", "new zealand", "ireland",
    "portugal", "sweden", "norway", "denmark", "finland", "belgium", "netherlands",
    "austria", "switzerland", "poland", "ukraine", "turkey", "iran", "iraq", "saudi arabia",
    "uae", "united arab emirates", "qatar", "israel", "lebanon", "singapore",
    
    # US States
    "california", "texas", "florida", "new york state", "pennsylvania", "illinois",
    "ohio", "georgia", "north carolina", "michigan", "new jersey", "virginia",
    "washington", "arizona", "massachusetts", "tennessee", "indiana", "missouri",
    "maryland", "wisconsin", "colorado", "minnesota", "south carolina", "alabama",
    "louisiana", "kentucky", "oregon", "oklahoma", "connecticut", "utah", "iowa",
    "nevada", "arkansas", "mississippi", "kansas", "new mexico", "nebraska",
    "west virginia", "idaho", "hawaii", "new hampshire", "maine", "montana",
    "rhode island", "delaware", "south dakota", "north dakota", "alaska", "vermont",
    "wyoming"
}

# Common location words to ignore
IGNORE_LOCATIONS = {
    'twitter', 'internet', 'home', 'work', 'everywhere', 'nowhere', 'online',
    'inbox', 'cloud', 'worldwide', 'global', 'earth', 'planet', 'universe',
    'website', 'app', 'web', 'platform', 'social media', 'facebook', 'instagram',
    'snapchat', 'tiktok', 'linkedin', 'youtube', 'twitch', 'reality', 'cyberspace',
    'metaverse', 'matrix', 'zoom', 'microsoft', 'apple', 'google', 'amazon'
}

class LocationParser:
    """Enhanced class for extracting and geocoding location information from tweets."""
    
    def __init__(self, user_agent: str = "FanSense_App", cache_file: str = "location_cache.json"):
        """
        Initialize the location parser.
        
        Args:
            user_agent: User agent to use for geocoding services
            cache_file: File to store location cache
        """
        self.geolocator = Nominatim(user_agent=user_agent)
        self.cache_file = cache_file
        self.location_cache = self._load_cache()
        self.request_count = 0
        self.last_request_time = 0
        logger.info("Enhanced location parser initialized")
        
    def _load_cache(self) -> Dict[str, Any]:
        """Load location cache from file."""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logger.warning(f"Failed to load location cache: {str(e)}")
            return {}
    
    def _save_cache(self) -> None:
        """Save location cache to file."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.location_cache, f)
        except Exception as e:
            logger.warning(f"Failed to save location cache: {str(e)}")
    
    def _respect_rate_limit(self) -> None:
        """Respect geocoding service rate limits."""
        # Nominatim has a limit of 1 request per second
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
            
        self.last_request_time = time.time()
        self.request_count += 1
        
        # Save cache every 10 requests
        if self.request_count % 10 == 0:
            self._save_cache()

    def find_locations_in_text(self, text: str) -> List[str]:
        """
        Find all potential locations in text.
        
        Args:
            text: Text to search for locations
            
        Returns:
            List of potential location strings
        """
        if not text:
            return []
            
        locations = []
        
        # Try to match location patterns
        for pattern in LOCATION_PATTERNS:
            matches = re.findall(pattern, text)
            for match in matches:
                if match.lower() not in IGNORE_LOCATIONS:
                    locations.append(match)
        
        # Look for known locations in text
        words = re.findall(r'\b[A-Z][a-z]+\b', text)  # Find capitalized words
        for word in words:
            if word.lower() in MAJOR_LOCATIONS and word.lower() not in IGNORE_LOCATIONS:
                locations.append(word)
                
        # Check phrases against known locations
        text_lower = text.lower()
        for location in MAJOR_LOCATIONS:
            if location in text_lower and location not in IGNORE_LOCATIONS:
                # Find the actual capitalized version in the original text
                pattern = re.compile(re.escape(location), re.IGNORECASE)
                match = pattern.search(text)
                if match:
                    locations.append(match.group(0))
        
        return list(set(locations))  # Remove duplicates

    def extract_location_from_text(self, text: str) -> Optional[str]:
        """
        Extract the most likely location from text.
        
        Args:
            text: Tweet text
            
        Returns:
            Extracted location or None
        """
        if not text:
            return None
            
        locations = self.find_locations_in_text(text)
        
        if not locations:
            return None
            
        # Return the first location found
        # In a more advanced version, we could score locations by reliability
        return locations[0]

    def extract_location_from_user_profile(self, user: Dict[str, Any]) -> Optional[str]:
        """
        Extract location from user profile.
        
        Args:
            user: User dictionary
            
        Returns:
            Location string or None
        """
        if not user:
            return None
            
        # Check location field
        location = user.get('location')
        if location and isinstance(location, str) and location.strip().lower() not in IGNORE_LOCATIONS:
            return location.strip()
            
        # Check description field for locations
        description = user.get('description')
        if description and isinstance(description, str):
            locations = self.find_locations_in_text(description)
            if locations:
                return locations[0]
                
        return None

    def extract_location_from_tweet(self, tweet: Dict[str, Any]) -> Optional[str]:
        """
        Extract location from various fields in a tweet.
        
        Priority order:
        1. Tweet geo coordinates or place data
        2. User location field
        3. Location mentions in tweet text
        
        Args:
            tweet: Dictionary containing tweet data
            
        Returns:
            Most reliable location string found, or None
        """
        # Check if tweet has geo data
        if tweet.get('geo') and tweet['geo'].get('coordinates'):
            coords = tweet['geo']['coordinates']
            return f"{coords[0]},{coords[1]}"
            
        # Check if tweet has place data
        if tweet.get('place') and tweet['place'].get('full_name'):
            return tweet['place']['full_name']
            
        # Check if tweet has user data
        user = tweet.get('user', {})
        user_location = self.extract_location_from_user_profile(user)
        if user_location:
            return user_location
            
        # Check tweet text for location mentions
        return self.extract_location_from_text(tweet.get('text', ''))
    
    def geocode_location(self, location_str: str) -> Optional[Dict[str, Any]]:
        """
        Convert a location string to coordinates and structured location data.
        Uses caching to avoid repeated geocoding calls.
        
        Args:
            location_str: Location string to geocode
            
        Returns:
            Dictionary with location data or None if geocoding failed
        """
        # Return from cache if available
        if location_str in self.location_cache:
            return self.location_cache[location_str]
            
        if not location_str:
            return None
            
        try:
            # Respect rate limits
            self._respect_rate_limit()
            
            # Check if it's a known major location first
            if location_str.lower() in MAJOR_LOCATIONS:
                # Attempt geocoding with the known location name
                location = self.geolocator.geocode(location_str, exactly_one=True)
            else:
                # Try to find a known location in the string first
                found_location = None
                for loc in MAJOR_LOCATIONS:
                    if loc in location_str.lower():
                        found_location = loc
                        break
                
                # If a known location is found, use it; otherwise, use the original string
                search_term = found_location if found_location else location_str
                location = self.geolocator.geocode(search_term, exactly_one=True)
            
            if location:
                result = {
                    'input': location_str,
                    'address': location.address,
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'raw': location.raw
                }
                
                # Store in cache
                self.location_cache[location_str] = result
                return result
                
        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as e:
            logger.warning(f"Geocoding error for '{location_str}': {str(e)}")
        
        # If geocoding fails, try to extract a more focused location
        words = location_str.split()
        if len(words) > 1:
            for word in words:
                if word.lower() in MAJOR_LOCATIONS:
                    try:
                        # Respect rate limits
                        self._respect_rate_limit()
                        
                        location = self.geolocator.geocode(word, exactly_one=True)
                        if location:
                            result = {
                                'input': location_str,
                                'address': location.address,
                                'latitude': location.latitude,
                                'longitude': location.longitude,
                                'raw': location.raw
                            }
                            
                            # Store in cache
                            self.location_cache[location_str] = result
                            return result
                    except:
                        pass
        
        return None
    
    def parse_tweet_location(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract and geocode location data from a tweet.
        
        Args:
            tweet: Dictionary containing tweet data
            
        Returns:
            Tweet dictionary with added location data
        """
        location_str = self.extract_location_from_tweet(tweet)
        geocoded_data = None
        
        if location_str:
            geocoded_data = self.geocode_location(location_str)
        
        # Add location data to tweet
        tweet['location'] = {
            'raw_location': location_str,
            'geocoded': geocoded_data
        }
        
        return tweet
    
    def parse_tweets_location(self, tweets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse location for a list of tweets.
        
        Args:
            tweets: List of tweet dictionaries
            
        Returns:
            List of tweet dictionaries with added location data
        """
        logger.info(f"Parsing location for {len(tweets)} tweets")
        result = []
        
        for tweet in tweets:
            result.append(self.parse_tweet_location(tweet))
            
        # Save cache after processing all tweets
        self._save_cache()
        
        return result

    def extract_and_save_coordinates(self, location_str: str) -> Optional[Dict[str, Any]]:
        """
        Extract coordinates from a location string and save to the cache.
        Useful for manually adding known locations.
        
        Args:
            location_str: Location string
            
        Returns:
            Location data or None
        """
        result = self.geocode_location(location_str)
        if result:
            self._save_cache()
        return result
    
    def add_known_location(self, location_str: str, latitude: float, longitude: float, 
                           address: str = None, city: str = None, country: str = None) -> None:
        """
        Manually add a known location to the cache.
        
        Args:
            location_str: Location string
            latitude: Latitude
            longitude: Longitude
            address: Full address
            city: City name
            country: Country name
        """
        address = address or f"{city or ''}, {country or ''}"
        raw = {
            "place_id": f"manual_{location_str.replace(' ', '_')}",
            "display_name": address,
            "address": {
                "city": city,
                "country": country
            }
        }
        
        result = {
            'input': location_str,
            'address': address,
            'latitude': latitude,
            'longitude': longitude,
            'raw': raw
        }
        
        self.location_cache[location_str] = result
        self._save_cache()
        logger.info(f"Added known location: {location_str} ({latitude}, {longitude})")
        
        # Also add variations
        variations = [
            f"in {location_str}",
            f"from {location_str}",
            f"at {location_str}",
            f"near {location_str}",
            f"to {location_str}"
        ]
        
        for var in variations:
            self.location_cache[var] = result
            
        self._save_cache()


# Example usage
if __name__ == "__main__":
    # Example tweet
    example_tweet = {
        "id": "1234567890",
        "text": "Amazing match today in London! The team played brilliantly! #GoTeam",
        "created_at": "2025-05-01T12:34:56Z",
        "user": {
            "id": "9876543210",
            "screen_name": "fan123",
            "location": "Manchester, UK"
        }
    }
    
    # Initialize parser
    parser = LocationParser()
    
    # Add some known locations
    parser.add_known_location("London", 51.5074, -0.1278, city="London", country="United Kingdom")
    parser.add_known_location("Manchester", 53.4808, -2.2426, city="Manchester", country="United Kingdom")
    parser.add_known_location("New York", 40.7128, -74.0060, city="New York", country="United States")
    parser.add_known_location("Tokyo", 35.6762, 139.6503, city="Tokyo", country="Japan")
    parser.add_known_location("Sydney", -33.8688, 151.2093, city="Sydney", country="Australia")
    parser.add_known_location("Paris", 48.8566, 2.3522, city="Paris", country="France")
    
    # Parse tweet location
    result = parser.parse_tweet_location(example_tweet)
    
    # Print results
    print(f"Text: {result['text']}")
    print(f"Raw location: {result['location']['raw_location']}")
    if result['location']['geocoded']:
        print(f"Geocoded address: {result['location']['geocoded']['address']}")
        print(f"Coordinates: {result['location']['geocoded']['latitude']}, {result['location']['geocoded']['longitude']}")
