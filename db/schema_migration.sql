-- Migration script to update the existing tweets table and add new tables

-- First, create the emotion type enum if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'emotion_type') THEN
        CREATE TYPE emotion_type AS ENUM (
            'very_negative',
            'negative',
            'neutral',
            'positive',
            'very_positive'
        );
    END IF;
END$$;

-- Alter the existing tweets table to add new columns
ALTER TABLE tweets 
  -- Only add columns if they don't exist
  ADD COLUMN IF NOT EXISTS sentiment_negative FLOAT,
  ADD COLUMN IF NOT EXISTS sentiment_neutral FLOAT,
  ADD COLUMN IF NOT EXISTS sentiment_positive FLOAT,
  ADD COLUMN IF NOT EXISTS sentiment_compound FLOAT,
  ADD COLUMN IF NOT EXISTS emotion emotion_type,
  ADD COLUMN IF NOT EXISTS location_raw TEXT,
  ADD COLUMN IF NOT EXISTS location_address TEXT,
  ADD COLUMN IF NOT EXISTS latitude FLOAT,
  ADD COLUMN IF NOT EXISTS longitude FLOAT,
  ADD COLUMN IF NOT EXISTS country VARCHAR(255),
  ADD COLUMN IF NOT EXISTS state_province VARCHAR(255),
  ADD COLUMN IF NOT EXISTS city VARCHAR(255),
  ADD COLUMN IF NOT EXISTS retweet_count INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS favorite_count INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS hashtags TEXT[],
  ADD COLUMN IF NOT EXISTS mentions TEXT[],
  ADD COLUMN IF NOT EXISTS search_query VARCHAR(255),
  ADD COLUMN IF NOT EXISTS search_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  -- Only add collected_at if it doesn't exist (to record when we collected the tweet)
  ADD COLUMN IF NOT EXISTS collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets(created_at);
CREATE INDEX IF NOT EXISTS idx_tweets_emotion ON tweets(emotion) WHERE emotion IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tweets_location ON tweets(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tweets_author_id ON tweets(author_id);
CREATE INDEX IF NOT EXISTS idx_tweets_hashtags ON tweets USING GIN(hashtags) WHERE hashtags IS NOT NULL;

-- Create the tracked hashtags table (for hashtags we're actively monitoring)
CREATE TABLE IF NOT EXISTS tracked_hashtags (
    id SERIAL PRIMARY KEY,
    hashtag VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create the teams table (for sports teams or other entities we're tracking)
CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    hashtags TEXT[],
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create the events table (for specific events we're analyzing)
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    team_id INT REFERENCES teams(id),
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    description TEXT,
    hashtags TEXT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create view for aggregated sentiment data by hour
CREATE OR REPLACE VIEW sentiment_by_hour AS
SELECT 
    date_trunc('hour', created_at) AS hour,
    emotion,
    COUNT(*) AS tweet_count
FROM tweets
WHERE emotion IS NOT NULL
GROUP BY hour, emotion
ORDER BY hour, emotion;

-- Create view for location-based sentiment
CREATE OR REPLACE VIEW geo_sentiment AS
SELECT 
    latitude,
    longitude,
    city,
    state_province,
    country,
    emotion,
    COUNT(*) AS tweet_count
FROM tweets
WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND emotion IS NOT NULL
GROUP BY latitude, longitude, city, state_province, country, emotion;