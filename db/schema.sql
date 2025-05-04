-- FanSense Database Schema
-- PostgreSQL database schema for storing tweets with sentiment and location

CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    author_id TEXT,
    username TEXT,
    text TEXT,
    created_at TIMESTAMP,
    lang TEXT,
    geo_place_id TEXT,
    raw_data JSONB
);
