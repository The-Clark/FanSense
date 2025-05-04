"""
FanSense - Simple Streamlit Visualization App
--------------------------------------------
A simplified version of the visualization app to avoid compatibility issues.
"""

import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import json

# Database configuration
DB_CONFIG = {
    "dbname": "fansense",
    "user": "dee",
    "password": "",
    "host": "localhost",
    "port": 5432,
}

# Basic page setup
st.title("FanSense: Fan Engagement Analysis")
st.write("Visualize fan engagement and sentiment across geography and time.")

# Helper function for database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# Helper function for running queries
def run_query(query, params=None):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# Date filter (simple version)
st.sidebar.header("Filters")
days_back = st.sidebar.slider("Days to look back", 1, 30, 7)
end_date = datetime.now()
start_date = end_date - timedelta(days=days_back)

st.sidebar.write(f"Analyzing data from: {start_date.date()} to {end_date.date()}")

# Basic sentiment count
st.header("Sentiment Overview")

sentiment_query = """
SELECT 
    emotion, 
    COUNT(*) as count
FROM 
    tweets
WHERE 
    created_at BETWEEN %s AND %s
    AND emotion IS NOT NULL
GROUP BY 
    emotion
ORDER BY 
    count DESC
"""

sentiment_df = run_query(sentiment_query, (start_date, end_date))

if not sentiment_df.empty:
    # Simple bar chart
    st.write("Distribution of tweet sentiments:")
    fig = px.bar(sentiment_df, x='emotion', y='count', 
                 labels={'emotion': 'Sentiment', 'count': 'Number of Tweets'},
                 color='emotion')
    st.plotly_chart(fig)
else:
    st.info("No sentiment data available. Make sure tweets have been processed with sentiment analysis.")

# Top locations (simple version)
st.header("Top Locations")

location_query = """
SELECT 
    country, 
    COUNT(*) as count
FROM 
    tweets
WHERE 
    created_at BETWEEN %s AND %s
    AND country IS NOT NULL
GROUP BY 
    country
ORDER BY 
    count DESC
LIMIT 10
"""

location_df = run_query(location_query, (start_date, end_date))

if not location_df.empty:
    st.write("Top countries by tweet volume:")
    fig = px.pie(location_df, values='count', names='country')
    st.plotly_chart(fig)
else:
    st.info("No location data available. Make sure tweets have been processed with geolocation.")

# Sample tweets
st.header("Sample Tweets")

tweets_query = """
SELECT 
    text, 
    username, 
    created_at,
    emotion,
    country
FROM 
    tweets
WHERE 
    created_at BETWEEN %s AND %s
ORDER BY 
    created_at DESC
LIMIT 5
"""

tweets_df = run_query(tweets_query, (start_date, end_date))

if not tweets_df.empty:
    for _, row in tweets_df.iterrows():
        st.markdown(f"**@{row['username']}**: {row['text']}")
        st.caption(f"{row['created_at']} | Sentiment: {row['emotion'] or 'Unknown'} | Location: {row['country'] or 'Unknown'}")
        st.markdown("---")
else:
    st.info("No tweets found for the selected time period.")

# Debugging information
st.sidebar.header("Debug Info")
if st.sidebar.checkbox("Show database status"):
    conn = get_db_connection()
    if conn:
        st.sidebar.success("Database connection successful")
        
        # Count total tweets
        count_query = "SELECT COUNT(*) FROM tweets"
        count_df = pd.read_sql_query(count_query, conn)
        st.sidebar.write(f"Total tweets in database: {count_df.iloc[0, 0]}")
        
        # Count processed tweets
        processed_query = "SELECT COUNT(*) FROM tweets WHERE emotion IS NOT NULL"
        processed_df = pd.read_sql_query(processed_query, conn)
        st.sidebar.write(f"Tweets with sentiment analysis: {processed_df.iloc[0, 0]}")
        
        conn.close()
    else:
        st.sidebar.error("Database connection failed")

st.sidebar.caption("FanSense MVP v0.1")
