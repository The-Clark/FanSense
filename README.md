# FanSense: Geo & Emotion-Based Fan Heatmaps

**FanSense** visualizes fan engagement intensity and sentiment across geography and time. This tool helps teams, brands, and event organizers understand how fans around the world react to events, campaigns, and announcements.

## Core Value Proposition
"Visualize fan engagement intensity and sentiment across geography and time."

## Overview

FanSense combines social media data (currently Twitter/X), sentiment analysis, and geolocation information to generate real-time heatmaps showing how fans are responding to events. It helps answer questions like:

- Which regions have the most positive fan reactions to a new announcement?
- How does fan sentiment change before, during, and after an event?
- Where are the engagement hotspots during key moments?
- How do rival fanbases compare in terms of engagement and sentiment?

## Features (MVP v0.1)

- **Hashtag/Handle Ingestion**: Collect tweets by keyword, hashtag, or user handle
- **Geolocation Parsing**: Estimate tweet origin using geo-tags or user bios
- **Sentiment Analysis**: Assign sentiment/emotion score to tweets
- **Heatmap Visualization**: Interactive map showing fan emotion and volume over time
- **Time-Series View**: View sentiment trend lines before, during, and after events

## Tech Stack

| Layer | Tools |
|-------|-------|
| Ingestion | Twitter API (v2 Developer App) |
| ETL + Processing | Python, NLTK/VADER |
| Storage | PostgreSQL |
| NLP | VADER, RoBERTa, or BERTweet |
| Geolocation | Geopy, Carmen |
| Visualization | Streamlit, Plotly |

## Project Structure

```
fansense/
├── data_ingestion/
│   ├── twitter_stream.py  # Pull tweets via filtered stream
│   └── utils.py           # Handle rate limits, retries
├── data_processing/
│   ├── sentiment_model.py # Sentiment scoring
│   ├── location_parser.py # Bio parsing for location
│   └── clean_text.py      # Preprocessing utils
├── db/
│   ├── schema.sql         # PostgreSQL table creation
│   └── db_writer.py       # Insert processed tweets
├── visualisation/
│   ├── app.py             # Streamlit or Dash app
│   └── map_utils.py       # Kepler/Mapbox integration
├── notebooks/
│   └── EDA.ipynb          # Exploration and testing
├── tests/
│   └── test_sentiment.py
├── requirements.txt
└── README.md
```

## Setup Instructions

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Twitter Developer Account (with API key)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/fansense.git
   cd fansense
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   # Create a .env file with the following content
   TWITTER_BEARER_TOKEN=your_twitter_bearer_token
   DB_NAME=fansense
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_PORT=5432
   ```

4. Create the database schema:
   ```bash
   psql -U your_db_user -d fansense -f db/schema.sql
   ```

5. Initialize required NLTK data:
   ```bash
   python -c "import nltk; nltk.download('vader_lexicon')"
   ```

### Running the Application

1. Start the tweet collector:
   ```bash
   python data_processing/process_tweets.py --batch-size 100
   ```

2. Launch the Streamlit visualization app:
   ```bash
   streamlit run visualisation/app.py
   ```

3. Open your browser and navigate to `http://localhost:8501`

## Usage Guide

1. **Configure Data Collection**:
   - Set up the hashtags or keywords you want to track
   - Configure the Twitter API to collect relevant tweets

2. **Process Data**:
   - Run the sentiment analysis and geolocation processing
   - Store results in the PostgreSQL database

3. **Explore Visualizations**:
   - Use the time slider to see how sentiment changes over time
   - Zoom in/out on the map to explore regional differences
   - Filter by specific emotions or event timeframes
   - Export insights as reports or screenshots

## Future Development (Post-MVP)

- Cluster fans by sentiment trajectory (e.g., consistently negative regions)
- Compare team A vs team B fanbase after derby games
- Add topic detection per region ("ref decisions", "player criticism")
- Notify brand teams when sentiment dips in key regions
- Expand to other social media platforms beyond Twitter/X
- Machine learning to predict sentiment trends based on historical data

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.