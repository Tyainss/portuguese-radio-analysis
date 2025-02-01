# 🎶 Portuguese Radio Analysis

Portuguese Radio Analysis is a comprehensive project designed to scrape and analyze data from 4 Portuguese radio stations, providing insights into the music being played. The project consists of two main parts:

1. **ETL Process**: Scrapes and processes data from radio websites and integrates additional information from APIs.
2. **Streamlit Dashboard**: Visualizes the analyzed data in an interactive and user-friendly web application.

---

## 🛠️ Features

### ETL Process

- **Data Scraping**: Extracts playlists and track metadata from multiple radio websites.
- **API Integration**: Enriches the scraped data with additional information from APIs such as:
  - Spotify
  - Genius Lyrics
  - Wikipedia
  - MusicBrainz
- **Sentiment Analysis**: Uses machine learning to analyze song lyrics for emotions and sentiment.
- **Data Transformation**: Cleans and standardizes data for seamless analysis and visualization.
- **Asyncio for Concurrency**: The use of Python's `asyncio` allows concurrent API requests, significantly improving performance.
- **Polars for Efficiency**: The project uses Polars for fast and memory-efficient DataFrame operations.

### Streamlit Dashboard

- **Overview Comparison**: Compare data from four radio stations side-by-side, including:
  - Total tracks and playtime.
  - Main track language and artists countries, and how Portuguese/Portugal place.
  - Decade comparison of tracks and artists
  - Popular genres, track durations, and sentiment analysis.
- **Deep Dive**: Focuses on one radio station and compares it to others grouped together.
- **Self-Service Exploration**: Allows users to explore and extract data for custom analysis.
- **Interactive Visualizations**: Provides charts and filters for dynamic insights.

The dashboard is structured as follows:
- `overview_comparison.py`: Provides a broad comparison across multiple radios.
- `radio_deep_dive.py`: Offers detailed insights into a selected radio and its comparison with others.
- `self_service.py`: Allows users to freely explore and download the dataset.

Each page uses helper modules from `dashboard/utils/` for data processing, filtering, and visualization. The main entry point (`app.py`) initializes the dashboard and integrates all pages seamlessly.


---

## 🗂️ Project Structure

```plaintext
portuguese-radio-analysis/
├── dashboard/                       # Streamlit dashboard files
│   ├── app.py                       # Main entry point for the Streamlit app
│   ├── pages/                       # Code for each dashboard page
│   ├── spec/                        # Self-Service page specification file
│   ├── logo/                        # Logos used in the dashboard
│   └── utils/                       # Helper modules for plotting and data handling
│       ├── calculations.py          # Contains calculation logic for the dashboard
│       ├── filters.py               # Filtering functionality for plots
│       ├── helper.py                # Helper functions
│       └── storage.py               # Handles data loading for the dashboard
├── data_extract/                    # ETL process files
│   ├── logs/                        # Logging for the ETL process
│   ├── asynchronous_spotify_api.py  # Spotify API integration with asyncio
│   ├── config_manager.py            # Configuration manager
│   ├── data_storage.py              # Handles data storage and loading
│   ├── genius.api.py                # Genius Lyrics API integration
│   ├── logger.py                    # Logging utilities
│   ├── lyrics.py                    # Lyrics sentiment analysis
│   ├── musicbrainz_api.py           # MusicBrainz API integration
│   ├── radio_music_etl.py           # Main ETL pipeline
│   ├── radio_scraper.py             # Web scraping logic
│   └── wikipedia_api.py             # Wikipedia API integration
├── documentation/                   # Additional documentation and metric explanations
├── extract_folder/                  # Raw data extracted from scrapers and APIs
├── transform_folder/                # Transformed and cleaned data
├── .env.example                     # Example environment variables file
├── config.json                      # File paths and scraper configurations
├── schema.json                      # Data schema definitions
├── setup.py                         # Converts data_extract into a package for reuse
├── requirements.txt                 # Python dependencies
└── README.md                        # Project documentation (this file)
```

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed:

- **Python 3.8+**
- **Pip**
- **Google Chrome**: Required for Selenium-based web scraping.
- **ChromeDriver**: Must be installed to match your Chrome version. Download it from [ChromeDriver Downloads](https://sites.google.com/chromium.org/driver/). Place the executable in a folder and update the `CHROME_DRIVER_PATH` in `.env` or `config.json`.

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/radio-song-analysis.git
   cd radio-song-analysis
   ```
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Set Up Environment Variables**:
   - Copy the `.env.example` file and rename it to `.env`.
   - Fill in the required API credentials and paths in the `.env` file.

---

## 📊 Running the Project

### Running the ETL Process

The ETL process scrapes radio station data, enriches it with API information, and transforms it for analysis. It works in the following steps:

1. **Scraping Radio Data**:

   - `radio_scraper.py` defines scrapers for each radio station (e.g., RFM, MegaHits, Cidade FM).
   - Uses Selenium to navigate radio websites and extract playlist data (e.g., track title, artist, playtime).
   - Stores scraped data as CSV files in the `extract_folder`.

2. **API Enrichment**:

   - Missing track or artist details are fetched from external APIs.
     - **Spotify API** for track popularity and metadata.
     - **Genius API** for lyrics.
     - **Wikipedia and MusicBrainz APIs** for artist and track details.
   - Asynchronous API calls optimize performance when processing large datasets.

3. **Lyrics Sentiment Analysis**:

   - `lyrics.py` analyzes the sentiment of lyrics using machine learning models (e.g., Twitter RoBERTa-based emotion classifier).
   - Identifies emotions such as joy, sadness, and anger, and counts occurrences of words like "love" in multiple languages.

4. **Data Transformation**:

   - Standardizes and cleans data (e.g., title-casing artist names, removing duplicates).
   - Joins enriched data (e.g., lyrics sentiments, Spotify details) with scraped radio data.
   - Saves the final transformed data into the `transform_folder` for use in the dashboard.

Run the ETL process with:

```bash
python -m data_extract.radio_music_etl
```

### Launching the Streamlit Dashboard

Start the dashboard to visualize and explore the processed data:

```bash
streamlit run dashboard/app.py
```

Open the provided local URL (e.g., `http://localhost:8501`) in your browser to access the dashboard.

---

## 🛠️ Configuration

### `.env` File
The `.env` file stores all sensitive credentials and API keys. Use the provided `.env.example` as a reference:

```plaintext
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
GENIUS_API_KEY=your_genius_api_key
CHROME_DRIVER_PATH=/path/to/chromedriver
```

### `config.json`
Defines paths for data storage and scraper configurations:

```json
{
    "RADIO_CSV_PATH": "./transform_folder/radio_data.csv",
    "TRACK_INFO_CSV_PATH": "./transform_folder/track_info.csv",
    "ARTIST_INFO_CSV_PATH": "./transform_folder/artist_info.csv",

    "SPOTIFY_INFO_CSV_PATH": "./extract_folder/spotify_info_extract.csv",
    "LYRICS_INFO_CSV_PATH": "./extract_folder/lyrics_info_extract.csv",
    "MUSICBRAINZ_INFO_CSV_PATH": "./extract_folder/musicbrainz_info_extract.csv",
    "WIKIPEDIA_INFO_CSV_PATH": "./extract_folder/wikipedia_info_extract.csv",

    "WEB_SCRAPPER": {
        "CHROME_DRIVER_PATH": "/path/to/chromedriver",
        "CSV_PATH": "./extract_folder/{radio}_track_data_extract.csv",
        "WAIT_DURATION": 15,
        "WEB_SITES": {
            "Comercial": {
                "url": "https://radiocomercial.pt/passou",
                "radio_name": "Rádio Comercial",
                "cookies_button_accept": "CONCORDO"
            },
            "CidadeFM": {
                "url": "https://cidade.fm/passou",
                "radio_name": "CIDADE",
                "cookies_button_accept": "CONCORDO"
            },
            "RFM": {
                "url": "https://rfm.sapo.pt/que-musica-era",
                "radio_name": "RFM",
                "cookies_button_accept": "ACEITAR"
            },
            "MegaHits": {
                "url": "https://megahits.sapo.pt/acabou-de-tocar",
                "radio_name": "MegaHits",
                "cookies_button_accept": "ACEITAR"
            }
        }
    }
}
```

---

## 🙋 Frequently Asked Questions

### 1. What happens if a scraper encounters an error?

The ETL process includes logging to track and debug errors during scraping or API requests.

### 2. Can I add more radio stations to scrape?

Yes! Extend the `radio_scraper.py` module by creating a new scraper class for the additional station.

### 3. How do I customize the dashboard?

Modify the code in the `dashboard/pages` and `dashboard/utils` directories to add or change visualizations.

---

Enjoy exploring and getting insights into the main Portuguese Radios with Portuguese Radio Analysis! 🎶