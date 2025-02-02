# 🎶 Portuguese Radio Analysis

Ever wondered what songs dominate Portuguese radio? This project scrapes and analyzes music data from 4 major Portuguese radio stations (CidadeFM, RFM, MegaHits and Rádio Comercial), providing a deep dive into what’s being played, from trending hits to long-standing classics. Using **web scraping, machine learning, and interactive visualizations**, this project helps uncover trends in music preferences across stations.

The project consists of two main parts:

1. **ETL Process**: Scrapes and processes data from radio websites and integrates additional information from APIs.
2. **Streamlit Dashboard**: Visualizes the analyzed data in an interactive and user-friendly web application.

---

## 🛠️ Features

### ETL Process

- **Data Scraping**: Scrapes real-time playlists from four Portuguese radio stations, capturing song titles, artists, and playtimes.
- **API Integration**: Enhances music metadata by integrating with:
  - 🎵 **Spotify**: Track popularity & metadata.
  - 📝 **Genius**: Lyrics for sentiment analysis.
  - 📖 **Wikipedia & MusicBrainz**: Artist biographies & song history.
- **Lyrics Sentiment Analysis**: Uses **Machine Learning (ML)** models to detect emotions like joy, sadness, and anger.
- **Data Transformation**: Standardizes and cleans data for seamless analysis and visualization.
- **Optimized Performance**:
  - 🚀 **Asyncio** for concurrent API requests, improving speed.
  - ⚡ **Polars** for fast and memory-efficient DataFrame operations.


### Streamlit Dashboard

- **Overview Comparison**: See how four major Portuguese radio stations stack up:
  - 🎵 **Top Artists & Songs**: Most played tracks and their total air time.
  - 🌍 **Language & Origin**: Breakdown of song languages and artist nationalities.
  - 📅 **Decades & Genres**: What eras dominate the airwaves?
  - 😊 **Lyrics & Emotions**: Sentiment analysis of the lyrics being played.
- **Deep Dive**: Focuses on one radio station and compares it to others.
- **Self-Service Exploration**: Allows users to explore and extract data for custom analysis.
- **Interactive Visualizations**: Provides charts and filters for dynamic insights.

The dashboard is structured as follows:
- `overview_comparison.py`: Broad comparison across multiple radios.
- `radio_deep_dive.py`: Detailed insights into a selected radio station.
- `self_service.py`: Free exploration and data export.

Each page uses helper modules from `dashboard/utils/` for data processing, filtering, and visualization. The main entry point (`app.py`) initializes the dashboard and integrates all pages seamlessly.


---

## 🗂️ Project Structure

The project is divided into two main parts:
- 📂 **Dashboard Files** (Streamlit-based visualization)
- 📂 **ETL Pipeline Files** (Data Scraping, Processing & API Enrichment)

```plaintext
portuguese-radio-analysis/

📂 Dashboard Files
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

📂 ETL Pipeline Files
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

📂 Documentation
├── documentation/                   # Additional documentation and metric explanations
│   └── spotify_metrics.md           # Documentation on Spotify metrics

📂 Data Folders
├── extract_folder/                  # Raw data extracted from scrapers and APIs
├── transform_folder/                # Transformed and cleaned data

📂 Configuration
├── .env.example                     # Example environment variables file
├── config.json                      # File paths and scraper configurations
├── schema.json                      # Data schema definitions

📂 Environment and Dependencies
├── venv/                            # Python virtual environment (local)
├── setup.py                         # Converts data_extract into a package for reuse
├── requirements.txt                 # Python dependencies

📂 Other
├── README.md                        # Project documentation (this file)
└── .gitignore                       # Git ignore rules
```

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed:

- **Python 3.8+**
- **Pip**
- **Google Chrome**: Required for Selenium-based web scraping.
- **ChromeDriver**: Must be installed to match your Chrome version. Download it from [ChromeDriver Downloads](https://sites.google.com/chromium.org/driver/). Place the executable in a folder and update the `CHROME_DRIVER_PATH` in `config.json`.

### Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/radio-song-analysis.git
   cd radio-song-analysis
   ```
2. **Create a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   venv\Scripts\activate     # On Windows
   ```
3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
4. **Set Up Environment Variables**:
   - Copy the `.env.example` file and rename it to `.env`.
   - Fill in the required API credentials and paths in the `.env` file.

---

## 📊 How to Run

### Running the ETL Process

The ETL process scrapes real-time radio playlists, enriches them with metadata from external APIs, performs sentiment analysis on lyrics, and stores the transformed data for visualization.
Run the ETL pipeline with:

```bash
python -m data_extract.radio_music_etl
```

Start the dashboard to visualize and explore the processed data:

```bash
streamlit run dashboard/app.py
```

Open `http://localhost:8501` in your browser to access the dashboard.

---

## 🙋 Frequently Asked Questions

### ❓ **1. Can I add more radio stations to scrape?**
> Yes! Extend the `radio_scraper.py` module by creating a new scraper class for the additional station.

### ❓ **2. How do I customize the dashboard?**
> Modify the code in `dashboard/pages` and `dashboard/utils` to add or change visualizations.

### ❓ **3. How do I update the dataset with fresh radio data?**
> Just re-run the ETL pipeline:
> ```bash
> python -m data_extract.radio_music_etl
> ```

### ❓ **4. Can I export data from the dashboard?**
> Yes! The **Self-Service Exploration** page allows data filtering and downloads.

### ❓ **5. What happens if a scraper encounters an error?**
> The ETL process includes logging to track and debug errors during scraping or API requests.


---

Enjoy exploring Portuguese radio trends with **Portuguese Radio Analysis**! 🎶