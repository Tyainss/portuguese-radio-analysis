import os
import json
import polars as pl
from typing import Dict
from dotenv import load_dotenv

from data_extract import logger

class ConfigManager:
    CONFIG_PATH_DEFAULT = 'config.json'
    SCHEMA_PATH_DEFAULT = 'schema.json'
    json_to_polars_types = {
        "date": pl.Date,
        "time": pl.Time,
        "str": pl.Utf8,
        "int64": pl.Int64,
        "bool": pl.Boolean,
        "float64": pl.Float64,
    }
    def __init__(self, config_path = CONFIG_PATH_DEFAULT, schema_path = SCHEMA_PATH_DEFAULT) -> None:
        self.config = self.load_json(config_path)
        load_dotenv()

        self._initialize_schema_config(schema_path)        
        self._initialize_secrets()
        self._initialize_web_scrapper_config()
        self._initialize_other_config()
        self._initialize_columns_config()
    
    def _initialize_schema_config(self, schema_path) -> None:
        self.schema = self.load_polars_schema(schema_path)
        self.RADIO_SCRAPPER_SCHEMA = self.schema['RADIO_SCRAPPER']
        self.TRACK_INFO_SCHEMA = self.schema['TRACK_INFO']
        self.ARTIST_INFO_SCHEMA = self.schema['ARTIST_INFO']
        self.SPOTIFY_INFO_SCHEMA = self.schema['SPOTIFY_INFO']
        self.LYRICS_INFO_SCHEMA = self.schema['LYRICS_INFO']
        self.MUSICBRAINZ_INFO_SCHEMA = self.schema['MUSICBRAINZ_INFO']
        self.WIKIPEDIA_INFO_SCHEMA = self.schema['WIKIPEDIA_INFO']

    def _initialize_columns_config(self) -> None:
        self.SPOTIFY_GENRE_COLUMN = 'spotify_genres'
        self.SPOTIFY_POPULARITY_COLUMN = 'spotify_popularity'
        self.SPOTIFY_RELEASE_DATE_COLUMN = 'spotify_release_date'

    def _initialize_secrets(self) -> None:
        self.SPOTIFY_CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
        self.SPOTIFY_CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
        self.MUSICBRAINZ_CLIENT_ID = os.environ.get('MUSICBRAINZ_CLIENT_ID')
        self.MUSICBRAINZ_CLIENT_SECRET = os.environ.get('MUSICBRAINZ_CLIENT_SECRET')
        self.GENIUS_ACCESS_TOKEN = os.environ.get('GENIUS_ACCESS_TOKEN')
        self.WIKI_ACCESS_TOKEN = os.environ.get('WIKIPEDIA_ACCESS_TOKEN')
        self.WIKI_CLIENT_SECRET = os.environ.get('WIKIPEDIA_CLIENT_SECRET')

    def _initialize_web_scrapper_config(self) -> None:
        self.RADIO_COLUMN = "radio"
        self.DAY_COLUMN = "day"
        self.TIME_PLAYED_COLUMN = "time_played"
        self.TRACK_TITLE_COLUMN = "track_title"
        self.ARTIST_NAME_COLUMN = "artist_name"

        self.WEB_SCRAPPER = self.config.get('WEB_SCRAPPER', {})
        self.CHROME_DRIVER_PATH = self.WEB_SCRAPPER.get('CHROME_DRIVER_PATH')
        self.CSV_PATH_FORMAT = self.WEB_SCRAPPER.get('CSV_PATH')
        self.WAIT_DURATION = self.WEB_SCRAPPER.get('WAIT_DURATION')
        self.WEB_SITES = self.WEB_SCRAPPER.get('WEB_SITES')

    def _initialize_other_config(self) -> None:
        self.RADIO_CSV_PATH = self.config.get('RADIO_CSV_PATH')
        self.TRACK_INFO_CSV_PATH = self.config.get('TRACK_INFO_CSV_PATH')
        self.ARTIST_INFO_CSV_PATH = self.config.get('ARTIST_INFO_CSV_PATH')
        self.SPOTIFY_INFO_CSV_PATH = self.config.get('SPOTIFY_INFO_CSV_PATH')
        self.LYRICS_INFO_CSV_PATH = self.config.get('LYRICS_INFO_CSV_PATH')
        self.MUSICBRAINZ_INFO_CSV_PATH = self.config.get('MUSICBRAINZ_INFO_CSV_PATH')
        self.WIKIPEDIA_INFO_CSV_PATH = self.config.get('WIKIPEDIA_INFO_CSV_PATH')

    def get_scraper_csv_path(self, radio, path_format):
        csv_path = path_format.format(radio=radio)
        return csv_path

    def load_json(self, path: str, encoding: str = 'utf-8') -> dict:
        try:
            with open(path, 'r', encoding=encoding) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Error: The file {path} does not exist.")
            return {}
        except json.JSONDecodeError:
            logger.error(f"Error: The file {path} is not a valid JSON.")
            return {}
        
    def load_polars_schema(self, path: str, encoding: str = 'utf-8') -> Dict[str, Dict[str, pl.DataType]]:
        # Use the load_json method to read the JSON file
        schemas_json = self.load_json(path, encoding)
        
        # Initialize a dictionary to hold all schemas
        schemas_polars = {}
        
        # Iterate over each schema in the JSON
        for schema_name, schema in schemas_json.items():
            # Map JSON schema type to Polars types for each schema
            schemas_polars[schema_name] = {col: self.json_to_polars_types[dtype] for col, dtype in schema.items()}
        
        return schemas_polars