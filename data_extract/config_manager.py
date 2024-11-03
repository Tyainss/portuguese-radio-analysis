import json
import polars as pl
from typing import Dict

from logger import setup_logging

# Set up logging
logger = setup_logging()


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

        self._initialize_schema_config(schema_path)        
        self._initialize_secrets()
        self._initialize_web_scrapper_config()
        self._initialize_other_config()
    
    def _initialize_schema_config(self, schema_path) -> None:
        self.schema = self.load_polars_schema(schema_path)
        self.RADIO_SCRAPPER_SCHEMA = self.schema['RADIO_SCRAPPER']
        self.TRACK_INFO_SCHEMA = self.schema['TRACK_INFO']
        self.ARTIST_INFO_SCHEMA = self.schema['ARTIST_INFO']

    def _initialize_secrets(self) -> None:
        self.SPOTIFY_CLIENT_ID = self.config['SPOTIFY_CLIENT_ID']
        self.SPOTIFY_CLIENT_SECRET = self.config['SPOTIFY_CLIENT_SECRET']

        self.MUSICBRAINZ_CLIENT_ID = self.config['MUSICBRAINZ_CLIENT_ID']
        self.MUSICBRAINZ_CLIENT_SECRET = self.config['MUSICBRAINZ_CLIENT_SECRET']

        self.GENIUS_ACCESS_TOKEN = self.config['GENIUS_ACCESS_TOKEN']       

        self.WIKI_ACCESS_TOKEN = self.config.get('WIKIPEDIA_ACCESS_TOKEN', None)
        self.WIKI_CLIENT_SECRET = self.config.get('WIKIPEDIA_CLIENT_SECRET', None)

    def _initialize_web_scrapper_config(self) -> None:
        self.RADIO_COLUMN = "radio"
        self.DAY_COLUMN = "day"
        self.TIME_PLAYED_COLUMN = "time_played"
        self.TRACK_TITLE_COLUMN = "track_title"
        self.ARTIST_NAME_COLUMN = "artist_name"

        self.WEB_SCRAPPER = self.config['WEB_SCRAPPER']
        self.CHROME_DRIVER_PATH = self.WEB_SCRAPPER['CHROME_DRIVER_PATH']
        self.CSV_PATH_FORMAT = self.WEB_SCRAPPER['CSV_PATH']
        self.WAIT_DURATION = self.WEB_SCRAPPER['WAIT_DURATION']
        self.WEB_SITES = self.WEB_SCRAPPER['WEB_SITES']

    def _initialize_other_config(self) -> None:
        self.TRACK_INFO_CSV_PATH = self.config['TRACK_INFO_CSV_PATH']
        self.ARTIST_INFO_CSV_PATH = self.config['ARTIST_INFO_CSV_PATH']

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