import json
import polars as pl
from typing import Dict

from logger import setup_logging

# Set up logging
logger = setup_logging()


class ConfigManager:
    CONFIG_PATH_DEFAULT = '../config.json'
    SCHEMA_PATH_DEFAULT = '../schema.json'
    json_to_polars_types = {
        "date": pl.Date,
        "time": pl.Time,
        "str": pl.Utf8
    }
    def __init__(self, config_path = CONFIG_PATH_DEFAULT, schema_path = SCHEMA_PATH_DEFAULT) -> None:
        self.config = self.load_json(config_path)
        self.schema = self.load_polars_schema(schema_path)

        self.SPOTIFY_CLIENT_ID = self.config['SPOTIFY_CLIENT_ID']
        self.SPOTIFY_CLIENT_SECRET = self.config['SPOTIFY_CLIENT_SECRET']

        self.MUSICBRAINZ_CLIENT_ID = self.config['MUSICBRAINZ_CLIENT_ID']
        self.MUSICBRAINZ_CLIENT_SECRET = self.config['MUSICBRAINZ_CLIENT_SECRET']

        self.GENIUS_ACCESS_TOKEN = self.config['GENIUS_ACCESS_TOKEN']

        self.CHROME_DRIVER_PATH = self.config['WEB_SCRAPPER']['CHROME_DRIVER_PATH']
        
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
        
    def load_polars_schema(self, path: str, encoding: str = 'utf-8') -> Dict[str, pl.DataType]:
        try:
            with open(path, 'r', encoding=encoding) as f:
                schema_json = json.load(f)
            
            # Map JSON schema type to Polars types
            schema_polars = {col: self.json_to_polars_types[dtype] for col, dtype in schema_json.items()}
            return schema_polars
        
        except FileNotFoundError:
            logger.error(f"Error: The file {path} does not exist.")
            return {}
        except json.JSONDecodeError:
            logger.error(f"Error: The file {path} is not a valid JSON.")
            return {}