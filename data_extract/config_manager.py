import json

from logger import setup_logging

# Set up logging
logger = setup_logging()


class ConfigManager:

    def __init__(self, config_path = '../config.json') -> None:
        self.config = self.load_json(config_path)

        self.SPOTIFY_CLIENT_ID = self.config['SPOTIFY_CLIENT_ID']
        self.SPOTIFY_CLIENT_SECRET = self.config['SPOTIFY_CLIENT_SECRET']

        self.MUSICBRAINZ_CLIENT_ID = self.config['MUSICBRAINZ_CLIENT_ID']
        self.MUSICBRAINZ_CLIENT_SECRET = self.config['MUSICBRAINZ_CLIENT_SECRET']

        self.GENIUS_ACCESS_TOKEN = self.config['GENIUS_ACCESS_TOKEN']

        self.CHROME_DRIVER_PATH = self.config['WEB_SCRAPPER']['CHROME_DRIVER_PATH']

    def load_json(self, path: str) -> dict:
        try:
            with open(path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Error: The file {path} does not exist.")
            return {}
        except json.JSONDecodeError:
            logger.error(f"Error: The file {path} is not a valid JSON.")
            return {}