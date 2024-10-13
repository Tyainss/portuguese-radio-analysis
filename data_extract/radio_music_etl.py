import polars as pl

from config_manager import ConfigManager
from data_storage import DataStorage
from radio_scraper import RFMRadioScraper, MegaHitsRadioScraper, PassouTypeRadioScraper
from genius_api import GeniusAPI
from musicbrainz_api import MusicBrainzAPI
from spotify_api import SpotifyAPI
from wikipedia_api import WikipediaAPI

from logger import setup_logging

# Set up logging
logger = setup_logging()


class RadioMusicETL:

    def __init__(self, config_path: str, schema_path: str) -> None:
        self.config_manager = ConfigManager(config_path, schema_path)
        self.data_storage = DataStorage()
        self.genius_api = GeniusAPI()
        self.mb_api = MusicBrainzAPI()
        self.spotify_api = SpotifyAPI()
        self.wikipedia_api = WikipediaAPI()

    def _identify_unregistered_tracks(self, new_track_df, registered_tracks):
        # Combine TRACK_TITLE and TRACK_ARTIST to create a unique identifier
        new_track_df = new_track_df.with_columns(
            pl.concat_str(["TRACK_TITLE", "TRACK_ARTIST"], separator=" - ").alias("track_id")
        )
        registered_tracks = registered_tracks.with_columns(
            pl.concat_str(["TRACK_TITLE", "TRACK_ARTIST"], separator=" - ").alias("track_id")
        )

        # Find tracks in new_track_df that are not in registered_tracks
        unregistered_tracks = new_track_df.filter(
            ~pl.col("track_id").is_in(registered_tracks["track_id"])
        )

        # Select only TRACK_TITLE and TRACK_ARTIST columns and remove duplicates
        result = unregistered_tracks.select(["TRACK_TITLE", "TRACK_ARTIST"]).unique()

        return result


    def run(self):
        # Scrape data from radios
        scrapers = [
            PassouTypeRadioScraper(self.config_manager.WEB_SITES['Comercial'])
            # , PassouTypeRadioScraper(self.config_manager.WEB_SITES['Cidade'])
            # , RFMRadioScraper(self.config_manager.WEB_SITES['RFM'])
            # , MegaHitsRadioScraper(self.config_manager.WEB_SITES['MegaHits'])
        ]
        
        all_scrape_dfs = []

        for scraper in scrapers:
            tracks_df = scraper.scrape(save_csv=True)
            all_scrape_dfs.extend(tracks_df)

        combined_df = pl.concat(all_scrape_dfs)

        already_extracted_data = self.data_storage.read_csv(
            path=self.config_manager.TRACK_INFO_CSV_PATH,
            schema=self.config_manager.TRACK_INFO_SCHEMA
        )

        new_tracks = self._identify_unregistered_tracks(
            new_track_df=combined_df,
            registered_tracks=already_extracted_data
        )

        # Check Track Info data extracted
        # For every song never extracted, get data from spotify, wikipedia, musicbrainz
        # (Don't save genius lyrics since it will make the files too big)
        # Save the new track info into a csv file


