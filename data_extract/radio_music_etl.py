import polars as pl

from config_manager import ConfigManager
from data_storage import DataStorage
from radio_scraper import  PassouTypeRadioScraper, RFMRadioScraper, MegaHitsRadioScraper
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
        self._initialize_column_names()

    def _initialize_column_names(self):
        self.RADIO_COLUMN = self.config_manager.RADIO_COLUMN
        self.DAY_COLUMN = self.config_manager.DAY_COLUMN
        self.TIME_PLAYED_COLUMN = self.config_manager.TIME_PLAYED_COLUMN
        self.TRACK_TITLE_COLUMN = self.config_manager.TRACK_TITLE_COLUMN
        self.TRACK_ARTIST_COLUMN = self.config_manager.TRACK_ARTIST_COLUMN

    def _identify_unregistered_tracks(self, new_track_df, registered_tracks):
        # Combine TRACK_TITLE and TRACK_ARTIST to create a unique identifier
        new_track_df = new_track_df.with_columns(
            pl.concat_str([self.TRACK_TITLE_COLUMN, self.TRACK_ARTIST_COLUMN], separator=" - ").alias("track_id")
        )
        registered_tracks = registered_tracks.with_columns(
            pl.concat_str([self.TRACK_TITLE_COLUMN, self.TRACK_ARTIST_COLUMN], separator=" - ").alias("track_id")
        )

        # Find tracks in new_track_df that are not in registered_tracks
        unregistered_tracks = new_track_df.filter(
            ~pl.col("track_id").is_in(registered_tracks["track_id"])
        )

        # Select only TRACK_TITLE and TRACK_ARTIST columns and remove duplicates
        result = unregistered_tracks.select([self.TRACK_TITLE_COLUMN, self.TRACK_ARTIST_COLUMN]).unique()

        return result
    
    def _identify_unregistered_artists(self, new_artist_df, registered_artists):

        unregistered_artists = new_artist_df.filter(
            ~pl.col(self.TRACK_ARTIST_COLUMN).is_in(registered_artists[self.TRACK_ARTIST_COLUMN])
        )
        result = unregistered_artists.select([self.TRACK_ARTIST_COLUMN]).unique()
        
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
        already_extracted_artists = already_extracted_data.select([self.TRACK_ARTIST_COLUMN])

        new_tracks = self._identify_unregistered_tracks(
            new_track_df=combined_df,
            registered_tracks=already_extracted_data
        )

        new_artists = self._identify_unregistered_artists(
            new_artist_df=combined_df,
            registered_artists=already_extracted_artists
        )

        # Check Track Info data extracted
        # For every song never extracted, get data from spotify, wikipedia, musicbrainz
        # (Don't save genius lyrics since it will make the files too big)
        # Save the new track info into a csv file


