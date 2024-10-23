import polars as pl
import asyncio

from config_manager import ConfigManager
from data_storage import DataStorage
from radio_scraper import  PassouTypeRadioScraper, RFMRadioScraper, MegaHitsRadioScraper
from genius_api import GeniusAPI
from musicbrainz_api import MusicBrainzAPI
from spotify_api import SpotifyAPI
from asynchronous_spotify_api import AsyncSpotifyAPI
from wikipedia_api import WikipediaAPI
from asynchronous_wikipedia_api import AsyncWikipediaAPI

from logger import setup_logging

# Set up logging
logger = setup_logging()


class RadioMusicETL:
    CONFIG_PATH_DEFAULT = 'config.json'
    SCHEMA_PATH_DEFAULT = 'schema.json'

    def __init__(self, config_path: str = CONFIG_PATH_DEFAULT, schema_path: str = SCHEMA_PATH_DEFAULT) -> None:
        self.config_manager = ConfigManager(config_path, schema_path)
        self.data_storage = DataStorage()
        self.genius_api = GeniusAPI()
        self.mb_api = MusicBrainzAPI()
        self.spotify_api = SpotifyAPI()
        self.async_spotify_api = AsyncSpotifyAPI()
        self.wikipedia_api = WikipediaAPI()
        self.async_wikipedia_api = AsyncWikipediaAPI()
        self._initialize_column_names()

    def _initialize_column_names(self):
        self.RADIO_COLUMN = self.config_manager.RADIO_COLUMN
        self.DAY_COLUMN = self.config_manager.DAY_COLUMN
        self.TIME_PLAYED_COLUMN = self.config_manager.TIME_PLAYED_COLUMN
        self.TRACK_TITLE_COLUMN = self.config_manager.TRACK_TITLE_COLUMN
        self.ARTIST_NAME_COLUMN = self.config_manager.ARTIST_NAME_COLUMN

    def _identify_unregistered_tracks(self, new_track_df, registered_tracks):
        # Combine TRACK_TITLE and ARTIST_NAME to create a unique identifier
        new_track_df = new_track_df.with_columns(
            pl.concat_str([self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN], separator=" - ").alias("track_id")
        )
        if not registered_tracks.is_empty():
            registered_tracks = registered_tracks.with_columns(
                pl.concat_str([self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN], separator=" - ").alias("track_id")
            )

            # Find tracks in new_track_df that are not in registered_tracks
            unregistered_tracks = new_track_df.filter(
                ~pl.col("track_id").is_in(registered_tracks["track_id"])
            )
        else:
            unregistered_tracks = new_track_df

        # Select only TRACK_TITLE and ARTIST_NAME columns and remove duplicates
        result = unregistered_tracks.select([self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN]).unique()

        return result
    
    def _identify_unregistered_artists(self, new_artist_df, registered_artists):
        if not registered_artists.is_empty():
            unregistered_artists = new_artist_df.filter(
                ~pl.col(self.ARTIST_NAME_COLUMN).is_in(registered_artists[self.ARTIST_NAME_COLUMN])
            )
        else:
            unregistered_artists = new_artist_df

        result = unregistered_artists.select([self.ARTIST_NAME_COLUMN]).unique()
        
        return result

    # async def fetch_all


    async def run(self):
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
            all_scrape_dfs.append(tracks_df)

        combined_df = pl.concat(all_scrape_dfs)

        already_extracted_data = self.data_storage.read_csv(
            path=self.config_manager.TRACK_INFO_CSV_PATH,
            schema=self.config_manager.TRACK_INFO_SCHEMA
        )
        if not already_extracted_data.is_empty():
            already_extracted_artists = already_extracted_data.select([self.ARTIST_NAME_COLUMN])
        else:
            already_extracted_artists = pl.DataFrame()

        new_tracks_df = self._identify_unregistered_tracks(
            new_track_df=combined_df,
            registered_tracks=already_extracted_data
        )

        new_artists_df = self._identify_unregistered_artists(
            new_artist_df=combined_df,
            registered_artists=already_extracted_artists
        )

        spotify_track_df = await self.async_spotify_api.process_data(new_tracks_df)
        print('Spotify \n', spotify_track_df)

        # Save track info df
        track_info_df = spotify_track_df
        self.data_storage.output_csv(
            df=track_info_df,
            path=self.config_manager.TRACK_INFO_CSV_PATH,
            schema=self.config_manager.TRACK_INFO_SCHEMA
        )        

        wikipedia_artist_df = await self.async_wikipedia_api.process_data(new_artists_df)
        print('Wikipedia \n', wikipedia_artist_df)

        mb_artist_df = self.mb_api.process_data(new_artists_df)
        print('MusicBrainz \n', mb_artist_df)

        # Save artist info df
        artist_info_df = mb_artist_df.join(wikipedia_artist_df, on=[self.config_manager.ARTIST_NAME_COLUMN], how='left')
        self.data_storage.output_csv(
            df=artist_info_df,
            path=self.config_manager.ARTIST_INFO_CSV_PATH,
            schema=self.config_manager.ARTIST_INFO_SCHEMA
        )   

        # track_combined_df = new_tracks_df.join(spotify_track_df, on=[self.config_manager.TRACK_TITLE_COLUMN, self.config_manager.ARTIST_NAME_COLUMN], how="left")

        

        return track_info_df, wikipedia_artist_df

        # Continue testing codes and see if there's still errors with wikipedia api
        # Does it make sense to do asynchronous with MusicBrainz?
            # Fix the class to NOT use mbid and search with Artist Name instead

        # (Don't save genius lyrics since it will make the files too big)
        # Save the new track info into a csv file

if __name__ == "__main__":
    etl = RadioMusicETL()
    
    async def run_test():
        result = await etl.run()
        print(result)

    asyncio.run(run_test())