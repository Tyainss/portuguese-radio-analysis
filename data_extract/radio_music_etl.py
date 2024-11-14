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
from lyrics import LyricsAnalyzer

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
        self.lyrics_analyzer = LyricsAnalyzer()
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


    async def run(self, scrape_radios=True, fetch_info=True):
        # Scrape data from radios
        scrapers = [
            PassouTypeRadioScraper(self.config_manager.WEB_SITES['Comercial'])
            , PassouTypeRadioScraper(self.config_manager.WEB_SITES['CidadeFM'])
            , RFMRadioScraper(self.config_manager.WEB_SITES['RFM'])
            , MegaHitsRadioScraper(self.config_manager.WEB_SITES['MegaHits'])
        ]
        
        all_scrape_dfs = []
        all_scrape_csv_paths = []

        for scraper in scrapers:
            # Scrape information and save it as CSV
            if scrape_radios:
                logger.info("Scraping data from radios.")
                scraper.scrape(save_csv=True)

            # Add csv path to list
            all_scrape_csv_paths.append(scraper.csv_path)


        if not fetch_info:
            logger.info("Not fetching extra info from APIs. Exiting function gracefully.")
            return

        # Read all extracted data so far
        for path in all_scrape_csv_paths:
            radio_df = self.data_storage.read_csv(path=path, schema=self.config_manager.RADIO_SCRAPPER_SCHEMA)
            all_scrape_dfs.append(radio_df)

        combined_df = pl.concat(all_scrape_dfs)

        # Get unique tracks and artists that have been scraped so far
        combined_df_unique_tracks = combined_df.unique(
            subset=[self.config_manager.TRACK_TITLE_COLUMN, self.config_manager.ARTIST_NAME_COLUMN], 
            maintain_order = True
        )
        combined_df_unique_artists = combined_df.unique(
            subset=[self.config_manager.TRACK_TITLE_COLUMN], 
            maintain_order = True
        )

        # Get dataframes of tracks and artists already extracted
        already_extracted_tracks = self.data_storage.read_csv_if_exists(
            path=self.config_manager.TRACK_INFO_CSV_PATH,
            schema=self.config_manager.TRACK_INFO_SCHEMA,
            columns=[self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN]
        )
        already_extracted_artists = self.data_storage.read_csv_if_exists(
            path=self.config_manager.ARTIST_INFO_CSV_PATH,
            schema=self.config_manager.ARTIST_INFO_SCHEMA,
            columns=[self.ARTIST_NAME_COLUMN]
        )

        # Filter for tracks and artist not yet registered
        new_tracks_df = self._identify_unregistered_tracks(
            new_track_df=combined_df_unique_tracks,
            registered_tracks=already_extracted_tracks
        )
        new_artists_df = self._identify_unregistered_artists(
            new_artist_df=combined_df_unique_artists,
            registered_artists=already_extracted_artists
        )

        print('New artists size:', new_artists_df.height)
        print('New tracks size:', new_tracks_df.height)

        if new_tracks_df.is_empty():
            logger.info('No new track data to process')
        else:
            spotify_track_df = await self.async_spotify_api.process_data(new_tracks_df)
            print('Spotify \n', spotify_track_df)

            lyrics_df = self.lyrics_analyzer.process_data(new_tracks_df)
            print('Lyrics \n', lyrics_df)

            # Save track info df
            track_info_df = spotify_track_df.join(
                lyrics_df, 
                how='left', 
                on=[self.config_manager.ARTIST_NAME_COLUMN, self.config_manager.TRACK_TITLE_COLUMN]
            )

            self.data_storage.output_csv(
                df=track_info_df,
                path=self.config_manager.TRACK_INFO_CSV_PATH,
                schema=self.config_manager.TRACK_INFO_SCHEMA
            )
            print('Saved track dataframe: \n', track_info_df)

        if new_artists_df.is_empty():
            logger.info('No new artist data to process')
        else:
            wikipedia_artist_df = self.wikipedia_api.process_data(new_artists_df)
            print('Wikipedia \n', wikipedia_artist_df)

            mb_artist_df = self.mb_api.process_data(new_artists_df)
            print('MusicBrainz \n', mb_artist_df)

            # Save artist info df
            artist_info_df = mb_artist_df.join(
                wikipedia_artist_df, 
                how='left', 
                on=[self.config_manager.ARTIST_NAME_COLUMN]
            )
            self.data_storage.output_csv(
                df=artist_info_df,
                path=self.config_manager.ARTIST_INFO_CSV_PATH,
                schema=self.config_manager.ARTIST_INFO_SCHEMA
            )   



if __name__ == "__main__":
    etl = RadioMusicETL()
    
    async def run_test():
        await etl.run(scrape_radios=True , fetch_info=True)

    asyncio.run(run_test())

    # TODO
    # 1. Create a second layer of ETL, with the scraped data unioned and with some treatment to the artist_name/track_title
    # 2. Transform values into TitleCase when saving track and artist info, for consistency

    # STREAMLIT
    # 1. Read track/artist with upper case, to keep naming consistent

    # Check for duplicated scraped data