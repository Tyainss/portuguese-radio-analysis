import polars as pl
import asyncio

from config_manager import ConfigManager
from data_storage import DataStorage
from radio_scraper import  PassouTypeRadioScraper, RFMRadioScraper, MegaHitsRadioScraper
from musicbrainz_api import MusicBrainzAPI
from asynchronous_spotify_api import AsyncSpotifyAPI
from wikipedia_api import WikipediaAPI
from lyrics import LyricsAnalyzer

from data_extract import logger


class RadioMusicETL:
    CONFIG_PATH_DEFAULT = 'config.json'
    SCHEMA_PATH_DEFAULT = 'schema.json'

    def __init__(self, config_path: str = CONFIG_PATH_DEFAULT, schema_path: str = SCHEMA_PATH_DEFAULT) -> None:
        self.config_manager = ConfigManager(config_path, schema_path)
        self.data_storage = DataStorage()
        self.mb_api = MusicBrainzAPI()
        self.async_spotify_api = AsyncSpotifyAPI()
        self.wikipedia_api = WikipediaAPI()
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
    
    def _to_titlecase(self, df, columns):
        df = df.with_columns(
            [pl.col(col).str.to_titlecase().alias(col) for col in columns]
        )
        return df

    async def _artist_process_helper(self, new_artist_df, csv_path, schema_path):
        already_extracted_df = self.data_storage.read_csv_if_exists(
            path=csv_path,
            schema=schema_path,
            columns=[self.ARTIST_NAME_COLUMN]
        )
        new_df = self._identify_unregistered_artists(
            new_artist_df=new_artist_df,
            registered_artists=already_extracted_df
        )
        return new_df

    async def artists_extract_process(self, combined_df):
        # Get unique artists that have been scraped so far
        combined_df_unique_artists = combined_df.unique(
            subset=[self.ARTIST_NAME_COLUMN], 
            maintain_order = True
        )

        # Wikipedia Process
        new_wiki_df = await self._artist_process_helper(
            new_artist_df=combined_df_unique_artists,
            csv_path=self.config_manager.WIKIPEDIA_INFO_CSV_PATH,
            schema_path=self.config_manager.WIKIPEDIA_INFO_SCHEMA
        )
        print('New Wiki size:', new_wiki_df.height)
        if new_wiki_df.is_empty():
            logger.info('No new Wiki artist data to process')
        else:
            wikipedia_artist_df = self.wikipedia_api.process_data(new_wiki_df)
            print('Wikipedia \n', wikipedia_artist_df)
            self.data_storage.output_csv(
                df=wikipedia_artist_df,
                path=self.config_manager.WIKIPEDIA_INFO_CSV_PATH,
                schema=self.config_manager.WIKIPEDIA_INFO_SCHEMA,
                mode='append',
            ) 

        # MusicBrainz Process
        new_mb_df = await self._artist_process_helper(
            new_artist_df=combined_df_unique_artists,
            csv_path=self.config_manager.MUSICBRAINZ_INFO_CSV_PATH,
            schema_path=self.config_manager.MUSICBRAINZ_INFO_SCHEMA
        )
        print('New MusicBrainz size:', new_mb_df.height)
        if new_mb_df.is_empty():
            logger.info('No new MusicBrainz artist data to process')
        else:
            mb_artist_df = self.mb_api.process_data(new_mb_df)
            print('MusicBrainz \n', mb_artist_df)
            self.data_storage.output_csv(
                df=mb_artist_df,
                path=self.config_manager.MUSICBRAINZ_INFO_CSV_PATH,
                schema=self.config_manager.MUSICBRAINZ_INFO_SCHEMA,
                mode='append',
            )  

    async def _track_process_helper(self, new_track_df, csv_path, schema_path):
        already_extracted_df = self.data_storage.read_csv_if_exists(
            path=csv_path,
            schema=schema_path,
            columns=[self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN]
        )
        new_df = self._identify_unregistered_tracks(
            new_track_df=new_track_df,
            registered_tracks=already_extracted_df
        )
        return new_df
    
    async def tracks_extract_process(self, combined_df):
        # Get unique tracks that have been scraped so far
        combined_df_unique_tracks = combined_df.unique(
            subset=[self.TRACK_TITLE_COLUMN, self.ARTIST_NAME_COLUMN], 
            maintain_order = True
        )

        # Spotify process
        new_spotify_df = await self._track_process_helper(
            new_track_df=combined_df_unique_tracks,
            csv_path=self.config_manager.SPOTIFY_INFO_CSV_PATH,
            schema_path=self.config_manager.SPOTIFY_INFO_SCHEMA
        )
        print(f'New Spotify size: {new_spotify_df.height}')
        if new_spotify_df.is_empty():
            logger.info('No new Spotify data to process')
        else:
            spotify_track_df = await self.async_spotify_api.process_data(new_spotify_df)
            print('Spotify \n', spotify_track_df)
            self.data_storage.output_csv(
                df=spotify_track_df,
                path=self.config_manager.SPOTIFY_INFO_CSV_PATH,
                schema=self.config_manager.SPOTIFY_INFO_SCHEMA,
                mode='append',
            )

        # Lyrics process
        new_lyrics_df = await self._track_process_helper(
            new_track_df=combined_df_unique_tracks,
            csv_path=self.config_manager.LYRICS_INFO_CSV_PATH,
            schema_path=self.config_manager.LYRICS_INFO_SCHEMA
        )
        print(f'New Lyrics size: {new_lyrics_df.height}')
        if new_lyrics_df.is_empty():
            logger.info('No new Lyrics data to process')
        else:
            lyrics_df = self.lyrics_analyzer.process_data(new_lyrics_df)
            print('Lyrics \n', lyrics_df)
            self.data_storage.output_csv(
                df=lyrics_df,
                path=self.config_manager.LYRICS_INFO_CSV_PATH,
                schema=self.config_manager.LYRICS_INFO_SCHEMA,
                mode='append',
            )

    async def tracks_transformation(self, scraped_df):
        """ Load and join Spotify and Lyrics track data"""
        join_columns = [self.ARTIST_NAME_COLUMN, self.TRACK_TITLE_COLUMN]
        tracks_core = scraped_df.select(join_columns).unique()
        
        spotify_df = self.data_storage.read_csv(
            path=self.config_manager.SPOTIFY_INFO_CSV_PATH,
            schema=self.config_manager.SPOTIFY_INFO_SCHEMA,
        )
        spotify_df = self._to_titlecase(df=spotify_df, columns=join_columns).unique(subset=join_columns)

        lyrics_df = self.data_storage.read_csv(
            path=self.config_manager.LYRICS_INFO_CSV_PATH,
            schema=self.config_manager.LYRICS_INFO_SCHEMA,
        )
        lyrics_df = self._to_titlecase(df=lyrics_df, columns=join_columns).unique(subset=join_columns)
        
        # Join track related info
        track_info_df = tracks_core.join(
            spotify_df, 
            how='left', 
            on=join_columns,
        ).join(
            lyrics_df, 
            how='left', 
            on=join_columns,
        )

        self.data_storage.output_csv(
            df=track_info_df,
            path=self.config_manager.TRACK_INFO_CSV_PATH,
            schema=self.config_manager.TRACK_INFO_SCHEMA,
            mode='deduplicate_append',
            sort_keys=[self.ARTIST_NAME_COLUMN, self.TRACK_TITLE_COLUMN], 
        )
        print('Saved track dataframe: \n', track_info_df)

    async def artists_transformation(self, scraped_df):
        """ Load and join MusicBrainz and Wikipedia artist data"""
        join_columns = [self.ARTIST_NAME_COLUMN]
        artists_core = scraped_df.select(join_columns).unique()
        
        musicbrainz_df = self.data_storage.read_csv(
            path=self.config_manager.MUSICBRAINZ_INFO_CSV_PATH,
            schema=self.config_manager.MUSICBRAINZ_INFO_SCHEMA,
        )
        musicbrainz_df = self._to_titlecase(df=musicbrainz_df, columns=join_columns).unique(subset=join_columns)

        wikipedia_df = self.data_storage.read_csv(
            path=self.config_manager.WIKIPEDIA_INFO_CSV_PATH,
            schema=self.config_manager.WIKIPEDIA_INFO_SCHEMA,
        )
        wikipedia_df = self._to_titlecase(df=wikipedia_df, columns=join_columns).unique(subset=join_columns)
        
        # Join artist related info
        artist_info_df = artists_core.join(
            musicbrainz_df, 
            how='left', 
            on=join_columns,
        ).join(
            wikipedia_df, 
            how='left', 
            on=join_columns,
        )

        # Combine both MB and Wiki nationality columns into one
        artist_info_df = artist_info_df.with_columns(
            pl.when((pl.col('mb_artist_country').is_not_null()) & (~pl.col('mb_artist_country').is_in(['Unknown', ''])))
            .then(pl.col('mb_artist_country'))
            .otherwise(pl.col('wiki_nationality'))
            .alias('combined_nationality')
        )

        # Combine both Wiki and MusicBrainz start date columns into one
        artist_info_df = artist_info_df.with_columns(
            pl.when((pl.col('wiki_artist_start_date').is_not_null()))
            .then(pl.col('wiki_artist_start_date'))
            .otherwise(pl.col('mb_artist_career_begin'))
            .alias('combined_artist_start_date')
        )

        self.data_storage.output_csv(
            df=artist_info_df,
            path=self.config_manager.ARTIST_INFO_CSV_PATH,
            schema=self.config_manager.ARTIST_INFO_SCHEMA,
            mode='deduplicate_append',
        )
        print('Saved artist dataframe: \n', artist_info_df)

    async def extract_data(self, scrape_radios=True, fetch_info=True):
        """
        Extracts data by scraping data from the radio websites, and then extracting
        information for the tracks/artists from APIs (Spotify, Genius, MusicBrainz, Wikipedia)

        Saves the "raw" data into the ./extract_folder
        """
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

        # Read all extracted data so far
        for path in all_scrape_csv_paths:
            radio_df = self.data_storage.read_csv(path=path, schema=self.config_manager.RADIO_SCRAPPER_SCHEMA)
            all_scrape_dfs.append(radio_df)

        combined_df = pl.concat(all_scrape_dfs)

        if not fetch_info:
            logger.info("Not fetching extra info from APIs. Exiting function gracefully.")
            return combined_df

        await self.tracks_extract_process(combined_df)
        await self.artists_extract_process(combined_df)

        return combined_df

    async def transform_data(self, scraped_df):
        """
        Applies transformations/treatment to the raw data, saving it in the ./transform_folder
        """
        # Transform all tracks and artists names into title case for consistency, removing duplicates
        scraped_df = scraped_df.with_columns(
            pl.col(self.TRACK_TITLE_COLUMN).str.to_titlecase(),
            pl.col(self.ARTIST_NAME_COLUMN).str.to_titlecase(),
        )
        
        print(scraped_df)
        self.data_storage.output_csv(
            df=scraped_df,
            path=self.config_manager.RADIO_CSV_PATH,
            schema=self.config_manager.RADIO_SCRAPPER_SCHEMA,
            mode='deduplicate_append',
            sort_keys=[self.config_manager.RADIO_COLUMN, self.config_manager.DAY_COLUMN, self.config_manager.TIME_PLAYED_COLUMN], 
        )

        # Load all raw files from APIs
        await self.tracks_transformation(scraped_df)
        await self.artists_transformation(scraped_df)


    async def run(self, scrape_radios=True, fetch_info=True, transform_data=True):
        scraped_df = await self.extract_data(scrape_radios, fetch_info)

        if transform_data:
            await self.transform_data(scraped_df)



if __name__ == "__main__":
    etl = RadioMusicETL()
    
    # df = pl.DataFrame({
    # 'track_title': ['Houdini', 'Please Please Please', 'She Will Be Loved'],
    # 'artist_name': ['Dua Lipa', 'Sabrina Carpenter', 'Maroon 5']
    # })

    async def run_test():
        await etl.run(scrape_radios=True , fetch_info=True, transform_data=True)
        # await etl.transform_data(df)

    asyncio.run(run_test())
