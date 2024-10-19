import polars as pl
import re
import aiohttp
import asyncio

from config_manager import ConfigManager

class AsyncWikipediaAPI:

    def __init__(self, config_path='config.json') -> None:
        self.config_manager = ConfigManager(config_path)

    def _process_artist_name(self, artist_name):
        # This method cleans up an artist name by:
        # 1. Splitting the name at the first occurrence of certain delimiters
        # 2. Removing any non-alphanumeric characters
        # 3. Stripping whitespace from the beginning and end
        delimiters = [',', 'feat.', 'ft.', '&', ' and ']
        pattern = '|'.join(map(re.escape, delimiters))
        artist_name = re.split(pattern, artist_name, maxsplit=1, flags=re.IGNORECASE)[0]
        return re.sub(r'[^\w\s]', '', artist_name).strip()

    async def get_artist_nationality_wikidata(self, artist_name):
        try:
            # First, search Wikipedia for the artist
            wiki_api_url = "https://en.wikipedia.org/w/api.php"
            search_params = {
                'action': 'query',
                'list': 'search',
                'srsearch': artist_name,
                'format': 'json',
                'srlimit': 1,
                'redirects': 1
            }

            async with aiohttp.ClientSession() as session:
                # Send asynchronous request to Wikipedia API for artist search
                async with session.get(wiki_api_url, params=search_params) as response:
                    search_data = await response.json()

                if search_data['query']['search']:
                    # If a Wikipedia page is found, get its Wikidata ID
                    page_title = search_data['query']['search'][0]['title']
                    page_params = {
                        'action': 'query',
                        'prop': 'pageprops',
                        'titles': page_title,
                        'format': 'json'
                    }

                    # Send asynchronous request to get page properties (Wikidata ID)
                    async with session.get(wiki_api_url, params=page_params) as page_response:
                        page_data = await page_response.json()

                    pages = list(page_data['query']['pages'].values())

                    if 'pageprops' in pages[0] and 'wikibase_item' in pages[0]['pageprops']:
                        wikidata_id = pages[0]['pageprops']['wikibase_item']
                        
                        # Use the Wikidata ID to fetch the artist's data
                        wikidata_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
                        async with session.get(wikidata_api_url) as wikidata_response:
                            wikidata_data = await wikidata_response.json()

                        # Look for nationality claims in the Wikidata
                        nationality_claims = wikidata_data['entities'][wikidata_id]['claims'].get('P27', [])
                        nationalities = []

                        for claim in nationality_claims:
                            # For each nationality claim, fetch the country name
                            country_id = claim['mainsnak']['datavalue']['value']['id']
                            country_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json"
                            async with session.get(country_api_url) as country_response:
                                country_data = await country_response.json()
                            country_name = country_data['entities'][country_id]['labels']['en']['value']
                            nationalities.append(country_name)

                        # Return the first nationality found, or "Unknown" if none found
                        if nationalities:
                            return {'nationality' : nationalities[0]}
            
            return "Unknown"
        
        except Exception as e:
            # If any error occurs during the process, print it and return "Unknown"
            print(f"Error with artist '{artist_name}': {e}")
            return "Unknown"

    async def fetch_all_artist_nationality_wikidata(self, df):
        """Fetch Wikipedia nationality info for all rows in the dataframe."""
        tasks = []
        for row in df.iter_rows(named=True):
            artist_name = row[self.config_manager.TRACK_ARTIST_COLUMN]
            tasks.append(self.get_artist_nationality_wikidata(artist_name))
        return await asyncio.gather(*tasks)
    
    async def process_data(self, df):
        """Fetch artist nationality info and return a dataframe with Wikipedia data joined."""
        # Fetch track info for all rows
        wiki_data = await self.fetch_all_artist_nationality_wikidata(df)

        # Filter out None values and create a new dataframe with the Spotify data
        wiki_data_filtered = [track for track in wiki_data if track]
        wiki_df = pl.DataFrame(wiki_data_filtered)

        # Merge with the original dataframe
        return wiki_df


if __name__ == "__main__":
    # Create an instance of AsyncWikipediaAPI
    wiki_api = AsyncWikipediaAPI()

    # Define an async function to fetch artist nationality
    async def fetch_artist_nationality(artist_name):
        nationality = await wiki_api.get_artist_nationality_wikidata(artist_name)
        print(f"{artist_name}'s nationality: {nationality}")

    async def process(df):
        wiki_df = await wiki_api.process_data(df)
        print(wiki_df)

    df = pl.DataFrame({
    'TRACK_ARTIST': ['Dua Lipa', 'Sabrina Carpenter']
    })

    asyncio.run(process(df))
    # # Example artist
    # artist_name = "Ed Sheeran"

    # # Run the event loop
    # asyncio.run(fetch_artist_nationality(artist_name))