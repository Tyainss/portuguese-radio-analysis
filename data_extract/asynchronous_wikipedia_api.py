import polars as pl
import re
import aiohttp
import asyncio

from config_manager import ConfigManager
from logger import setup_logging

# Set up logging
logger = setup_logging()

class AsyncWikipediaAPI:

    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.wiki_access_token = self.config_manager.WIKI_ACCESS_TOKEN
        self.wiki_client_secret = self.config_manager.WIKI_CLIENT_SECRET

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
            headers = {}
            if self.wiki_access_token:
                headers['Authorization'] = f'Bearer {self.wiki_access_token}'
            elif self.wiki_client_secret:
                headers['Client-Secret'] = self.wiki_client_secret

            async with aiohttp.ClientSession(headers=headers) as session:
                # Send asynchronous request to Wikipedia API for artist search
                async with session.get(wiki_api_url, params=search_params) as response:
                    if "application/json" in response.headers.get('Content-Type', ''):
                        search_data = await response.json()
                    else:
                        logger.warning(f"Unexpected content type for artist '{artist_name}': {response.headers['Content-Type']}")
                        return "Unknown"


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
                        if "application/json" in page_response.headers.get('Content-Type', ''):
                            page_data = await page_response.json()
                        else:
                            logger.warning(f"Unexpected content type for artist '{artist_name}': {page_response.headers['Content-Type']}")
                            return "Unknown"

                    pages = list(page_data['query']['pages'].values())
                    if 'pageprops' in pages[0] and 'wikibase_item' in pages[0]['pageprops']:
                        wikidata_id = pages[0]['pageprops']['wikibase_item']
                        
                        # Use the Wikidata ID to fetch the artist's data
                        wikidata_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
                        async with session.get(wikidata_api_url) as wikidata_response:
                            if "application/json" in wikidata_response.headers.get('Content-Type', ''):
                                wikidata_data = await wikidata_response.json()
                            else:
                                logger.warning(f"Unexpected content type for artist '{artist_name}': {wikidata_response.headers['Content-Type']}")
                                return "Unknown"

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

                    return nationalities[0] if nationalities else "Unknown"
            
            return "Unknown"
        
        except Exception as e:
            # If any error occurs during the process, print it and return "Unknown"
            print(f"Error with artist '{artist_name}': {e}")
            return "Unknown"

    async def get_artist_info(self, artist_name):
        nationality = await self.get_artist_nationality_wikidata(artist_name)
        result = {
            'artist_name': artist_name
            , 'wiki_nationality': nationality
        }
        return result

    async def fetch_all_artist_info(self, df):
        """Fetch Wikipedia nationality info for all rows in the dataframe."""
        tasks = []
        for row in df.iter_rows(named=True):
            artist_name = row[self.config_manager.ARTIST_NAME_COLUMN]
            tasks.append(self.get_artist_info(artist_name))
        return await asyncio.gather(*tasks)
    
    async def process_data(self, df):
        """Fetch artist nationality info and return a dataframe with Wikipedia data joined."""
        # Fetch track info for all rows
        wiki_data = await self.fetch_all_artist_info(df)
        wiki_df = pl.DataFrame(wiki_data)

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
    'artist_name': ['Dua Lipa', 'Sabrina Carpenter']
    })

    asyncio.run(process(df))