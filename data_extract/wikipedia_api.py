import polars as pl
import re
import requests
from tqdm import tqdm

from config_manager import ConfigManager
from helper import Helper

from data_extract import logger

class WikipediaAPI:

    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.helper = Helper()
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
    
    def get_artist_nationality_wikidata(self, artist_name):
        # This method attempts to find the nationality of an artist using Wikipedia and Wikidata
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

            search_data = requests.get(wiki_api_url, params=search_params).json()

            if search_data['query']['search']:
                # If a Wikipedia page is found, get its Wikidata ID
                page_title = search_data['query']['search'][0]['title']
                page_params = {
                    'action': 'query',
                    'prop': 'pageprops',
                    'titles': page_title,
                    'format': 'json'
                }
                page_data = requests.get(wiki_api_url, params=page_params).json()
                pages = list(page_data['query']['pages'].values())

                if 'pageprops' in pages[0] and 'wikibase_item' in pages[0]['pageprops']:
                    wikidata_id = pages[0]['pageprops']['wikibase_item']
                    
                    # Use the Wikidata ID to fetch the artist's data
                    wikidata_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
                    wikidata_response = requests.get(wikidata_api_url)
                    wikidata_data = wikidata_response.json()

                    # Look for nationality claims in the Wikidata
                    nationality_claims = wikidata_data['entities'][wikidata_id]['claims'].get('P27', [])
                    nationalities = []

                    for claim in nationality_claims:
                        # For each nationality claim, fetch the country name
                        country_id = claim['mainsnak']['datavalue']['value']['id']
                        country_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{country_id}.json"
                        country_data = requests.get(country_api_url).json()
                        country_name = country_data['entities'][country_id]['labels']['en']['value']
                        nationalities.append(country_name)

                    # Return the first nationality found, or "Unknown" if none found
                    if nationalities:
                        return nationalities[0]
            
            return "Unknown"
        
        except Exception as e:
            # If any error occurs during the process, print it and return "Unknown"
            print(f"Error with artist '{artist_name}': {e}")
            return "Unknown"
        
    def get_artist_start_year(self, artist_name):
        """
        Extracts the start year of an artist's career if a band, 
        or the birth year if a solo artist.
        
        Returns:
            'wiki_artist_start_date' (YYYY-MM-DD format).
        """
        career_start = None
        birth_year = None

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
            search_data = requests.get(wiki_api_url, params=search_params).json()

            if search_data['query']['search']:
                # Get the Wikipedia page title and Wikidata ID
                page_title = search_data['query']['search'][0]['title']
                page_params = {
                    'action': 'query',
                    'prop': 'pageprops',
                    'titles': page_title,
                    'format': 'json'
                }
                page_data = requests.get(wiki_api_url, params=page_params).json()
                pages = list(page_data['query']['pages'].values())

                if 'pageprops' in pages[0] and 'wikibase_item' in pages[0]['pageprops']:
                    wikidata_id = pages[0]['pageprops']['wikibase_item']
                    
                    # Fetch Wikidata entity
                    wikidata_api_url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
                    wikidata_response = requests.get(wikidata_api_url)
                    wikidata_data = wikidata_response.json()

                    entity_data = wikidata_data['entities'][wikidata_id]

                    # Extract birth date (for solo artists)
                    if 'P569' in entity_data['claims']:
                        birth_date = entity_data['claims']['P569'][0]['mainsnak']['datavalue']['value']['time']
                        birth_year = self._format_wikidata_date(birth_date)

                    # Extract career start date (for bands)
                    if 'P2031' in entity_data['claims']:
                        career_start_date = entity_data['claims']['P2031'][0]['mainsnak']['datavalue']['value']['time']
                        career_start = self._format_wikidata_date(career_start_date)

        except Exception as e:
            print(f"Error retrieving artist start year for '{artist_name}': {e}")

        # Return career start if available, otherwise birth year
        res = self.helper.format_date(career_start or birth_year)
        return res

    def _format_wikidata_date(self, wikidata_date):
        """
        Formats Wikidata date into a YYYY-MM-DD format, defaulting missing parts to '01'.
        """
        match = re.match(r"\+(\d{4})(?:-(\d{2}|00))?(?:-(\d{2}|00))?", wikidata_date)
        if match:
            year = match.group(1)
            month = match.group(2) if match.group(2) and match.group(2) != "00" else "01"
            day = match.group(3) if match.group(3) and match.group(3) != "00" else "01"
            return f"{year}-{month}-{day}"
        return None



    def get_artist_info(self, artist_name):
        nationality = self.get_artist_nationality_wikidata(artist_name)
        start_year = self.get_artist_start_year(artist_name)
        result = {
            self.config_manager.ARTIST_NAME_COLUMN: artist_name
            , 'wiki_nationality': nationality
            , 'wiki_artist_start_date': start_year
        }
        return result

    def process_data(self, df):
        wiki_data = []
        for row in tqdm(df.iter_rows(named=True), total=len(df), desc='Processing Wikipedia Artist Info', unit='row'):
            artist_name = row[self.config_manager.ARTIST_NAME_COLUMN]
            result = self.get_artist_info(artist_name)
            wiki_data.append(result)
        
        wiki_df = pl.DataFrame(wiki_data)
        return wiki_df
        

if __name__ == '__main__':
    wiki = WikipediaAPI()
    # res = wiki.get_artist_start_year(artist_name='Richie Campbell')
    # print(res)
    df = pl.DataFrame({
    'artist_name': ['Dua Lipa', 'Sabrina Carpenter', 'Richie Campbell', 'Coldplay']
    })

    print(wiki.process_data(df))