import re
import requests

class Wikipedia:

    def __init__(self) -> None:
        pass

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
