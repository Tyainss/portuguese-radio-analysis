import os
import polars as pl
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from config_manager import ConfigManager
from data_storage import DataStorage

class RadioScraper:
    # Define column names as class attributes
    DAY_COLUMN = "DAY"
    TIME_PLAYED_COLUMN = "TIME PLAYED"
    TRACK_TITLE_COLUMN = "TRACK TITLE"
    TRACK_ARTIST_COLUMN = "TRACK ARTIST"

    def __init__(self, url, wait_time=None) -> None:
        self.url = url
        self.config_manager = ConfigManager()
        self.data_storage = DataStorage()

        self.CHROME_DRIVER_PATH = self.config_manager.config['WEB_SCRAPPER']['CHROME_DRIVER_PATH']
        self.CSV_PATH_FORMAT = self.config_manager.config['WEB_SCRAPPER']['CSV_PATH']
        self.WAIT_DURATION = self.config_manager.config['WEB_SCRAPPER']['WAIT_DURATION']

        self.wait_time = wait_time if wait_time is not None else self.WAIT_DURATION

        service = Service(executable_path=self.CHROME_DRIVER_PATH)
        self.driver = webdriver.Chrome(service=service)
        self.driver.get(url)
        self.wait = WebDriverWait(self.driver, self.wait_time)

    def _get_csv_path(self, radio):
        csv_path = self.CSV_PATH_FORMAT.format(radio=radio)
        return csv_path

    def _accept_cookies(self, cookies_button_accept_text, cookies_button='qc-cmp2-summary-buttons') -> None:
        try:
            cookies_banner = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, cookies_button)))
            accept_button = cookies_banner.find_element(By.XPATH, f".//button[span[text()='{cookies_button_accept_text}']]")
            accept_button.click()
        except Exception as e:
            print("Error accepting cookie banner:", e)

    def _select_radio(self, radio_name, radio_element_id='radio') -> None:
        self.wait.until(EC.presence_of_element_located((By.ID, radio_element_id)))

        radio_select = Select(self.driver.find_element(By.ID, radio_element_id))
        radio_select.select_by_visible_text(radio_name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if self.driver:
            self.driver.quit()

# # Usage with context manager
# with RadioScraper(url="https://example.com") as scraper:
#     # Perform scraping tasks
#     pass

# # Usage with explicit cleanup
# scraper = RadioScraper(url="https://example.com")
# try:
#     # Perform scraping tasks
#     pass
# finally:
#     scraper.close()

class PassouTypeRadioScraper(RadioScraper):
    """
    Scraper for radios of type "Passou" that share a similar structure.
    The radios that have that type of structure are Radio Comercial and Cidade FM.

    The Passou Type does not require the selection of any filter.
    It also allows to select the radio station to search for, as well as providing the selection of a day up
    until 6 days prior to the current day.
    """

    def __init__(self, url, radio_select, time_played_name, track_name, artist_name) -> None:
        super().__init__(url)
        self.radio_select = radio_select
        self.time_played_name = time_played_name
        self.track_name = track_name
        self.artist_name = artist_name
        self.cookies_button_accept_text = 'CONCORDO'
        self.day_element_id = 'day'
        self.search_button_text = 'Procurar'

    def _get_days_list(self) -> list:
        self.wait.until(EC.presence_of_element_located((By.ID, self.day_element_id)))
        day_select = Select(self.driver.find_element(By.ID, self.day_element_id))
        day_values = [option.get_attribute("value") for option in day_select.options if option.get_attribute("value")]
        return day_values

    def _select_day(self, day_value, last_time_played=None):
        day_select = Select(self.driver.find_element(By.ID, self.day_element_id))
        day_select.select_by_value(day_value)

        search_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[span[text()='{self.search_button_text}']]")))
        search_button.click()

        self.wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, self.time_played_name)))
        times_played = self.driver.find_elements(By.CLASS_NAME, self.time_played_name)
        tracks_title = self.driver.find_elements(By.CLASS_NAME, self.track_name)
        artists_name = self.driver.find_elements(By.CLASS_NAME, self.artist_name)

        day_track_data = []

        if not times_played:
            return day_track_data
        
        for time_played, track_title, artist_name in zip(times_played, tracks_title, artists_name):
            if last_time_played and day_value == last_time_played[self.DAY_COLUMN] and time_played.text <= last_time_played[self.TIME_PLAYED_COLUMN]:
                continue
            track_data = {
                    self.DAY_COLUMN: day_value,
                    self.TIME_PLAYED_COLUMN: time_played.text,
                    self.TRACK_TITLE_COLUMN: track_title.text,
                    self.TRACK_ARTIST_COLUMN: artist_name.text
            }
            day_track_data.append(track_data)
        
        return day_track_data

    def _get_most_recent_date_and_time(self, df):
        if df.is_empty():
            return None
        most_recent_row = df.sort(by=[self.DAY_COLUMN, self.TIME_PLAYED_COLUMN], reverse=True).head(1)
        return {
            self.DAY_COLUMN: most_recent_row[self.DAY_COLUMN][0],
            self.TIME_PLAYED_COLUMN: most_recent_row[self.TIME_PLAYED_COLUMN][0]
        }
        
    def scrape(self, max_days=None, save_csv=True):
        self._accept_cookies(self.cookies_button_accept_text)
        self._select_radio(radio_name=self.radio_select)
        day_values = self._get_days_list()

        csv_path = self._get_csv_path(radio=self.radio_select)
        schema = self.config_manager.schema

        last_time_played = None
        if os.path.exists(csv_path):
            existing_data = self.data_storage.read_csv(path=csv_path, schema=schema)
            last_time_played = self._get_most_recent_date_and_time(existing_data)
            if last_time_played:
                day_values = [day for day in day_values if day >= last_time_played[self.DAY_COLUMN]]

        if max_days:
            day_values = day_values[:min(max_days, len(day_values))]

        all_data = []
        for day_value in day_values:
            day_track_data = self._select_day(day_value=day_value, last_time_played=last_time_played)
            if day_track_data:
                all_data.extend(day_track_data)

        df_all_data = pl.DataFrame(all_data)
        if save_csv:
            self.data_storage.output_csv(path=csv_path, df=df_all_data, schema=schema, append=True)
        return df_all_data


            # Also add some checks to see if data was already extracted, and extract only startting from most recent day extracted etc