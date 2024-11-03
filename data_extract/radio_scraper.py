import os
import polars as pl
from datetime import datetime, timedelta, date, time as datetime_time
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

from config_manager import ConfigManager
from data_storage import DataStorage

class RadioScraper:

    def __init__(self, radio_config, wait_time=None) -> None:
        self.url = radio_config.get('url')
        self.config_manager = ConfigManager()
        self.data_storage = DataStorage()

        self.schema = self.config_manager.RADIO_SCRAPPER_SCHEMA
        self._initialize_column_names()
        self.wait_time = wait_time if wait_time is not None else self.config_manager.WAIT_DURATION

        service = Service(executable_path=self.config_manager.CHROME_DRIVER_PATH)
        self.driver = webdriver.Chrome(service=service)
        self.driver.get(self.url)
        self.wait = WebDriverWait(self.driver, self.wait_time)

    def _initialize_column_names(self):
        self.RADIO_COLUMN = self.config_manager.RADIO_COLUMN
        self.DAY_COLUMN = self.config_manager.DAY_COLUMN
        self.TIME_PLAYED_COLUMN = self.config_manager.TIME_PLAYED_COLUMN
        self.TRACK_TITLE_COLUMN = self.config_manager.TRACK_TITLE_COLUMN
        self.ARTIST_NAME_COLUMN = self.config_manager.ARTIST_NAME_COLUMN

    # def _get_csv_path(self, radio):
    #     csv_path = self.config_manager.CSV_PATH_FORMAT.format(radio=radio)
    #     return csv_path

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

    def _get_option_list(self, element_id) -> list:
        self.wait.until(EC.presence_of_element_located((By.ID, element_id)))
        select = Select(self.driver.find_element(By.ID, element_id))
        values = [option.get_attribute("value") for option in select.options if option.get_attribute("value")]
        return values

    def _get_most_recent_date_and_time(self, df):
        if df.is_empty():
            return None
        most_recent_row = df.sort(by=[self.DAY_COLUMN, self.TIME_PLAYED_COLUMN], descending=True).head(1)
        return {
            self.DAY_COLUMN: str(most_recent_row[self.DAY_COLUMN][0]),
            self.TIME_PLAYED_COLUMN: str(most_recent_row[self.TIME_PLAYED_COLUMN][0])
        }

    def _get_day_value(self, day_text):
        if day_text == 'Quando': return None
        subtract_days = {
            'today': 0,
            'yesterday': 1
        }
        days_subtracted = subtract_days[day_text]
        date = (datetime.now() - timedelta(days=days_subtracted)).strftime('%Y-%m-%d')
        return date

    def _get_last_time_played(self, csv_path, schema=None):
        last_time_played = None
        if os.path.exists(csv_path):
            existing_data = self.data_storage.read_csv(path=csv_path, schema=schema)
            last_time_played = self._get_most_recent_date_and_time(existing_data)
        return last_time_played

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

    def __init__(self, radio_config, wait_time=None) -> None:
        super().__init__(radio_config, wait_time)
        self.radio_name = radio_config.get('radio_name')
        self.time_played_name = radio_config.get('time_played_name')
        self.track_name = radio_config.get('track_name')
        self.artist_name = radio_config.get('artist_name')
        self.cookies_button_accept_text = radio_config.get('cookies_button_accept', 'CONCORDO')
        self.day_element_id = radio_config.get('day_element_id', 'day')
        self.search_button_text = radio_config.get('seach_button', 'Procurar')
        self.csv_path = self.config_manager.get_scraper_csv_path(radio=self.radio_name, path_format=self.config_manager.CSV_PATH_FORMAT)

    def _ignore_last_option(self, options):
        """Ignore last option since it shows the same values as "Today" """
        return options[:-1]

    def _extract_day_data(self, day_value, last_time_played=None):
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
            formatted_time_played = time_played.text.strip('[] ')
            if last_time_played and day_value == last_time_played[self.DAY_COLUMN] and formatted_time_played <= last_time_played[self.TIME_PLAYED_COLUMN]:
                continue
            track_data = {
                self.RADIO_COLUMN: self.radio_name,
                self.DAY_COLUMN: day_value,
                self.TIME_PLAYED_COLUMN: formatted_time_played,
                self.TRACK_TITLE_COLUMN: track_title.text,
                self.ARTIST_NAME_COLUMN: artist_name.text
            }
            day_track_data.append(track_data)
        
        return day_track_data

        
    def scrape(self, max_days=None, save_csv=True):
        self._accept_cookies(self.cookies_button_accept_text)
        self._select_radio(radio_name=self.radio_name)
        # day_values = self._get_days_list()
        day_values = self._ignore_last_option(self._get_option_list(self.day_element_id))


        last_time_played = self._get_last_time_played(self.csv_path, self.schema)
        if last_time_played:
            day_values = [day for day in day_values if day >= last_time_played[self.DAY_COLUMN]]

        if max_days:
            day_values = day_values[:min(max_days, len(day_values))]

        all_data = []
        for day_value in day_values:
            day_track_data = self._extract_day_data(day_value=day_value, last_time_played=last_time_played)
            if day_track_data:
                all_data.extend(day_track_data)

        df_all_data = pl.DataFrame(all_data)
        if save_csv and not df_all_data.is_empty():
            self.data_storage.output_csv(path=self.csv_path, df=df_all_data, schema=self.schema, append=True)
        return df_all_data


class RFMRadioScraper(RadioScraper):
    """
    Scraper for RFM Radio station

    It requires the selection of Day, Period and Hour filter.
    It only allows the extraction of the current day data and from the previous day (Today and Yesterday)
    """
    def __init__(self, radio_config, wait_time=None) -> None:
        super().__init__(radio_config, wait_time)
        self.radio_name = radio_config.get('radio_name', 'RFM')
        self.time_played_name = radio_config.get('time_played_name', 't-hor')
        self.track_name = radio_config.get('track_name', 'medium.no-margin.align-left')
        self.artist_name = radio_config.get('artist_name', 'large.no-margin.align-left')
        self.cookies_button_accept_text = radio_config.get('cookies_button_accept_text', 'ACEITAR')
        self.day_element_id = radio_config.get('day_element_id', 'dp-dia')
        self.period_element_id = radio_config.get('period_element_id', 'dp-periodo')
        self.hour_element_id = radio_config.get('hour_element_id', 'dp-hora')
        self.search_button_text = radio_config.get('search_button', 'pesquisa_quemusicaera')
        self.ad_wait_time = radio_config.get('ad_wait_time', 1)
        self.csv_path = self.config_manager.get_scraper_csv_path(radio=self.radio_name, path_format=self.config_manager.CSV_PATH_FORMAT)

    def _ignore_first_option(self, options):
        """Ignore first option since it's the label of the element selection and not a real option"""
        return options[1:]

    def _wait_for_ad_to_finish(self):
        """Wait for ad to finish"""
        time.sleep(self.ad_wait_time)
        try:
            # Wait for any iframe (ad container) to be visible
            ad_iframe = self.wait.until(EC.visibility_of_element_located((By.TAG_NAME, 'iframe')))
            print("Ad iframe detected and visible, waiting for it to finish...")

            # Wait until the iframe is no longer visible (i.e ad finishes)
            self.wait.until(EC.invisibility_of_element_located((By.TAG_NAME, 'iframe')))
            print("Ad iframe finished (now invisible).")

            # Additional check for the ad image inside the iframe
            try:
                # Switch to iframe to check the content inside
                self.driver.switch_to.frame(ad_iframe)

                # Look for the ad image inside the frame
                ad_image = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "img[src*='googlesyndication.com']")))
                print('Ad image detected, waiting for it to disappear...')

                # Wait for the ad image to disappear (zero size or hidden)
                self.wait.until(lambda _: ad_image.size['width'] == 0 or ad_image.size['height'] == 0 or not ad_image.is_displayed())
                print("Ad image has disappeared or become invisible.")

            finally:
                # Ensure we return to the main content
                self.driver.switch_to.default_content()

            print("Ad image finished.")
        except TimeoutException:
            # If no ad is detected or the ad finishes quickly, continue
            print("No ad detected or ad finished already.")
            pass

    def _extract_day_data(self, day_value, last_time_played=None):
        day_select = Select(self.driver.find_element(By.ID, self.day_element_id))
        day_select.select_by_value(day_value)
        day = self._get_day_value(day_text=day_value)
        
        day_track_data = []

        period_values = self._ignore_first_option(self._get_option_list(self.period_element_id))
        for period in period_values:
            period_select = Select(self.driver.find_element(By.ID, self.period_element_id))
            period_select.select_by_value(period)

            hour_values = self._ignore_first_option(self._get_option_list(self.hour_element_id))
            for hour in hour_values:
                hour_select = Select(self.driver.find_element(By.ID, self.hour_element_id))
                hour_select.select_by_value(hour)

                search_button = self.wait.until(EC.element_to_be_clickable((By.ID, self.search_button_text)))
                search_button.click()

                try:
                    self.wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, self.time_played_name)))
                    times_played = self.driver.find_elements(By.CLASS_NAME, self.time_played_name)
                    tracks_title = self.driver.find_elements(By.CLASS_NAME, self.track_name)
                    artists_name = self.driver.find_elements(By.CLASS_NAME, self.artist_name)

                    for time_played, track_title, artist_name in zip(times_played, tracks_title, artists_name):
                        if last_time_played and day == last_time_played[self.DAY_COLUMN] and time_played.text <= last_time_played[self.TIME_PLAYED_COLUMN]:
                            continue
                        track_data = {
                            self.RADIO_COLUMN: self.radio_name,
                            self.DAY_COLUMN: day,
                            self.TIME_PLAYED_COLUMN: time_played.text,
                            self.TRACK_TITLE_COLUMN: track_title.text,
                            self.ARTIST_NAME_COLUMN: artist_name.text
                        }
                        day_track_data.append(track_data)
                
                except Exception as e:
                    print(f'No track found for {period}, time: {hour}. Error: {e}')
                    return day_track_data
        
        return day_track_data

    def scrape(self, max_days=None, save_csv=True):
        self._accept_cookies(self.cookies_button_accept_text)
        self._wait_for_ad_to_finish()
        day_values = self._ignore_first_option(self._get_option_list(self.day_element_id))[::-1]
        
        # csv_path = self._get_csv_path(radio=self.radio_name)

        last_time_played = self._get_last_time_played(self.csv_path, self.schema)
        if last_time_played:
            day_values = [day for day in day_values if day >= last_time_played[self.DAY_COLUMN]]

        if max_days:
            day_values = day_values[:min(max_days, len(day_values))]

        all_data = []
        for day_value in day_values:
            day_track_data = self._extract_day_data(day_value=day_value, last_time_played=last_time_played)
            if day_track_data:
                all_data.extend(day_track_data)
    
        df_all_data = pl.DataFrame(all_data)
        if save_csv and not df_all_data.is_empty():
            self.data_storage.output_csv(path=self.csv_path, df=df_all_data, schema=self.schema, append=True)
        return df_all_data
    
class MegaHitsRadioScraper(RadioScraper):
    """
    Scraper for Mega Hits Radio

    It requires the Selection of day and the input of Hour and Minute of the search.
    After inputing both hour and minute, it provides all songs played in an interval of 15 minutes prior and after the time we chose.
    """
    def __init__(self, radio_config, wait_time=None) -> None:
        super().__init__(radio_config, wait_time)
        self.radio_name = radio_config.get('radio_name', 'MegaHits')
        self.time_played_name = radio_config.get('time_played_name', 'ac-horas1')
        self.track_name = radio_config.get('track_name', 'ac-nomem1')
        self.artist_name = radio_config.get('artist_name', 'ac-autor1')
        self.cookies_button_accept_text = radio_config.get('cookies_button_accept_text', 'ACEITAR')
        self.day_element_id = radio_config.get('day_element_id', 'dias')
        self.search_hour = radio_config.get('search_hour', 'txtHoraPesq')
        self.search_minute = radio_config.get('search_minute', 'txtMinutoPesq')
        self.search_button_text = radio_config.get('search_button_text', 'pesquisa')
        self.max_retries = radio_config.get('max_retries', 3)
        self.csv_path = self.config_manager.get_scraper_csv_path(radio=self.radio_name, path_format=self.config_manager.CSV_PATH_FORMAT)

    def _extract_day_data(self, day_value, last_time_played=None):
        day_select = Select(self.driver.find_element(By.ID, self.day_element_id))
        day_select.select_by_value(day_value)
        day = self._get_day_value(day_text=day_value)
        
        min_hour_range = 0
        if day == last_time_played[self.DAY_COLUMN]:
            min_hour_range = int(last_time_played[self.TIME_PLAYED_COLUMN].split(':')[0])

        day_track_data = []
        for hour in range(min_hour_range, 24):
            for minute in [15, 45]:
                for attempt in range (self.max_retries):
                    try:
                        # Clear and enter hour
                        hour_input = self.wait.until(EC.visibility_of_element_located((By.ID, self.search_hour)))
                        hour_input.clear()
                        hour_input.send_keys(f"{hour:02}")
                        
                        # Clear and enter minute
                        minute_input = self.wait.until(EC.visibility_of_element_located((By.ID, self.search_minute)))
                        minute_input.clear()
                        minute_input.send_keys(f"{minute:02}")

                        search_button = self.wait.until(EC.element_to_be_clickable((By.ID, self.search_button_text)))
                        search_button.click()
                        time.sleep(1)
                        
                        times_played = self.driver.find_elements(By.CLASS_NAME, self.time_played_name)
                        tracks_title = self.driver.find_elements(By.CLASS_NAME, self.track_name)
                        artists_name = self.driver.find_elements(By.CLASS_NAME, self.artist_name)

                        for time_played, track_title, artist_name in zip(times_played, tracks_title, artists_name):
                            if last_time_played and day == last_time_played[self.DAY_COLUMN] and time_played.text <= last_time_played[self.TIME_PLAYED_COLUMN]:
                                continue
                            track_data = {
                                self.RADIO_COLUMN: self.radio_name,
                                self.DAY_COLUMN: day,
                                self.TIME_PLAYED_COLUMN: time_played.text,
                                self.TRACK_TITLE_COLUMN: track_title.text,
                                self.ARTIST_NAME_COLUMN: artist_name.text
                            }
                            day_track_data.append(track_data)
                        
                        # Break out of retry loop if successful
                        break

                    except StaleElementReferenceException:
                        print(f"Stale element reference at {hour}:{minute}, retrying {attempt + 1}/{self.max_retries}")
                        time.sleep(1) 

                    except TimeoutException:
                        print(f"Timeout at {hour}:{minute}, skipping to next time.")
                        break  # Skip to next time if timeout occurs
                    
                    except Exception as e:
                        print(f"An error occurred while extracting data: {e}")
                        break
        
        return day_track_data

    def scrape(self, max_days=None, save_csv=True):
        self._accept_cookies(self.cookies_button_accept_text)
        day_values = self._get_option_list(self.day_element_id)[::-1]

        last_time_played = self._get_last_time_played(self.csv_path, self.schema)
        if last_time_played:
            day_values = [day for day in day_values if self._get_day_value(day) >= last_time_played[self.DAY_COLUMN]]

        if max_days:
            day_values = day_values[:min(max_days, len(day_values))]

        all_data = []
        for day_value in day_values:
            day_track_data = self._extract_day_data(day_value=day_value, last_time_played=last_time_played)
            if day_track_data:
                all_data.extend(day_track_data)
    
        df_all_data = pl.DataFrame(all_data)
        if save_csv and not df_all_data.is_empty():
            self.data_storage.output_csv(path=self.csv_path, df=df_all_data, schema=self.schema, append=True)
        return df_all_data