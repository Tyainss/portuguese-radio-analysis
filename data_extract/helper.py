import pycountry
from datetime import datetime
import pandas as pd

class Helper:
    def __init__(self) -> None:
        pass

    def get_image_text(self, image_list, size):
        for image in image_list:
            if image['size'] == size:
                return image['#text']
        return None
    
    def get_country_name_from_iso_code(self, iso_code):
        try:
            country = pycountry.countries.get(alpha_2=iso_code.upper())
            return country.name if country else 'Unknown'
        except AttributeError:
            return 'Unknown'
        except Exception as e:
            print(f'Error: {e}')
            return 'Unknown'
        
    def get_unix_latest_track_date(self, date):
        if date:
            date_obj = datetime.strptime(date, '%d %b %Y, %H:%M')
            return str(int(date_obj.timestamp()))
        else:
            return None

    def convert_to_bool(self, x):
        if pd.isna(x):
            return pd.NA # Use pandas' NA for missing values in boolean columns
        return bool(x)
        
    # Data treatment: Replace nan values based on schema dtype
    def replace_nan(self, df, schema):
        for column, dtype in schema.items():
            if dtype == 'str':
                df[column].fillna('', inplace=True)
            elif dtype == 'int64':
                df[column].fillna(0, inplace=True)