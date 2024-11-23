import polars as pl
import streamlit as st

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data

df = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
# st.write(df)

app_config = cm.load_json(path='dashboard/app_config.json')
# st.write(app_config)

ncols = len(app_config)
cols = st.columns(ncols)
# st.image('dashboard/logo/RadioComercial_logo.jpg')
for i, (key, val) in enumerate(app_config.items()):
    with cols[i]:
        radio_name = val.get('name')
        logo = val.get('logo')
        st.image(logo, use_container_width=True)
        # st.title(radio_name)

        radio_df = df.filter(
            pl.col(cm.RADIO_COLUMN) == radio_name
        )
        st.write(radio_df)
        



# st.title(radio_name)



