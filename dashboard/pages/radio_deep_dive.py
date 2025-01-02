import polars as pl
import streamlit as st

from data_extract.config_manager import ConfigManager

from utils.storage import (
    load_data
)

cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

# Load the data
df_radio_data = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

radio_options = app_config.keys()

@st.cache_data
def load_df(pandas_format=False):
    df = df_radio_data.join(
            df_artist_info, 
            on=cm.ARTIST_NAME_COLUMN,
            how='left',
        ).join(
            df_track_info,
            on=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
            how='left',
        )
    if pandas_format:
        df = df.to_pandas()
    return df

@st.cache_data
def filter_dataframe_by_radio(_df, radio_name):
    return _df.filter(pl.col(cm.RADIO_COLUMN) == radio_name)

df_joined = load_df()
min_date = df_joined[cm.DAY_COLUMN].min()
max_date = df_joined[cm.DAY_COLUMN].max()

if 'date_period' not in st.session_state:
    st.session_state['date_period'] = (min_date, max_date)
if 'genre_options' not in st.session_state:
    st.session_state['genre_options'] = list(df_joined['spotify_genres'].unique())

def reset_settings():
    st.session_state['date_period'] = (min_date, max_date)
    st.session_state['genre_options'] = list(df_joined['spotify_genres'].unique())

# Sidebar Filters
with st.sidebar:
    st.title(':gear: Page Settings')

    radio_chosen = st.selectbox(
        label='Pick the Radio',
        options=radio_options,
        index=0,
        key='radio_name_filter',
    )
    radio_df = filter_dataframe_by_radio(df_joined, app_config[radio_chosen].get('name'))

    new_date_period = st.date_input(
        label=':calendar: Select the time period',
        # value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='date_period'
    )

    # Try adding it inside Expander or using a data_editor column to select
    # genres_chosen = st.multiselect(
    #     'teste',
    #     st.session_state['genre_options'],
    #     st.session_state['genre_options']
    # )

st.write(radio_df.select('spotify_genres').unique())
radio_df

st.write(st.session_state)

st.write("Placeholder for Radio Deep Dive")