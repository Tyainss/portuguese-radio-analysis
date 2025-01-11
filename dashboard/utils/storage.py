import polars as pl
import streamlit as st

from utils.helper import clean_name_column
from data_extract.data_storage import DataStorage

ds = DataStorage()

@st.cache_data
def generate_csv(_data):
    return _data.to_pandas().to_csv(index=False)

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data
    

def load_joined_data(
    df_radio_data: pl.DataFrame,
    df_artist_info: pl.DataFrame,
    df_track_info: pl.DataFrame,
    artist_col: str,
    track_col: str,
    how="left",
    pandas_format=False,
) -> pl.DataFrame:
    """
    Joins the radio, artist, and track info data into a single dataframe.
    """
    # Clean artist and track names in all datasets
    df_radio_data = clean_name_column(df_radio_data, artist_col)
    df_radio_data = clean_name_column(df_radio_data, track_col)

    df_artist_info = clean_name_column(df_artist_info, artist_col)
    df_track_info = clean_name_column(df_track_info, track_col)
    
    # Ensure all tables have unique values to prevent duplicates
    df_artist_info = df_artist_info.unique(subset=[artist_col])
    df_track_info = df_track_info.unique(subset=[track_col, artist_col])

    df = (
        df_radio_data
        .join(df_artist_info, on=artist_col, how=how)
        .join(df_track_info, on=[track_col, artist_col], how=how)
    )
    if pandas_format:
        df = df.to_pandas()
    return df