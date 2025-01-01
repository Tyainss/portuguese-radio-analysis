import polars as pl
import streamlit as st

from data_extract.data_storage import DataStorage

ds = DataStorage()

@st.cache_data
def generate_csv(_data):
    return _data.to_pandas().to_csv(index=False)

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data
