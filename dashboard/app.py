import streamlit as st
import polars as pl

st.set_page_config(layout='wide', page_title='Radio Songs Analysis', page_icon=':radio:')

with st.sidebar:
    st.title(':radio: Radio Song Analysis')
    