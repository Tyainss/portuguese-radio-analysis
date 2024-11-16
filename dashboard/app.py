import streamlit as st
import polars as pl

st.set_page_config(layout='wide', page_title='Radio Songs Analysis', page_icon=':radio:')

query_params = st.query_params

with st.sidebar:
    st.title(':radio: Radio Song Analysis')
    # Create Radio Buttons to Select Page
    page_options = ['Overview Comparison'
                    , 'Radio Deep Dive'
                    , 'Self-Service'
                    , 'teste']
    default_page = query_params.get('page', None)
    default_index = 0
    if default_page and default_page in page_options:
        default_index = page_options.index(default_page)
    print(default_index)
    selected_page = st.radio(
        key='select_page',
        label='**Navigate**',
        options=page_options,
        index=default_index,
        )
    
st.title('Overview Comparison')
