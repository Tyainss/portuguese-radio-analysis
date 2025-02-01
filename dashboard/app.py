import streamlit as st

def define_pages():
    
    overview_page = st.Page('pages/overview_comparison.py', title='Overview Comparison', icon='📊')
    radio_page = st.Page('pages/radio_deep_dive.py', title='Radio Deep Dive', icon='📻')
    self_service_page = st.Page('pages/self_service.py', title='Self Service', icon='👷‍♂️')

    pg = st.navigation([overview_page, radio_page, self_service_page])
    st.set_page_config(layout='wide', page_title='Radio Songs Analysis', page_icon=':radio:')


    return pg
    
pg = define_pages()

st.logo('dashboard/logo/personal_mark.png')

pg.run()