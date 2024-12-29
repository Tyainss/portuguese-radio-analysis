import streamlit as st
from st_pages import add_page_title, get_nav_from_toml

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

def login():
    if st.button("Log in"):
        st.session_state.logged_in = True
        st.rerun()

def logout():
    if st.button("Log out"):
        st.session_state.logged_in = False
        st.rerun()

def define_pages():
    # login_page = st.Page(login, title="Log in", icon=":material/login:")
    # logout_page = st.Page(logout, title="Log out", icon=":material/logout:")

    home_page = st.Page('pages/home.py', title='Home Page', icon='🏠')
    overview_page = st.Page('pages/overview_comparison.py', title='Overview Comparison', icon='📊')
    radio_page = st.Page('pages/radio_deep_dive.py', title='Radio Deep Dive', icon='📻')
    self_service_page = st.Page('pages/self_service.py', title='Self Service', icon='👷‍♂️')

    pg = st.navigation([overview_page, radio_page, self_service_page])
    st.set_page_config(layout='wide', page_title='Radio Songs Analysis', page_icon=':radio:')

    # st.write(f'Logged In? : {st.session_state.logged_in}')

    # if st.session_state.logged_in:
    #     pg = st.navigation([
    #         logout_page,
    #         home_page,
    #         overview_page, radio_page, self_service_page,
    #         # {
    #         #     'Account': [logout_page],
    #         #     'Home': [home_page],
    #         #     'Dashboard': [overview_page, radio_page, self_service_page],
    #         #     # "Tools": [search, history],
    #         # }
    #         ]
    #     )
    # else:
    #     pg = st.navigation([login_page])

    return pg

def define_pages_2():
    # add_page_title()
    nav = get_nav_from_toml('.streamlit/pages.toml')
    pg = st.navigation(nav)

    add_page_title(pg)
    return pg


    