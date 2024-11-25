import polars as pl
import streamlit as st

from define_pages import define_pages, define_pages_2


pg = define_pages()


pg.run()


# dashboard = st.Page(
#     "reports/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True
# )


# def update_params(key, value):
#     st.query_params[key] = value
#     st.write('Key :', key, 'value :', value)
#     # st.query_params['page'] = 'Overview Comparison'

# with st.sidebar:
#     st.title(':radio: Radio Song Analysis')
#     # Create Radio Buttons to Select Page
#     page_options = ['Overview Comparison'
#                     , 'Radio Deep Dive'
#                     , 'Self-Service'
#                     , 'teste']
#     st.session_state.page = query_params.get('page', None)
#     default_index = 0
#     if st.session_state.page and st.session_state.page in page_options:
#         default_index = page_options.index(st.session_state.page)
#     print(default_index)
#     selected_page = st.radio(
#         key='select_page',
#         label='**Navigate**',
#         options=page_options,
#         index=default_index,
#         on_change=update_params,
#         kwargs={'key': 'page', 'value': st.session_state.page}
#         )
    
# st.title('Overview Comparison')
# st.write(selected_page)
# st.write(query_params)

# Try st.page_link or st.switch_page instead