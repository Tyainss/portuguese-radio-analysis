import streamlit as st

st.header('st.button')

if st.button('Say Hello'):
    st.write('Why hello there')
else:
    st.write('Goodbye')