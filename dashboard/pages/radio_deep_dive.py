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

radio_options = list(app_config.keys())

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

if "radio_name_filter" not in st.session_state:
    st.session_state['radio_name_filter'] = radio_options[0]

def filter_genre():
    """
    Reads 'edited_rows' from st.session_state['genre_editor']
    and applies them to the polars_df input. Then saves back to session_state.
    """
    # 1. Grab the changes from the Data Editor state
    data_editor_state = st.session_state["genre_editor"]
    edited_rows = data_editor_state.get("edited_rows", {})
    if not edited_rows:
        return  # No edits, so do nothing

    # 2. Pull the existing 'Selected?' column into a Python list so we can mutate by index
    selected_col = st.session_state["genres_selection"].get_column("Selected?").to_list()
    # 3. For each row that was changed, update that row in selected_col
    for row_str, changed_fields in edited_rows.items():
        row_idx = int(row_str)
        # If the user edited the "Selected?" checkbox...
        if "Selected?" in changed_fields:
            selected_col[row_idx] = changed_fields["Selected?"]

    # 4. Overwrite the Polars DF with updated values
    updated_df = st.session_state["genres_selection"].with_columns(pl.Series("Selected?", selected_col))
    st.session_state["genres_selection"] = updated_df

    # st.rerun()
    # st.session_state.clear()

def reset_settings():
    st.session_state['date_period'] = (min_date, max_date)
    st.session_state.pop("genres_selection", None)


# Sidebar Filters
with st.sidebar:
    st.title(':gear: Page Settings')

    # Radio Filter
    radio_chosen = st.selectbox(
        label='Pick the Radio',
        options=radio_options,
        index=0,
        key='radio_name_filter',
    )
    radio_df = filter_dataframe_by_radio(df_joined, app_config[radio_chosen].get('name'))

    min_date = radio_df[cm.DAY_COLUMN].min()
    max_date = radio_df[cm.DAY_COLUMN].max()

    # Date Filter
    new_date_period = st.date_input(
        label=':calendar: Select the time period',
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        # key='date_period'
    )

    # Radio Filter
    genres = radio_df.select('spotify_genres').unique()
    if 'genres_selection' not in st.session_state:
        st.session_state["genres_selection"] = pl.DataFrame({
            'spotify_genres': genres.to_series(),
            'Selected?': [True] * len(genres)
        })
    # st.session_state
    with st.expander(label='Filter by Genres'):
        st.data_editor(
            st.session_state["genres_selection"],
            use_container_width=True,
            column_config={
                "spotify_genres": {"width": 150},
                "Selected?": {"width": 100},
            },
            # 3) Provide a key so Streamlit can track the widget state
            key="genre_editor",
            on_change=filter_genre,
            hide_index=True
        )

radio_chosen


# Filter the dataframe by selected genres
selected_genres = (
    st.session_state["genres_selection"]
    .filter(pl.col("Selected?"))
    ["spotify_genres"]
)

filtered_radio_df  = radio_df.filter(
    pl.col('spotify_genres').is_in(selected_genres)
)

st.write(filtered_radio_df.select('spotify_genres'))

st.write(st.session_state)


st.write("Placeholder for Radio Deep Dive")

# Genre filter not working