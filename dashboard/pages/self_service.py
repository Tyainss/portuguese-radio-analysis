import polars as pl
import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer
from pathlib import Path
import ydata_profiling
from streamlit_pandas_profiling import st_profile_report

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data

# Load the data
df_radio_data = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

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

st.header('👷‍♂️ Self-Service')

st.info('''
    Use this page to explore the data yourself!\n
    **Data Exploration** uses a Tableau-like library to allow for free exploration of the whole dataset\n
    **Data Profiling** provides information about the whole dataset, including a description for each column
''')

# Get the path to the current script
current_dir = Path(__file__).parent

# Build the path to the spec file
spec_file_path = current_dir / ".." / "spec" / "self_service_spec.json"

@st.cache_resource
def get_pyg_renderer() -> "StreamlitRenderer":
    df = load_df()
    # Convert day and time_played into a single datetime column
    df = df.with_columns(
        (pl.col("day").cast(pl.Datetime) + pl.col("time_played").cast(pl.Duration)).alias("datetime_played")
    )
    # Drop the original columns if you no longer need them
    df = df.drop(["time_played"])

    columns = df.columns
    # Reorder by placing 'datetime' after 'radio'
    columns = ["radio", "day", "datetime_played"] + [col for col in columns if col not in ["radio", "day", "datetime_played"]]
    # Reorder the DataFrame
    df = df.select(columns)

    return StreamlitRenderer(df, spec=str(spec_file_path), spec_io_mode="r") 

tab1, tab2 = st.tabs(['Data Exploration', 'Data Profiling'])

with tab1:
    st.markdown('The tool below was created using :blue-background[**pygwalker**]. \n\n Learn more about it, including how to use it, [here](https://kanaries.net/pygwalker)')
    renderer = get_pyg_renderer()

    renderer.explorer()

with tab2:
    df = load_df(pandas_format=True)
    pr = df.profile_report()

    st.title("Profiling in Streamlit")
    # st.write(df)
    # st_profile_report(pr)

# Improve Profiling process