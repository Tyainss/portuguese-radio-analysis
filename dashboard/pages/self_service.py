import polars as pl
import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer
from pathlib import Path
from ydata_profiling import ProfileReport
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

# Initialize session state for profiling generation
if "profiling_started" not in st.session_state:
    st.session_state["profiling_started"] = False

# Get the path to the current script
current_dir = Path(__file__).parent

# Build the path to the spec file
spec_file_path = current_dir / ".." / "spec" / "self_service_spec.json"
# config_file_path = Path(__file__).parent / "profiling_config.yaml"

# @st.cache_resource
# def get_pyg_renderer() -> "StreamlitRenderer":
#     df = load_df()
#     # Convert day and time_played into a single datetime column
#     df = df.with_columns(
#         (pl.col("day").cast(pl.Datetime) + pl.col("time_played").cast(pl.Duration)).alias("datetime_played")
#     )
#     # Drop the original columns if you no longer need them
#     df = df.drop(["time_played"])

#     columns = df.columns
#     # Reorder by placing 'datetime' after 'radio'
#     columns = ["radio", "day", "datetime_played"] + [col for col in columns if col not in ["radio", "day", "datetime_played"]]
#     # Reorder the DataFrame
#     df = df.select(columns)

#     return StreamlitRenderer(df, spec=str(spec_file_path), spec_io_mode="r") 

tab1, tab2 = st.tabs(['Data Exploration', 'Data Profiling'])

# Data Exploration Tab
with tab1:
    st.markdown(
        "The tool below was created using :blue-background[**pygwalker**]. \n\n Learn more about it, including how to use it, [here](https://kanaries.net/pygwalker)"
    )
    @st.cache_resource
    def get_pyg_renderer() -> "StreamlitRenderer":
        df = load_df()
        df = df.with_columns(
            (pl.col("day").cast(pl.Datetime) + pl.col("time_played").cast(pl.Duration)).alias("datetime_played")
        )
        df = df.drop(["time_played"])

        columns = df.columns
        columns = ["radio", "day", "datetime_played"] + [col for col in columns if col not in ["radio", "day", "datetime_played"]]
        df = df.select(columns)
        return StreamlitRenderer(df, spec=str(Path(__file__).parent / ".." / "spec" / "self_service_spec.json"), spec_io_mode="r")

    renderer = get_pyg_renderer()
    renderer.explorer()

# Data Profiling Tab
with tab2:
    st.title("Data Profiling")

    if st.session_state["profiling_started"]:
        # Profiling already started, reuse the saved report
        with st.spinner("Loading the existing profiling report..."):
            st_profile_report(st.session_state["profiling_report"])
    else:
        # Generate the profiling report and save to session_state
        with st.spinner("Generating the profiling report. This may take a few moments..."):
            df = load_df(pandas_format=True)
            profiling_report = ProfileReport(
                df,
                title="Profiling Report",
                explorative=True,
            )
            # Save the report in session state
            st.session_state["profiling_started"] = True
            st.session_state["profiling_report"] = profiling_report

            # Display the profiling report
            st_profile_report(profiling_report)