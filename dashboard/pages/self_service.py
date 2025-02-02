import polars as pl
import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer
from pathlib import Path
from streamlit_extras.stylable_container import stylable_container

from data_extract.config_manager import ConfigManager

from utils.storage import (
    load_data, generate_csv
)

cm = ConfigManager()
app_config = cm.load_json(path='dashboard/app_config.json')

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
    df = df.with_columns(
        (pl.col("day").cast(pl.Datetime) + pl.col("time_played").cast(pl.Duration)).alias("datetime_played")
    )
    df = df.drop(["time_played"])

    columns = df.columns
    columns = ["radio", "day", "datetime_played"] + [col for col in columns if col not in ["radio", "day", "datetime_played"]]
    df = df.select(columns)
    if pandas_format:
        df = df.to_pandas()
    return df

st.header('👷‍♂️ Self-Service')

st.info('''
    Use this page to explore the data yourself!\n
    **Data Exploration** uses a Tableau-like library to allow for free exploration of the whole dataset\n
    **Extract Dataset** Extract the whole dataset for your own use.
''')

# Get the path to the current script
current_dir = Path(__file__).parent
spec_file_path = current_dir / ".." / "spec" / "self_service_spec.json"

tab1, tab2 = st.tabs(['Data Exploration', 'Extract Dataset'])

# Data Exploration Tab
with tab1:
    st.markdown(
        "The tool below was created using :blue-background[**pygwalker**]. \n\n Learn more about it, including how to use it, [here](https://kanaries.net/pygwalker)"
    )
    @st.cache_resource
    def get_pyg_renderer() -> "StreamlitRenderer":
        df = load_df()
        return StreamlitRenderer(df, spec=str(spec_file_path), spec_io_mode="r")

    renderer = get_pyg_renderer()
    renderer.explorer()

# Data Extract Tab
with tab2:
    df = load_df()
    csv = generate_csv(df)
    with stylable_container(
        key=f'csv_export_button',
        css_styles="""
            button {
                width: 275px;
                height: 60px;
                background-color: #add8e6; /* Light Blue */
                color: white;
                border-radius: 5px;
                white-space: nowrap;
            }
            """,
    ):
        _, csv_col, _ = st.columns([1,2,1])
        with csv_col:
            st.download_button(
                label=f"Export whole dataset as CSV",
                data=csv,
                file_name=f"radio_data.csv",
                mime="text/csv",
                key=f"data_export_button"
            )