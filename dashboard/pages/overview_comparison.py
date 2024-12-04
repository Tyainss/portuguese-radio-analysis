import polars as pl
import streamlit as st
import plotly.express as px

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()

@st.cache_data
def load_data(path, schema = None):
    data = ds.read_csv(path, schema)
    return data

def _convert_ms(duration_ms, output_unit: str = 'hours') -> float:
    # Convert into the desired duration
    conversion_factors = {
        'milliseconds': 1,
        'seconds': 1 / 1000,
        'minutes': 1 / (1000 * 60),
        'hours': 1 / (1000 * 60 * 60)
    }
    factor = conversion_factors.get(output_unit, 1)
    return round(duration_ms * factor, 2)

def prepare_hourly_metrics(df: pl.DataFrame, metric: str, **kwargs) -> pl.DataFrame:
    """
    Prepares hourly metrics for plotting.

    Args:
        df: Polars DataFrame with 'time' and the relevant columns.
        metric: One of 'avg_tracks', 'avg_time_played', or 'avg_popularity'.

    Returns:
        A Polars DataFrame with 'hour' and the calculated metric.
    """
    # Extract the hour from the 'time' column
    df = df.with_columns(
            pl.col(cm.TIME_PLAYED_COLUMN)
            .dt.hour()  # Extract the hour component
            .alias("hour")
        )
    
    # Calculate the chosen metric
    if metric == "avg_tracks":
        hourly_data = (
            df.group_by("hour")
            .agg(
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days"),
                pl.col(cm.DAY_COLUMN).count().alias("tracks"),
            )
            .with_columns(
                (pl.col("tracks") / pl.col('unique_days')).alias("avg_tracks"),
            )
        )
    elif metric == "avg_time_played":
        output_unit = kwargs.get('output_unit', 'hours')
        conversion_factor = {
            'milliseconds': 1,
            'seconds': 1 / 1000,
            'minutes': 1 / (1000 * 60),
            'hours': 1 / (1000 * 60 * 60)
        }.get(output_unit, 1)

        hourly_data = (
            df.group_by("hour")
            .agg(
                pl.col("spotify_duration_ms").sum().alias("total_duration_ms"),
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days")
            )
            .with_columns(
                ((pl.col("total_duration_ms") / pl.col("unique_days")) * conversion_factor)
                .alias("avg_time_played")
            )
        )
    elif metric == "avg_popularity":
        hourly_data = (
            df.group_by("hour")
            .agg(pl.col("spotify_popularity").mean().alias("avg_popularity"))
        )
    else:
        raise ValueError(f"Invalid metric: {metric}")
    
    # Return the selected metric
    result = hourly_data.select(["hour", metric]).sort('hour', descending=False)
    # result = hourly_data
    return result

def prepare_weekday_metrics(df: pl.DataFrame, metric: str, output_unit: str = 'hours') -> pl.DataFrame:
    """
    Prepares weekday metrics for plotting.

    Args:
        df: Polars DataFrame with 'day' and the relevant columns.
        metric: One of 'avg_tracks', 'avg_time_played', or 'avg_popularity'.

    Returns:
        A Polars DataFrame with 'weekday' and the calculated metric.
    """
    # Extract the weekday from the 'day' column
    df = df.with_columns(
        pl.col(cm.DAY_COLUMN).dt.to_string('%A').alias("weekday"),
        pl.col(cm.DAY_COLUMN).dt.weekday().alias("weekday_number"),
    )

    # Calculate the chosen metric
    if metric == "avg_tracks":
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days"),
                pl.count().alias("tracks"),
            )
            .with_columns(
                (pl.col("tracks") / pl.col("unique_days")).alias("avg_tracks")
            )
        )
    elif metric == "avg_time_played":
        conversion_factor = {
            'milliseconds': 1,
            'seconds': 1 / 1000,
            'minutes': 1 / (1000 * 60),
            'hours': 1 / (1000 * 60 * 60)
        }.get(output_unit, 1)
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(
                pl.col("spotify_duration_ms").sum().alias("total_duration_ms"),
                pl.col(cm.DAY_COLUMN).n_unique().alias("unique_days")
            )
            .with_columns(
                ((pl.col("total_duration_ms") / pl.col("unique_days")) * conversion_factor)
                .alias("avg_time_played")
            )
        )
    elif metric == "avg_popularity":
        weekday_data = (
            df.group_by("weekday", "weekday_number")
            .agg(pl.col("spotify_popularity").mean().alias("avg_popularity"))
        )
    else:
        raise ValueError(f"Invalid metric: {metric}")

    # Sort by weekday
    weekday_data = weekday_data.sort("weekday_number")

    # Map weekday numbers to names
    weekday_data = weekday_data.with_columns(
        pl.col("weekday").alias("weekday_name")
    )

    return weekday_data.select(["weekday_name", metric])

def calculate_avg_tracks(df: pl.DataFrame, adjusted_calc=True) -> float:
    if not adjusted_calc:
        total_tracks = df.height
        unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
        avg_tracks = total_tracks / unique_days
    else:
        hourly_data = prepare_hourly_metrics(df, metric='avg_tracks')
        avg_tracks = hourly_data.select(pl.col('avg_tracks').sum())[0,0]

    return round(avg_tracks, 2)

def calculate_avg_time(df: pl.DataFrame, output_unit: str = "hours", adjusted_calc=True) -> float:
    if not adjusted_calc:
        total_duration_ms = df.select(pl.col('spotify_duration_ms').sum())[0, 0]
        unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
        avg_duration_ms = total_duration_ms / unique_days
        avg_time = _convert_ms(avg_duration_ms, output_unit=output_unit)
    else:
        hourly_data = prepare_hourly_metrics(df, metric='avg_time_played', kwargs=output_unit)
        avg_time = hourly_data.select(pl.col('avg_time_played').sum())[0, 0]

    return round(avg_time, 2)
    
    
def calculate_avg_popularity(df: pl.DataFrame) -> float:
    avg = df.select(pl.col('spotify_popularity').mean())[0, 0]
    return round(avg, 2)


def plot_weekday_metrics(df: pl.DataFrame, metric: str, radio_name: str):
    """
    Plots weekday metrics using Plotly.

    Args:
        df: Polars DataFrame with 'weekday_name' and the chosen metric.
        metric: The metric to plot (column name).
        radio_name: Name of the radio station for the plot title.
    """
    # Convert to a Pandas DataFrame for Plotly compatibility
    pandas_df = df.to_pandas()

    # Create a Plotly line chart
    fig = px.line(
        pandas_df,
        x="weekday_name",
        y=metric,
        title=f"{radio_name} - {metric.replace('_', ' ').capitalize()} by Day of Week",
        labels={"weekday_name": "Day of Week", metric: metric.replace('_', ' ').capitalize()},
        markers=True
    )
    fig.update_layout(
        xaxis_title="Day of Week",
        yaxis_title=metric.replace('_', ' ').capitalize(),
    )
    st.plotly_chart(fig, use_container_width=True)


# Load the data
df_radio_data = load_data(cm.RADIO_CSV_PATH, cm.RADIO_SCRAPPER_SCHEMA)
df_artist_info = load_data(cm.ARTIST_INFO_CSV_PATH, cm.ARTIST_INFO_SCHEMA)
df_track_info = load_data(cm.TRACK_INFO_CSV_PATH, cm.TRACK_INFO_SCHEMA)

df_joined = df_radio_data.join(
        df_artist_info, 
        on=cm.ARTIST_NAME_COLUMN,
        how='left',
    ).join(
        df_track_info,
        on=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
        how='left',
    )

# st.write(df)

app_config = cm.load_json(path='dashboard/app_config.json')
# st.write(app_config)

ncols = len(app_config)
cols = st.columns(ncols)
# st.image('dashboard/logo/RadioComercial_logo.jpg')
for i, (key, val) in enumerate(app_config.items()):
    with cols[i]:
        radio_name = val.get('name')
        logo = val.get('logo')
        st.image(logo, use_container_width=True)
        # st.title(radio_name)

        radio_df = df_joined.filter(
            pl.col(cm.RADIO_COLUMN) == radio_name
        )
        
        kpi_1, kpi_2, kpi_3 = st.columns(3)
        kpi_1.metric(
            label='# Avg Tracks per Day',
            value=calculate_avg_tracks(df=radio_df) # Need to revise this metric due data issues - Not all dates have complete information
            # Do an average of the hourly avg
        )
        kpi_2.metric(
            label='Avg Hours Played',
            value=calculate_avg_time(df=radio_df, output_unit='hours')
        )
        kpi_3.metric(
            label='Avg Popularity',
            value=calculate_avg_popularity(df=radio_df)
        )

        # Prepare Weekday Metrics
        avg_tracks_df = prepare_weekday_metrics(radio_df, metric='avg_tracks')
        avg_time_df = prepare_weekday_metrics(radio_df, metric='avg_time_played', output_unit='hours')
        avg_popularity_df = prepare_weekday_metrics(radio_df, metric='avg_popularity')

        
        # Plot the metrics
        st.subheader("Average Tracks per Day")
        plot_weekday_metrics(avg_tracks_df, metric='avg_tracks', radio_name=radio_name)

        st.subheader("Average Hours Played per Day")
        plot_weekday_metrics(avg_time_df, metric='avg_time_played', radio_name=radio_name)

        st.subheader("Average Popularity per Day")
        plot_weekday_metrics(avg_popularity_df, metric='avg_popularity', radio_name=radio_name)

        # hourly_data = prepare_hourly_metrics(radio_df, metric='avg_time_played')
        # st.write(hourly_data)
        st.write(radio_df)
        