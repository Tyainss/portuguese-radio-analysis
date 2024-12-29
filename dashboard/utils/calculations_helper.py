import streamlit as st
import polars as pl
import plotly.express as px
from typing import List

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

ds = DataStorage()
cm = ConfigManager()

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
    if df.is_empty():
        return 0.0
    if not adjusted_calc:
        total_tracks = df.height
        unique_days = df.select(pl.col(cm.DAY_COLUMN).n_unique())[0, 0]
        avg_tracks = total_tracks / unique_days
    else:
        hourly_data = prepare_hourly_metrics(df, metric='avg_tracks')
        avg_tracks = hourly_data.select(pl.col('avg_tracks').sum())[0,0]

    return round(avg_tracks, 2)

def calculate_avg_time(df: pl.DataFrame, output_unit: str = "hours", adjusted_calc=True) -> float:
    if df.is_empty():
        return 0.0
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
    if df.is_empty():
        return 0.0
    avg = df.select(pl.col('spotify_popularity').mean())[0, 0]
    return round(avg, 2)


def plot_metrics(
    df: pl.DataFrame, 
    metric: str, 
    radio_name: str, 
    x_axis_column: str = 'weekday_name',
    x_axis_label: str = 'Day of Week',
    y_axis_range: tuple[float, float] = None,
    **kwargs
):
    """
    Plots metrics using Plotly for either weekday or hourly data.

    Args:
        df: Polars DataFrame with the chosen x-axis column and metric.
        metric: The metric to plot (column name).
        radio_name: Name of the radio station for the plot title.
        x_axis_column: Column name for the x-axis ('weekday_name' or 'hour').
        x_axis_label: Label for the x-axis.
        y_axis_range: Optional tuple specifying (min, max) for the y-axis range.
    """
    # Convert to a Pandas DataFrame for Plotly compatibility
    pandas_df = df.to_pandas()

    title = kwargs.get('title', f"{radio_name} - {metric.replace('_', ' ').capitalize()} by {x_axis_label}")
    metric_name = metric.replace('_', ' ').capitalize()
    
    # Create a Plotly line chart
    fig = px.line(
        pandas_df,
        x=x_axis_column,
        y=metric,
        title=title,
        labels={x_axis_column: x_axis_label, metric: metric_name},
        markers=True
    )
    fig.update_layout(
        xaxis_title=x_axis_label,
        yaxis_title=metric.replace('_', ' ').capitalize(),
        hoverlabel_align="left",
    )
    fig.update_traces(
        hovertemplate=(
            f"<b>{x_axis_label}</b>: " "%{x}<br>"
            f"<b>{metric_name}</b>: " "%{y:.2f}<br>"
        )
    )

    # Apply the y-axis range if provided
    if y_axis_range:
        fig.update_yaxes(range=y_axis_range)

    st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_{metric}_{x_axis_column}')


def calculate_country_counts(
    df: pl.DataFrame,
    country_col: str,
    count_columns: List[str],
    metric_type: str = 'unique',
    include_most_played: str = None  # Options: "track", "artist", or None
) -> pl.DataFrame:
    """
    Calculate counts or averages for a given column, grouped by flags,
    based on the selected metric type, with optional most played artist or track.

    Args:
        df (pl.DataFrame): Input DataFrame containing the data.
        country_col (str): Column to group by (e.g., 'lyrics_language' or 'combined_nationality').
        count_columns (List[str]): Columns to count occurrences of.
        metric_type (str, optional): Metric type ('unique', 'total', or 'average'). Defaults to 'unique'.
        include_most_played (str, optional): Whether to include most played track or artist. Defaults to None.

    Returns:
        pl.DataFrame: Grouped DataFrame with counts or averages, flags, and optional most played details.
    """
    cols = [pl.col(col) for col in count_columns] + [pl.col(country_col)]

    # Calculate metrics based on the selected type
    if metric_type == "unique":
        counts = df.select(cols).unique()
        result = (
            counts
            .group_by(country_col)
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "total":
        counts = df.select(cols)
        result = (
            counts
            .group_by(country_col)
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "average":
        unique_counts = (
            df.select(cols).unique()
            .group_by(country_col)
            .count()
            .rename({"count": "unique_count"})
        )
        total_counts = (
            df.select(cols)
            .group_by(country_col)
            .count()
            .rename({"count": "total_count"})
        )
        result = unique_counts.join(total_counts, on=country_col)
        result = result.with_columns(
            (pl.col("total_count") / pl.col("unique_count")).alias("metric")
        ).select([country_col, "metric"])
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}. Choose 'unique', 'total', or 'average'.")
    
    # Optionally add most played track or artist
    if include_most_played == "track":
        most_played = (
            df.group_by([country_col, cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN])
            .count()
            .sort([country_col, "count"], descending=True)
            .group_by(country_col)
            .agg([
                pl.col(cm.TRACK_TITLE_COLUMN).first().alias("most_played_track"),
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("count").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on=country_col, how="left")
    elif include_most_played == "artist":
        most_played = (
            df.group_by([country_col, cm.ARTIST_NAME_COLUMN])
            .count()
            .sort([country_col, "count"], descending=True)
            .group_by(country_col)
            .agg([
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("count").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on=country_col, how="left")

    return result.sort(by="metric", descending=True)

def calculate_decade_metrics(
    df: pl.DataFrame,
    date_column: str,
    count_columns: List[str],
    metric_type: str = "unique",
    include_most_played: str = None  # Options: "track", "artist", or None
) -> pl.DataFrame:
    """
    Calculate decade-based metrics (Unique Tracks, Total Tracks, Avg Tracks),
    with optional most played track or artist details.

    Args:
        df (pl.DataFrame): Input DataFrame.
        date_column (str): Column with date information (e.g., 'mb_artist_career_begin').
        count_columns (List[str]): Columns to count (e.g., 'artist_name').
        metric_type (str, optional): Metric type ('unique', 'total', 'average'). Defaults to 'unique'.
        include_most_played (str, optional): Whether to include most played track or artist. Defaults to None.

    Returns:
        pl.DataFrame: Decade-level metrics with optional most played track or artist details.
    """
    # Filter out rows with null dates
    df_with_date = df.filter(~pl.col(date_column).is_null())

    # Extract decades
    df_with_date = df_with_date.with_columns([
        (pl.col(date_column).dt.year() // 10 * 10).alias("decade_year"),  # Full year of the decade
        ((pl.col(date_column).dt.year() % 100) // 10 * 10).alias("decade_label")  # Decade label (e.g., 80, 90)
    ])

    if metric_type == "unique":
        # Unique metric: Count unique combinations of decade and group_by_column
        result = (
            df_with_date
            .select(["decade_year", "decade_label"] + count_columns)
            .unique()
            .group_by(["decade_year", "decade_label"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "total":
        # Total metric: Count all rows grouped by decade
        result = (
            df_with_date
            .group_by(["decade_year", "decade_label"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "average":
        # Average metric: Compute total divided by unique
        unique_counts = (
            df_with_date
            .select(["decade_year", "decade_label"] + count_columns)
            .unique()
            .group_by(["decade_year", "decade_label"])
            .count()
            .rename({"count": "unique_count"})
        )
        total_counts = (
            df_with_date
            .group_by(["decade_year", "decade_label"])
            .count()
            .rename({"count": "total_count"})
        )
        result = unique_counts.join(total_counts, on=["decade_year", "decade_label"])
        result = result.with_columns(
            (pl.col("total_count") / pl.col("unique_count")).alias("metric")
        ).select(["decade_year", "decade_label", "metric"])
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}. Choose 'unique', 'total', or 'average'.")

    # Optionally add most played track or artist
    if include_most_played == "track":
        most_played = (
            df_with_date
            .group_by(["decade_year", "decade_label", cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN])
            .count()
            .sort(["decade_year", "decade_label", "count"], descending=True)
            .group_by(["decade_year", "decade_label"])
            .agg([
                pl.col(cm.TRACK_TITLE_COLUMN).first().alias("most_played_track"),
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("count").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on=["decade_year", "decade_label"], how="left")
    elif include_most_played == "artist":
        # Sum play counts for each artist within a decade
        artist_counts = (
            df_with_date
            .group_by(["decade_year", "decade_label", cm.ARTIST_NAME_COLUMN])
            .count()
            .group_by(["decade_year", "decade_label", cm.ARTIST_NAME_COLUMN])
            .agg([
                pl.col("count").sum().alias("total_plays")
            ])
        )

        # Find the artist with the most total plays per decade
        most_played = (
            artist_counts
            .sort(["decade_year", "decade_label", "total_plays"], descending=True)
            .group_by(["decade_year", "decade_label"])
            .agg([
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("total_plays").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on=["decade_year", "decade_label"], how="left")

    return result.sort("decade_year")

def calculate_duration_metrics(
    df: pl.DataFrame,
    duration_column: str,
    count_columns: List[str],
    metric_type: str = "unique",
    include_most_played: bool = False
) -> pl.DataFrame:
    """
    Calculate duration-based metrics (Unique Tracks, Total Tracks, Avg Tracks), with optional most played track details.

    Args:
        df (pl.DataFrame): Input DataFrame.
        duration_column (str): Column with duration information (e.g., 'spotify_duration_ms').
        count_columns (List[str]): Columns to count (e.g., 'artist_name').
        metric_type (str, optional): Metric type ('unique', 'total', 'average'). Defaults to 'unique'.
        include_most_played (bool, optional): Whether to include most played track details. Defaults to False.

    Returns:
        pl.DataFrame: Metrics with 'duration_minutes', 'metric', and optionally most played track details.
    """
    # Filter out rows with null durations
    df_duration = df.filter(~pl.col(duration_column).is_null())

    # Convert duration to minutes and truncate
    df_duration = df_duration.with_columns([
        (pl.col(duration_column) / 60000).floor().cast(int).alias("duration_minutes")  # Convert ms to minutes
    ])

    if metric_type == "unique":
        result = (
            df_duration
            .select(["duration_minutes"] + count_columns)
            .unique()
            .group_by(["duration_minutes"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "total":
        result = (
            df_duration
            .group_by(["duration_minutes"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "average":
        unique_counts = (
            df_duration
            .select(["duration_minutes"] + count_columns)
            .unique()
            .group_by(["duration_minutes"])
            .count()
            .rename({"count": "unique_count"})
        )
        total_counts = (
            df_duration
            .group_by(["duration_minutes"])
            .count()
            .rename({"count": "total_count"})
        )
        result = unique_counts.join(total_counts, on=["duration_minutes"])
        result = result.with_columns(
            (pl.col("total_count") / pl.col("unique_count")).alias("metric")
        ).select(["duration_minutes", "metric"])
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}. Choose 'unique', 'total', or 'average'.")

    # Optionally add most played track details
    if include_most_played:
        most_played = (
            df_duration
            .group_by(["duration_minutes", cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN])
            .count()
            .sort(["duration_minutes", "count"], descending=True)
            .group_by("duration_minutes")
            .agg([
                pl.col(cm.TRACK_TITLE_COLUMN).first().alias("most_played_track"),
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("count").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on="duration_minutes", how="left")

    return result.sort("duration_minutes")


def calculate_genre_metrics(
    df: pl.DataFrame,
    genre_column: str,
    count_columns: List[str],
    metric_type: str = "unique",
    include_most_played: bool = False,
) -> pl.DataFrame:
    """
    Calculate metrics for genres, including optional most played track details.

    Args:
        df (pl.DataFrame): Input DataFrame.
        genre_column (str): Column with genre information.
        count_columns (List[str]): Columns to count (e.g., 'artist_name').
        metric_type (str, optional): Metric type ('unique', 'total', 'average'). Defaults to 'unique'.
        include_most_played (bool, optional): Whether to include most played track details. Defaults to False.

    Returns:
        pl.DataFrame: Genre-level metrics with an optional 'most_played_track' and 'most_played_artist' column.
    """
    # Filter out rows with null or empty genres
    df_genres = df.filter(
        (~pl.col(genre_column).is_null())
        & (pl.col(genre_column) != "")
    )

    # Split genres and explode into rows
    df_genres = df_genres.with_columns([
        pl.col(genre_column).str.split(", ").alias("split_genres")  # Split comma-separated genres into lists
    ]).explode("split_genres")  # Explode the lists into rows

    # Calculate metric
    if metric_type == "unique":
        # Unique metric: Count unique combinations of decade and group_by_column
        result = (
            df_genres
            .select(["split_genres"] + count_columns)
            .unique()
            .group_by(["split_genres"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "total":
        # Total metric: Count all rows grouped by decade
        result = (
            df_genres
            .group_by(["split_genres"])
            .count()
            .rename({"count": "metric"})
        )
    elif metric_type == "average":
        # Average metric: Compute total divided by unique
        unique_counts = (
            df_genres
            .select(["split_genres"] + count_columns)
            .unique()
            .group_by(["split_genres"])
            .count()
            .rename({"count": "unique_count"})
        )
        total_counts = (
            df_genres
            .group_by(["split_genres"])
            .count()
            .rename({"count": "total_count"})
        )
        result = unique_counts.join(total_counts, on=["split_genres"])
        result = result.with_columns(
            (pl.col("total_count") / pl.col("unique_count")).alias("metric")
        ).select(["split_genres", "metric"])
    else:
        raise ValueError(f"Unsupported metric_type: {metric_type}. Choose 'unique', 'total', or 'average'.")

    # Optionally add most played track details
    if include_most_played:
        most_played = (
            df_genres
            .group_by(["split_genres", cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN])
            .count()
            .sort(["split_genres", "count"], descending=True)
            .group_by("split_genres")
            .agg([
                pl.col(cm.TRACK_TITLE_COLUMN).first().alias("most_played_track"),
                pl.col(cm.ARTIST_NAME_COLUMN).first().alias("most_played_artist"),
                pl.col("count").first().alias("most_played_count")
            ])
        )
        result = result.join(most_played, on="split_genres", how="left")

    return result.sort("metric", descending=False).tail(10)