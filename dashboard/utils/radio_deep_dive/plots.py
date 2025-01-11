import polars as pl
import streamlit as st
import plotly.express as px
import plotly.colors as pc
import plotly.graph_objects as go
from datetime import timedelta
from typing import Optional

from data_extract.config_manager import ConfigManager
from utils.helper import number_formatter

cm = ConfigManager()

def display_sparkline(radio_df: pl.DataFrame, view_option: str):
    """Displays a sparkline with top X and date-range filters."""

    with st.expander("Trend of Plays Over Time", expanded=True, icon='📈'):
        # Handle empty dataframe scenario
        if radio_df.is_empty():
            st.warning("No available data to display.")
            return
        with st.popover(label='Settings', icon='⚙️', use_container_width=False):
            # Toggle for cumulative vs non-cumulative
            cumulative_toggle = st.toggle(
                "Cumulative View",
                value=False, 
                help="Check to see a cumulative sum of plays over time."
            )
            # Add a filter to select top X
            top_x = st.number_input(
                "Number of top items", 
                min_value=1, 
                max_value=50, 
                value=5, 
                step=1,
                help="Select how many top artists/tracks to display."
            )

            # Add a filter to select the range of dates
            #    By default last 30 days, but user can expand
            min_date_in_data = radio_df[cm.DAY_COLUMN].min()
            max_date_in_data = radio_df[cm.DAY_COLUMN].max()

            # Ensure valid min/max dates before using timedelta
            if min_date_in_data is None or max_date_in_data is None:
                st.warning("No date data available.")
                return

            default_end = max_date_in_data
            default_start = max(min_date_in_data, max_date_in_data - timedelta(days=30))

            date_range = st.date_input(
                label="Select date range",
                value=(default_start, default_end),
                min_value=min_date_in_data,
                max_value=max_date_in_data,
                help="Pick a date range to display. \n\nIt will combine it with the time period defined on Page Settings"
            )

        # Unpack the user-chosen range
        start_date, end_date = date_range if len(date_range) == 2 else (default_start, default_end)        

        # Data transformations: Filter the main DataFrame by user-selected date range
        df_filtered = radio_df.filter(
            (pl.col(cm.DAY_COLUMN) >= start_date) & (pl.col(cm.DAY_COLUMN) <= end_date)
        )

        if view_option == "Artist":
            group_cols = [cm.ARTIST_NAME_COLUMN]
            legend_title = "Artist Name"
        else:  # "Track"
            group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
            legend_title = "Track Name"

        # Aggregate daily plays (each row is a play)
        plays_by_day = (
            df_filtered
            .group_by(group_cols + [cm.DAY_COLUMN])
            .agg(pl.count().alias('play_count'))
        )

        # Ensure all days are covered (fill missing dates with 0 plays)
        all_dates = pl.DataFrame({cm.DAY_COLUMN: pl.date_range(
            start=df_filtered[cm.DAY_COLUMN].min(),
            end=df_filtered[cm.DAY_COLUMN].max(),
            interval='1d',
            eager=True
        )})

        distinct_entities = plays_by_day.select(group_cols).unique()
        all_combinations = distinct_entities.join(all_dates, how='cross')

        filled_data = (
            all_combinations
            .join(plays_by_day, on=group_cols + [cm.DAY_COLUMN], how='left')
            .with_columns(pl.col('play_count').fill_null(0))
        )

        # Compute cumulative sum if toggle is enabled
        if cumulative_toggle:
            filled_data = (
                filled_data.sort(group_cols + [cm.DAY_COLUMN])
                .with_columns(pl.col('play_count').cum_sum().over(group_cols).alias('cumulative_play_count'))
            )
            value_col = 'cumulative_play_count'
        else:
            value_col = 'play_count'

        # Decide top X by total plays in the selected date range
        sorted_top_entities = (
            filled_data.group_by(group_cols)
            .agg(pl.col(value_col).sum().alias("total_plays"))
            .sort("total_plays", descending=True)
            .head(top_x)
        )

        # Filter main data to only top X entities
        top_data = filled_data.join(sorted_top_entities, on=group_cols, how="inner")

        # Ensure fixed sorting order for display labels
        if view_option == 'Track':
            sorted_top_entities = sorted_top_entities.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            top_data = top_data.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            color_col = 'display_label'
        else:
            color_col = cm.ARTIST_NAME_COLUMN

        # Fix ordering for consistent colors
        top_data = top_data.join(sorted_top_entities.select(group_cols), on=group_cols, how='left')

        # Plot the sparkline
        fig = px.line(
            top_data.to_pandas(), 
            x=cm.DAY_COLUMN, 
            y=value_col,
            color=color_col,
            category_orders={color_col: sorted_top_entities[color_col].to_list()},
            title=f'Trend of {view_option} Plays Over Time',
            labels={value_col: 'Plays', cm.DAY_COLUMN: 'Date', color_col: legend_title},
            template='plotly_white',
            line_shape="spline",
        )
        # Determine the min and max values for the y-axis
        y_max = top_data[value_col].max()
        fig.update_yaxes(range=[0, y_max * 1.1])  # Add some padding (10%) for better visibility

        # Increase height
        fig.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                legend_title_text=legend_title,
                margin=dict(l=10, r=30, t=30, b=0),
                height=600,
                hoverlabel_align="left",
            )

        # Define line width based on total plays
        line_widths = {label: 2 + (total_plays / sorted_top_entities['total_plays'].max()) * 1.5
                    for label, total_plays in zip(sorted_top_entities[color_col], sorted_top_entities['total_plays'])}

        for trace in fig.data:
            trace.line.width = line_widths.get(trace.name, 2)  # Adjust width dynamically

        st.write(
            "**Tip**: You can hover over the lines to see exact values. "
            "You can also click legend entries to toggle them on/off."
        )

        st.plotly_chart(fig, use_container_width=True)


def display_plot_dataframe(radio_df: pl.DataFrame, view_option: str):
    st.subheader("60-Day Overview")
    # Handle empty dataframe scenario
    if radio_df.is_empty():
        st.warning("No available data to display.")
        return
    
    # Select dimensions based on user choice
    if view_option == 'Artist':
        group_cols = [cm.ARTIST_NAME_COLUMN, 'spotify_genres']
    else:
        group_cols = [cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN, 'spotify_genres']

    # Use all filtered data for total plays
    df_all_time = radio_df

    # Compute total plays over the entire date period
    total_plays_all = (
        df_all_time
        .group_by(group_cols)
        .agg([
            pl.count().alias('Total Plays'),
        ])
    )

    # Identify last 60 days from the max date for the sparkline
    max_date_in_df = df_all_time[cm.DAY_COLUMN].max()
    last_60_days_start = max_date_in_df - timedelta(days=61)
    last_60_days_end   = max_date_in_df - timedelta(days=1)
    df_60_days = radio_df.filter(
        (pl.col(cm.DAY_COLUMN) >= last_60_days_start)
        & (pl.col(cm.DAY_COLUMN) <= last_60_days_end)
    )

    # Build the date range for zero-filling
    date_series = pl.date_range(
        start=last_60_days_start,
        end=last_60_days_end,
        interval="1d",
        eager=True  # returns a Polars Series directly
    )
    all_dates = pl.DataFrame({cm.DAY_COLUMN: date_series})

    # Cross-join all dates with dimension combos
    dim_combos = df_60_days.select(group_cols).unique()
    all_combinations = dim_combos.join(all_dates, how='cross')

    # Count daily plays within last 60 days
    daily_counts_60 = (
        df_60_days
        .group_by(group_cols + [cm.DAY_COLUMN])
        .agg([pl.count().alias('plays_per_day')])
    )

    # Zero-fill missing dates for the sparkline
    zero_filled = (
        all_combinations
        .join(daily_counts_60, on=group_cols + [cm.DAY_COLUMN], how='left')
        .with_columns(pl.col('plays_per_day').fill_null(0))
    )

    # Collect daily plays into a list for the sparkline
    sparkline_df = (
        zero_filled
        .group_by(group_cols)
        .agg([
            pl.col('plays_per_day').sort_by(cm.DAY_COLUMN).alias('plays_list'),
        ])
    )

    # Combine overall total plays with sparkline info
    final_df = (
        sparkline_df
        .join(total_plays_all, on=group_cols, how='left')
    )
    
    # Compute fraction of max (based on full-period total plays)
    max_plays = final_df['Total Plays'].max()
    final_df = final_df.with_columns(
        (pl.col('Total Plays') / max_plays).alias('fraction_of_max')
    )
    final_df = final_df.fill_null(0).sort('Total Plays', descending=True)
    
    # Limit to top 50
    final_df = final_df.head(50)

    # Configure columns
    col_config = {}
    if view_option == 'Artist':
        col_config[cm.ARTIST_NAME_COLUMN] = st.column_config.Column(label="Artist", width="small")
        col_config['spotify_genres'] = st.column_config.Column(label="Genre", width="small")
    else:
        col_config[cm.TRACK_TITLE_COLUMN] = st.column_config.Column(label="Track Title", width="small")
        col_config[cm.ARTIST_NAME_COLUMN] = st.column_config.Column(label="Artist", width="small")
        col_config['spotify_genres'] = st.column_config.Column(label="Genre", width="small")

    col_config["plays_list"] = st.column_config.LineChartColumn(
        label="Daily Plays (Last 60d)",
        width="big",
        help="Zero-filled daily plays for last 60 days"
    )

    col_config["Total Plays"] = st.column_config.Column(
        label="Total Plays",
        width="small"
    )

    col_config["fraction_of_max"] = st.column_config.ProgressColumn(
        label="",
        help="Relative share of the highest total plays",
        min_value=0.0,
        max_value=1.0,
        width='big',
        format=" "
    )

    # Convert to pandas and render
    df_to_display = final_df.to_pandas()

    st.data_editor(
        df_to_display,
        column_config=col_config,
        hide_index=True,
        use_container_width=True
    )

def display_top_bar_chart(radio_df: pl.DataFrame, view_option: str, other_radios_df: Optional[pl.DataFrame] = None):
    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]

    st.subheader(f'Top 10 {view_option}s')

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()  # Use a single column if no comparison is needed

    def generate_bar_chart(df: pl.DataFrame, title_suffix: str):
        """Generate a formatted bar chart from the dataframe."""
        bar_chart_df = (
            df.group_by(group_cols)
            .agg(pl.count().alias('play_count'))
            .sort('play_count', descending=True)
            .head(10)
            # .with_columns(pl.col('play_count').map_elements(number_formatter).alias('formatted_play_count'))
            .with_columns(
                pl.col('play_count').map_elements(number_formatter, return_dtype=pl.Utf8).alias('formatted_play_count')
            )
        )

        if view_option == 'Track':
            bar_chart_df = bar_chart_df.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            color_col = 'display_label'
        else:
            color_col = cm.ARTIST_NAME_COLUMN

        # Sort for visual consistency
        bar_chart_df = bar_chart_df.sort('play_count', descending=False)

        # Create the bar chart
        bar_chart_fig = px.bar(
            bar_chart_df.to_pandas(),
            x='play_count',
            y=color_col,
            text='formatted_play_count',
            # title=f'Top 10 {view_option}s {title_suffix}',
            orientation='h',
        )
        bar_chart_fig.update_traces(
            textposition="outside",
            cliponaxis=False  # Prevent labels from being clipped
        )
        bar_chart_fig.update_layout(
            xaxis_title=None,
            yaxis_title=None,
            margin=dict(l=10, r=30, t=30, b=0),
            height=400,
            hoverlabel_align="left",
        )
        return bar_chart_fig

    # Display left chart (selected radio)
    with col1:
        st.markdown(f"**{radio_df[cm.RADIO_COLUMN][0]}**")  # Display selected radio name
        st.plotly_chart(generate_bar_chart(radio_df, "(Selected Radio)"), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None:
        with col2:
            st.markdown("**All Other Radios**")
            st.plotly_chart(generate_bar_chart(other_radios_df, "(Other Radios)"), use_container_width=True)


def display_top_by_week_chart(radio_df: pl.DataFrame, view_option: str, other_radios_df: Optional[pl.DataFrame] = None):
    """
    Displays a vertical bar chart comparing the top artist/track by total plays per week
    for the selected radio vs all other radios (if provided), while ensuring consistent colors for overlapping artists.
    """
    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
        legend_title = "Artist Name"
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
        legend_title = "Track Name"

    st.subheader(f'Top {view_option} by Week')

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_weekly_top(df: pl.DataFrame) -> pl.DataFrame:
        """Extracts the top artist/track per week from the given dataframe."""
        df = df.with_columns([
            pl.col(cm.DAY_COLUMN).dt.year().alias("year"),
            pl.col(cm.DAY_COLUMN).dt.week().alias("week")
        ])

        # Aggregate total plays per artist/track per week
        weekly_top_df = (
            df.group_by(["year", "week"] + group_cols)
            .agg(pl.count().alias("play_count"))
        )

        # Find the top artist/track per week
        weekly_top_df = (
            weekly_top_df
            .sort(["year", "week", "play_count"], descending=[False, False, True])
            .group_by(["year", "week"])
            .head(1)  # Keep only the top artist/track per week
        )

        # Format the week label
        weekly_top_df = weekly_top_df.with_columns(
            (pl.col("year").cast(pl.Utf8) + "-W" + pl.col("week").cast(pl.Utf8).str.zfill(2)).alias("week_label")
        )

        # Sort bars by year first, then week
        weekly_top_df = weekly_top_df.sort(["year", "week"])

        # Format play count for display
        weekly_top_df = weekly_top_df.with_columns(
            pl.col("play_count").map_elements(number_formatter, return_dtype=pl.Utf8).alias("formatted_play_count")
        )

        if view_option == "Track":
            weekly_top_df = weekly_top_df.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + " - " + pl.col(cm.ARTIST_NAME_COLUMN)).alias("display_label")
            )
            color_col = "display_label"
        else:
            color_col = cm.ARTIST_NAME_COLUMN

        return weekly_top_df, color_col

    # Process both selected radio and other radios (if provided)
    radio_weekly_top, color_col_1 = process_weekly_top(radio_df)
    if other_radios_df is not None:
        other_weekly_top, color_col_2 = process_weekly_top(other_radios_df)

    # Assign Colors for Artists in Selected Radio
    all_colors = pc.qualitative.Pastel1  # Select a color palette
    # all_colors = pc.qualitative.Bold

    # Extract unique artists from both datasets
    selected_artists = radio_weekly_top[color_col_1].unique().to_list()
    other_artists = other_weekly_top[color_col_2].unique().to_list() if other_radios_df is not None else []

    # Assign colors to artists in the selected radio chart
    artist_colors = {artist: all_colors[i % len(all_colors)] for i, artist in enumerate(selected_artists)}

    # Ensure Artists in 'Other Radios' Use the Same Colors
    for artist in other_artists:
        if artist not in artist_colors:
            # Assign a new color only if the artist is not already colored
            next_color = all_colors[len(artist_colors) % len(all_colors)]
            artist_colors[artist] = next_color

    def generate_bar_chart(df: pl.DataFrame, color_col: str, title_suffix: str):
        """Generates a vertical bar chart from the processed weekly data with consistent colors."""
        fig = px.bar(
            df.to_pandas(),
            x="week_label",
            y="play_count",
            text="formatted_play_count",
            color=color_col,
            labels={color_col: legend_title},
            title=f"Top {view_option} by Week {title_suffix}",
            color_discrete_map=artist_colors  # Ensure consistent colors
        )

        fig.update_traces(
            textposition="outside",
            cliponaxis=False
        )

        fig.update_layout(
            xaxis_title="Week",
            yaxis_title="Total Plays",
            margin=dict(l=10, r=30, t=30, b=40),
            legend_title_text=legend_title,
            height=500,
            hoverlabel_align="left",
        )

        return fig

    # Display left chart (selected radio)
    with col1:
        st.markdown(f"**{radio_df[cm.RADIO_COLUMN][0]}**")  # Display selected radio name
        st.plotly_chart(generate_bar_chart(radio_weekly_top, color_col_1, "(Selected Radio)"), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None:
        with col2:
            st.markdown("**All Other Radios**")
            st.plotly_chart(generate_bar_chart(other_weekly_top, color_col_2, "(Other Radios)"), use_container_width=True)


def display_play_count_histogram(radio_df: pl.DataFrame, view_option: str, other_radios_df: Optional[pl.DataFrame] = None):
    """
    Displays a histogram of the number of artists/tracks that fall into predefined play count buckets.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
        buckets = [
            (1, 25), (26, 50), (51, 100), (101, 250), (251, None)
        ]
        legend_title = "Artist Name"
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
        buckets = [
            (1, 15), (16, 40), (41, 75), (76, 150), (151, None)
        ]
        legend_title = "Track Name"

    st.subheader(f'Distribution of {view_option} Plays')

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_histogram_data(df: pl.DataFrame) -> pl.DataFrame:
        """Aggregates play counts for artists/tracks and assigns them to buckets."""
        # Count total plays per artist/track
        df = (
            df.group_by(group_cols)
            .agg(pl.count().alias("play_count"))
        )

        # Assign each row to a bucket
        def categorize_play_count(play_count):
            for lower, upper in buckets:
                if upper is None or lower <= play_count <= upper:
                    return f"{lower}-{upper if upper else '+'}"
            return "Other"  # Fallback case (shouldn't happen)

        df = df.with_columns(
            pl.col("play_count").map_elements(categorize_play_count, return_dtype=pl.Utf8).alias("play_bucket")
        )

        # Aggregate count of artists/tracks in each bucket
        df = df.group_by("play_bucket").agg(pl.count().alias("count"))

        # Define a manual ordering for the buckets
        ordered_buckets = [f"{low}-{up if up else '+'}" for low, up in buckets]
        
        # Ensure proper sorting using a manual mapping
        bucket_order_mapping = {bucket: i for i, bucket in enumerate(ordered_buckets)}

        df = df.with_columns(
            pl.col("play_bucket").map_elements(lambda x: bucket_order_mapping.get(x, 9999), return_dtype=pl.Int32).alias("bucket_order")
        ).sort("bucket_order").drop("bucket_order")

        return df

    # Process histograms for both selected radio and other radios
    radio_histogram_df = process_histogram_data(radio_df)
    if other_radios_df is not None:
        other_histogram_df = process_histogram_data(other_radios_df)

    def generate_histogram(df: pl.DataFrame, title_suffix: str):
        """Generates a histogram bar chart from the processed data."""
        fig = px.bar(
            df.to_pandas(),
            x="play_bucket",
            y="count",
            text="count",
            title=f"{view_option} Play Distribution {title_suffix}",
        )

        fig.update_traces(
            textposition="outside",
            cliponaxis=False
        )

        fig.update_layout(
            xaxis_title="Number of Plays",
            # yaxis_title="Number of Artists/Tracks",
            yaxis_title=None,
            legend_title_text=legend_title,
            margin=dict(l=10, r=30, t=30, b=40),
            height=500,
            hoverlabel_align="left",
        )

        return fig

    # Display left chart (selected radio)
    with col1:
        st.markdown(f"**{radio_df[cm.RADIO_COLUMN][0]}**")  # Display selected radio name
        st.plotly_chart(generate_histogram(radio_histogram_df, "(Selected Radio)"), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None:
        with col2:
            st.markdown("**All Other Radios**")
            st.plotly_chart(generate_histogram(other_histogram_df, "(Other Radios)"), use_container_width=True)


def display_popularity_vs_plays_quadrant(radio_df: pl.DataFrame, view_option: str, other_radios_df: Optional[pl.DataFrame] = None, top_n_labels: int = 10):
    """
    Displays a quadrant chart comparing Popularity vs. Number of Plays, with labels for the top N played artists/tracks and quadrant legends.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]

    st.subheader(f'Popularity vs. Number of Plays ({view_option}s) - Quadrant Chart')

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None:
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_scatter_data(df: pl.DataFrame) -> pl.DataFrame:
        """Aggregates play counts and total popularity per artist/track."""
        df = (
            df.group_by(group_cols)
            .agg(
                pl.count().alias("play_count"),
                pl.col("spotify_popularity").mean().alias("total_popularity")
            )
        )
        return df

    radio_scatter_df = process_scatter_data(radio_df)
    if other_radios_df is not None:
        other_scatter_df = process_scatter_data(other_radios_df)

    def generate_quadrant_chart(df: pl.DataFrame, title_suffix: str):
        """Generates a scatterplot quadrant chart with labels for top-played artists/tracks and quadrant lines."""
        df_pd = df.to_pandas()

        # Calculate median values for quadrants
        median_plays = df_pd["play_count"].median()
        median_popularity = df_pd["total_popularity"].median()

        # Select only the top N most played artists/tracks for labeling
        top_played = df_pd.nlargest(top_n_labels, "play_count")

        fig = px.scatter(
            df_pd,
            x="play_count",
            y="total_popularity",
            title=f"Popularity vs. Plays - {title_suffix}",
        )

        # Add quadrant dividing lines (with labels)
        fig.add_shape(go.layout.Shape(
            type="line", x0=median_plays, x1=median_plays, y0=df_pd["total_popularity"].min(), y1=df_pd["total_popularity"].max(),
            line=dict(color="red", width=2, dash="dot")
        ))

        fig.add_shape(go.layout.Shape(
            type="line", x0=df_pd["play_count"].min(), x1=df_pd["play_count"].max(), y0=median_popularity, y1=median_popularity,
            line=dict(color="red", width=2, dash="dot")
        ))

        # Labels for quadrant lines
        fig.add_annotation(
            x=median_plays * 1.1, y=df_pd["total_popularity"].max() * 1.02,
            text="Median Plays", showarrow=False, font=dict(size=12, color="red")
        )

        fig.add_annotation(
            x=df_pd["play_count"].max() * 1.02, y=median_popularity * 1.1,
            text="Median Popularity", showarrow=False, font=dict(size=12, color="red")
        )

        # Add labels only for top-played artists/tracks
        for _, row in top_played.iterrows():
            fig.add_annotation(
                x=row["play_count"],
                y=row["total_popularity"],
                text=row[group_cols[-1]],  # Show track/artist name
                showarrow=True,
                arrowhead=2,
                ax=20,  # Adjust label positioning
                ay=-20
            )

        fig.update_traces(marker=dict(size=10, opacity=0.7))

        fig.update_layout(
            xaxis_title="Number of Plays",
            yaxis_title="Popularity",
            margin=dict(l=10, r=30, t=30, b=40),
            height=500,
            hoverlabel_align="left",
        )

        return fig

    # Display left chart (selected radio)
    with col1:
        st.markdown(f"**{radio_df[cm.RADIO_COLUMN][0]}**")  
        st.plotly_chart(generate_quadrant_chart(radio_scatter_df, "(Selected Radio)"), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None:
        with col2:
            st.markdown("**All Other Radios**")
            st.plotly_chart(generate_quadrant_chart(other_scatter_df, "(Other Radios)"), use_container_width=True)


def display_underplayed_overplayed_highlights(radio_df: pl.DataFrame, other_radios_df: pl.DataFrame, view_option: str):
    """
    Highlights the most underplayed and most overplayed artist/track.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]

    # Get the selected radio name (it's a unique value in cm.RADIO_COLUMN)
    selected_radio_name = radio_df[cm.RADIO_COLUMN].unique().item()

    # Aggregate total plays for each artist/track
    radio_plays = (
        radio_df.group_by(group_cols)
        .agg(pl.count().alias("radio_play_count"))
    )

    other_radios_plays = (
        other_radios_df.group_by(group_cols)
        .agg(pl.count().alias("other_play_count"))
    )

    # Ensure we remove empty and None values
    def clean_artist_names(df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(
            (pl.col(group_cols[-1]).is_not_null()) &  # Remove None
            (pl.col(group_cols[-1]).cast(pl.Utf8).str.strip_chars() != "")  # Remove empty strings
        )

    radio_plays = clean_artist_names(radio_plays)
    other_radios_plays = clean_artist_names(other_radios_plays)

    # Merge both datasets
    play_comparison = radio_plays.join(other_radios_plays, on=group_cols, how="outer").fill_null(0)

    # Fix issue where artist name exists only in `other_radios_plays`
    artist_col_right = group_cols[-1] + "_right"
    if artist_col_right in play_comparison.columns:
        play_comparison = play_comparison.with_columns(
            pl.when(pl.col(group_cols[-1]).is_null())
            .then(pl.col(artist_col_right))
            .otherwise(pl.col(group_cols[-1]))
            .alias(group_cols[-1])
        ).drop(artist_col_right)

    # Identify potential underplayed and overplayed artists/tracks
    potential_underplayed = (
        play_comparison.filter((pl.col("radio_play_count") == 0) & (pl.col("other_play_count") > 50))
        .sort("other_play_count", descending=True)
    )

    potential_overplayed = (
        play_comparison.filter((pl.col("radio_play_count") > 50) & (pl.col("other_play_count") == 0))
        .sort("radio_play_count", descending=True)
    )

    # Get **lists of all artist names** in both main and other radios, lowercased
    main_radio_artist_names = set(radio_plays[group_cols[-1]].str.to_lowercase().to_list())
    other_radio_artist_names = set(other_radios_plays[group_cols[-1]].str.to_lowercase().to_list())

    # Filter Overplayed Artists:
    # Exclude artists that appear as part of another artist's name in other radios
    def is_part_of_other_artist(artist_name: str) -> bool:
        """
        Returns True if `artist_name` exists inside any artist name in `other_radios_df`.
        """
        artist_name = artist_name.lower()
        return any(artist_name in other_name for other_name in other_radio_artist_names)

    filtered_overplayed = (
        potential_overplayed.filter(
            ~pl.col(group_cols[-1])
            .str.to_lowercase()
            .map_elements(is_part_of_other_artist, return_dtype=pl.Boolean)
        )
    )

    # Filter Underplayed Artists:
    # Exclude artists that appear as part of another artist's name in the main radio
    def is_part_of_main_artist(artist_name: str) -> bool:
        """
        Returns True if `artist_name` exists inside any artist name in `radio_df`.
        """
        artist_name = artist_name.lower()
        return any(artist_name in other_name for other_name in main_radio_artist_names)

    filtered_underplayed = (
        potential_underplayed.filter(
            ~pl.col(group_cols[-1])
            .str.to_lowercase()
            .map_elements(is_part_of_main_artist, return_dtype=pl.Boolean)
        )
    )

    # Select the most underplayed and overplayed artist/track after filtering
    most_underplayed = filtered_underplayed.head(1)
    most_overplayed = filtered_overplayed.head(1)

    st.subheader("🎵 Underplayed & Overplayed Artists/Tracks")

    col1, col2 = st.columns(2)

    # Display **Most Underplayed**
    with col1:
        if not most_underplayed.is_empty():
            row = most_underplayed.row(0)  # Returns a tuple
            artist_name = row[most_underplayed.columns.index(group_cols[-1])]

            # Ensure no "None" or empty values are displayed
            if artist_name and artist_name.strip():
                underplayed_text = f"""
                <div style="background-color:#e8f5e9;padding:12px;border-radius:8px;">
                🔥 <b>Missed Opportunity!</b><br>
                <b>{artist_name}</b><br>
                <b>{row[most_underplayed.columns.index("radio_play_count")]} plays on {selected_radio_name}</b><br>
                but <b>heavily played on other radios ({row[most_underplayed.columns.index("other_play_count")]} plays)</b>!
                </div>
                """
                st.markdown(underplayed_text, unsafe_allow_html=True)
        else:
            st.write("✅ No major underplayed tracks found!")

    # Display **Most Overplayed**
    with col2:
        if not most_overplayed.is_empty():
            row = most_overplayed.row(0)  # Returns a tuple
            artist_name = row[most_overplayed.columns.index(group_cols[-1])]

            if artist_name and artist_name.strip():
                other_play_count = row[most_overplayed.columns.index("other_play_count")]
                if other_play_count > 0:
                    overplayed_text = f"""
                    <div style="background-color:#ffebee;padding:12px;border-radius:8px;">
                    📢 <b>Unique Pick!</b><br>
                    <b>{artist_name}</b><br>
                    <b>{row[most_overplayed.columns.index("radio_play_count")]} plays on {selected_radio_name}</b><br>
                    but <b>barely played on other radios ({other_play_count} plays)</b>!
                    </div>
                    """
                else:
                    overplayed_text = f"""
                    <div style="background-color:#ffebee;padding:12px;border-radius:8px;">
                    📢 <b>Unique Pick!</b><br>
                    <b>{artist_name}</b><br>
                    <b>{row[most_overplayed.columns.index("radio_play_count")]} plays on {selected_radio_name}</b><br>
                    and <b>doesn't have any plays on other radios!</b>
                    </div>
                    """
                st.markdown(overplayed_text, unsafe_allow_html=True)
        else:
            st.write("✅ No major overplayed tracks found!")