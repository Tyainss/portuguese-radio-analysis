import polars as pl
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.colors as pc
import plotly.graph_objects as go
from datetime import timedelta
from typing import Optional

from data_extract.config_manager import ConfigManager
from utils.helper import number_formatter, week_dates_start_end, hex_to_rgb

cm = ConfigManager()

def display_sparkline(radio_df: pl.DataFrame, view_option: str):
    """
    Displays a sparkline chart illustrating the trend of plays over time for the selected view option (Artist or Track).

    Parameters:
        radio_df (pl.DataFrame): Input data containing play counts, dates, and relevant metadata.
        view_option (str): Determines the grouping, either "Artist" or "Track".

    Functionality:
        - Filters data based on a user-selected date range.
        - Aggregates play counts by day, with an option for cumulative views.
        - Limits display to the top X entities based on total plays in the date range.
        - Ensures all dates are covered by filling missing days with zero plays.
        - Visualizes trends using an interactive line chart.
    """

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
                f"Number of top {view_option.lower()}s", 
                min_value=1, 
                max_value=50, 
                value=5, 
                step=1,
                help=f"Select how many top {view_option.lower()}s to display."
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

        # Aggregate daily plays
        plays_by_day = (
            df_filtered
            .group_by(group_cols + [cm.DAY_COLUMN])
            .agg(pl.count().alias('play_count'))
        )

        # Ensure all days are covered (fill missing dates with 0 plays)
        all_dates = pl.DataFrame(
            {cm.DAY_COLUMN: pl.date_range(
                start=df_filtered[cm.DAY_COLUMN].min(),
                end=df_filtered[cm.DAY_COLUMN].max(),
                interval='1d',
                eager=True
            )
        })

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

        top_data_pandas = top_data.to_pandas()

        # Plot the sparkline
        fig = px.line(
            top_data_pandas, 
            x=cm.DAY_COLUMN, 
            y=value_col,
            color=color_col,
            category_orders={color_col: sorted_top_entities[color_col].to_list()},
            title=f'Trend of {view_option} Plays Over Time',
            labels={value_col: 'Plays', cm.DAY_COLUMN: 'Date', color_col: legend_title},
            template='plotly_white',
            line_shape="spline",
            hover_data=None, 
            custom_data=[color_col],
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
            yaxis=dict(gridcolor="#E0E0E0"),
        )

        # Define line width based on total plays
        line_widths = {
            label: 2 + (total_plays / sorted_top_entities['total_plays'].max()) * 1.5
            for label, total_plays in zip(sorted_top_entities[color_col], sorted_top_entities['total_plays'])
        }

        for trace in fig.data:
            # Set dynamic line width
            trace.line.width = line_widths.get(trace.name, 2)
            
            # Use the trace's line color for the bold text
            line_color = trace.line.color if trace.line.color else '#000'

            # Updated hovertemplate with colored, bold labels
            trace.update(
                hovertemplate=(
                    f"<span style='font-size:16px; font-weight:bold; color:{line_color};'>%{{customdata[0]}}</span> <br>"
                    f"<span style='font-weight:bold;'>%{{y}} Plays</span><br>"
                    "%{x}<extra></extra>"
                )
            )

        st.write(
            "**Tip**: You can hover over the lines to see exact values."
            " You can also click legend entries to toggle them on/off."
        )

        st.plotly_chart(fig, use_container_width=True)


def display_plot_dataframe(radio_df: pl.DataFrame, view_option: str, last_x_days: int = 60):
    """
    Displays a data table overview with sparkline charts showing daily plays for the top 50 entities (Artists or Tracks) over the past X days.

    Parameters:
        radio_df (pl.DataFrame): Input data containing play counts, dates, and metadata.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        last_x_days (int): Number of recent days to include in the sparkline data (default: 60).

    Functionality:
        - Aggregates total plays and computes daily play counts for the last X days.
        - Adds sparkline visuals for daily plays, with missing dates filled with zeros.
        - Normalizes play counts to indicate relative performance.
        - Displays an editable table for exploration with configurable columns.
    """

    st.subheader(f"Top 50 {view_option} :blue[Table Overview]")
    # Handle empty dataframe scenario
    if radio_df.is_empty():
        st.warning("No available data to display.")
        return
    
    # Select dimensions based on user choice
    if view_option == 'Artist':
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.SPOTIFY_GENRE_COLUMN]
    else:
        group_cols = [cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN, cm.SPOTIFY_GENRE_COLUMN]

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

    # Identify last days from the max date for the sparkline
    max_date_in_df = df_all_time[cm.DAY_COLUMN].max()
    last_days_start = max_date_in_df - timedelta(days=1 + last_x_days)
    last_days_end   = max_date_in_df - timedelta(days=1)
    df_days = radio_df.filter(
        (pl.col(cm.DAY_COLUMN) >= last_days_start)
        & (pl.col(cm.DAY_COLUMN) <= last_days_end)
    )

    # Build the date range for zero-filling
    date_series = pl.date_range(
        start=last_days_start,
        end=last_days_end,
        interval="1d",
        eager=True  # returns a Polars Series directly
    )
    all_dates = pl.DataFrame({cm.DAY_COLUMN: date_series})

    # Cross-join all dates with dimension combos
    dim_combos = df_days.select(group_cols).unique()
    all_combinations = dim_combos.join(all_dates, how='cross')

    # Count daily plays within last days
    daily_counts = (
        df_days
        .group_by(group_cols + [cm.DAY_COLUMN])
        .agg([pl.count().alias('plays_per_day')])
    )

    # Zero-fill missing dates for the sparkline
    zero_filled = (
        all_combinations
        .join(daily_counts, on=group_cols + [cm.DAY_COLUMN], how='left')
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
        col_config[cm.SPOTIFY_GENRE_COLUMN] = st.column_config.Column(label='Genre', width="small")
    else:
        col_config[cm.TRACK_TITLE_COLUMN] = st.column_config.Column(label="Track Title", width="small")
        col_config[cm.ARTIST_NAME_COLUMN] = st.column_config.Column(label="Artist", width="small")
        col_config[cm.SPOTIFY_GENRE_COLUMN] = st.column_config.Column(label='Genre', width="small")

    col_config["plays_list"] = st.column_config.LineChartColumn(
        label=f"Daily Plays (Last {last_x_days}d)",
        width="big",
        help=f"Zero-filled daily plays for last {last_x_days} days"
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

def display_top_bar_chart(
    radio_df: pl.DataFrame, 
    view_option: str, 
    other_radios_df: Optional[pl.DataFrame] = None,
    radio_name: str = 'This Radio', 
    radio_color: str = "#4E87F9"
):
    """
    Displays side-by-side bar charts comparing the top 10 Artists or Tracks in the selected radio versus other radios.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing play counts and metadata.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        other_radios_df (Optional[pl.DataFrame]): Data for other radios to include in the comparison (default: None).

    Functionality:
        - Aggregates play counts for the top 10 entities in each dataset.
        - Generates horizontal bar charts with dynamic tooltips showing play counts.
        - Allows side-by-side comparisons if data for other radios is provided.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
    
    st.subheader(f'🎤 :blue[Top 10 {view_option}s:] How Does {radio_name} Compare?')
    st.markdown(
        f"""
        A side-by-side look at the most played {view_option}s.
        """
    )
    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()  # Use a single column if no comparison is needed

    def generate_bar_chart(df: pl.DataFrame, radio_color: str = "#4E87F9"):
        """
        Generate a formatted bar chart from the dataframe with customizable tooltip color.
        
        Parameters:
            df (pl.DataFrame): The dataframe to visualize.
            radio_color (str): Hex color for tooltip styling (default: light blue).
        """
        bar_chart_df = (
            df.group_by(group_cols)
            .agg(pl.count().alias('play_count'))
            .sort('play_count', descending=True)
            .head(10)
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

        # Create gradient colors
        base_rgb = hex_to_rgb(radio_color)
        gradient_colors = [
            f"rgba({base_rgb[0]}, {base_rgb[1]}, {base_rgb[2]}, {0.1 + 0.1 * i})"
            for i in range(len(bar_chart_df))
        ]

        # Create the bar chart
        bar_chart_fig = px.bar(
            bar_chart_df.to_pandas(),
            x='play_count',
            y=color_col,
            text='formatted_play_count',
            orientation='h',
            color_discrete_sequence=gradient_colors,  # Apply gradient colors
        )
        bar_chart_fig.update_traces(
            textposition="outside",
            cliponaxis=False  # Prevent labels from being clipped
        )
        # Update tooltips with the light blue color
        bar_chart_fig.update_traces(
            hovertemplate=(
                f"<span style='font-size:16px; font-weight:bold; color:{radio_color};'>%{{y}}</span> <br>"
                f"<span style='font-weight:bold;'>%{{customdata[0]}} Plays</span><br>"
                "<extra></extra>"
            ),
            customdata=bar_chart_df[['formatted_play_count']].to_pandas().values,
            marker=dict(color=gradient_colors)  # Explicitly set the gradient colors
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
        st.plotly_chart(generate_bar_chart(radio_df, radio_color=radio_color), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        with col2:
            st.plotly_chart(generate_bar_chart(other_radios_df, radio_color='#A1A1A0'), use_container_width=True)


def display_top_by_week_chart(radio_df: pl.DataFrame, view_option: str, other_radios_df: Optional[pl.DataFrame] = None):
    """
    Displays a weekly leaderboard chart for the most played Artist or Track, with an option to compare the selected radio to other radios.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing weekly play counts.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        other_radios_df (Optional[pl.DataFrame]): Data for other radios to include in the comparison (default: None).

    Functionality:
        - Identifies the top entity each week by total plays.
        - Ensures consistent color mapping across entities.
        - Visualizes weekly leaders with interactive vertical bar charts.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
        legend_title = "Artist Name"
        st.subheader("📅 :blue[Weekly Leaders:] Who Topped the Charts Each Week?")
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
        legend_title = "Track Name"
        st.subheader("📅 :blue[Weekly Leaders:] What Topped the Charts Each Week?")

    st.markdown(
        f"""
        This section highlights the **most played {view_option.lower()} each week**.  
        See which names had consistent dominance and which ones had breakout moments.  
        """
    )

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_weekly_top(df: pl.DataFrame) -> pl.DataFrame:
        """Extracts the top artist/track per week from the given dataframe."""
        df = df.with_columns([
            pl.col(cm.DAY_COLUMN).dt.strftime("%G-W%V").alias("week_label")  # Ensures correct ISO weeks
        ])

        # Aggregate total plays per artist/track per week
        weekly_top_df = (
            df.group_by(["week_label"] + group_cols)
            .agg(pl.count().alias("play_count"))
        )

        # Find the top artist/track per week
        weekly_top_df = (
            weekly_top_df
            .sort(["week_label", "play_count"], descending=[False, True])
            .group_by(["week_label"])
            .head(1)  # Keep only the top artist/track per week
        )

        # Convert week_label to Date for correct sorting
        weekly_top_df = (
            weekly_top_df
            .with_columns([
                pl.col("week_label").str.slice(0, 4).cast(pl.Int64).alias("year"),  # Extract year
                pl.col("week_label").str.slice(6, 2).cast(pl.Int64).alias("week"),  # Extract week number
            ])
            .sort(["year", "week", "play_count"], descending=[False, False, True])  # Sort chronologically
            .drop(["year", "week"])  # Remove auxiliary columns
        )

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
    if other_radios_df is not None and not other_radios_df.is_empty():
        other_weekly_top, color_col_2 = process_weekly_top(other_radios_df)

    # Assign Colors for Artists in Selected Radio
    all_colors = pc.qualitative.Pastel2  # Select a color palette
    
    all_artists = sorted(
        set(radio_weekly_top[color_col_1].unique().to_list()) |
        (set(other_weekly_top[color_col_2].unique().to_list()) if other_radios_df is not None and not other_radios_df.is_empty() else set()),
        reverse=True
    )
    color_map = {
        artist: all_colors[i % len(all_colors)]
        for i, artist in enumerate(all_artists)
    }

    # Extract unique artists from both datasets
    selected_artists = radio_weekly_top[color_col_1].unique().to_list()
    other_artists = other_weekly_top[color_col_2].unique().to_list() if other_radios_df is not None and not other_radios_df.is_empty() else []

    # Assign colors to artists in the selected radio chart
    artist_colors = {artist: all_colors[i % len(all_colors)] for i, artist in enumerate(selected_artists)}

    # Ensure Artists in 'Other Radios' Use the Same Colors
    for artist in other_artists:
        if artist not in artist_colors:
            # Assign a new color only if the artist is not already colored
            next_color = all_colors[len(artist_colors) % len(all_colors)]
            artist_colors[artist] = next_color

    def generate_bar_chart(df: pl.DataFrame, color_col: str):
        """Generates a vertical bar chart from the processed weekly data with consistent colors."""
        # Prepare data for tooltips
        df = df.with_columns([
            pl.col("week_label").map_elements(lambda w: week_dates_start_end(w), return_dtype=pl.List(pl.Utf8)).alias("week_dates"),
        ])
        df = df.with_columns([
            pl.col("week_dates").list.get(0).alias("start_date"),
            pl.col("week_dates").list.get(1).alias("end_date"),
            pl.col("week_label").str.slice(0, 4).cast(pl.Int64).alias("year"),  # Extract year
            pl.col("week_label").str.slice(6, 2).cast(pl.Int64).alias("week"),  # Extract week number
        ])

        # Sort the dataframe by year and week for chronological order
        df = df.sort(["year", "week"])

        # Remove auxiliary sorting columns before plotting
        df = df.drop(["year", "week"])

        # Ensure `customdata` is properly aligned with each row of the dataframe
        df = df.with_columns([
            pl.col(color_col).alias("hover_label"),  # Ensure hover label matches the `color_col`
        ])

        # Obtain an ordered list of week labels for category ordering
        ordered_weeks = df["week_label"].to_list()

        fig = px.bar(
            df.to_pandas(),
            x="week_label",
            y="play_count",
            text="formatted_play_count",
            color=color_col,
            labels={color_col: legend_title},
            title='',
            color_discrete_map=color_map,
            category_orders={"week_label": ordered_weeks}  # Enforce ordering for week_label
        )

        # Add custom data for tooltips
        customdata_values = df[["hover_label", "formatted_play_count", "start_date", "end_date"]].to_pandas().values
        for _, trace in enumerate(fig.data):
            # Match the trace's name to the corresponding hover label
            trace_hover_label = trace.name
            trace_customdata = [
                data_row for data_row in customdata_values if data_row[0] == trace_hover_label
            ]

            # Extract bar color from the trace
            bar_color = trace.marker.color if trace.marker.color else '#000'  # Fallback to black

            # Set the hovertemplate
            trace.update(
                hovertemplate=(
                    f"<span style='font-size:16px; font-weight:bold; color:{bar_color};'>%{{customdata[0]}}</span> <br>"
                    f"<span style='font-weight:bold;'>%{{customdata[1]}} Plays</span><br>"
                    f"<span>%{{customdata[2]}} to %{{customdata[3]}}</span><br>"
                    "<extra></extra>"
                ),
                customdata=trace_customdata,
                textposition="outside",
                cliponaxis=False,
            )

        fig.update_layout(
            yaxis=dict(visible=False),
            xaxis_title=None,
            yaxis_title=None,
            margin=dict(l=10, r=30, t=30, b=40),
            legend_title_text=legend_title,
            height=500,
            hoverlabel_align="left",
        )

        return fig


    # Display left chart (selected radio)
    with col1:
        st.plotly_chart(generate_bar_chart(radio_weekly_top, color_col_1), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        with col2:
            st.plotly_chart(generate_bar_chart(other_weekly_top, color_col_2), use_container_width=True)


def display_play_count_histogram(
    radio_df: pl.DataFrame, 
    view_option: str, 
    other_radios_df: Optional[pl.DataFrame] = None, 
    radio_color: str = "#4E87F9"
):
    """
    Displays a histogram illustrating the distribution of play counts for Artists or Tracks in predefined play count ranges.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing play counts and metadata.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        other_radios_df (Optional[pl.DataFrame]): Data for other radios to include in the comparison (default: None).

    Functionality:
        - Aggregates play counts into predefined ranges (buckets) for Artists or Tracks.
        - Visualizes the count of entities in each range as a bar chart.
        - Highlights the distribution differences when comparing to other radios.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
        buckets = [
            (1, 25), (26, 50), (51, 100), (101, 250), (251, None)
        ]
        legend_title = "Artist Name"
        st.subheader("📊 :blue[Play Distribution:] How Many Plays Do Artists Get?")
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
        buckets = [
            (1, 15), (16, 40), (41, 75), (76, 150), (151, None)
        ]
        legend_title = "Track Name"
        st.subheader("📊 :blue[Play Distribution:] How Often Are Tracks Played?")

    st.markdown(
        f"""
        This chart shows how **{view_option.lower()}s** are distributed based on their total number of plays.  
        Does the station concentrate on their top {view_option.lower()}s, or is it playing a wide variety evenly?  
        """
    )

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None and not other_radios_df.is_empty():
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
    if other_radios_df is not None and not other_radios_df.is_empty():
        other_histogram_df = process_histogram_data(other_radios_df)

    def generate_histogram(df: pl.DataFrame, show_yaxis_title: bool = True, radio_color: str = "#4E87F9"):
        """Generates a histogram bar chart from the processed data with enhanced tooltips, including percentages."""
        # Calculate the percentage of the total for each bucket
        total_count = df["count"].sum()
        df = df.with_columns(
            (pl.col("count") / total_count * 100).round(2).alias("percentage")
        )

        # Add custom data for the tooltips
        df = df.with_columns(
            pl.col("play_bucket").alias("hover_label")  # Assign hover_label as play_bucket
        )

        customdata_values = df[["hover_label", "count", "percentage"]].to_pandas().values

        fig = px.bar(
            df.to_pandas(),
            x="play_bucket",
            y="count",
            text="count",
            title='',
        )

        for _, trace in enumerate(fig.data):

            # Set hovertemplate with enhanced formatting
            trace.update(
                hovertemplate=(
                    f"<span style='font-size:16px; font-weight:bold; color:{radio_color};'>%{{customdata[0]}}</span> <br>"
                    f"<span style='font-weight:bold;'>%{{customdata[1]}} {view_option}s</span><br>"
                    f"<span >%{{customdata[2]}}% of Total</span><br>"
                    "<extra></extra>"
                ),
                customdata=customdata_values,
                textposition="outside",
                cliponaxis=False,
            )

        fig.update_traces(
            marker_color=radio_color
        )

        fig.update_layout(
            xaxis_title="Number of Plays",
            yaxis_title=f"Number of {view_option}s" if show_yaxis_title else None,
            yaxis=dict(showticklabels=False, showgrid=False),
            legend_title_text=legend_title,
            margin=dict(l=50, r=50, t=30, b=40),
            height=500,
            hoverlabel_align="left",
        )

        return fig

    # Display left chart (selected radio)
    with col1:
        st.plotly_chart(
            generate_histogram(
                radio_histogram_df, show_yaxis_title=True, radio_color=radio_color
            ), 
            use_container_width=True
        )

    # Display right chart (other radios) if provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        with col2:
            st.plotly_chart(
                generate_histogram(
                    other_histogram_df, show_yaxis_title=False, radio_color='#A1A1A0'
                ), 
            use_container_width=True
        )


def display_popularity_vs_plays_quadrant(
    radio_df: pl.DataFrame, 
    view_option: str, 
    other_radios_df: Optional[pl.DataFrame] = None, 
    top_n_labels: int = 10,
    radio_color: str = "#4E87F9"
):
    """
    Displays a quadrant chart comparing popularity vs. play counts for Artists or Tracks, highlighting top-performing entities.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing play counts and popularity scores.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        other_radios_df (Optional[pl.DataFrame]): Data for other radios to include in the comparison (default: None).
        top_n_labels (int): Number of top-played entities to label in the chart (default: 10).

    Functionality:
        - Aggregates total play counts and average popularity scores for entities.
        - Plots a scatterplot divided into quadrants based on median play count and popularity.
        - Highlights top-performing entities with labels.
        - Optionally compares data from the selected radio to other radios.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]

    st.subheader(f'🔍 Does :blue[Popularity] Always Mean More Plays?')

    st.markdown(
        f"""
        This quadrant chart highlights how **{view_option.lower()}s** perform based on total plays and popularity.  
        - The **top-right quadrant** represents highly played and highly popular {view_option.lower()}s.  
        - The **bottom-right quadrant** highlights frequently played but lower-popularity {view_option.lower()}s.  
        - Median lines divide the space, helping to spot trends and outliers.  
        """
    )

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_scatter_data(df: pl.DataFrame) -> pl.DataFrame:
        """Aggregates play counts and total popularity per artist/track."""
        df = (
            df.group_by(group_cols)
            .agg(
                pl.count().alias("play_count"),
                pl.col(cm.SPOTIFY_POPULARITY_COLUMN).mean().alias("total_popularity")
            )
        )
        return df

    radio_scatter_df = process_scatter_data(radio_df)
    if other_radios_df is not None and not other_radios_df.is_empty():
        other_scatter_df = process_scatter_data(other_radios_df)

    def generate_quadrant_chart(
        df: pl.DataFrame, 
        radio_color: str = "#4E87F9",
        show_yaxis_title: bool = True,
    ):
        """
        Generates a scatterplot quadrant chart with labels for top-played artists/tracks and quadrant lines.
        
        Parameters:
            df (pl.DataFrame): The dataframe to visualize.
            radio_color (str): Hex color for tooltip styling (default: light blue).
        """
        if view_option == 'Track':
            df = df.with_columns(
                (pl.col(cm.TRACK_TITLE_COLUMN) + ' - ' + pl.col(cm.ARTIST_NAME_COLUMN)).alias('display_label')
            )
            color_col = 'display_label'
        else:
            color_col = cm.ARTIST_NAME_COLUMN

        # Convert Polars dataframe to Pandas for Plotly compatibility
        df_pd = df.to_pandas()

        # Calculate median values for quadrants
        median_plays = df_pd["play_count"].median()
        median_popularity = df_pd["total_popularity"].median()

        # Select only the top N most played artists/tracks for labeling
        top_played = df_pd.nlargest(top_n_labels, "play_count")

        # Prepare customdata for tooltips
        df_pd["customdata"] = df_pd.apply(
            lambda row: [row[color_col], number_formatter(row["play_count"]), number_formatter(row["total_popularity"])],
            axis=1
        )

        # Create the scatter plot
        fig = px.scatter(
            df_pd,
            x="play_count",
            y="total_popularity",
            hover_data={"play_count": False, "total_popularity": False},  # Exclude these from default hover
            title='',
            color_discrete_sequence=[radio_color]
        )

        # Enhanced tooltips
        for trace in fig.data:
            # Set hovertemplate with custom formatting
            trace.update(
                hovertemplate=(
                     f"<span style='font-size:16px; font-weight:bold; color:{radio_color};'>%{{customdata[0]}}</span><br>"
                    "<span style='font-size:14px; font-weight:bold;'>🎵 %{customdata[1]} Plays</span><br>"
                    "<span style='font-size:14px; font-weight:bold;'>⭐ %{customdata[2]} Popularity</span><br>"
                    "<extra></extra>"
                ),
                customdata=df_pd["customdata"].tolist(),
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
                text=row[group_cols[-1]],
                showarrow=True,
                arrowhead=2,
                ax=25,
                ay=-20
            )

        fig.update_traces(marker=dict(size=10, opacity=0.7))

        fig.update_layout(
            xaxis_title="Number of Plays",
            yaxis_title="Popularity" if show_yaxis_title else None,
            margin=dict(l=10, r=30, t=30, b=40),
            height=500,
            hoverlabel_align="left",
            yaxis=dict(gridcolor="#E0E0E0"),
        )

        return fig

    # Display left chart (selected radio)
    with col1:
        st.plotly_chart(generate_quadrant_chart(radio_scatter_df, radio_color=radio_color, show_yaxis_title=True), use_container_width=True)

    # Display right chart (other radios) if provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        with col2:
            st.plotly_chart(generate_quadrant_chart(other_scatter_df, radio_color='#A1A1A0', show_yaxis_title=False), use_container_width=True)


def display_underplayed_overplayed_highlights(
    radio_df: pl.DataFrame, 
    other_radios_df: pl.DataFrame, 
    view_option: str,
    radio_name: str = 'This Radio'
):
    """
    Highlights the most underplayed and overplayed Artists or Tracks in the selected radio compared to other radios.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing play counts.
        other_radios_df (pl.DataFrame): Data for other radios to use in the comparison.
        view_option (str): Determines the grouping, either "Artist" or "Track".
        radio_name (str): The name of the radio to be displayed.

    Functionality:
        - Identifies entities with high play counts in other radios but low in the selected radio (underplayed).
        - Identifies entities with high play counts in the selected radio but low in other radios (overplayed).
        - Displays detailed highlights for the most underplayed and overplayed entities.
        - Provides a full list of underplayed and overplayed entities for further exploration.
    """

    if view_option == "Artist":
        group_cols = [cm.ARTIST_NAME_COLUMN]
        display_col = 'Artist Name'
    else:  # "Track"
        group_cols = [cm.ARTIST_NAME_COLUMN, cm.TRACK_TITLE_COLUMN]
        display_col = 'Track Title'

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
    def clean_names(df: pl.DataFrame) -> pl.DataFrame:
        return df.filter(
            (pl.col(group_cols[-1]).is_not_null()) &  # Remove None
            (pl.col(group_cols[-1]).cast(pl.Utf8).str.strip_chars() != "")  # Remove empty strings
        )

    radio_plays = clean_names(radio_plays)
    other_radios_plays = clean_names(other_radios_plays)

    # Merge both datasets
    play_comparison = radio_plays.join(other_radios_plays, on=group_cols, how="outer").fill_null(0)

    # Fix issue where artist/track name exists only in `other_radios_plays`
    for col in group_cols:
        col_right = col + "_right"
        if col_right in play_comparison.columns:
            play_comparison = play_comparison.with_columns(
                pl.when(pl.col(col).is_null())
                .then(pl.col(col_right))
                .otherwise(pl.col(col))
                .alias(col)
            ).drop(col_right)

    if view_option == "Artist":
        play_comparison = play_comparison.with_columns(
            pl.col(cm.ARTIST_NAME_COLUMN)
            .alias(display_col)
        )
    if view_option == "Track":
        play_comparison = play_comparison.with_columns(
            pl.concat_str([pl.col(cm.TRACK_TITLE_COLUMN), pl.col(cm.ARTIST_NAME_COLUMN)], separator=" - ")
            .alias(display_col)
        )

    # Identify potential underplayed and overplayed artists/tracks
    potential_underplayed = (
        play_comparison.filter((pl.col("radio_play_count") == 0) & (pl.col("other_play_count") > 50))
        .sort("other_play_count", descending=True)
    )

    potential_overplayed = (
        play_comparison.filter((pl.col("radio_play_count") > 50) & (pl.col("other_play_count") == 0))
        .sort("radio_play_count", descending=True)
    )

    # Get lists of all artist names in both main and other radios, lowercased
    main_radio_artist_names = set(radio_plays[group_cols[-1]].str.to_lowercase().to_list())
    other_radio_artist_names = set(other_radios_plays[group_cols[-1]].str.to_lowercase().to_list())

    # Restore the "is_part_of_other_artist" filtering logic
    def is_part_of_other_artist(artist_name: str) -> bool:
        """Returns True if `artist_name` exists inside any artist name in `other_radios_df`."""
        artist_name = artist_name.lower()
        return any(artist_name in other_name for other_name in other_radio_artist_names)

    filtered_overplayed = (
        potential_overplayed.filter(
            ~pl.col(group_cols[-1])
            .str.to_lowercase()
            .map_elements(is_part_of_other_artist, return_dtype=pl.Boolean)
        )
    )

    # Restore the "is_part_of_main_artist" filtering logic
    def is_part_of_main_artist(artist_name: str) -> bool:
        """Returns True if `artist_name` exists inside any artist name in `radio_df`."""
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

    st.subheader(f"🎵 Underplayed & Overplayed {view_option}s")

    col1, col2 = st.columns(2)

    # Display Most Underplayed
    with col1:
        if not most_underplayed.is_empty():
            row = most_underplayed.row(0)
            entity_name = row[most_underplayed.columns.index(display_col)]

            # Ensure no "None" or empty values are displayed
            if entity_name and entity_name.strip():
                underplayed_text = f"""
                <div style="background-color:#e8f5e9;padding:12px;border-radius:8px;">
                🔥 <b>Missed Opportunity!</b><br>
                <b>{entity_name}</b><br>
                <b>{row[most_underplayed.columns.index("radio_play_count")]} plays on {radio_name}</b><br>
                but <b>heavily played on other radios ({row[most_underplayed.columns.index("other_play_count")]:,} plays)</b>!
                </div>
                """
                st.markdown(underplayed_text, unsafe_allow_html=True)
        else:
            st.write("✅ No major underplayed tracks found!")

    # Display Most Overplayed
    with col2:
        if not most_overplayed.is_empty():
            row = most_overplayed.row(0)
            entity_name = row[most_overplayed.columns.index(display_col)]

            if entity_name and entity_name.strip():
                other_play_count = row[most_overplayed.columns.index("other_play_count")]
                if other_play_count > 0:
                    overplayed_text = f"""
                    <div style="background-color:#ffebee;padding:12px;border-radius:8px;">
                    📢 <b>Unique Pick!</b><br>
                    <b>{entity_name}</b><br>
                    <b>{row[most_overplayed.columns.index("radio_play_count")]:,} plays on {radio_name}</b><br>
                    but <b>barely played on other radios ({other_play_count:,} plays)</b>!
                    </div>
                    """
                else:
                    overplayed_text = f"""
                    <div style="background-color:#ffebee;padding:12px;border-radius:8px;">
                    📢 <b>Unique Pick!</b><br>
                    <b>{entity_name}</b><br>
                    <b>{row[most_overplayed.columns.index("radio_play_count")]:,} plays on {radio_name}</b><br>
                    and <b>doesn't have any plays on other radios!</b>
                    </div>
                    """
                st.markdown(overplayed_text, unsafe_allow_html=True)
        else:
            st.write("✅ No major overplayed tracks found!")

    # Expander for Full List
    with st.expander(f"🔎 See More Underplayed & Overplayed {view_option}s"):
        col3, col4 = st.columns(2)

        with col3:
            st.subheader(f"🔥 Top Underplayed {view_option}s")
            st.dataframe(
                filtered_underplayed
                .select([display_col, "radio_play_count", "other_play_count"])
                .head(10)
                .rename({
                    "radio_play_count": f"Plays on {radio_name}",
                    "other_play_count": "Plays on Other Radios"
                })
            )

        with col4:
            st.subheader(f"📢 Top Overplayed {view_option}s")
            st.dataframe(
                filtered_overplayed
                .select([display_col, "radio_play_count", "other_play_count"])
                .head(10)
                .rename({
                    "radio_play_count": f"Plays on {radio_name}",
                    "other_play_count": "Plays on Other Radios"
                })
            )


def display_top_genres_evolution(radio_df: pl.DataFrame, other_radios_df: Optional[pl.DataFrame] = None):
    """
    Displays a bump chart tracking the weekly evolution of the top 5 genres by total plays.

    Parameters:
        radio_df (pl.DataFrame): Input data for the selected radio containing play counts and genre information.
        other_radios_df (Optional[pl.DataFrame]): Data for other radios to include in the comparison (default: None).

    Functionality:
        - Ranks genres weekly by total plays and identifies the top 5.
        - Ensures consistent ranking and visualizes changes in popularity over time.
        - Allows comparison of genre trends between the selected radio and other radios.
    """
    st.subheader("🎼 How Have the :blue[Top Genres] Shifted Over Time?")

    st.markdown(
        """
        This chart tracks the **top 5 genres** each week based on total plays.  
        See which genres **stay on top**, which ones **rise and fall**, and how preferences **shift over time**.
        """
    )

    # Create two columns if `other_radios_df` is provided
    if other_radios_df is not None and not other_radios_df.is_empty():
        col1, col2 = st.columns(2)
    else:
        col1 = st.container()

    def process_genre_evolution(df: pl.DataFrame) -> pl.DataFrame:
        """
        - Convert date to ISO-8601 "YYYY-WW"
        - Compute total plays -> top-5 rank each week
        - Keep only rows where rank <= 5
        - Sort by (week_label, rank)
        """
        # Filter out empty genres
        df = df.filter(pl.col(cm.SPOTIFY_GENRE_COLUMN) != "")

        df = df.with_columns([
            # ISO week label
            pl.col(cm.DAY_COLUMN).dt.strftime("%G-W%V").alias("week_label")
        ])

        weekly_genre_plays = (
            df.group_by(["week_label", cm.SPOTIFY_GENRE_COLUMN])
              .agg(pl.count().alias("total_plays"))
        )

        # Rank descending by total_plays within each week
        weekly_top5 = (
            weekly_genre_plays
            .with_columns(
                pl.col("total_plays")
                  .rank("dense", descending=True)
                  .over("week_label")
                  .alias("rank")
            )
            .filter(pl.col("rank") <= 5)
            .sort(["week_label", "rank"])
        )
        return weekly_top5

    radio_genre_evolution = process_genre_evolution(radio_df)
    
    if other_radios_df is not None and not other_radios_df.is_empty():
        other_genre_evolution = process_genre_evolution(other_radios_df)
    else:
        other_genre_evolution = None

    def generate_bump_chart(
        df: pl.DataFrame, 
        all_genres: set, 
        sorted_genres_desc: list, 
        color_map: dict,
        margin: dict,
        show_legend: bool = True,
        show_yaxis_title: bool = True,
    ):
        """
        Generates a bump chart showing the ranking evolution of top 5 genres 
        with improved visuals, ensuring the legend includes all genres.
        """

        df = df.with_columns([
            pl.col("week_label").map_elements(lambda w: week_dates_start_end(w), return_dtype=pl.List(pl.Utf8)).alias("week_dates"),
        ])
        df = df.with_columns([
            pl.col("week_dates").list.get(0).alias("start_date"),
            pl.col("week_dates").list.get(1).alias("end_date"),
            pl.col("week_label").str.slice(0, 4).cast(pl.Int64).alias("year"),  # Extract year
            pl.col("week_label").str.slice(6, 2).cast(pl.Int64).alias("week"),  # Extract week number
        ])

        # Sort the dataframe by year and week for chronological order
        df = df.sort(["year", "week"])

        # Remove auxiliary sorting columns before plotting
        df = df.drop(["year", "week"])

        # Convert Polars → Pandas
        df_pd = df.to_pandas()

        # Sort rows by "week_label" to ensure lines move forward
        df_pd = df_pd.sort_values("week_label")

        # Expand data so each genre has a row for EVERY week
        if not df_pd.empty:
            unique_week_labels = df_pd["week_label"].unique()
            current_genres = set(df_pd[cm.SPOTIFY_GENRE_COLUMN].dropna())

            # Cross product => all (genre, week_label) combos
            full_index = pd.MultiIndex.from_product(
                [current_genres, unique_week_labels],
                names=[cm.SPOTIFY_GENRE_COLUMN, "week_label"]
            )
            df_expanded = (
                df_pd.set_index([cm.SPOTIFY_GENRE_COLUMN, "week_label"])
                    .reindex(full_index)
                    .reset_index()
            )
        else:
            # If empty, just create an empty DataFrame
            df_expanded = df_pd.copy()

        # Add dummy rows for genres that never appear in the final DataFrame => force them to appear in legend
        if not df_expanded.empty:
            all_missing = all_genres - set(df_expanded[cm.SPOTIFY_GENRE_COLUMN].dropna())
            if all_missing:
                # Use the first week_label from df_expanded
                dummy_week = df_expanded["week_label"].dropna().unique()[0]

                # Get all columns from df_expanded
                all_columns = df_expanded.columns.tolist()

                # Create list of dummy rows, including all necessary columns
                dummy_rows = []
                for genre in all_missing:
                    dummy_row = {col: None for col in all_columns}  # Initialize all columns with None
                    dummy_row[cm.SPOTIFY_GENRE_COLUMN] = genre
                    dummy_row["week_label"] = dummy_week
                    dummy_rows.append(dummy_row)

                # Create dummy_df with all columns
                dummy_df = pd.DataFrame(dummy_rows, columns=all_columns)

                # Build a dtype mapping from df_expanded for columns existing in dummy_df
                dtype_dict = {col: dtype for col, dtype in df_expanded.dtypes.to_dict().items() if col in dummy_df.columns}

                # Modify dtype_dict to use nullable integer types where needed
                modified_dtype_dict = {}
                for col, dtype in dtype_dict.items():
                    if pd.api.types.is_integer_dtype(dtype):
                        modified_dtype_dict[col] = "Int64"  # use nullable integer type
                    else:
                        modified_dtype_dict[col] = dtype

                # Convert dummy_df columns to the specified dtypes using the modified mapping
                dummy_df = dummy_df.astype(modified_dtype_dict)

                # Concatenate the dummy_df with df_expanded
                df_expanded = pd.concat([df_expanded, dummy_df], ignore_index=True)

        # Make sure to sort again by week_label
        df_expanded = df_expanded.sort_values("week_label")

        # Prepare custom data for each genre
        genre_customdata_map = {}
        for genre in df_expanded[cm.SPOTIFY_GENRE_COLUMN].unique():
            genre_data = df_expanded[df_expanded[cm.SPOTIFY_GENRE_COLUMN] == genre]
            genre_customdata_map[genre] = genre_data[[cm.SPOTIFY_GENRE_COLUMN, "rank", "total_plays", "start_date", "end_date"]].values

        fig = px.line(
            df_expanded,
            x="week_label",
            y="rank",
            color=cm.SPOTIFY_GENRE_COLUMN,
            line_shape="spline", 
            category_orders={
                cm.SPOTIFY_GENRE_COLUMN: sorted_genres_desc,
                "rank": [1, 2, 3, 4, 5]
            },
            color_discrete_map=color_map
        )
        # Enhanced tooltips
        for trace in fig.data:
            # Extract the line color for dynamic tooltip styling
            line_color = trace.line.color if trace.line.color else "#000"

            # Get the relevant custom data for the current trace
            trace_genre = trace.name
            trace_customdata = genre_customdata_map.get(trace_genre, [])

            # Set hovertemplate with enhanced formatting
            trace.update(
                hovertemplate=(
                    f"<span style='font-size:16px; font-weight:bold; color:{line_color};'> %{{customdata[1]}} - %{{customdata[0]}}</span><br>" 
                    f"<span style='font-weight:bold;'>%{{customdata[2]}} Plays</span><br>"
                    f"<span>%{{customdata[3]}} to %{{customdata[4]}}</span><br>"
                    "<extra></extra>"
                ),
                customdata=trace_customdata,
            )

        fig.update_traces(
            mode="lines+markers",
            line=dict(width=2),
            marker=dict(size=7),
            connectgaps=False
        )

        # Tweak layout
        fig.update_layout(
            title='',
            xaxis_title=None,
            yaxis_title='Rank' if show_yaxis_title else None,
            yaxis=dict(
                autorange="reversed",
                tickmode="array",
                tickvals=[1, 2, 3, 4, 5],
                showgrid=True,
                gridcolor="lightgray"
            ),
            height=500,
            margin=margin,
            legend_title_text="Genres",
            legend_traceorder="reversed+grouped", 
            xaxis=dict(
                tickmode="array",
                # Show every 3rd or 4th label to reduce clutter
                tickvals=df_expanded["week_label"].unique()[::3],
                tickangle=-45
            ),
            showlegend=show_legend
        )

        return fig

    # Process data
    all_radio_genres = set(radio_genre_evolution[cm.SPOTIFY_GENRE_COLUMN].unique())
    if other_genre_evolution is not None and other_genre_evolution.height > 0:
        all_radio_genres.update(
            other_genre_evolution[cm.SPOTIFY_GENRE_COLUMN].unique()
        )
    all_radio_genres = sorted(all_radio_genres)

    union_df = pl.concat(
        [
            radio_genre_evolution.select(
                cm.SPOTIFY_GENRE_COLUMN, "total_plays"
            ),
            other_genre_evolution.select(
                cm.SPOTIFY_GENRE_COLUMN, "total_plays"
            )
        ]
    ) if other_genre_evolution is not None else radio_genre_evolution

    # Sum total plays by genre, then sort descending
    union_sums = (
        union_df
        .group_by(cm.SPOTIFY_GENRE_COLUMN)
        .agg(pl.col("total_plays").sum().alias("sum_plays"))
        .sort("sum_plays", descending=True)
    )

    # Get all unique genres appearing in either dataset
    all_genres = set(radio_genre_evolution[cm.SPOTIFY_GENRE_COLUMN].unique().to_list())
    if other_radios_df is not None and not other_radios_df.is_empty():
        all_genres.update(other_genre_evolution[cm.SPOTIFY_GENRE_COLUMN].unique().to_list())
    sorted_genres_desc = union_sums[cm.SPOTIFY_GENRE_COLUMN].to_list()

    # Create a color map from the union
    pastel_colors = px.colors.qualitative.Pastel
    color_map = {
        genre: pastel_colors[i % len(pastel_colors)]
        for i, genre  in enumerate(sorted_genres_desc)
    }

    # Generate the left chart (Selected Radio)
    with col1:
        fig_left = generate_bump_chart(
            df=radio_genre_evolution,
            color_map=color_map,
            all_genres=all_genres,
            sorted_genres_desc=sorted_genres_desc,
            margin=dict(l=50, r=120, t=30, b=40),
            show_legend=(other_genre_evolution is None),  # only show legend if there's no "other" chart
            show_yaxis_title=True
        )
        st.plotly_chart(fig_left, use_container_width=True)

    if other_radios_df is not None and not other_radios_df.is_empty():
        with col2:
            fig_right = generate_bump_chart(
                df=other_genre_evolution,
                color_map=color_map,
                all_genres=all_genres,
                sorted_genres_desc=sorted_genres_desc,
                margin=dict(l=0, r=0, t=30, b=40),
                show_legend=True,
                show_yaxis_title=False
            )
            st.plotly_chart(fig_right, use_container_width=True)