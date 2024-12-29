import polars as pl
import pandas as pd
import streamlit as st
from streamlit_extras.stylable_container import stylable_container
import plotly.express as px
from datetime import datetime

from data_extract.data_storage import DataStorage
from data_extract.config_manager import ConfigManager

from utils.calculations_helper import (
    calculate_avg_tracks, calculate_avg_popularity, calculate_avg_time,
    prepare_weekday_metrics, prepare_hourly_metrics, plot_metrics, 
    calculate_column_counts, calculate_decade_metrics, calculate_duration_metrics,
    calculate_genre_metrics
)
from utils.helper import (
    country_to_flag, nationality_to_flag, number_formatter
)

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

df_joined = df_radio_data.join(
        df_artist_info, 
        on=cm.ARTIST_NAME_COLUMN,
        how='left',
    ).join(
        df_track_info,
        on=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
        how='left',
    )

min_date = df_joined[cm.DAY_COLUMN].min()
max_date = df_joined[cm.DAY_COLUMN].max()

if 'date_period' not in st.session_state:
    st.session_state['date_period'] = (min_date, max_date)
if 'ts_graph' not in st.session_state:
    st.session_state['ts_graph'] = 'Avg Tracks'

def reset_settings():
    st.session_state['date_period'] = (min_date, max_date)
    st.session_state['ts_graph'] = 'Avg Tracks'
    st.session_state['metric_type'] = 'Unique'

# Sidebar Settings
with st.sidebar:
    st.title(':gear: Page Settings')

    new_date_period = st.date_input(
        label = ':calendar: Select the time period',
        # value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        key='date_period'
    )
    new_graph_option = st.radio(
        '📈 Select :blue-background[**Time Series**] to display:',
        ['Avg Tracks', 'Avg Hours Played', 'Avg Popularity'],
        index=0,
        key='ts_graph'
    )

    metric_type_option = st.radio(
        label='📊 Select :blue-background[**Metric**] Type',
        options=['Unique', 'Total', 'Average'],
        index=0,
        horizontal=False,
        key='metric_type',
        help="""Display either unique or total combinations, or average.
            \nApplies for :red-background[**Tracks**] and :red-background[**Artists**] metrics"""
    )

    # Reset settings button
    st.button('Reset Page Settings', on_click=reset_settings)


# Apply Date Filter Only After Both Dates Are Selected
if len(st.session_state['date_period']) == 2:
    start_date, end_date = st.session_state['date_period']
    df_filtered = df_joined.filter(
        (pl.col(cm.DAY_COLUMN) >= start_date) & (pl.col(cm.DAY_COLUMN) <= end_date)
    )
elif len(st.session_state['date_period']) == 1:
    start_date = st.session_state['date_period'][0]
    df_filtered = df_joined.filter(
        pl.col(cm.DAY_COLUMN) >= start_date
    )
else:
    df_filtered = df_joined  # Keep unfiltered data if only one date is selected


# Calculate global min and max values
metrics = ['avg_tracks', 'avg_time_played', 'avg_popularity']
metric_ranges = {}

# Initialize config for each radio
for i, (key, val) in enumerate(app_config.items()):
    app_config[key]['radio_df'] = df_filtered.filter(
            pl.col(cm.RADIO_COLUMN) == val.get('name')
        )
    for metric in metrics:
        weekday_metric_df = prepare_weekday_metrics(app_config[key]['radio_df'], metric=metric)
        hour_metric_df = prepare_hourly_metrics(app_config[key]['radio_df'], metric=metric)
        if metric not in metric_ranges:
            metric_ranges[metric] = {
                'weekday': {'min': float('inf'), 'max': float('-inf')},
                'hour': {'min': float('inf'), 'max': float('-inf')},
            }
        
        # Update weekday min/max if data exists
        if not weekday_metric_df.is_empty():
            weekday_min = weekday_metric_df[metric].min()
            weekday_max = weekday_metric_df[metric].max()

            if weekday_min is not None:
                metric_ranges[metric]['weekday']['min'] = min(
                    metric_ranges[metric]['weekday']['min'], weekday_min
                )
            if weekday_max is not None:
                metric_ranges[metric]['weekday']['max'] = max(
                    metric_ranges[metric]['weekday']['max'], weekday_max
                )

        # Update hour min/max if data exists
        if not hour_metric_df.is_empty():
            hour_min = hour_metric_df[metric].min()
            hour_max = hour_metric_df[metric].max()

            if hour_min is not None:
                metric_ranges[metric]['hour']['min'] = min(
                    metric_ranges[metric]['hour']['min'], hour_min
                )
            if hour_max is not None:
                metric_ranges[metric]['hour']['max'] = max(
                    metric_ranges[metric]['hour']['max'], hour_max
                )


graph_metric_map = {
    'Avg Tracks': 'avg_tracks',
    'Avg Hours Played': 'avg_time_played',
    'Avg Popularity': 'avg_popularity'
}

metric_type_map = {
    'Unique Tracks': 'unique',
    'Unique Artists': 'unique',
    'Unique': 'unique',
    'Total Tracks': 'total',
    'Total Artists': 'total',
    'Total': 'total',
    'Avg Tracks': 'average',
    'Avg Artists': 'average',
    'Average': 'average',
}

mapped_metric_type = metric_type_map.get(st.session_state['metric_type'])
selected_metric = graph_metric_map[st.session_state['ts_graph']]

### Header KPIs + Logo
ncols = len(app_config)
header_cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with header_cols[i]:
        radio_name = val.get('name')
        logo = val.get('logo')
        radio_df = val.get('radio_df')
        # st.image(logo, use_container_width=True)
        with st.container(border=True):
            kpi_1, kpi_2, kpi_3 = st.columns(3)
            kpi_1.metric(
                label='# Avg Daily Tracks',
                value=calculate_avg_tracks(df=radio_df)
            )
            kpi_2.metric(
                label='Avg Daily Hours Played',
                value=calculate_avg_time(df=radio_df, output_unit='hours')
            )
            kpi_3.metric(
                label='Avg Popularity',
                value=calculate_avg_popularity(df=radio_df),
                help='''Popularity is a **score** that reflects **how frequently a track has been played, 
                saved, or added to playlists** by users on :green[Spotify], with recent activity weighing more heavily than older interactions.'''
            )


# Time Series Plots Expander
expander = st.expander(label=f'Time Series plots - *{st.session_state['ts_graph']}*', expanded=True, icon='📈')
with expander:
    ### Hourly Graphs
    st.subheader(f'{st.session_state['ts_graph']} by hour', divider="gray")
    hour_graph_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with hour_graph_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            hourly_df = prepare_hourly_metrics(radio_df, metric=selected_metric)
            plot_metrics(
                hourly_df,
                metric=selected_metric,
                radio_name=radio_name,
                x_axis_column='hour',
                x_axis_label='Hour of Day',
                y_axis_range=(
                    metric_ranges[selected_metric]['hour']['min'] * 0.95,
                    metric_ranges[selected_metric]['hour']['max'] * 1.05
                ),
                title=''
            )

    ### WeekDay Graphs
    st.subheader(f'{st.session_state['ts_graph']} by weekday', divider="gray")
    weekday_graph_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with weekday_graph_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            # Prepare the selected Weekday Metric
            weekday_df = prepare_weekday_metrics(radio_df, metric=selected_metric)
            plot_metrics(
                weekday_df,
                metric=selected_metric,
                radio_name=radio_name,
                x_axis_column='weekday_name',
                x_axis_label='Day of Week',
                y_axis_range=(
                    metric_ranges[selected_metric]['weekday']['min'] * 0.95,
                    metric_ranges[selected_metric]['weekday']['max'] * 1.05
                ),
                title=''
            )


### Track Statistics
st.header(f':musical_note: Track Statistics', divider="gray")
track_kpis_cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with track_kpis_cols[i]:
        radio_name = val.get('name')
        radio_df = val.get('radio_df')

        total_tracks = radio_df.height
   
        # Track KPIs
        track_kpi_total, track_kpi_percent = st.columns(2, border=True)
        with track_kpi_total:
            st.metric(
                label='Total Tracks',
                value=number_formatter(total_tracks)
            )
            unique_tracks = radio_df.select([pl.col(cm.TRACK_TITLE_COLUMN), pl.col(cm.ARTIST_NAME_COLUMN)]).unique().height

        percent_unique_tracks = f'{(unique_tracks / total_tracks * 100):.2f}%' if total_tracks > 0 else "N/A"
        with track_kpi_percent:
            st.metric(
                label='% Unique Tracks',
                value=percent_unique_tracks
            )
        
        avg_plays_per_track = f'{(total_tracks / unique_tracks if unique_tracks > 0 else 0):.2f}'
        st.write(f'That means each track is played **{avg_plays_per_track}** times on average')


# Track Plots Expander
track_plots_expander = st.expander(label=f'Track Plots', expanded=True, icon='📊')
with track_plots_expander:
    # with stylable_container(
    #     key='track_plots_settings',
    #     css_styles="""
    #         button {
    #             width: 150px;
    #             height: 60px;
    #             background-color: green;
    #             color: white;
    #             border-radius: 5px;
    #             white-space: nowrap;
    #         }
    #         """,
    # ):
    with st.popover(label='Settings', icon='⚙️', use_container_width=False):
        # tracks_metric_type = st.radio(
        #     label='Select Metric Type',
        #     options=['Unique Tracks', 'Total Tracks', 'Avg Tracks'],
        #     index=0,
        #     horizontal=True,
        #     key='Tracks Metric Type'
        # )
        num_languages = st.number_input(
            label='Top Number of Languages',
            value=5,
            min_value=1,
            max_value=10,
            step=1,
        )

    ### Track Language Statistics
    st.subheader(f':earth_africa: Top {num_languages} Languages by {st.session_state['metric_type']} Tracks', divider=False)
    track_plots_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with track_plots_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            
            language_counts = calculate_column_counts(
                df=radio_df,
                group_by_cols='lyrics_language',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
            )

            # Separate top languages and "Others"
            top_languages = language_counts.head(num_languages)
            others = language_counts.tail(language_counts.shape[0] - num_languages)

            if mapped_metric_type == "average":
                others_aggregated = pl.DataFrame({
                    "lyrics_language": ["Others"],
                    "metric": [others["metric"].mean()]
                })
            else:
                others_aggregated = pl.DataFrame({
                    "lyrics_language": ["Others"],
                    "metric": [others["metric"].sum()]
                })

            # Ensure the 'metric' column types match
            others_aggregated = others_aggregated.with_columns(
                pl.col("metric").cast(top_languages["metric"].dtype)
            )

            all_languages = top_languages.vstack(others_aggregated)

            # Calculate percentage only for 'Unique Tracks' or 'Total Tracks'
            if mapped_metric_type in ["unique", "total"]:
                all_languages = all_languages.with_columns(
                    (pl.col("metric") / all_languages["metric"].sum() * 100).alias("percentage")
                )

            # Convert to Pandas for Plotly
            data_df = all_languages.to_pandas()
            data_df['flag'] = data_df['lyrics_language'].map(country_to_flag)

            # Ensure "Others" is always last in the plot
            data_df['order'] = data_df['lyrics_language'].apply(lambda x: 1 if x == "Others" else 0)
            data_df = data_df.sort_values(by=['order', 'metric'], ascending=[False, True])

            # Update label for average
            label_col = "metric"
            # st.write(data_df)
            if mapped_metric_type == "average":
                data_df["label"] = data_df["metric"].apply(lambda x: f"{x:.1f}")
            else:
                data_df["label"] = data_df["percentage"].apply(lambda x: f"{x:.1f}%")

            # Plot top languages
            fig = px.bar(
                data_df,
                x="metric",
                y="flag",
                text="label",
                title="",
                orientation='h',
            )

            # Apply conditional coloring for "Portugal" or "PT"
            colors = [
                "#1f77b4" if lang != "pt" else "#ff7f0e"  # Default color vs highlight color
                for lang in data_df["lyrics_language"]
            ]
            fig.update_traces(
                marker_color=colors,  # Apply colors manually
                textposition="outside",
                cliponaxis=False  # Prevent labels from being clipped
            )
            fig.update_layout(
                xaxis_title=None,  # Remove x-axis label
                yaxis_title=None,  # Remove y-axis label
                margin=dict(l=10, r=30, t=30, b=0),  # Add padding around the plot
            )
            st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_tracks_by_language')

    st.divider()

    ### Track Language Statistics
    st.subheader(f':date: {st.session_state['metric_type']} Tracks by *decade*', divider=False)
    track_decade_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with track_decade_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            df_decades_tracks = calculate_decade_metrics(
                df=radio_df,
                date_column='spotify_release_date',
                count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type
            )

            # Plot unique tracks by decade
            df_tracks_decade_pandas = df_decades_tracks.to_pandas()

            # df_tracks_decade_pandas["formatted_metric"] = df_tracks_decade_pandas["metric"].apply(
            #     lambda x: number_formatter(x)
            # )
            
            # Display the values percentages
            total_tracks = df_tracks_decade_pandas["metric"].sum()
            df_tracks_decade_pandas["percentage"] = (df_tracks_decade_pandas["metric"] / total_tracks) * 100

            df_tracks_decade_pandas["formatted_metric"] = df_tracks_decade_pandas.apply(
                lambda row: f"{row['percentage']:.1f}%"
                , axis=1
            )

            fig_tracks = px.bar(
                df_tracks_decade_pandas,
                x="decade_label",
                y="metric",
                title="",
                labels={"decade_label": "Decade", "metric": "Tracks"},
                orientation='v',
                text="formatted_metric",  # Display the count on the bars
            )
            fig_tracks.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                xaxis=dict(type='category', categoryorder='array', categoryarray=df_tracks_decade_pandas["decade_label"].tolist()),  # Ensure proper ordering
            )
            st.plotly_chart(fig_tracks, use_container_width=True, key=f'{radio_name}_unique_tracks_by_decade')

            unique_2024_tracks = radio_df.filter(pl.col('spotify_release_date').dt.year() == 2024).select([pl.col(cm.TRACK_TITLE_COLUMN), pl.col(cm.ARTIST_NAME_COLUMN)]).unique().height
            total_2024_tracks = radio_df.filter(pl.col('spotify_release_date').dt.year() == 2024).height
            # Avoid division by zero
            if unique_2024_tracks > 0:
                average_plays_per_track = total_2024_tracks / unique_2024_tracks
            else:
                average_plays_per_track = 0.0
            st.markdown(
                f'''**{unique_2024_tracks}** unique tracks released in :blue-background[2024], and were played a total of **{number_formatter(total_2024_tracks)}** times
                \ni.e. **{average_plays_per_track:.1f}** times per track'''
            )


### Artist Statistics
st.header(f':microphone: Artist Statistics', divider="gray")
artist_kpis_cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with artist_kpis_cols[i]:
        radio_name = val.get('name')
        radio_df = val.get('radio_df')
        total_tracks = radio_df.height

         # Artists KPIs
        artist_kpi_total, artist_kpi_percent = st.columns(2, border=True)
        with artist_kpi_total:
            unique_artists = radio_df.select(pl.col(cm.ARTIST_NAME_COLUMN)).unique().height
            st.metric(
                label='Unique Artists',
                value=number_formatter(unique_artists)
            )
            
        with artist_kpi_percent:
            percent_unique_artists = f'{(unique_artists / total_tracks * 100):.2f}%' if total_tracks > 0 else "N/A"
            avg_plays_per_artist = f'{(total_tracks / unique_artists if unique_artists > 0 else 0):.2f}'
            st.metric(
                label='Avg Tracks per Artist',
                value=avg_plays_per_artist
            )

# Artist Plots Expander
artist_plots_expander = st.expander(label=f'Artists Plots', expanded=True, icon='📊')
with artist_plots_expander:
    with st.popover(label='Settings', icon='⚙️', use_container_width=False):
        # artists_metric_type = st.radio(
        #     label='Select Metric Type',
        #     options=['Unique Tracks', 'Total Tracks', 'Avg Tracks'],
        #     index=0,
        #     horizontal=True,
        #     key='Artists Metric Type'
        # )
        num_countries = st.number_input(
            label='Top Number of Countries',
            value=5,
            min_value=1,
            max_value=10,
            step=1,
        )
    ### Artists Country Statistics
    st.header(f':earth_africa: Top 5 countries by {st.session_state['metric_type']} Artists', divider="gray")
    artist_plots_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with artist_plots_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')
            
            country_counts = calculate_column_counts(
                df=radio_df,
                group_by_cols='combined_nationality',
                count_columns=[cm.ARTIST_NAME_COLUMN],                
                metric_type=mapped_metric_type,
            )

            # Separate top countries and "Others"
            top_countries = country_counts.head(num_countries)
            others = country_counts.tail(country_counts.shape[0] - num_countries)
            if mapped_metric_type == "average":
                others_aggregated = pl.DataFrame({
                    "combined_nationality": ["Others"],
                    "metric": [others["metric"].mean()]
                })
            else:
                others_aggregated = pl.DataFrame({
                    "combined_nationality": ["Others"],
                    "metric": [others["metric"].sum()]
                })

            # Ensure the 'metric' column types match
            others_aggregated = others_aggregated.with_columns(
                pl.col("metric").cast(top_countries["metric"].dtype)
            )
            
            all_countries = top_countries.vstack(others_aggregated)

            # Calculate percentage only for 'Unique Tracks' or 'Total Tracks'
            if mapped_metric_type in ["unique", "total"]:
                all_countries = all_countries.with_columns(
                    (pl.col("metric") / all_countries["metric"].sum() * 100).alias("percentage")
                )

           # Convert to Pandas for Plotly
            data_df = all_countries.to_pandas()
            data_df['flag'] = data_df['combined_nationality'].map(nationality_to_flag)
            data_df['order'] = data_df['combined_nationality'].apply(lambda x: 1 if x == "Others" else 0)
            data_df = data_df.sort_values(by=['order', 'metric'], ascending=[False, True])

            # Update label for average
            label_col = "metric"
            if mapped_metric_type == "average":
                data_df["label"] = data_df["metric"].apply(lambda x: f"{x:.1f}")
            else:
                data_df["label"] = data_df["percentage"].apply(lambda x: f"{x:.1f}%")

            # Plot
            fig = px.bar(
                data_df,
                x="metric",
                y='flag',
                text="label",
                title="",
                orientation='h',
            )
            # Apply conditional coloring for "Portugal" or "PT"
            colors = [
                "#1f77b4" if country != "Portugal" else "#ff7f0e"  # Default color vs highlight color
                for country in data_df["combined_nationality"]
            ]
            fig.update_traces(
                marker_color=colors,
                textposition="outside",
                cliponaxis=False
            )
            fig.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
            )
            st.plotly_chart(fig, use_container_width=True, key=f'{radio_name}_artists_by_country')
    
    ### Artists Country Statistics
    st.header(f':date: {st.session_state['metric_type']} Artists by *decade*', divider="gray")
    artist_decade_cols = st.columns(ncols)

    for i, (key, val) in enumerate(app_config.items()):
        with artist_decade_cols[i]:
            radio_name = val.get('name')
            radio_df = val.get('radio_df')

            # Calculate metrics by decade
            df_decades_artists = calculate_decade_metrics(
                df=radio_df,
                date_column="mb_artist_career_begin",
                count_columns=[cm.ARTIST_NAME_COLUMN],
                metric_type=mapped_metric_type,
            )

            # Plot unique tracks by decade
            df_artists_decade_pandas = df_decades_artists.to_pandas()

            # Apply number_formatter to the 'metric' column for labels
            # df_artists_decade_pandas["formatted_metric"] = df_artists_decade_pandas["metric"].apply(
            #     lambda x: number_formatter(x)
            # )

            # Display the values percentages
            total_tracks = df_artists_decade_pandas["metric"].sum()
            df_artists_decade_pandas["percentage"] = (df_artists_decade_pandas["metric"] / total_tracks) * 100

            df_artists_decade_pandas["formatted_metric"] = df_artists_decade_pandas.apply(
                lambda row: f"{row['percentage']:.1f}%"
                , axis=1
            )
            
            fig_tracks = px.bar(
                df_artists_decade_pandas,
                x="decade_year",
                y="metric",
                title="",
                labels={"decade_year": "Decade", "metric": "Artists"},
                orientation='v',
                text="formatted_metric",  # Display the count on the bars
            )
            fig_tracks.update_layout(
                xaxis_title=None,
                yaxis_title=None,
                margin=dict(l=10, r=30, t=30, b=0),
                xaxis=dict(type='category', categoryorder='array', categoryarray=df_artists_decade_pandas["decade_year"].tolist()),  # Ensure proper ordering
            )
            st.plotly_chart(fig_tracks, use_container_width=True, key=f'{radio_name}_artists_by_decade')



### Track Duration
st.subheader(f':clock4: Track Duration (minutes)', divider="gray")
track_duration_cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with track_duration_cols[i]:
        radio_name = val.get('name')
        radio_df = val.get('radio_df')

        # Calculate duration metrics with most played track details
        df_duration_tracks = calculate_duration_metrics(
            df=radio_df,
            duration_column='spotify_duration_ms',
            count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
            metric_type=mapped_metric_type,
            include_most_played=True,
        )

        # Convert to Pandas for Plotly
        df_duration_pandas = df_duration_tracks.to_pandas()

        # Calculate percentage
        total_tracks = df_duration_pandas["metric"].sum()
        df_duration_pandas["percentage"] = (
            (df_duration_pandas["metric"] / total_tracks) * 100
        ).apply(lambda x: f"{x:.2f}%")

        # Add formatted metric and hover text
        df_duration_pandas["formatted_metric"] = df_duration_pandas["metric"].apply(lambda x: number_formatter(x))
        df_duration_pandas["most_played_info"] = df_duration_pandas.apply(
            lambda row: (
                f"{row['most_played_track']} | {row['most_played_artist']} ({number_formatter(row['most_played_count'])} plays)"
                if pd.notnull(row['most_played_track']) else "N/A"
            ),
            axis=1
        )
        df_duration_pandas["tooltip_text"] = df_duration_pandas.apply(
            lambda row: (
                f"<b>Track Minute Duration:</b> {row['duration_minutes']}<br>"
                f"<b>{metric_type_option} Tracks:</b> {row['formatted_metric']}<br>"
                f"<b>Percentage:</b> {row['percentage']}<br>"
                f"<b>Most Played Track:</b> {row['most_played_info']}"
            ),
            axis=1
        )

        # Plot duration-based bar chart
        fig_duration = px.bar(
            df_duration_pandas,
            x="duration_minutes",
            y="metric",
            title="",
            labels={"duration_minutes": "Track Duration (Minutes)", "metric": metric_type_option},
            orientation="v",
            text="formatted_metric",  # Use formatted metric on bars
            hover_data={"tooltip_text": True},  # Use custom tooltips
        )
        fig_duration.update_traces(
            marker_color="#d3d3d3",  # Light gray for bars
            texttemplate="%{text}",
            textposition="outside",
            hovertemplate="%{customdata[0]}",
            customdata=df_duration_pandas[["tooltip_text"]].to_numpy(),  # Attach custom tooltip text
            cliponaxis=False  # Prevent labels from being clipped
        )
        fig_duration.update_layout(
            xaxis_title=None,
            yaxis_title=None,
            margin=dict(l=10, r=30, t=30, b=0),
            xaxis=dict(type="category"),  # Treat durations as categories
            yaxis=dict(title=metric_type_option, tickformat=","),
            hoverlabel_align="left"  # Ensure left alignment for tooltips
        )
        st.plotly_chart(fig_duration, use_container_width=True, key=f"{radio_name}_tracks_by_duration")



### Genres
st.subheader(f':musical_score: Top 10 Genres', divider="gray")
genre_cols = st.columns(ncols)

for i, (key, val) in enumerate(app_config.items()):
    with genre_cols[i]:
        radio_name = val.get('name')
        radio_df = val.get('radio_df')
        
        # Calculate genre metrics
        df_genres_cleaned = calculate_genre_metrics(
            df=radio_df,
            genre_column='spotify_genres',
            count_columns=[cm.TRACK_TITLE_COLUMN, cm.ARTIST_NAME_COLUMN],
            metric_type=mapped_metric_type,
            include_most_played=True,
        )

        # Convert to Pandas for Plotly
        df_genres_pandas = df_genres_cleaned.rename({"split_genres": "spotify_genres"}).to_pandas()

        # Add percentage column
        total_metric = df_genres_pandas["metric"].sum()
        df_genres_pandas["percentage"] = (
            df_genres_pandas["metric"] / total_metric * 100
        ).apply(lambda x: f"{x:.2f}%")

        # Add formatted columns for tooltips
        df_genres_pandas["formatted_metric"] = df_genres_pandas["metric"].apply(
            lambda x: number_formatter(x)
        )
        df_genres_pandas["most_played_info"] = df_genres_pandas.apply(
            lambda row: (
                f"{row['most_played_track']} | {row['most_played_artist']} ({number_formatter(row['most_played_count'])} plays)"
                if pd.notnull(row['most_played_track']) else "N/A"
            ),
            axis=1
        )
        df_genres_pandas["tooltip_text"] = df_genres_pandas.apply(
            lambda row: (
                f"<b>Genre:</b> {row['spotify_genres']}<br>"
                f"<b>{metric_type_option} Tracks:</b> {row['formatted_metric']}<br>"
                f"<b>Percentage:</b> {row['percentage']}<br>"
                f"<b>Most Played Track:</b> {row['most_played_info']}"
            ),
            axis=1
        )

        # Plot horizontal bar chart
        fig_genres = px.bar(
            df_genres_pandas,
            x="metric",
            y="spotify_genres",
            title="",
            labels={"metric": metric_type_option, "spotify_genres": "Genre"},
            orientation="h",
            text="formatted_metric",  # Show count on bars
            hover_data={"tooltip_text": True},  # Use custom tooltips
        )
        fig_genres.update_traces(
            texttemplate="%{text}", 
            textposition="outside",
            hovertemplate="%{customdata[0]}",
            customdata=df_genres_pandas[["tooltip_text"]].to_numpy(),  # Attach custom tooltip text
            cliponaxis=False  # Prevent labels from being clipped
        )
        fig_genres.update_layout(
            xaxis_title=None,
            yaxis_title=None,
            margin=dict(l=150, r=30, t=30, b=10),  # Adjust for long genre names
            hoverlabel_align = 'left',
        )
        st.plotly_chart(fig_genres, use_container_width=True, key=f"{radio_name}_top_genres")



        # st.write(radio_df)
        

## Visuals
# Create logos for radios of the same size
# Improve visual by trying to add some borders or background colors
# Color PT bar differently
# Define color pallete for each radio

## Minor details
# Reduce file size with helper functions, if possible
# Reduce white space between graphs and headers if possible
# Perhaps refactor calculations_helper.py

# Improve graph tooltips


# Group "United States" and "United States of America" together, and other similar countries

