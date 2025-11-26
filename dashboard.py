# Optimized Dashboard for faster page load times
import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
import glob
import re
import numpy as np
import pvlib
from pvlib.location import Location
from timezonefinder import TimezoneFinder
import pytz
from typing import Dict, List, Optional
import functools
from pathlib import Path
import logging

# Setup logging to file
log_dir = "logs/dashboard"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"dashboard_{datetime.now().strftime('%Y%m%d')}.log")

# Configure logging - more readable format
logging.basicConfig(
    level=logging.INFO,  # Changed from DEBUG to INFO
    format='%(asctime)s - %(levelname)s - %(message)s',  # Simplified format
    handlers=[
        logging.FileHandler(log_file),
        # Remove console handler to reduce noise
    ]
)
logger = logging.getLogger('dashboard')

# Set specific loggers to WARNING to reduce noise
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('streamlit').setLevel(logging.WARNING)
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('PIL').setLevel(logging.WARNING)
logging.getLogger('watchdog').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.inotify_buffer').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.inotify').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.polling').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.fsevents').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.read_directory_changes').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.kqueue').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.inotify').setLevel(logging.WARNING)
logging.getLogger('watchdog.observers.inotify_buffer').setLevel(logging.WARNING)

# Set page config for crisis room use
st.set_page_config(
    page_title="Solar Data Monitor",
    page_icon="☀️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for crisis room design
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .station-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        margin: 1rem 0;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .plot-container {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 0.5rem 0;
    }
    .stSelectbox > div > div {
        background-color: white;
    }
    .stDataFrame {
        font-size: 0.9rem;
    }
    .subsection-header {
        font-size: 1.2rem;
        font-weight: bold;
        color: #2c3e50;
        margin: 1rem 0 0.5rem 0;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid #e9ecef;
    }
    .variable-selector {
        background-color: #f8f9fa;
        padding: 0.5rem;
        border-radius: 0.3rem;
        margin-bottom: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.markdown('<h1 class="main-header">Solar Data Monitor</h1>', unsafe_allow_html=True)
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Solar Radiation & Meteorological Data (Preferably Last 72 Hours)</p>', unsafe_allow_html=True)


# =============================================================================
# OPTIMIZED CACHING FUNCTIONS
# =============================================================================

@st.cache_data(ttl=30)  # Cache for 30 seconds - reduced for faster detection of new data
def get_available_stations_cached():
    """Get list of available stations from the interim directory - CACHED"""
    interim_dir = os.path.expanduser("data/interim")
    
    if not os.path.exists(interim_dir):
        return []
    
    # Use pathlib for better performance
    station_dirs = [d.name.lower() for d in Path(interim_dir).iterdir() if d.is_dir()]
    return sorted(station_dirs)

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_station_metadata_cached(station_dirs):
    """Get metadata of available stations - CACHED"""
    interim_dir = os.path.expanduser("data/interim")
    location_csv = os.path.join(interim_dir, 'INPESONDA_Stations.csv')

    if not os.path.exists(interim_dir) or not os.path.exists(location_csv):
        return pd.DataFrame(columns=['station', 'latitude', 'longitude'])

    # Read CSV with optimized parameters
    df_locations = pd.read_csv(location_csv, dtype={'station': str})
    df_locations['station_normalized'] = df_locations['station'].str.strip().str.lower()

    # Filter and deduplicate
    df_filtered = df_locations[df_locations['station_normalized'].isin(station_dirs)].copy()
    df_filtered = df_filtered.drop_duplicates(subset='station_normalized')

    return df_filtered.sort_values('station').reset_index(drop=True)

@st.cache_data(ttl=15)  # Cache for 15 seconds - reduced for faster detection of new files
def get_latest_files_for_station_cached(station):
    """Get the latest parquet file for a specific station - CACHED"""
    interim_dir = os.path.expanduser("data/interim")
    station_path = os.path.join(interim_dir, station)
    
    if not os.path.exists(station_path):
        return {}
    
    # Use pathlib for better performance
    station_path_obj = Path(station_path)
    files = station_path_obj.glob("*.parquet")
    
    # Compile regex pattern once for better performance
    pattern = re.compile(r'_([A-Z]{2})_\d{8}_\d{6}\.parquet$')
    
    # Group files by data type (SD, MD, WD) and track latest
    latest_files = {}
    
    for file in files:
        filename = file.name
        # Extract data type from filename
        match = pattern.search(filename)
        if match:
            data_type = match.group(1)
            file_path = str(file)
            
            # Only keep the latest file for each data type
            if data_type not in latest_files:
                latest_files[data_type] = file_path
            else:
                # Compare modification times - only if needed
                if os.path.getmtime(file_path) > os.path.getmtime(latest_files[data_type]):
                    latest_files[data_type] = file_path
    
    return latest_files

def filter_last_72_hours(df, timestamp_col='TIMESTAMP'):
    """Filter dataframe to last 72 hours of data"""
    if timestamp_col not in df.columns:
            return df
    
    # Ensure timestamp column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
    
    # Get current time and 72 hours ago
    now = datetime.now()
    cutoff_time = now - timedelta(hours=72)
    
    # Filter data to last 72 hours
    filtered_df = df[df[timestamp_col] >= cutoff_time].copy()
    
    return filtered_df

@st.cache_data(ttl=15)  # Cache for 15 seconds - reduced for faster detection of new data
def load_latest_data_for_station_cached(station):
    """Load data from the latest files for a specific station - CACHED (Shows available data with warnings)"""
    latest_files = get_latest_files_for_station_cached(station)
    
    if not latest_files:
        return None
    
    data_dict = {}
    failed_files = []
    old_data_warnings = []
    
    for data_type, file_path in latest_files.items():
        try:
            # Use optimized parquet reading with specific columns if possible
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            if df.empty:
                failed_files.append(f"{data_type} (empty file)")
                continue
            
            # Fast timestamp conversion first
            df = safe_convert_timestamp_optimized(df)
            
            # Check if data is recent (last 72 hours)
            df_recent = filter_last_72_hours(df.copy())
            
            # If no recent data, use all available data but add warning
            if df_recent.empty:
                if 'TIMESTAMP' in df.columns:
                    latest_timestamp = df['TIMESTAMP'].max()
                    if pd.notna(latest_timestamp):
                        hours_old = (datetime.now() - latest_timestamp).total_seconds() / 3600
                        old_data_warnings.append(f"{data_type} (data is {hours_old:.0f}h old)")
                    else:
                        old_data_warnings.append(f"{data_type} (invalid timestamps)")
                # Use all available data
                df_to_use = df
            else:
                # Use recent data
                df_to_use = df_recent
            
            # Only clean numeric columns if we have data
            df_to_use = clean_numeric_columns_optimized(df_to_use)
            
            # Add minimal metadata - more efficient
            df_to_use.loc[:, 'source_file'] = os.path.basename(file_path)
            df_to_use.loc[:, 'station'] = station
            df_to_use.loc[:, 'data_type'] = data_type
            df_to_use.loc[:, 'file_path'] = file_path
            
            data_dict[data_type] = df_to_use
            
        except Exception as e:
            failed_files.append(f"{data_type} (error: {str(e)[:50]}...)")
            continue
    
    # Show warnings for old data
    if old_data_warnings:
        st.warning(f"⚠️ Showing older data for station {station.upper()}: {', '.join(old_data_warnings)}")
    
    if failed_files:
        st.info(f"ℹ️ Could not load some data files for station {station.upper()}: {', '.join(failed_files)}")
    
    return data_dict if data_dict else None

# =============================================================================
# OPTIMIZED DATA PROCESSING FUNCTIONS
# =============================================================================

def clean_numeric_columns_optimized(df):
    """Optimized numeric column cleaning - FASTER"""
    # Only process object columns that might be numeric
    object_cols = df.select_dtypes(include=['object']).columns
    exclude_cols = {'TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path'}
    
    # Process only a few columns at a time to avoid memory issues
    for col in object_cols:
        if col in exclude_cols:
            continue
            
        try:
            # Fast numeric conversion with downcast
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='float')
        except:
            continue
    
    return df

def safe_convert_timestamp_optimized(df, timestamp_col='TIMESTAMP'):
    """Optimized timestamp conversion"""
    if timestamp_col not in df.columns:
        # Look for timestamp column
        possible_cols = ['TIMESTAMP', 'timestamp', 'time', 'date', 'datetime']
        for col in possible_cols:
            if col in df.columns:
                timestamp_col = col
                break
        else:
            # Create synthetic timestamps
            start_time = datetime.now() - timedelta(minutes=len(df))
            df['TIMESTAMP'] = [start_time + timedelta(minutes=i) for i in range(len(df))]
            return df
    
    # Fast datetime conversion
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    
    return df

@st.cache_data(ttl=600)  # Cache clear sky calculations for 10 minutes
def calculate_clearsky_ineichen_cached(df, latitude, longitude, altitude=0, tz=None):
    """Cached clear sky calculations (for last 24 hours data) - OPTIMIZED"""
    # Skip if already has clear sky data
    if any(col in df.columns for col in ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']):
        return df
    
    df = df.copy()
    
    # Find timestamp column
    timestamp_col = 'TIMESTAMP'
    if 'TIMESTAMP' not in df.columns:
        possible_timestamp_cols = ['TIMESTAMP', 'timestamp', 'time', 'date', 'datetime']
        for col in possible_timestamp_cols:
            if col in df.columns:
                timestamp_col = col
                df = df.rename(columns={col: 'TIMESTAMP'})
                break
        else:
            return df  # Return original if no timestamp
    
    # Fast datetime conversion
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    df = df.dropna(subset=['TIMESTAMP'])
    
    if df.empty:
        return df

    # Use default timezone if not provided
    if tz is None:
        tz = 'America/Sao_Paulo'  # Default to Brazil timezone

    # Timezone handling - simplified
    if df['TIMESTAMP'].dt.tz is None:
        df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_localize('UTC').dt.tz_convert(tz)

    # Set index for pvlib
    df_indexed = df.set_index('TIMESTAMP')

    try:
        # Create location and calculate clear sky
        location = Location(latitude, longitude, tz=tz, altitude=altitude)
        clearsky = location.get_clearsky(df_indexed.index)

        # Add clear sky data
        df['clearsky_GHI'] = clearsky['ghi'].values
        df['clearsky_DNI'] = clearsky['dni'].values
        df['clearsky_DHI'] = clearsky['dhi'].values
    except Exception:
        # If clear sky calculation fails, return original data
        pass

    return df

# =============================================================================
# OPTIMIZED UTILITY FUNCTIONS
# =============================================================================

def get_available_variables_optimized(df):
    """Optimized variable extraction"""
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude_cols = {'TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path', 
                   'Id', 'Min', 'RECORD', 'Year', 'Jday'}
    return [col for col in numeric_cols if col not in exclude_cols]

def create_variable_selector_optimized(available_vars, default_vars, key_prefix):
    """Optimized variable selector"""
    return st.multiselect(
        "Select variables to display:",
        options=available_vars,
        default=default_vars,
        key=f"{key_prefix}_selector"
    )

def plot_selected_variables_optimized(df, selected_vars, plot_title, height=300):
    """Optimized plotting function - FASTER"""
    if not selected_vars:
        st.warning(f"⚠️ No variables selected for {plot_title}")
        return False
    
    if 'TIMESTAMP' not in df.columns:
        st.error(f"❌ Timestamp data not available for {plot_title}")
        return False
                
    # Check for missing variables
    missing_vars = [var for var in selected_vars if var not in df.columns]
    if missing_vars:
        available_vars = [var for var in selected_vars if var in df.columns]
        if not available_vars:
            st.error(f"❌ None of the selected variables are available for {plot_title}")
            return False
        selected_vars = available_vars
        st.info(f"ℹ️ Plotting available variables: {', '.join(selected_vars)}")
    
    try:
        # Optimized data preparation - limit to essential columns
        plot_data = df[['TIMESTAMP'] + selected_vars].copy()
        
        # Fast data cleaning
        plot_data = plot_data.dropna(subset=['TIMESTAMP'])
        plot_data = plot_data.set_index('TIMESTAMP')
        
        # Limit data points for better performance (max 1000 points)
        if len(plot_data) > 1000:
            plot_data = plot_data.iloc[::len(plot_data)//1000]
        
        if plot_data.empty:
            st.warning(f"⚠️ No valid data available for {plot_title}")
            return False
        
        # Create plot
        st.line_chart(plot_data, height=height, width='stretch')
        st.caption(f"📊 {plot_title}: {', '.join(selected_vars)} ({len(plot_data)} data points)")
        return True
        
    except Exception as e:
        st.error(f"❌ Error creating plot for {plot_title}: {str(e)}")
        return False


    

# =============================================================================
# MAIN DASHBOARD LOGIC
# =============================================================================

# Get available stations (cached)
available_stations = get_available_stations_cached()

if not available_stations:
    st.error("❌ No stations found in the directory 'data/interim'.")
    
    # Show fresh install instructions
    st.markdown("""
    ## 🚀 Fresh Installation Detected
    
    It looks like this is a fresh installation with no data yet.
    
    ### 📋 Getting Started
    
    To populate the dashboard with data, use Airflow to trigger the data pipeline:
    
    **Using Airflow UI:**
    1. Access Airflow at `http://localhost:8080`
    2. Trigger the `initial_data_setup` or `refresh_data_pipeline` DAG
    
    **Using Command Line:**
    ```bash
    # Trigger the complete refresh pipeline
    docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline
    
    # Or trigger the initial setup (for fresh installs)
    docker-compose exec airflow-apiserver airflow dags trigger initial_data_setup
    ```
    
    **Data Pipeline Process:**
    1. 📥 Downloads raw data from FTP servers
    2. 🔄 Processes data into dashboard format
    3. ✅ Verifies data quality
    
    **Note:** This process may take 15-60 minutes depending on data size and number of stations.
    
    The dashboard will automatically detect new data when it becomes available.
    """)
    
    st.stop()

# Get station metadata (cached)
station_metadata = get_station_metadata_cached(available_stations)

# Station selector - Radio buttons in horizontal layout
st.markdown("---")
st.markdown('<h3 style="text-align: center; color: #2c3e50; margin-bottom: 1rem;">📡 Select Station</h3>', unsafe_allow_html=True)

# Create radio buttons for station selection
selected_station = st.radio(
        "Select Station:",
        options=available_stations,
    format_func=lambda x: x.upper(),
    horizontal=True,
    label_visibility="collapsed"
)

st.markdown("---")
st.markdown(f'<h2 class="station-header">Station: {selected_station.upper()}</h2>', unsafe_allow_html=True)

# Load data for selected station (cached) with loading indicator
with st.spinner(f"Loading data for station {selected_station.upper()}..."):
    data_dict = load_latest_data_for_station_cached(selected_station)

# Data Overview Section
if data_dict is not None and len(data_dict) > 0:
    st.header("📊 Data Overview")

    # Display file information - optimized
    file_info = []
    for data_type, df in data_dict.items():
        timestamp_col = 'TIMESTAMP'
        time_range = "N/A"
        if timestamp_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            # Use faster min/max operations
            time_min = df[timestamp_col].min()
            time_max = df[timestamp_col].max()
            # Check for NaT (Not a Time) values before formatting
            if pd.notna(time_min) and pd.notna(time_max):
                time_range = f"{time_min.strftime('%m-%d %H:%M')} to {time_max.strftime('%m-%d %H:%M')}"
            else:
                time_range = "Invalid timestamps"
        
        # Get file modification time more efficiently
        try:
            file_path = df['file_path'].iloc[0] if 'file_path' in df.columns else ""
            last_modified = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%m-%d %H:%M') if file_path else "N/A"
        except:
            last_modified = "N/A"
            
        file_info.append({
            'Data Type': data_type,
            'Records': len(df),
            'Columns': len(df.columns),
            'Time Range': time_range,
            'Last Modified': last_modified
        })
    
    # Display metrics
    cols = st.columns(len(file_info))
    for i, row in enumerate(file_info):
        with cols[i]:
            st.metric(
                label=f"{row['Data Type']} Data",
                value=f"{row['Records']:,}",
                delta=f"{row['Columns']} cols"
            )
            st.caption(f"Updated: {row['Last Modified']}")

    # Data Visualization Section
    st.header("📈 Real-time Data Visualization")
    col1, col2, col3 = st.columns([1, 1, 1])

    # Column 1: Solar Data (SD)
    with col1:
        st.subheader("☀️ Solar Data (SD)")
        
        if 'SD' in data_dict:
            df = data_dict['SD'].copy()
            
            # Get station metadata for clear sky calculation (only if needed)
            if any('glo' in var.lower() for var in df.columns):
                filtered_row = station_metadata[station_metadata['station'].str.lower() == selected_station.lower()]

            if not filtered_row.empty:
                try:
                    latitude = filtered_row['latitude'].values[0]
                    longitude = filtered_row['longitude'].values[0]
                    
                    # Apply clear sky model (cached) - only for solar data
                    df = calculate_clearsky_ineichen_cached(df, latitude, longitude, tz='America/Sao_Paulo')
                except Exception as e:
                    st.warning(f"⚠️ Could not calculate clear sky data: {str(e)}")
            
            available_vars = get_available_variables_optimized(df)
            
            if available_vars and 'TIMESTAMP' in df.columns:
                # Solar Radiation Plot
                st.markdown('<p class="subsection-header">Solar Radiation</p>', unsafe_allow_html=True)
                solar_vars = [var for var in available_vars if any(x in var.lower() for x in ['glo', 'dir', 'dif']) and any(x in var.lower() for x in ['avg', 'std'])]
                
                clearsky_vars = ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']
                available_clearsky_vars = [var for var in clearsky_vars if var in df.columns]
                
                if solar_vars or available_clearsky_vars:
                    all_solar_vars = solar_vars + available_clearsky_vars
                    ghi_vars = [var for var in all_solar_vars if 'glo' in var.lower() or var == 'clearsky_GHI']
                    default_solar_vars = ghi_vars if ghi_vars else all_solar_vars[:2]
                    
                    selected_solar = create_variable_selector_optimized(all_solar_vars, default_solar_vars, "solar")
                    plot_selected_variables_optimized(df, selected_solar, "Solar Radiation")
                else:
                    st.info("No solar radiation variables found.")
                
                # Other plots...
                st.markdown('<p class="subsection-header">Longwave Radiation</p>', unsafe_allow_html=True)
                lw_vars = [var for var in available_vars if 'lw' in var.lower() and any(x in var.lower() for x in ['avg', 'std'])]
                if lw_vars:
                    selected_lw = create_variable_selector_optimized(lw_vars, lw_vars, "longwave")
                    plot_selected_variables_optimized(df, selected_lw, "Longwave Radiation")
                else:
                    st.info("No longwave radiation variables found.")
            else:
                st.warning("No numeric data available for SD.")
        else:
            st.info("No SD data available for this station.")

    # Column 2: Environmental Data (MD and WD)
    with col2:
        st.subheader("🌤️ Environmental Data")
        
        # Meteorological Data
        st.markdown('<p class="subsection-header">Meteorological Data</p>', unsafe_allow_html=True)
        if 'MD' in data_dict:
            df_md = data_dict['MD'].copy()
            available_vars_md = get_available_variables_optimized(df_md)
            
            if available_vars_md and 'TIMESTAMP' in df_md.columns:
                meteo_vars = [var for var in available_vars_md if any(x in var.lower() for x in ['tp_sfc', 'humid', 'press', 'rain'])]
                if meteo_vars:
                    selected_meteo = create_variable_selector_optimized(meteo_vars, meteo_vars, "meteorological")
                    plot_selected_variables_optimized(df_md, selected_meteo, "Meteorological Data")
                else:
                    st.info("No meteorological variables found in MD data.")
            else:
                st.warning("No numeric data available for MD.")
        else:
            st.info("No MD data available for this station.")
        
        # Wind Data
        st.markdown('<p class="subsection-header">Wind Data</p>', unsafe_allow_html=True)
        
        wind_heights = ['10m', '25m', '50m']
        for height in wind_heights:
            wind_vars = []
            df_source = None
            
            for data_type in ['MD', 'WD']:
                if data_type in data_dict:
                    df_temp = data_dict[data_type].copy()
                    available_vars_temp = get_available_variables_optimized(df_temp)
                    height_vars = [var for var in available_vars_temp if height in var]
                    if height_vars:
                        wind_vars = height_vars
                        df_source = df_temp
                        break
            
            if wind_vars and df_source is not None:
                st.write(f"**Wind at {height}**")
                selected_wind = create_variable_selector_optimized(wind_vars, wind_vars, f"wind_{height}")
                plot_selected_variables_optimized(df_source, selected_wind, f"Wind at {height}", height=200)
            else:
                st.info(f"No wind data available at {height}.")

    # Column 3: Detailed View
    with col3:
        st.subheader("🔍 Detailed View")
        
        # Collect variables for detailed view - optimized
        detailed_variables = []
        
        if 'SD' in data_dict:
            df_sd = data_dict['SD'].copy()
            
            # Apply clear sky calculation if needed (only for solar data)
            filtered_row = station_metadata[station_metadata['station'].str.lower() == selected_station.lower()]
            if not filtered_row.empty:
                try:
                    latitude = filtered_row["latitude"].values[0]
                    longitude = filtered_row["longitude"].values[0]
                    df_sd = calculate_clearsky_ineichen_cached(df_sd, latitude, longitude, tz="America/Sao_Paulo")
                except:
                    pass                        
            
            available_vars_sd = get_available_variables_optimized(df_sd)
            
            # Add variables to detailed view
            solar_vars = [var for var in available_vars_sd if any(x in var.lower() for x in ['glo', 'dir', 'dif']) and any(x in var.lower() for x in ['avg', 'std'])]
            detailed_variables.extend([f"SD: {var}" for var in solar_vars])
            
            clearsky_vars = ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']
            available_clearsky_vars = [var for var in clearsky_vars if var in df_sd.columns]
            detailed_variables.extend([f"SD: {var}" for var in available_clearsky_vars])
        
        if detailed_variables:
            selected_detailed_var = st.selectbox(
                "Choose variable:",
                options=detailed_variables,
                key="detailed_var_selector"
            )
            
            if selected_detailed_var:
                data_type, var_name = selected_detailed_var.split(": ", 1)
                df_detail = data_dict[data_type].copy()
                
                if 'TIMESTAMP' in df_detail.columns and var_name in df_detail.columns:
                    st.write(f"**{var_name} ({data_type})**")
                    
                    plot_data = df_detail[['TIMESTAMP', var_name]].set_index('TIMESTAMP')
                    st.line_chart(plot_data, height=400, width='stretch')
                    
                    # Show statistics
                    col_stat1, col_stat2 = st.columns(2)
                    with col_stat1:
                        st.metric("Mean", f"{df_detail[var_name].mean():.2f}")
                        st.metric("Min", f"{df_detail[var_name].min():.2f}")
                    with col_stat2:
                        st.metric("Max", f"{df_detail[var_name].max():.2f}")
                        st.metric("Std Dev", f"{df_detail[var_name].std():.2f}")
        else:
            st.info("No variables available for detailed view.")

    # Raw Data Section (collapsible)
    with st.expander("📋 Raw Data (Click to expand)"):
        for data_type, df in data_dict.items():
            st.write(f"**{selected_station.upper()} - {data_type} Data**")
            st.write(f"Shape: {df.shape}")
            st.dataframe(df.head(20), width='stretch')

else:
    st.error(f"❌ No recent data available for station '{selected_station.upper()}'")
    
    # Check if files exist but data is old - optimized with pathlib
    station_path = Path("data/interim") / selected_station
    
    if station_path.exists():
        # Use pathlib for faster file listing
        parquet_files = list(station_path.glob("*.parquet"))
        
        if parquet_files:
            # Files exist but no data in last 72 hours
            st.warning("📅 **Data is Outdated**")
            st.markdown(f"""
            **Station {selected_station.upper()} has data files, but no data from the last 72 hours.**
            
            **Situation:**
            - ✅ Data files exist: {len(parquet_files)} file(s) found
            - ⚠️ All data is older than 72 hours
            - 📊 Dashboard only displays data from the last 72 hours
            
            **Most recent file:**
            """)
            
            # Show most recent file info - optimized
            try:
                latest_file = max(parquet_files, key=lambda f: f.stat().st_mtime)
                file_mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)
                hours_old = (datetime.now() - file_mtime).total_seconds() / 3600
                
                st.info(f"""
                📁 **{latest_file.name}**
                - Last modified: {file_mtime.strftime('%Y-%m-%d %H:%M:%S')}
                - Age: {hours_old:.1f} hours ({hours_old/24:.1f} days)
                """)
            except Exception as e:
                st.warning(f"Could not read file information: {e}")
            
            st.markdown("""
            **Possible causes:**
            - Station is not currently transmitting data
            - FTP server has not received recent data from this station
            - Station equipment may be offline or malfunctioning
            
            **What to do:**
            1. Check if the station is operational
            2. Verify the FTP server has recent data for this station
            3. Run the download pipeline via Airflow to get any new data
            4. Contact station operators if the station appears to be offline
            """)
        else:
            # Directory exists but no parquet files
            st.warning("🚨 **No Data Files Found**")
            st.markdown("""
            **No processed data files exist for this station.**
    
    **Possible causes:**
            - Data processing pipeline has not completed yet
            - Processing failed for this station
    
    **What to do:**
            1. Check Airflow logs for processing errors
            2. Run the processing pipeline via Airflow
            """)
    else:
        # No directory at all
        st.warning("🚨 **Station Directory Not Found**")
        st.markdown("""
        **No data directory exists for this station.**
        
        **Possible causes:**
        - Station has never been downloaded
        - FTP download failed for this station
        - Station name may be incorrect
        
        **What to do:**
        1. Check Airflow logs for download/processing errors
        2. Verify the station name is correct
        3. Run the data pipeline via Airflow to download and process data
        
        **Airflow Commands:**
        ```bash
        # Trigger complete data refresh
        docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline
        ```
    """)

# Footer
st.markdown("---")
st.markdown('<p style="text-align: center; color: #666; font-size: 0.9rem;">Solar Data Monitoring System - Crisis Room Dashboard</p>', unsafe_allow_html=True)
