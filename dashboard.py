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
import requests
import time
import json
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
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Real-time Solar Radiation & Meteorological Data (Last 24 Hours)</p>', unsafe_allow_html=True)


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
    files = list(station_path_obj.glob("*.parquet"))
    
    # Group files by data type (SD, MD, WD)
    files_by_type = {}
    for file in files:
        filename = file.name
        # Extract data type from filename
        match = re.search(r'_([A-Z]{2})_\d{8}_\d{6}\.parquet$', filename)
        if match:
            data_type = match.group(1)
            if data_type not in files_by_type:
                files_by_type[data_type] = []
            files_by_type[data_type].append(str(file))
    
    # Get the latest file for each data type
    latest_files = {}
    for data_type, file_list in files_by_type.items():
        if file_list:
            # Sort by modification time and get the latest
            latest_file = max(file_list, key=os.path.getmtime)
            latest_files[data_type] = latest_file
    
    return latest_files

def filter_last_24_hours(df, timestamp_col='TIMESTAMP'):
    """Filter dataframe to last 24 hours of data"""
    if timestamp_col not in df.columns:
            return df
    
    # Ensure timestamp column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
    
    # Get current time and 24 hours ago
    now = datetime.now()
    cutoff_time = now - timedelta(hours=24)
    
    # Filter data to last 24 hours
    filtered_df = df[df[timestamp_col] >= cutoff_time].copy()
    
    return filtered_df

@st.cache_data(ttl=15)  # Cache for 15 seconds - reduced for faster detection of new data
def load_latest_data_for_station_cached(station):
    """Load data from the latest files for a specific station - CACHED (Last 24 hours only)"""
    latest_files = get_latest_files_for_station_cached(station)
    
    if not latest_files:
        return None
    
    data_dict = {}
    failed_files = []
    
    for data_type, file_path in latest_files.items():
        try:
            # Use optimized parquet reading with specific columns if possible
            df = pd.read_parquet(file_path, engine='pyarrow')
            
            if df.empty:
                failed_files.append(f"{data_type} (empty file)")
                continue
            
            # Fast timestamp conversion first
            df = safe_convert_timestamp_optimized(df)
            
            # Filter to last 24 hours BEFORE cleaning (much faster)
            df = filter_last_24_hours(df)
            
            if df.empty:
                failed_files.append(f"{data_type} (no data in last 24 hours)")
                continue
            
            # Only clean numeric columns if we have data
            df = clean_numeric_columns_optimized(df)
            
            # Add minimal metadata
            df['source_file'] = os.path.basename(file_path)
            df['station'] = station
            df['data_type'] = data_type
            df['file_path'] = file_path
            
            data_dict[data_type] = df
            
        except Exception as e:
            failed_files.append(f"{data_type} (error: {str(e)[:50]}...)")
            continue
    
    if failed_files:
        st.warning(f"⚠️ Could not load some data files for station {station.upper()}: {', '.join(failed_files)}")
    
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


def refresh_data_pipeline(airflow_client, selected_station: str = None) -> Dict[str, bool]:
    """
    Execute the complete data refresh pipeline using the new refresh_data_pipeline DAG:
    1. Trigger the refresh pipeline DAG
    2. Monitor the pipeline progress
    """
    logger.info("Starting refresh data pipeline")
    
    results = {
        'pipeline': False,
        'overall': False
    }
    
    if not airflow_client.connected:
        logger.error("Not connected to Airflow")
        st.error("❌ Not connected to Airflow. Please check your connection settings.")
        return results
    
    try:
        # Trigger the refresh pipeline DAG
        logger.info("Triggering refresh_data_pipeline DAG")
        st.info("🔄 Triggering complete data refresh pipeline...")
        
        pipeline_success = airflow_client.trigger_dag('refresh_data_pipeline')
        results['pipeline'] = pipeline_success
        
        if not pipeline_success:
            logger.error("Failed to trigger refresh pipeline DAG")
            st.error("❌ Failed to trigger refresh pipeline DAG")
            return results
            
        logger.info("Refresh pipeline DAG triggered successfully")
        st.success("✅ Refresh pipeline started successfully")
        
        # Give Airflow time to register the DAG run
        time.sleep(3)
        
        # Monitor the pipeline progress
        st.info("⏳ Monitoring pipeline progress...")
        pipeline_timeout = 45  # 45 minutes total timeout
        
        pipeline_complete = monitor_dag_progress(
            airflow_client, 
            'refresh_data_pipeline', 
            timeout_minutes=pipeline_timeout,
            wait_for_all_tasks=False
        )
        
        if pipeline_complete:
            st.success("✅ Data refresh pipeline completed successfully")
            results['overall'] = True
        else:
            st.warning("⚠️ Pipeline did not complete within timeout period. Check Airflow UI for details.")
        
    except Exception as e:
        logger.error(f"Error in refresh pipeline: {str(e)}")
        st.error(f"❌ Error in refresh pipeline: {str(e)}")
    
    return results

def monitor_dag_progress(airflow_client, dag_id: str, timeout_minutes: int = 10, wait_for_all_tasks: bool = False):
    """Monitor DAG execution progress with a progress bar and actual status checking"""
    logger.info(f"Monitoring {dag_id} with timeout of {timeout_minutes} minutes")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    st.info(f"🕐 Monitoring {dag_id} (timeout: {timeout_minutes} minutes)")
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Track the DAG run we're monitoring
    monitored_run_id = None
    
    while time.time() - start_time < timeout_seconds:
        try:
            elapsed = int(time.time() - start_time)
            
            token = airflow_client._get_jwt_token()
            if not token:
                status_text.warning(f"⚠️ Lost authentication token")
                break
                
            headers = {"Authorization": f"Bearer {token}"}
            
            # Get the latest DAG run
            response = requests.get(
                f"{airflow_client.base_url}/api/v2/dags/{dag_id}/dagRuns",
                headers=headers,
                timeout=5
            )
            
            if response.status_code == 200:
                dag_runs = response.json().get('dag_runs', [])
                if dag_runs:
                    # Find the most recent run that we're monitoring
                    if monitored_run_id is None:
                        latest_run = dag_runs[0]
                        monitored_run_id = latest_run.get('dag_run_id')
                        logger.info(f"Monitoring run: {monitored_run_id}")
                        status_text.info(f"🔄 Monitoring DAG run: {monitored_run_id}")
                    else:
                        # Find the specific run we're monitoring
                        latest_run = None
                        for run in dag_runs:
                            if run.get('dag_run_id') == monitored_run_id:
                                latest_run = run
                                break
                        
                        if latest_run is None:
                            status_text.warning(f"⚠️ Could not find monitored run - retrying...")
                            time.sleep(5)
                            continue
                    
                    state = latest_run.get('state', 'unknown')
                    
                    progress = min((time.time() - start_time) / timeout_seconds, 0.9)
                    progress_bar.progress(progress)
                    
                    if state == 'success':
                        status_text.success(f"✅ {dag_id} completed successfully!")
                        progress_bar.progress(1.0)
                        return True
                    elif state == 'failed':
                        status_text.error(f"❌ {dag_id} failed!")
                        return False
                    elif state in ['running', 'queued']:
                        status_text.info(f"🔄 {dag_id} is {state}... ({elapsed}s elapsed)")
                    else:
                        status_text.info(f"🔄 {dag_id} status: {state}... ({elapsed}s elapsed)")
                else:
                    status_text.info(f"🔄 Waiting for {dag_id} to start... ({elapsed}s elapsed)")
            else:
                status_text.warning(f"⚠️ Error checking DAG status: {response.status_code} - retrying...")
                
        except Exception as e:
            status_text.warning(f"⚠️ Error monitoring {dag_id}: {str(e)} - retrying...")
        
        time.sleep(10)  # Check every 10 seconds
    
    progress_bar.progress(1.0)
    status_text.warning(f"⏰ {dag_id} monitoring timed out after {timeout_minutes} minutes")
    return False


def create_refresh_button(airflow_client, selected_station: str):
    """Create the refresh button with progress monitoring"""
    
    # Refresh button section
    st.markdown("---")
    st.markdown('<h3 style="text-align: center; color: #2c3e50;">🔄 Data Refresh (Last 24 Hours)</h3>', unsafe_allow_html=True)
    
    # Connection status
    if airflow_client.connected:
        st.success("✅ Connected to Airflow")
    else:
        st.error("❌ Not connected to Airflow. Please check your connection settings.")
        st.info("💡 Make sure Airflow is running and the configuration is correct.")
        
        # Show manual instructions
        st.markdown("### 🔧 Manual DAG Triggering")
        st.markdown("""
        Since the automatic connection is not working, you can manually trigger the DAGs using these commands:
        
        **To trigger the complete refresh pipeline:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline
        ```
        
        **To trigger individual DAGs:**
        ```bash
        # Download data
        docker-compose exec airflow-apiserver airflow dags trigger ftp_multi_station_download
        
        # Process data (after download completes)
        docker-compose exec airflow-apiserver airflow dags trigger process_multistation_data
        ```
        
        **To check DAG status:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags state refresh_data_pipeline
        docker-compose exec airflow-apiserver airflow dags state ftp_multi_station_download
        docker-compose exec airflow-apiserver airflow dags state process_multistation_data
        ```
        """)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button("🔄 Refresh Latest Data (24 Hours)", type="primary", width='stretch'):
            # Clear any previous status messages
            st.empty()
            
            # Execute refresh pipeline (this handles all monitoring internally)
            results = refresh_data_pipeline(airflow_client, selected_station)
            
            if results['overall']:
                st.success("🎉 Data refresh pipeline completed successfully!")
                st.info("💡 Refreshing dashboard with latest data...")
                
                # Force refresh of cached data
                st.cache_data.clear()
                time.sleep(2)  # Give a moment for the success message to be visible
                st.rerun()
            else:
                st.warning("⚠️ Pipeline did not complete successfully. Check Airflow UI for details.")
    
    # Add cache clearing button for manual use
    with col1:
        if st.button("🧹 Clear Cache", type="secondary", help="Clear dashboard cache to detect new data"):
            st.cache_data.clear()
            st.success("✅ Cache cleared!")
            st.info("💡 Refreshing page...")
            time.sleep(1)
            st.rerun()
    
    with col3:
        if st.button("🔄 Force Refresh", type="secondary", help="Force refresh the entire dashboard"):
            st.cache_data.clear()
            st.rerun()
    
# =============================================================================
# OPTIMIZED AIRFLOW INTEGRATION
# =============================================================================

@st.cache_data(ttl=60)  # Cache config for 1 minute (reduced for faster updates)
def load_airflow_config_cached():
    """Cached Airflow configuration loading"""
    config_file = "airflow_config.json"
    
    default_config = {
        "airflow": {
            "base_url": "http://localhost:8080",
            "username": "admin",
            "password": "admin",
            "timeout_minutes": {"download": 60, "process": 30}
        },
        "dags": {
            "download_dag": "ftp_multi_station_download",
            "process_dag": "process_multistation_data"
        }
    }
    
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                default_config.update(file_config)
    except Exception as e:
        st.warning(f"⚠️ Could not load Airflow config: {str(e)}")
    
    # Override with environment variables
    airflow_config = default_config.get("airflow", {})
    airflow_config["base_url"] = os.getenv("AIRFLOW_BASE_URL", airflow_config.get("base_url", "http://localhost:8080"))
    airflow_config["username"] = os.getenv("AIRFLOW_USERNAME", airflow_config.get("username", "admin"))
    airflow_config["password"] = os.getenv("AIRFLOW_PASSWORD", airflow_config.get("password", "admin"))
    
    return default_config

class AirflowClientOptimized:
    """Optimized Airflow client with JWT authentication"""
    
    def __init__(self, config: Dict = None):
        if config is None:
            config = load_airflow_config_cached()
        
        airflow_config = config.get('airflow', {})
        self.base_url = airflow_config.get('base_url', 'http://localhost:8080').rstrip('/')
        self.username = airflow_config.get('username', 'admin')
        self.password = airflow_config.get('password', 'admin')
        self.timeout_minutes = airflow_config.get('timeout_minutes', {'download': 5, 'process': 10})
        
        # Cache connection status and JWT token
        self._connection_status = None
        self._connection_check_time = 0
        self._jwt_token = None
        self._token_expiry = 0
    
    @property
    def connected(self):
        """Cached connection status"""
        current_time = time.time()
        if self._connection_status is None or (current_time - self._connection_check_time) > 60:  # Check every minute
            self._connection_status = self._test_connection()
            self._connection_check_time = current_time
        return self._connection_status
    
    def _get_jwt_token(self):
        """Get JWT token for authentication"""
        current_time = time.time()
        
        # Check if we have a valid token
        if self._jwt_token and current_time < self._token_expiry:
            return self._jwt_token
        
        try:
            # Request new JWT token
            token_url = f"{self.base_url}/auth/token"
            token_data = {
                "username": self.username,
                "password": self.password
            }
            
            response = requests.post(token_url, json=token_data, timeout=10)
            if response.status_code in [200, 201]:
                token_response = response.json()
                self._jwt_token = token_response.get('access_token')
                
                # Set token expiry (JWT tokens typically last 24 hours)
                self._token_expiry = current_time + (24 * 60 * 60)
                
                return self._jwt_token
            else:
                print(f"Token request failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error getting JWT token: {e}")
            return None
    
    def _test_connection(self):
        """Test connection with JWT authentication"""
        try:
            token = self._get_jwt_token()
            if not token:
              return False
    
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{self.base_url}/api/v2/dags", headers=headers, timeout=5)
            return response.status_code == 200

        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None) -> bool:
        """Optimized DAG triggering with JWT authentication"""
        logger.info(f"Attempting to trigger DAG: {dag_id}")
        try:
            token = self._get_jwt_token()
            if not token:
                logger.error("Failed to get authentication token")
                st.error("❌ Failed to get authentication token")
                return False
                
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            request_body = {
                "logical_date": datetime.now().isoformat() + "Z",
                "dag_run_id": f"manual_trigger_{int(time.time())}"
            }
            
            if conf:
                request_body["conf"] = conf
            
            logger.info(f"POST {url}")
            logger.debug(f"Request body: {request_body}")
            response = requests.post(url, json=request_body, headers=headers, timeout=10)
            logger.info(f"Response status: {response.status_code}")
            logger.debug(f"Response text: {response.text}")
            
            success = response.status_code in [200, 201]
            logger.info(f"Trigger DAG {dag_id} result: {success}")
            return success
        
        except Exception as e:
            logger.error(f"Error triggering DAG {dag_id}: {str(e)}", exc_info=True)
            st.error(f"❌ Error triggering DAG {dag_id}: {str(e)}")
        return False

# =============================================================================
# MAIN DASHBOARD LOGIC
# =============================================================================

# Check if we should clear cache (e.g., after initial setup completion)
def should_clear_cache():
    """Check if we should clear cache based on recent DAG runs"""
    try:
        # Check if initial_data_setup DAG completed recently (within last 10 minutes)
        import time
        import os
        
        # Look for recent completion markers or check DAG status
        # This is a simple heuristic - in production you might want to check Airflow API
        cache_clear_file = "logs/dashboard/cache_clear_needed"
        if os.path.exists(cache_clear_file):
            # Check if file is recent (within last 10 minutes)
            file_mtime = os.path.getmtime(cache_clear_file)
            if time.time() - file_mtime < 600:  # 10 minutes
                os.remove(cache_clear_file)  # Remove the marker
                return True
    except:
        pass
    return False

# Clear cache if needed
if should_clear_cache():
    st.cache_data.clear()

# Get available stations (cached)
available_stations = get_available_stations_cached()

if not available_stations:
    st.error("❌ No stations found in the directory 'data/interim'.")
    
    # Show fresh install instructions
    st.markdown("""
    ## 🚀 Fresh Installation Detected
    
    It looks like this is a fresh installation with no data yet. Here's how to get started:
    
    ### Option 1: Automatic Setup (Recommended)
    """)
    
    # Add manual cache clearing option
    st.markdown("### 🔧 Manual Cache Management")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🧹 Clear Dashboard Cache", type="secondary"):
            st.cache_data.clear()
            st.success("✅ Dashboard cache cleared!")
            st.info("💡 Refreshing page to detect any new data...")
            time.sleep(1)
            st.rerun()
    
    with col2:
        if st.button("🔄 Force Refresh Page", type="secondary"):
            st.rerun()
    
    if st.button("🔄 Initialize Data Pipeline", type="primary", width='stretch'):
        st.info("🔄 This will trigger the initial data setup DAG. Please wait...")
        
        try:
            airflow_client = AirflowClientOptimized()
            if airflow_client.connected:
                success = airflow_client.trigger_dag('initial_data_setup')
                if success:
                    st.success("✅ Initial data setup DAG triggered successfully!")
                    st.markdown("""
                    **🔄 Data Pipeline Started**
                    
                    The system is now:
                    1. 📥 Downloading raw data from FTP servers
                    2. 🔄 Processing data into dashboard format
                    3. ✅ Verifying data quality
                    
                    **This process may take 15-60 minutes depending on data size and number of stations.**
                    
                    **Please refresh this page in a few minutes to see the results.**
                    """)
                    
                    # Clear all caches to ensure fresh data detection
                    st.cache_data.clear()
                    st.info("🧹 Dashboard cache cleared - will detect new data when available")
                    
                    if st.button("🔄 Refresh Page Now", type="secondary"):
                        st.rerun()
                else:
                    st.error("❌ Failed to trigger initial data setup DAG")
            else:
                st.error("❌ Not connected to Airflow")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
    
    st.stop()

# Get station metadata (cached)
station_metadata = get_station_metadata_cached(available_stations)

# Station selector
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    selected_station = st.selectbox(
        "Select Station:",
        options=available_stations,
        index=0,
        format_func=lambda x: x.upper()
    )

st.markdown(f'<h2 class="station-header">Station: {selected_station.upper()} (Last 24 Hours)</h2>', unsafe_allow_html=True)

# Initialize optimized Airflow client
airflow_client = AirflowClientOptimized()

# Create refresh button section
create_refresh_button(airflow_client, selected_station)

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
            time_range = f"{time_min.strftime('%m-%d %H:%M')} to {time_max.strftime('%m-%d %H:%M')}"
        
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
        
        # Collect variables for detailed view
        detailed_variables = []
        
        if 'SD' in data_dict:
            df_sd = data_dict['SD'].copy()
            
            # Apply clear sky calculation if needed (only for solar data)
            if any('glo' in var.lower() for var in df_sd.columns):
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
    st.error(f"❌ No data files found for station '{selected_station.upper()}'")
    st.warning("🚨 **Data Download Failed**")
    st.markdown("""
    **This station failed to download data from the FTP server.**
    
    **Possible causes:**
    - FTP connection issues
    - Station data not available on the server
    - Network connectivity problems
    - Server-side data processing errors
    
    **What to do:**
    1. Check the Airflow logs for download errors
    2. Verify FTP connection settings
    3. Try running the download pipeline again
    4. Contact system administrator if the issue persists
    
    **Note:** No sample data will be created. Only real data from the FTP server is used.
    """)

# Footer
st.markdown("---")
st.markdown('<p style="text-align: center; color: #666; font-size: 0.9rem;">Solar Data Monitoring System - Crisis Room Dashboard</p>', unsafe_allow_html=True)
