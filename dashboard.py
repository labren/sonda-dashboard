# Create a new file: dashboard.py
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
st.markdown('<p style="text-align: center; font-size: 1.2rem; color: #666;">Real-time Solar Radiation & Meteorological Data</p>', unsafe_allow_html=True)


def calculate_clearsky_ineichen(df, latitude, longitude, altitude=0, tz=None):
    """
    Adiciona ao DataFrame estimativas de irradiância de céu limpo (GHI, DNI, DHI)
    usando o modelo Ineichen da biblioteca pvlib.

    Se o timezone (tz) não for especificado, ele será detectado automaticamente com base na latitude e longitude.
    """
    
    df = df.copy()
    
    # Find the timestamp column
    timestamp_col = 'TIMESTAMP'
    if 'TIMESTAMP' not in df.columns:
        # Look for common timestamp column names
        possible_timestamp_cols = ['TIMESTAMP', 'timestamp', 'Timestamp', 'time', 'Time', 'TIME', 'date', 'Date', 'DATE', 'datetime', 'Datetime', 'DATETIME']
        for col in possible_timestamp_cols:
            if col in df.columns:
                timestamp_col = col
                # Rename to TIMESTAMP for consistency
                df = df.rename(columns={col: 'TIMESTAMP'})
                break
        else:
            raise ValueError(f"No timestamp column found. Available columns: {df.columns.tolist()}")
    
    # Garante datetime com timezone
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
    df = df.dropna(subset=['TIMESTAMP'])

    # Detecta timezone automaticamente se não for fornecido
    if tz is None:
        tf = TimezoneFinder()
        tz = tf.timezone_at(lat=latitude, lng=longitude)
        if tz is None:
            raise ValueError("Não foi possível determinar o timezone automaticamente. Forneça um valor para o parâmetro 'tz'.")

    # Localiza ou converte o timezone
    if df['TIMESTAMP'].dt.tz is None:
        df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_convert(tz)

    # Usa DatetimeIndex
    df = df.set_index('TIMESTAMP')

    # Cria o objeto Location
    location = pvlib.location.Location(latitude, longitude, tz=tz, altitude=altitude)

    # Calcula céu limpo com DatetimeIndex
    clearsky = location.get_clearsky(df.index)

    # Adiciona ao DataFrame
    df['clearsky_GHI'] = clearsky['ghi']
    df['clearsky_DNI'] = clearsky['dni']
    df['clearsky_DHI'] = clearsky['dhi']

    return df.reset_index()



# def calculate_clearsky_ineichen(df, latitude, longitude, altitude=0, tz='UTC'):
#     """
#     Adiciona ao DataFrame estimativas de irradiância de céu limpo (GHI, DNI, DHI)
#     usando o modelo Ineichen da biblioteca pvlib.
#     """
    
#     df = df.copy()
    
#     # Garante datetime com timezone
#     df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], errors='coerce')
#     df = df.dropna(subset=['TIMESTAMP'])

#     # Localiza ou converte o timezone
#     if df['TIMESTAMP'].dt.tz is None:
#         df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_localize(tz)
#     else:
#         df['TIMESTAMP'] = df['TIMESTAMP'].dt.tz_convert(tz)

#     # Usa DatetimeIndex
#     df = df.set_index('TIMESTAMP')

#     # Cria o objeto Location
#     location = pvlib.location.Location(latitude, longitude, tz=tz, altitude=altitude)

#     # Calcula céu limpo com DatetimeIndex
#     clearsky = location.get_clearsky(df.index)

#     # Adiciona ao DataFrame
#     df['clearsky_GHI'] = clearsky['ghi']
#     df['clearsky_DNI'] = clearsky['dni']
#     df['clearsky_DHI'] = clearsky['dhi']

#     return df.reset_index()


# Function to get available stations
@st.cache_data
def get_available_stations():
    """Get list of available stations from the interim directory"""
    interim_dir = os.path.expanduser("data/interim")
    
    if not os.path.exists(interim_dir):
        return []
    
    # Normaliza os nomes das pastas das estações
    station_dirs = [d.strip().lower() for d in os.listdir(interim_dir) if os.path.isdir(os.path.join(interim_dir, d))]
    return sorted(station_dirs)


# Function to get available metadata
@st.cache_data
def get_station_metadata(station_dirs):
    """Get metadata of available stations (latitude and longitude), filtered by actual station folders"""
    interim_dir = os.path.expanduser("data/interim")
    location_csv = os.path.join(interim_dir, 'INPESONDA_Stations.csv')

    if not os.path.exists(interim_dir) or not os.path.exists(location_csv):
        st.warning("Arquivo de localização 'INPESONDA_Stations.csv' não encontrado.")
        return pd.DataFrame(columns=['station', 'latitude', 'longitude'])

    # Lê e normaliza os nomes das estações do CSV
    df_locations = pd.read_csv(location_csv)
    df_locations['station_normalized'] = df_locations['station'].astype(str).str.strip().str.lower()

    # Filtra as estações presentes nas pastas
    df_filtered = df_locations[df_locations['station_normalized'].isin(station_dirs)].copy()
    df_filtered = df_filtered.drop_duplicates(subset='station_normalized')

    return df_filtered.sort_values('station').reset_index(drop=True)


# Function to get the latest file for a specific station and data type
@st.cache_data
def get_latest_files_for_station(station):
    """Get the latest parquet file for a specific station and data type (SD, MD, WD)"""
    interim_dir = os.path.expanduser("data/interim")
    station_path = os.path.join(interim_dir, station)
    
    if not os.path.exists(station_path):
        return {}
    
    files = glob.glob(os.path.join(station_path, "*.parquet"))
    
    # Group files by data type (SD, MD, WD)
    files_by_type = {}
    for file in files:
        filename = os.path.basename(file)
        # Extract data type from filename (e.g., processed_data_PTR_SD_20250702_193759.parquet)
        match = re.search(r'_([A-Z]{2})_\d{8}_\d{6}\.parquet$', filename)
        if match:
            data_type = match.group(1)
            if data_type not in files_by_type:
                files_by_type[data_type] = []
            files_by_type[data_type].append(file)
    
    # Get the latest file for each data type
    latest_files = {}
    for data_type, file_list in files_by_type.items():
        if file_list:
            # Sort by modification time and get the latest
            latest_file = max(file_list, key=os.path.getmtime)
            latest_files[data_type] = latest_file
    
    return latest_files

def safe_convert_timestamp(df, timestamp_col='TIMESTAMP'):
    """
    Safely convert timestamp column to datetime, handling various formats and issues
    """
    # First, try to find a timestamp column if the specified one doesn't exist
    if timestamp_col not in df.columns:
        # Look for common timestamp column names
        possible_timestamp_cols = ['TIMESTAMP', 'timestamp', 'Timestamp', 'time', 'Time', 'TIME', 'date', 'Date', 'DATE', 'datetime', 'Datetime', 'DATETIME']
        for col in possible_timestamp_cols:
            if col in df.columns:
                timestamp_col = col
                break
        else:
            # If no timestamp column found, create a synthetic one
            print(f"Warning: No timestamp column found. Available columns: {df.columns.tolist()}")
            start_time = datetime.now() - timedelta(minutes=len(df))
            timestamps = [start_time + timedelta(minutes=i) for i in range(len(df))]
            df['TIMESTAMP'] = timestamps
            return df
    
    # Check if already datetime
    if pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
        return df
    
    # Get sample of timestamp values to understand the format
    sample_values = df[timestamp_col].dropna().head(10).astype(str)
    
    # Try to identify the format
    for sample in sample_values:
        if pd.isna(sample) or sample == '':
            continue
            
        # Check if it's already a valid datetime string
        try:
            pd.to_datetime(sample)
            # If successful, try to convert the whole column
            try:
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                return df
            except Exception:
                break
        except:
            continue
    
    # If we get here, the timestamps might be in a different format
    # Check if the values look like they might be numeric timestamps
    sample_values = df[timestamp_col].dropna().head(5).astype(str)
    numeric_like = all(val.replace('.', '').replace('-', '').isdigit() for val in sample_values if val)
    
    if numeric_like:
        # Create synthetic timestamps (assuming 1-minute intervals)
        start_time = datetime.now() - timedelta(minutes=len(df))
        timestamps = [start_time + timedelta(minutes=i) for i in range(len(df))]
        df[timestamp_col] = timestamps
    else:
        # Create synthetic timestamps (assuming 1-minute intervals)
        start_time = datetime.now() - timedelta(minutes=len(df))
        timestamps = [start_time + timedelta(minutes=i) for i in range(len(df))]
        df[timestamp_col] = timestamps
    
    return df

def clean_numeric_columns(df):
    """
    Clean numeric columns by converting to numeric and handling invalid values
    """
    for col in df.columns:
        if col in ['TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path']:
            continue
            
        # Try to convert to numeric, handling various formats
        try:
            # First try direct conversion
            df[col] = pd.to_numeric(df[col], errors='coerce')
        except:
            # If that fails, try to clean the data first
            try:
                # Remove common non-numeric characters and try again
                cleaned_values = df[col].astype(str).str.replace('NAN', 'NaN', case=False)
                cleaned_values = cleaned_values.str.replace('"', '')
                cleaned_values = cleaned_values.str.replace("'", '')
                df[col] = pd.to_numeric(cleaned_values, errors='coerce')
            except:
                # If all else fails, keep as is
                continue
    
    return df

# Function to load data from latest files for a specific station
@st.cache_data
def load_latest_data_for_station(station):
    """Load data from the latest files for a specific station and data type"""
    latest_files = get_latest_files_for_station(station)
    
    if not latest_files:
        return None
    
    data_dict = {}
    failed_files = []
    
    for data_type, file_path in latest_files.items():
        try:
            df = pd.read_parquet(file_path)
            print(f"Colunas disponíveis no arquivo {data_type}:", df.columns.tolist())
            print(f"Shape do arquivo {data_type}:", df.shape)
            
            if df.empty:
                print(f"Warning: {data_type} file is empty")
                failed_files.append(f"{data_type} (empty file)")
                continue
            
            # Clean the data
            df = clean_numeric_columns(df)
            df = safe_convert_timestamp(df)
            print(f"Após processamento - colunas {data_type}:", df.columns.tolist())
            
            # Add metadata columns
            df['source_file'] = os.path.basename(file_path)
            df['station'] = station
            df['data_type'] = data_type
            df['file_path'] = file_path
            
            data_dict[data_type] = df
            
        except Exception as e:
            print(f"Error loading {data_type} data: {str(e)}")
            failed_files.append(f"{data_type} (error: {str(e)[:50]}...)")
            continue
    
    # Display warnings for failed files in the UI
    if failed_files:
        st.warning(f"⚠️ Could not load some data files for station {station.upper()}: {', '.join(failed_files)}")
    
    return data_dict if data_dict else None

def get_available_variables(df):
    """Get available numeric variables from a DataFrame, excluding date/time related columns"""
    # Get numeric columns only (excluding metadata columns)
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    # Remove timestamp-related columns, metadata, and date/time related columns
    exclude_cols = ['TIMESTAMP', 'source_file', 'station', 'data_type', 'file_path', 
                   'Id', 'Min', 'RECORD', 'Year', 'Jday']
    available_vars = [col for col in numeric_cols if col not in exclude_cols]
    return available_vars

def create_variable_selector(available_vars, default_vars, key_prefix):
    """Create a multi-select widget for variables with default selection"""
    selected_vars = st.multiselect(
        "Select variables to display:",
        options=available_vars,
        default=default_vars,
        key=f"{key_prefix}_selector"
    )
    return selected_vars

# Airflow Integration Functions
@st.cache_data
def load_airflow_config():
    """Load Airflow configuration from file and environment variables"""
    config_file = "airflow_config.json"
    
    # Default configuration
    default_config = {
        "airflow": {
            "base_url": "http://localhost:8080",
            "username": "admin",
            "password": "admin",
            "timeout_minutes": {"download": 5, "process": 10}
        },
        "dags": {
            "download_dag": "ftp_multi_station_download",
            "process_dag": "process_multistation_data"
        }
    }
    
    # Load from file if it exists
    try:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                # Merge with defaults
                default_config.update(file_config)
    except Exception as e:
        st.warning(f"⚠️ Could not load Airflow config file: {str(e)}. Using defaults.")
    
    # Override with environment variables if they exist (for Docker)
    airflow_config = default_config.get("airflow", {})
    airflow_config["base_url"] = os.getenv("AIRFLOW_BASE_URL", airflow_config.get("base_url", "http://localhost:8080"))
    airflow_config["username"] = os.getenv("AIRFLOW_USERNAME", airflow_config.get("username", "admin"))
    airflow_config["password"] = os.getenv("AIRFLOW_PASSWORD", airflow_config.get("password", "admin"))
    
    return default_config

class AirflowClient:
    """Client for interacting with Airflow via CLI (more reliable than REST API)"""
    
    def __init__(self, config: Dict = None):
        if config is None:
            config = load_airflow_config()
        
        airflow_config = config.get('airflow', {})
        self.base_url = airflow_config.get('base_url', 'http://localhost:8080').rstrip('/')
        self.username = airflow_config.get('username', 'admin')
        self.password = airflow_config.get('password', 'admin')
        self.timeout_minutes = airflow_config.get('timeout_minutes', {'download': 5, 'process': 10})
        
        # Use CLI approach instead of REST API
        self.connected = self._test_cli_connection()
    
    def _test_cli_connection(self):
        """Test Airflow API connection with JWT authentication"""
        try:
            # Try to get a JWT token first
            token = self._get_jwt_token()
            if token:
                # Test API access with the token
                headers = {'Authorization': f'Bearer {token}'}
                response = requests.get(f"{self.base_url}/api/v2/dags", headers=headers, timeout=10)
                return response.status_code == 200
            return False
        except Exception:
            return False
    
    def _get_jwt_token(self):
        """Get JWT token from Airflow API"""
        try:
            # Try the token endpoint
            token_url = f"{self.base_url}/auth/token"
            auth_data = {
                "username": self.username,
                "password": self.password
            }
            
            response = requests.post(token_url, json=auth_data, timeout=10)
            if response.status_code in [200, 201]:  # 201 is Created, 200 is OK
                token_data = response.json()
                return token_data.get('access_token') or token_data.get('token')
            
            # If /auth/token doesn't work, try alternative endpoints
            alt_endpoints = [
                f"{self.base_url}/api/v2/auth/token",
                f"{self.base_url}/api/v1/auth/token",
                f"{self.base_url}/api/v2/auth/login"
            ]
            
            for endpoint in alt_endpoints:
                try:
                    response = requests.post(endpoint, json=auth_data, timeout=10)
                    if response.status_code in [200, 201]:  # 201 is Created, 200 is OK
                        token_data = response.json()
                        return token_data.get('access_token') or token_data.get('token')
                except:
                    continue
            
            return None
        except Exception:
            return None
    
    def trigger_dag(self, dag_id: str, conf: Optional[Dict] = None) -> bool:
        """Trigger a DAG run using JWT authenticated API"""
        try:
            # Get JWT token
            token = self._get_jwt_token()
            if not token:
                st.error(f"❌ Could not get authentication token for DAG {dag_id}")
                return False
            
            # Prepare request
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
            headers = {'Authorization': f'Bearer {token}'}
            
            # Prepare request body according to TriggerDAGRunPostBody schema
            from datetime import datetime
            request_body = {
                "logical_date": datetime.now().isoformat() + "Z",  # Required field
                "dag_run_id": f"manual_trigger_{int(time.time())}"
            }
            
            if conf:
                request_body["conf"] = conf
            
            response = requests.post(url, json=request_body, headers=headers, timeout=30)
            
            if response.status_code == 200:
                st.success(f"✅ Successfully triggered DAG: {dag_id}")
                return True
            else:
                st.error(f"❌ Failed to trigger DAG {dag_id} (Status: {response.status_code})")
                st.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            st.error(f"❌ Error triggering DAG {dag_id}: {str(e)}")
            return False
    
    def get_dag_status(self, dag_id: str) -> Optional[Dict]:
        """Get the latest DAG run status using JWT authenticated API"""
        try:
            # Get JWT token
            token = self._get_jwt_token()
            if not token:
                return None
            
            # Get DAG runs to find the latest one
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
            headers = {'Authorization': f'Bearer {token}'}
            params = {'limit': 1}
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                runs_data = response.json()
                dag_runs = runs_data.get('dag_runs', [])
                if dag_runs:
                    latest_run = dag_runs[0]
                    return {
                        'state': latest_run.get('state'),
                        'dag_id': dag_id,
                        'dag_run_id': latest_run.get('dag_run_id'),
                        'start_date': latest_run.get('start_date'),
                        'end_date': latest_run.get('end_date')
                    }
            return None
        except Exception as e:
            st.error(f"❌ Error getting DAG status for {dag_id}: {str(e)}")
            return None
    
    def get_dag_runs(self, dag_id: str, limit: int = 5) -> List[Dict]:
        """Get recent DAG runs using JWT authenticated API"""
        try:
            # Get JWT token
            token = self._get_jwt_token()
            if not token:
                return []
            
            # Get DAG runs
            url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
            headers = {'Authorization': f'Bearer {token}'}
            params = {'limit': limit}
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                runs_data = response.json()
                dag_runs = runs_data.get('dag_runs', [])
                return dag_runs
            return []
        except Exception as e:
            st.error(f"❌ Error getting DAG runs for {dag_id}: {str(e)}")
            return []

def refresh_data_pipeline(airflow_client: AirflowClient, selected_station: str = None) -> Dict[str, bool]:
    """
    Execute the complete data refresh pipeline:
    1. Download latest data from FTP
    2. Process the data
    3. Update the dashboard data
    
    Returns a dictionary with the status of each step
    """
    results = {
        'download': False,
        'process': False,
        'overall': False
    }
    
    if not airflow_client.connected:
        st.error("❌ Not connected to Airflow. Please check your connection settings.")
        return results
    
    try:
        config = load_airflow_config()
        dags_config = config.get('dags', {})
        
        # Step 1: Trigger FTP download DAG
        st.info("🔄 Step 1: Downloading latest data from FTP...")
        download_dag = dags_config.get('download_dag', 'ftp_multi_station_download')
        download_success = airflow_client.trigger_dag(download_dag)
        results['download'] = download_success
        
        if download_success:
            st.success("✅ FTP download triggered successfully")
            
            # Step 2: Trigger data processing DAG
            st.info("🔄 Step 2: Processing downloaded data...")
            process_dag = dags_config.get('process_dag', 'process_multistation_data')
            process_success = airflow_client.trigger_dag(process_dag)
            results['process'] = process_success
            
            if process_success:
                st.success("✅ Data processing triggered successfully")
                results['overall'] = True
            else:
                st.error("❌ Failed to trigger data processing")
        else:
            st.error("❌ Failed to trigger FTP download")
            
    except Exception as e:
        st.error(f"❌ Error in refresh pipeline: {str(e)}")
    
    return results

def monitor_dag_progress(airflow_client: AirflowClient, dag_id: str, timeout_minutes: int = 10):
    """Monitor DAG execution progress with a progress bar"""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    while time.time() - start_time < timeout_seconds:
        dag_status = airflow_client.get_dag_status(dag_id)
        
        if dag_status:
            state = dag_status.get('state', 'unknown')
            start_date = dag_status.get('start_date')
            end_date = dag_status.get('end_date')
            
            if state == 'success':
                progress_bar.progress(1.0)
                status_text.success(f"✅ {dag_id} completed successfully!")
                return True
            elif state == 'failed':
                progress_bar.progress(1.0)
                status_text.error(f"❌ {dag_id} failed!")
                return False
            elif state in ['running', 'queued']:
                elapsed = time.time() - start_time
                progress = min(elapsed / timeout_seconds, 0.9)  # Max 90% until completion
                progress_bar.progress(progress)
                status_text.info(f"🔄 {dag_id} is {state}... ({elapsed:.0f}s elapsed)")
            else:
                status_text.warning(f"⚠️ {dag_id} state: {state}")
        
        time.sleep(5)  # Check every 5 seconds
    
    progress_bar.progress(1.0)
    status_text.warning(f"⏰ {dag_id} monitoring timed out after {timeout_minutes} minutes")
    return False

def create_refresh_button(airflow_client: AirflowClient, selected_station: str):
    """Create the refresh button with progress monitoring"""
    
    # Refresh button section
    st.markdown("---")
    st.markdown('<h3 style="text-align: center; color: #2c3e50;">🔄 Data Refresh</h3>', unsafe_allow_html=True)
    
    # Connection status
    if airflow_client.connected:
        st.success("✅ Connected to Airflow")
    else:
        st.error("❌ Not connected to Airflow. Please check your connection settings in airflow_config.json")
        st.info("💡 Make sure Airflow is running and the configuration is correct.")
        
        # Show manual instructions
        st.markdown("### 🔧 Manual DAG Triggering")
        st.markdown("""
        Since the automatic connection is not working, you can manually trigger the DAGs using these commands:
        
        **To trigger the download DAG:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags trigger ftp_multi_station_download
        ```
        
        **To trigger the processing DAG:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags trigger process_multistation_data
        ```
        
        **To check DAG status:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags state ftp_multi_station_download
        docker-compose exec airflow-apiserver airflow dags state process_multistation_data
        ```
        
        **To view DAG runs:**
        ```bash
        docker-compose exec airflow-apiserver airflow dags list-runs --dag-id ftp_multi_station_download
        docker-compose exec airflow-apiserver airflow dags list-runs --dag-id process_multistation_data
        ```
        """)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        if st.button("🔄 Refresh Latest Data", type="primary", use_container_width=True):
            # Clear any previous status messages
            st.empty()
            
            # Execute refresh pipeline
            with st.spinner("Executing data refresh pipeline..."):
                results = refresh_data_pipeline(airflow_client, selected_station)
            
            if results['overall']:
                st.success("🎉 Data refresh pipeline completed successfully!")
                
                # Monitor the DAGs
                st.info("📊 Monitoring pipeline progress...")
                
                # Monitor download DAG
                config = load_airflow_config()
                download_timeout = config.get('airflow', {}).get('timeout_minutes', {}).get('download', 5)
                process_timeout = config.get('airflow', {}).get('timeout_minutes', {}).get('process', 10)
                
                download_dag = config.get('dags', {}).get('download_dag', 'ftp_multi_station_download')
                process_dag = config.get('dags', {}).get('process_dag', 'process_multistation_data')
                
                download_complete = monitor_dag_progress(airflow_client, download_dag, timeout_minutes=download_timeout)
                
                if download_complete:
                    # Monitor processing DAG
                    process_complete = monitor_dag_progress(airflow_client, process_dag, timeout_minutes=process_timeout)
                    
                    if process_complete:
                        st.success("🎉 All data has been refreshed successfully!")
                        st.info("💡 The dashboard will show the latest data on next page refresh.")
                        
                        # Force refresh of cached data
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.warning("⚠️ Data processing may still be running. Check Airflow UI for details.")
                else:
                    st.warning("⚠️ Data download may still be running. Check Airflow UI for details.")
            else:
                st.error("❌ Failed to start data refresh pipeline. Please check Airflow connection.")
    
    # Show recent DAG runs status
    with st.expander("📋 Recent Pipeline Status", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Download Pipeline")
            config = load_airflow_config()
            download_dag = config.get('dags', {}).get('download_dag', 'ftp_multi_station_download')
            download_runs = airflow_client.get_dag_runs(download_dag, limit=3)
            if download_runs:
                for run in download_runs:
                    state = run.get('state', 'unknown')
                    start_date = run.get('start_date', 'N/A')
                    state_emoji = {
                        'success': '✅',
                        'failed': '❌',
                        'running': '🔄',
                        'queued': '⏳'
                    }.get(state, '❓')
                    st.write(f"{state_emoji} {state.upper()} - {start_date}")
            else:
                st.write("No recent runs found")
        
        with col2:
            st.subheader("Processing Pipeline")
            process_dag = config.get('dags', {}).get('process_dag', 'process_multistation_data')
            process_runs = airflow_client.get_dag_runs(process_dag, limit=3)
            if process_runs:
                for run in process_runs:
                    state = run.get('state', 'unknown')
                    start_date = run.get('start_date', 'N/A')
                    state_emoji = {
                        'success': '✅',
                        'failed': '❌',
                        'running': '🔄',
                        'queued': '⏳'
                    }.get(state, '❓')
                    st.write(f"{state_emoji} {state.upper()} - {start_date}")
            else:
                st.write("No recent runs found")

def plot_selected_variables(df, selected_vars, plot_title, height=300):
    """Create a line plot for selected variables with comprehensive error handling"""
    if not selected_vars:
        st.warning(f"⚠️ No variables selected for {plot_title}. Please select at least one variable from the dropdown above.")
        return False
    
    if 'TIMESTAMP' not in df.columns:
        st.error(f"❌ Timestamp data not available for {plot_title}. Cannot create time series plot.")
        return False
    
    # Check if selected variables exist in dataframe
    missing_vars = [var for var in selected_vars if var not in df.columns]
    if missing_vars:
        available_vars = [var for var in selected_vars if var in df.columns]
        if missing_vars:
            st.warning(f"⚠️ The following variables are not available in {plot_title}: {', '.join(missing_vars)}")
        if not available_vars:
            st.error(f"❌ None of the selected variables are available for {plot_title}.")
            return False
        selected_vars = available_vars
        st.info(f"ℹ️ Plotting available variables for {plot_title}: {', '.join(selected_vars)}")
    
    try:
        # Check if there's actual data to plot
        plot_data = df[['TIMESTAMP'] + selected_vars].set_index('TIMESTAMP')
        
        # Remove rows where all selected variables are NaN
        plot_data_clean = plot_data.dropna(how='all')
        
        if plot_data_clean.empty:
            st.warning(f"⚠️ No valid data available for {plot_title}. All values are missing or invalid.")
            return False
        
        if len(plot_data_clean) < len(plot_data) * 0.1:  # Less than 10% valid data
            st.warning(f"⚠️ Limited data available for {plot_title} ({len(plot_data_clean)} out of {len(plot_data)} records have valid data).")
        
        st.line_chart(plot_data_clean, height=height, use_container_width=True)
        st.caption(f"📊 {plot_title}: {', '.join(selected_vars)} ({len(plot_data_clean)} data points)")
        return True
        
    except Exception as e:
        st.error(f"❌ Error creating plot for {plot_title}: {str(e)}")
        st.error("This might be due to data format issues or memory constraints.")
        return False


# Primeiro, obtém a lista de pastas das estações disponíveis
available_stations = get_available_stations()

# Depois, passa essa lista como argumento para a função de metadata
station_metadata = get_station_metadata(available_stations)      

if not available_stations:
    st.error("❌ No stations found in the directory 'data/interim'.")
    st.info("📁 Please ensure that station data has been processed and is available in the interim directory.")
    st.stop()
else:
    print("Available stations:", available_stations)

    
# Station selector - centered and prominent
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    selected_station = st.selectbox(
        "Select Station:",
        options=available_stations,
        index=0,
        format_func=lambda x: x.upper()
    )

st.markdown(f'<h2 class="station-header">Station: {selected_station.upper()}</h2>', unsafe_allow_html=True)

# Initialize Airflow client and create refresh button
@st.cache_resource
def get_airflow_client():
    """Get cached Airflow client instance"""
    return AirflowClient()

airflow_client = get_airflow_client()

# Create refresh button section
create_refresh_button(airflow_client, selected_station)

# Load data for selected station
data_dict = load_latest_data_for_station(selected_station)

# Data Overview Section - Compact and informative
if data_dict is not None and len(data_dict) > 0:
    st.header("📊 Data Overview")

    # Add legend for metrics
    st.markdown("""
<div style="font-size:1rem; color:#444; margin-bottom:0.5rem;">
<b>Legend:</b> <b>Records</b> = number of data rows; <b>Columns</b> = number of variables; <b>Updated</b> = last file modification time
</div>
""", unsafe_allow_html=True)

    # Display information about loaded files in a compact format
    file_info = []
    for data_type, df in data_dict.items():
        timestamp_col = 'TIMESTAMP'
        time_range = "N/A"
        if timestamp_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            time_range = f"{df[timestamp_col].min().strftime('%Y-%m-%d %H:%M')} to {df[timestamp_col].max().strftime('%Y-%m-%d %H:%M')}"
        file_info.append({
            'Data Type': data_type,
            'Records': len(df),
            'Columns': len(df.columns),
            'Time Range': time_range,
            'Last Modified': datetime.fromtimestamp(os.path.getmtime(df['file_path'].iloc[0])).strftime('%Y-%m-%d %H:%M')
        })
    file_df = pd.DataFrame(file_info)
    # Display in columns for better space usage
    cols = st.columns(len(file_info))
    for i, (_, row) in enumerate(file_df.iterrows()):
        with cols[i]:
            st.metric(
                label=f"{row['Data Type']} Data",
                value=f"{row['Records']:,}",
                delta=f"{row['Columns']} cols"
            )
            st.caption(f"Updated: {row['Last Modified']}")

    # Data Visualization Section - Three column layout
    st.header("📈 Real-time Data Visualization")
    col1, col2, col3 = st.columns([1, 1, 1])

    # Column 1: Solar Data (SD)
    with col1:
        st.subheader("☀️ Solar Data (SD)")
        
        if 'SD' in data_dict:
            df = data_dict['SD'].copy()
            
            # Get station metadata for clear sky calculation
            filtered_row = station_metadata[
                           station_metadata['station'].str.lower() == selected_station.lower()]

            if not filtered_row.empty:
                try:
                    latitude = filtered_row['latitude'].values[0]
                    longitude = filtered_row['longitude'].values[0]
                    
                    # Apply clear sky model to add clear sky variables
                    df = calculate_clearsky_ineichen(df, latitude, longitude, tz='America/Sao_Paulo')
                except Exception as e:
                    st.warning(f"⚠️ Could not calculate clear sky data for {selected_station.upper()}: {str(e)}")
            else:
                st.warning(f"⚠️ Station metadata not found for {selected_station.upper()}. Clear sky calculations will not be available.")
            
            available_vars = get_available_variables(df)
            
            if available_vars and 'TIMESTAMP' in df.columns:
                # Solar Radiation Plot (including clear sky data)
                st.markdown('<p class="subsection-header">Solar Radiation</p>', unsafe_allow_html=True)
                solar_vars = [var for var in available_vars if any(x in var.lower() for x in ['glo', 'dir', 'dif']) and any(x in var.lower() for x in ['avg', 'std'])]
                
                # Add clear sky variables if they exist
                clearsky_vars = ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']
                available_clearsky_vars = [var for var in clearsky_vars if var in df.columns]
                
                if solar_vars or available_clearsky_vars:
                    # Combine solar and clear sky variables
                    all_solar_vars = solar_vars + available_clearsky_vars
                    
                    # Set default to only GHI and clear sky GHI
                    ghi_vars = [var for var in all_solar_vars if 'glo' in var.lower() or var == 'clearsky_GHI']
                    default_solar_vars = ghi_vars if ghi_vars else all_solar_vars[:2]  # fallback to first 2 if no GHI found
                    
                    selected_solar = create_variable_selector(all_solar_vars, default_solar_vars, "solar")
                    plot_selected_variables(df, selected_solar, "Solar Radiation")
                else:
                    st.info("No solar radiation variables found.")
                
                # Longwave Radiation Plot
                st.markdown('<p class="subsection-header">Longwave Radiation</p>', unsafe_allow_html=True)
                lw_vars = [var for var in available_vars if 'lw' in var.lower() and any(x in var.lower() for x in ['avg', 'std'])]
                if lw_vars:
                    selected_lw = create_variable_selector(lw_vars, lw_vars, "longwave")
                    plot_selected_variables(df, selected_lw, "Longwave Radiation")
                else:
                    st.info("No longwave radiation variables found.")
                
                # PAR & LUX Plot
                st.markdown('<p class="subsection-header">PAR & LUX</p>', unsafe_allow_html=True)
                par_lux_vars = [var for var in available_vars if any(x in var.lower() for x in ['par', 'lux']) and any(x in var.lower() for x in ['avg', 'std'])]
                if par_lux_vars:
                    selected_par_lux = create_variable_selector(par_lux_vars, par_lux_vars, "par_lux")
                    plot_selected_variables(df, selected_par_lux, "PAR & LUX")
                else:
                    st.info("No PAR & LUX variables found.")
                
                # Temperature Plot
                st.markdown('<p class="subsection-header">Temperatures</p>', unsafe_allow_html=True)
                temp_vars = [var for var in available_vars if any(x in var.lower() for x in ['tp_', 'temp']) and any(x in var.lower() for x in ['avg', 'std'])]
                if temp_vars:
                    selected_temp = create_variable_selector(temp_vars, temp_vars, "temperature")
                    plot_selected_variables(df, selected_temp, "Temperatures")
                else:
                    st.info("No temperature variables found.")
            else:
                st.warning("No numeric data available for SD.")
        else:
            st.info("No SD data available for this station.")

    # Column 2: Environmental Data (MD and WD)
    with col2:
        st.subheader("🌤️ Environmental Data")
        
        # Meteorological Data Subsection
        st.markdown('<p class="subsection-header">Meteorological Data</p>', unsafe_allow_html=True)
        if 'MD' in data_dict:
            df_md = data_dict['MD'].copy()
            available_vars_md = get_available_variables(df_md)
            
            if available_vars_md and 'TIMESTAMP' in df_md.columns:
                meteo_vars = [var for var in available_vars_md if any(x in var.lower() for x in ['tp_sfc', 'humid', 'press', 'rain'])]
                if meteo_vars:
                    selected_meteo = create_variable_selector(meteo_vars, meteo_vars, "meteorological")
                    plot_selected_variables(df_md, selected_meteo, "Meteorological Data")
                else:
                    st.info("No meteorological variables found in MD data.")
            else:
                st.warning("No numeric data available for MD.")
        else:
            st.info("No MD data available for this station.")
        
        # Wind Data Subsection
        st.markdown('<p class="subsection-header">Wind Data</p>', unsafe_allow_html=True)
        
        # Wind at different heights
        wind_heights = ['10m', '25m', '50m']
        for height in wind_heights:
            wind_vars = []
            df_source = None
            
            # Check both MD and WD for wind data
            for data_type in ['MD', 'WD']:
                if data_type in data_dict:
                    df_temp = data_dict[data_type].copy()
                    available_vars_temp = get_available_variables(df_temp)
                    height_vars = [var for var in available_vars_temp if height in var]
                    if height_vars:
                        wind_vars = height_vars
                        df_source = df_temp
                        break
            
            if wind_vars and df_source is not None:
                st.write(f"**Wind at {height}**")
                selected_wind = create_variable_selector(wind_vars, wind_vars, f"wind_{height}")
                plot_selected_variables(df_source, selected_wind, f"Wind at {height}", height=200)
            else:
                st.info(f"No wind data available at {height}.")
        
        # Temperature per Level Subsection
        st.markdown('<p class="subsection-header">Temperature per Level</p>', unsafe_allow_html=True)
        if 'WD' in data_dict:
            df_wd = data_dict['WD'].copy()
            available_vars_wd = get_available_variables(df_wd)
            
            if available_vars_wd and 'TIMESTAMP' in df_wd.columns:
                temp_humid_vars = [var for var in available_vars_wd if any(x in var.lower() for x in ['tp_', 'humid_'])]
                if temp_humid_vars:
                    selected_temp_humid = create_variable_selector(temp_humid_vars, temp_humid_vars, "temp_humid_levels")
                    plot_selected_variables(df_wd, selected_temp_humid, "Temperature & Humidity per Level")
                else:
                    st.info("No temperature/humidity variables found in WD data.")
            else:
                st.warning("No numeric data available for WD.")
        else:
            st.info("No WD data available for this station.")

    # Column 3: Detailed View
    with col3:
        st.subheader("🔍 Detailed View")
        
        # Collect only variables that are shown in other sections
        detailed_variables = []
        
        # Get SD variables (Solar Data section)
        if 'SD' in data_dict:
            df_sd = data_dict['SD'].copy()
            
            # Get station metadata for clear sky calculation
            filtered_row = station_metadata[
                           station_metadata['station'].str.lower() == selected_station.lower()]

            if not filtered_row.empty:
                try:
                    latitude = filtered_row['latitude'].values[0]
                    longitude = filtered_row['longitude'].values[0]
                    
                    # Apply clear sky model to add clear sky variables
                    df_sd = calculate_clearsky_ineichen(df_sd, latitude, longitude, tz='America/Sao_Paulo')
                except Exception as e:
                    st.warning(f"⚠️ Could not calculate clear sky data for detailed view: {str(e)}")
            else:
                st.warning(f"⚠️ Station metadata not found for {selected_station.upper()}. Clear sky data will not be available in detailed view.")
            
            available_vars_sd = get_available_variables(df_sd)
            
            # Solar radiation variables (glo, dir, dif with avg/std)
            solar_vars = [var for var in available_vars_sd if any(x in var.lower() for x in ['glo', 'dir', 'dif']) and any(x in var.lower() for x in ['avg', 'std'])]
            detailed_variables.extend([f"SD: {var}" for var in solar_vars])
            
            # Clear sky variables
            clearsky_vars = ['clearsky_GHI', 'clearsky_DNI', 'clearsky_DHI']
            available_clearsky_vars = [var for var in clearsky_vars if var in df_sd.columns]
            detailed_variables.extend([f"SD: {var}" for var in available_clearsky_vars])
            
            # Longwave variables (lw with avg/std)
            lw_vars = [var for var in available_vars_sd if 'lw' in var.lower() and any(x in var.lower() for x in ['avg', 'std'])]
            detailed_variables.extend([f"SD: {var}" for var in lw_vars])
            
            # PAR & LUX variables (par, lux with avg/std)
            par_lux_vars = [var for var in available_vars_sd if any(x in var.lower() for x in ['par', 'lux']) and any(x in var.lower() for x in ['avg', 'std'])]
            detailed_variables.extend([f"SD: {var}" for var in par_lux_vars])
            
            # Temperature variables (tp_, temp with avg/std)
            temp_vars = [var for var in available_vars_sd if any(x in var.lower() for x in ['tp_', 'temp']) and any(x in var.lower() for x in ['avg', 'std'])]
            detailed_variables.extend([f"SD: {var}" for var in temp_vars])
        
        # Get MD variables (Environmental Data section)
        if 'MD' in data_dict:
            df_md = data_dict['MD'].copy()
            available_vars_md = get_available_variables(df_md)
            
            # Meteorological variables
            meteo_vars = [var for var in available_vars_md if any(x in var.lower() for x in ['tp_sfc', 'humid', 'press', 'rain'])]
            detailed_variables.extend([f"MD: {var}" for var in meteo_vars])
            
            # Wind variables at different heights
            wind_heights = ['10m', '25m', '50m']
            for height in wind_heights:
                height_vars = [var for var in available_vars_md if height in var]
                detailed_variables.extend([f"MD: {var}" for var in height_vars])
        
        # Get WD variables (Environmental Data section)
        if 'WD' in data_dict:
            df_wd = data_dict['WD'].copy()
            available_vars_wd = get_available_variables(df_wd)
            
            # Wind variables at different heights
            wind_heights = ['10m', '25m', '50m']
            for height in wind_heights:
                height_vars = [var for var in available_vars_wd if height in var]
                detailed_variables.extend([f"WD: {var}" for var in height_vars])
            
            # Temperature and humidity per level
            temp_humid_vars = [var for var in available_vars_wd if any(x in var.lower() for x in ['tp_', 'humid_'])]
            detailed_variables.extend([f"WD: {var}" for var in temp_humid_vars])
        
        if detailed_variables:
            # Variable selector for detailed view
            st.write("**Select variable for detailed view:**")
            
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
                    
                    # Create detailed plot
                    plot_data = df_detail[['TIMESTAMP', var_name]].set_index('TIMESTAMP')
                    st.line_chart(plot_data, height=400, use_container_width=True)
                    
                    # Show statistics
                    col_stat1, col_stat2 = st.columns(2)
                    with col_stat1:
                        st.metric("Mean", f"{df_detail[var_name].mean():.2f}")
                        st.metric("Min", f"{df_detail[var_name].min():.2f}")
                    with col_stat2:
                        st.metric("Max", f"{df_detail[var_name].max():.2f}")
                        st.metric("Std Dev", f"{df_detail[var_name].std():.2f}")
                else:
                    st.error(f"Variable {var_name} not found in {data_type} data.")
        else:
            st.info("No variables available for detailed view.")

    # Raw Data Section (collapsible) - Only show if needed
    with st.expander("📋 Raw Data (Click to expand)"):
        for data_type, df in data_dict.items():
            st.write(f"**{selected_station.upper()} - {data_type} Data**")
            st.write(f"Shape: {df.shape}")
            st.dataframe(df.head(20), use_container_width=True)  # Show first 20 rows only

else:
    st.error(f"❌ No data files found for station '{selected_station.upper()}'")
    st.info("📋 This could mean:")
    st.markdown("""
    - No processed data files exist for this station
    - The data processing pipeline hasn't run for this station  
    - The station folder exists but contains no valid parquet files
    - There was an error during data processing
    """)
    st.info("💡 Please check the data processing logs or re-run the ETL pipeline for this station.")

# Footer
st.markdown("---")
st.markdown('<p style="text-align: center; color: #666; font-size: 0.9rem;">Solar Data Monitoring System - Crisis Room Dashboard</p>', unsafe_allow_html=True)