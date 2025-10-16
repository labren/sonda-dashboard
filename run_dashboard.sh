#!/bin/bash

# Solar Data Monitor Dashboard Startup Script
# This script helps you start the dashboard with proper configuration

echo "🚀 Starting Solar Data Monitor Dashboard"
echo "========================================"

# Check if streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit is not installed. Please install it first:"
    echo "   pip install streamlit"
    exit 1
fi

# Check if airflow_config.json exists
if [ ! -f "airflow_config.json" ]; then
    echo "⚠️  airflow_config.json not found. Creating default configuration..."
    cat > airflow_config.json << EOF
{
    "airflow": {
        "base_url": "http://localhost:8080",
        "username": "admin",
        "password": "admin",
        "timeout_minutes": {
            "download": 5,
            "process": 10
        }
    },
    "dags": {
        "download_dag": "ftp_multi_station_download",
        "process_dag": "process_multistation_data",
        "header_discovery_dag": "header_discovery_dag"
    },
    "refresh_pipeline": {
        "auto_refresh_cache": true,
        "show_progress": true,
        "monitor_timeout": 15
    }
}
EOF
    echo "✅ Created default airflow_config.json"
fi

# Check if data directory exists
if [ ! -d "data/interim" ]; then
    echo "⚠️  data/interim directory not found. Creating it..."
    mkdir -p data/interim
    echo "✅ Created data/interim directory"
fi

# Test Airflow connection (optional)
echo "🔍 Testing Airflow connection..."
python3 -c "
import requests
import json
import os
try:
    # Check if we're in Docker environment
    if os.getenv('AIRFLOW_BASE_URL'):
        base_url = os.getenv('AIRFLOW_BASE_URL')
        print(f'🐳 Docker environment detected: {base_url}')
    else:
        with open('airflow_config.json', 'r') as f:
            config = json.load(f)
        base_url = config['airflow']['base_url']
        print(f'🏠 Local environment: {base_url}')
    
    response = requests.get(f'{base_url}/health', timeout=5)
    if response.status_code == 200:
        print('✅ Airflow is running and accessible')
    else:
        print('⚠️  Airflow is running but may have issues')
except Exception as e:
    print(f'❌ Cannot connect to Airflow: {e}')
    print('💡 Make sure Airflow is running on the configured URL')
    print('   For Docker: docker-compose up -d')
    print('   For local: airflow webserver --port 8080')
"

echo ""
echo "🌐 Starting Streamlit dashboard..."
echo "📊 Dashboard will be available at: http://localhost:8501"
echo "🔄 Refresh button will trigger the complete data pipeline"
echo ""
echo "Press Ctrl+C to stop the dashboard"
echo ""

# Start the dashboard
streamlit run dashboard.py --server.port 8501 --server.address localhost
