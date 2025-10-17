#!/bin/bash

# Docker-based Solar Data Monitor Dashboard Startup Script
# This script helps you start the complete system with Docker Compose

echo "🐳 Starting Solar Data Monitor with Docker"
echo "=========================================="

# Check if docker compose is available
if ! command -v docker compose &> /dev/null; then
    echo "❌ docker compose is not installed. Please install it first:"
    echo "   https://docs.docker.com/compose/install/"
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Check if airflow_config.json exists
if [ ! -f "airflow_config.json" ]; then
    echo "⚠️  airflow_config.json not found. Creating Docker-optimized configuration..."
    cat > airflow_config.json << EOF
{
    "airflow": {
        "base_url": "http://airflow-apiserver:8080",
        "username": "airflow",
        "password": "airflow",
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
    echo "✅ Created Docker-optimized airflow_config.json"
fi

# Check if data directory exists
if [ ! -d "data/interim" ]; then
    echo "⚠️  data/interim directory not found. Creating it..."
    mkdir -p data/interim
    echo "✅ Created data/interim directory"
fi

# Set AIRFLOW_UID if not set
if [ -z "$AIRFLOW_UID" ]; then
    export AIRFLOW_UID=$(id -u)
    echo "🔧 Set AIRFLOW_UID to $AIRFLOW_UID"
fi

echo ""
echo "🚀 Starting Docker services..."
echo "This will start:"
echo "  - PostgreSQL database"
echo "  - Redis cache"
echo "  - Airflow webserver (port 8082)"
echo "  - Airflow scheduler"
echo "  - Airflow workers"
echo "  - Streamlit dashboard (port 8501)"
echo ""

# Start the services
docker compose up -d

echo ""
echo "⏳ Waiting for services to start up..."
echo "This may take a few minutes on first run..."

# Wait for Airflow to be ready
echo "🔍 Checking Airflow status..."
for i in {1..30}; do
    if curl -s http://localhost:8082/health > /dev/null 2>&1; then
        echo "✅ Airflow is ready!"
        break
    else
        echo "⏳ Waiting for Airflow... ($i/30)"
        sleep 10
    fi
done

# Wait for Streamlit to be ready
echo "🔍 Checking Streamlit status..."
for i in {1..20}; do
    if curl -s http://localhost:8501 > /dev/null 2>&1; then
        echo "✅ Streamlit dashboard is ready!"
        break
    else
        echo "⏳ Waiting for Streamlit... ($i/20)"
        sleep 5
    fi
done

echo ""
echo "🎉 All services are running!"
echo ""
echo "📊 Dashboard: http://localhost:8501"
echo "🌐 Airflow UI: http://localhost:8082"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "🔄 The refresh button in the dashboard will:"
echo "   1. Download latest data from FTP"
echo "   2. Process the data"
echo "   3. Update the dashboard"
echo ""
echo "📋 Useful commands:"
echo "   View logs: docker compose logs -f streamlit"
echo "   Stop services: docker compose down"
echo "   Restart: docker compose restart"
echo ""
echo "Press Ctrl+C to stop all services"

# Keep the script running and show logs
docker compose logs -f streamlit
