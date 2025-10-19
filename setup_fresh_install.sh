#!/bin/bash

# Fresh Installation Setup Script for Sonda Dashboard
# This script helps set up the system on a fresh installation

echo "🚀 Sonda Dashboard - Fresh Installation Setup"
echo "=============================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Check if docker-compose.yml exists
if [ ! -f "docker-compose.yml" ]; then
    echo "❌ docker-compose.yml not found. Please run this script from the project root directory."
    exit 1
fi

echo "✅ Found docker-compose.yml"
echo ""

# Start the services
echo "🔄 Starting Airflow and Streamlit services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check if services are running
echo "🔍 Checking service status..."
docker-compose ps

# Wait for Airflow to be fully ready
echo ""
echo "⏳ Waiting for Airflow to be fully ready..."
sleep 60

# Test Airflow API connectivity
echo "🔍 Testing Airflow API connectivity..."
MAX_RETRIES=10
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s -f http://localhost:8082/api/v2/monitor/health > /dev/null 2>&1; then
        echo "✅ Airflow API is ready"
        break
    else
        echo "⏳ Waiting for Airflow API... (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)"
        sleep 10
        RETRY_COUNT=$((RETRY_COUNT + 1))
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "⚠️ Airflow API not ready after $MAX_RETRIES attempts"
    echo "💡 You may need to wait longer or check Airflow logs"
fi

# Generate JWT token for dashboard authentication
echo ""
echo "🔐 Generating JWT token for dashboard authentication..."

# Wait a bit more for Airflow to be fully ready
sleep 30

# Generate JWT token
JWT_TOKEN=""
MAX_TOKEN_RETRIES=5
TOKEN_RETRY_COUNT=0

while [ $TOKEN_RETRY_COUNT -lt $MAX_TOKEN_RETRIES ]; do
    echo "🔄 Attempting to generate JWT token... (attempt $((TOKEN_RETRY_COUNT + 1))/$MAX_TOKEN_RETRIES)"
    
    TOKEN_RESPONSE=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        -d '{"username":"airflow","password":"airflow"}' \
        http://localhost:8082/auth/token 2>/dev/null)
    
    if echo "$TOKEN_RESPONSE" | grep -q "access_token"; then
        JWT_TOKEN=$(echo "$TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
        echo "✅ JWT token generated successfully"
        break
    else
        echo "⏳ Token generation failed, retrying..."
        sleep 10
        TOKEN_RETRY_COUNT=$((TOKEN_RETRY_COUNT + 1))
    fi
done

if [ -z "$JWT_TOKEN" ]; then
    echo "⚠️ Failed to generate JWT token after $MAX_TOKEN_RETRIES attempts"
    echo "💡 Dashboard may not be able to connect to Airflow automatically"
    echo "💡 You can generate a token manually later using:"
    echo "   curl -X POST -H 'Content-Type: application/json' -d '{\"username\":\"airflow\",\"password\":\"airflow\"}' http://localhost:8082/auth/token"
else
    echo "✅ JWT token ready for dashboard use"
fi

# Configure FTP connection
echo ""
echo "🔧 Configuring FTP connection..."

# Create config file from example if it doesn't exist
if [ ! -f "ftp_connection_config.json" ]; then
    echo "📁 Creating FTP config file from example..."
    cp ftp_connection_config.example.json ftp_connection_config.json
    echo "💡 Please edit ftp_connection_config.json with your FTP credentials"
    echo "   Or run the interactive setup below"
    echo ""
fi

echo "This will prompt you for FTP connection details."
echo "Press Enter to continue..."
read -r

# Ensure we're in an interactive environment
if [ -t 0 ]; then
    python3 configure_ftp_connection.py --interactive
else
    echo "⚠️ Not running in interactive mode. Please run manually:"
    echo "   python3 configure_ftp_connection.py --interactive"
fi

if [ $? -eq 0 ]; then
    echo "✅ FTP connection configured successfully"
else
    echo "⚠️ FTP connection configuration failed or skipped"
    echo "💡 You can configure it manually later using:"
    echo "   python3 configure_ftp_connection.py"
fi

echo ""
echo "📋 Next Steps:"
echo "============="
echo ""
echo "1. 🌐 Open the dashboard: http://localhost:8501"
echo "2. 🔧 Open Airflow UI: http://localhost:8082"
echo "   - Username: airflow"
echo "   - Password: airflow"
echo ""
echo "3. 🚀 Initialize the data pipeline:"
echo "   - Click the 'Initialize Data Pipeline' button in the dashboard (recommended), OR"
echo "   - Run: docker-compose exec airflow-apiserver airflow dags trigger initial_data_setup"
echo ""
echo "4. 📊 Monitor progress in the Airflow UI"
echo ""
echo "5. 🔄 Once complete, refresh the dashboard to see the data"
echo ""
echo "💡 Troubleshooting:"
echo "==================="
echo "- If you see connection errors, wait a few more minutes for services to fully start"
echo "- Check logs with: docker-compose logs airflow-apiserver"
echo "- Check logs with: docker-compose logs streamlit"
echo "- If dashboard can't connect to Airflow, generate a new JWT token:"
echo "  curl -X POST -H 'Content-Type: application/json' -d '{\"username\":\"airflow\",\"password\":\"airflow\"}' http://localhost:8082/auth/token"
echo ""
echo "🎉 Setup complete! The dashboard should now be accessible with JWT authentication."
