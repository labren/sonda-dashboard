#!/bin/bash

# Generate JWT Token for Airflow API Authentication
# This script generates a JWT token for the dashboard to connect to Airflow

echo "🔐 Generating JWT Token for Airflow API Authentication"
echo "====================================================="
echo ""

# Check if Airflow is running
if ! curl -s -f http://localhost:8082/api/v2/monitor/health > /dev/null 2>&1; then
    echo "❌ Airflow API is not accessible at http://localhost:8082"
    echo "💡 Make sure Airflow is running: docker-compose up -d"
    exit 1
fi

echo "✅ Airflow API is accessible"
echo ""

# Generate JWT token
echo "🔄 Generating JWT token..."

TOKEN_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"username":"airflow","password":"airflow"}' \
    http://localhost:8082/auth/token)

if echo "$TOKEN_RESPONSE" | grep -q "access_token"; then
    JWT_TOKEN=$(echo "$TOKEN_RESPONSE" | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)
    echo "✅ JWT token generated successfully!"
    echo ""
    echo "🔑 Your JWT Token:"
    echo "=================="
    echo "$JWT_TOKEN"
    echo ""
    echo "💡 This token is valid for 24 hours"
    echo "💡 The dashboard will automatically generate new tokens as needed"
    echo ""
    echo "🧪 Test the token:"
    echo "curl -H 'Authorization: Bearer $JWT_TOKEN' http://localhost:8082/api/v2/dags"
else
    echo "❌ Failed to generate JWT token"
    echo "Response: $TOKEN_RESPONSE"
    echo ""
    echo "💡 Troubleshooting:"
    echo "- Check if Airflow is running: docker-compose ps"
    echo "- Check Airflow logs: docker-compose logs airflow-apiserver"
    echo "- Verify credentials: username=airflow, password=airflow"
    exit 1
fi
