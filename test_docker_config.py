#!/usr/bin/env python3
"""
Test script to verify Docker configuration for the refresh functionality
This script tests the configuration that will be used in the Docker environment
"""

import os
import sys
import requests
import json

def test_docker_environment():
    """Test if we're in a Docker environment and configuration is correct"""
    print("🐳 Testing Docker Environment Configuration")
    print("=" * 50)
    
    # Check environment variables
    airflow_base_url = os.getenv("AIRFLOW_BASE_URL")
    airflow_username = os.getenv("AIRFLOW_USERNAME")
    airflow_password = os.getenv("AIRFLOW_PASSWORD")
    
    print(f"Environment Variables:")
    print(f"  AIRFLOW_BASE_URL: {airflow_base_url}")
    print(f"  AIRFLOW_USERNAME: {airflow_username}")
    print(f"  AIRFLOW_PASSWORD: {'*' * len(airflow_password) if airflow_password else 'Not set'}")
    
    if airflow_base_url:
        print("✅ Docker environment detected")
        expected_url = "http://airflow-apiserver:8080"
        if airflow_base_url == expected_url:
            print(f"✅ Correct Airflow URL: {airflow_base_url}")
        else:
            print(f"⚠️  Unexpected Airflow URL: {airflow_base_url}")
            print(f"   Expected: {expected_url}")
    else:
        print("⚠️  Not in Docker environment (no AIRFLOW_BASE_URL)")
    
    # Test configuration loading
    print("\n🔍 Testing Configuration Loading:")
    try:
        config_file = "airflow_config.json"
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            airflow_config = config.get('airflow', {})
            print(f"  Config file base_url: {airflow_config.get('base_url')}")
            print(f"  Config file username: {airflow_config.get('username')}")
            print("✅ Configuration file loaded successfully")
        else:
            print("⚠️  Configuration file not found")
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
    
    return airflow_base_url is not None

def test_airflow_connection():
    """Test connection to Airflow in Docker environment"""
    print("\n🔗 Testing Airflow Connection:")
    
    # Determine the correct URL
    airflow_base_url = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    
    try:
        # Test health endpoint
        health_url = f"{airflow_base_url}/health"
        print(f"  Testing: {health_url}")
        
        response = requests.get(health_url, timeout=10)
        if response.status_code == 200:
            print("✅ Airflow health check passed")
            return True
        else:
            print(f"⚠️  Airflow health check failed (Status: {response.status_code})")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to Airflow at {airflow_base_url}")
        print("   Make sure Airflow is running:")
        print("   - Docker: docker-compose up -d")
        print("   - Local: airflow webserver --port 8080")
        return False
    except Exception as e:
        print(f"❌ Error testing Airflow connection: {e}")
        return False

def test_docker_network():
    """Test Docker network connectivity"""
    print("\n🌐 Testing Docker Network:")
    
    # Check if we can resolve the airflow-apiserver hostname
    try:
        import socket
        socket.gethostbyname('airflow-apiserver')
        print("✅ Can resolve airflow-apiserver hostname")
        return True
    except socket.gaierror:
        print("⚠️  Cannot resolve airflow-apiserver hostname")
        print("   This is normal if not running in Docker")
        return False
    except Exception as e:
        print(f"❌ Error testing network: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Docker Configuration Test")
    print("=" * 50)
    
    # Test 1: Environment detection
    is_docker = test_docker_environment()
    
    # Test 2: Network connectivity (only if in Docker)
    if is_docker:
        test_docker_network()
    
    # Test 3: Airflow connection
    airflow_ok = test_airflow_connection()
    
    print("\n" + "=" * 50)
    print("📋 Test Summary:")
    print(f"  Docker Environment: {'✅' if is_docker else '⚠️'}")
    print(f"  Airflow Connection: {'✅' if airflow_ok else '❌'}")
    
    if is_docker and airflow_ok:
        print("\n🎉 Docker configuration is working correctly!")
        print("   The refresh button should work in the dashboard.")
    elif is_docker and not airflow_ok:
        print("\n⚠️  Docker environment detected but Airflow is not accessible.")
        print("   Try: docker-compose up -d")
    else:
        print("\n💡 Not in Docker environment.")
        print("   For Docker: ./run_docker.sh")
        print("   For local: ./run_dashboard.sh")

if __name__ == "__main__":
    main()
