#!/usr/bin/env python3
"""
Script to configure FTP connection in Airflow
This script sets up the solter.ftp.1 connection required for data download
"""

import os
import sys
import json
import subprocess
import time
from pathlib import Path

# Note: This script now works without external dependencies like requests
# It uses built-in Python modules and system commands instead

def wait_for_airflow():
    """Wait for Airflow to be ready"""
    print("⏳ Waiting for Airflow to be ready...")
    
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            # Try to check if Airflow is ready using curl instead of requests
            result = subprocess.run([
                "curl", "-s", "-f", "http://localhost:8082/api/v2/version"
            ], capture_output=True, timeout=5)
            
            if result.returncode == 0:
                print("✅ Airflow is ready")
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        
        print(f"   Attempt {attempt + 1}/{max_attempts}...")
        time.sleep(10)
    
    print("❌ Airflow did not become ready within 5 minutes")
    return False

def check_connection_exists():
    """Check if the FTP connection already exists"""
    try:
        result = subprocess.run([
            "docker-compose", "exec", "-T", "airflow-apiserver", 
            "airflow", "connections", "get", "solter.ftp.1"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ FTP connection 'solter.ftp.1' already exists")
            return True
        else:
            print("ℹ️ FTP connection 'solter.ftp.1' does not exist")
            return False
    except subprocess.TimeoutExpired:
        print("⚠️ Timeout checking connection, assuming it doesn't exist")
        return False
    except Exception as e:
        print(f"⚠️ Error checking connection: {e}")
        return False

def create_connection_interactive():
    """Create FTP connection interactively"""
    print("\n🔧 FTP Connection Setup")
    print("=" * 30)
    print("Please provide the FTP connection details:")
    print("(Press Enter to use default values)")
    print()
    
    # Get connection details from user
    host = input("FTP Host [ftp.example.com]: ").strip() or "ftp.example.com"
    port = input("FTP Port [21]: ").strip() or "21"
    username = input("FTP Username: ").strip()
    password = input("FTP Password: ").strip()
    
    if not username or not password:
        print("❌ Username and password are required")
        return False
    
    # Create the connection using Airflow CLI
    try:
        cmd = [
            "docker-compose", "exec", "-T", "airflow-apiserver",
            "airflow", "connections", "add",
            "solter.ftp.1",
            "--conn-type", "ftp",
            "--conn-host", host,
            "--conn-port", port,
            "--conn-login", username,
            "--conn-password", password,
            "--conn-extra", '{"passive": true, "secure": false}'
        ]
        
        print("🔄 Creating FTP connection...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ FTP connection created successfully")
            
            # Save credentials to config file
            save_credentials_to_config(host, port, username, password)
            return True
        else:
            print(f"❌ Failed to create FTP connection: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout creating connection")
        return False
    except Exception as e:
        print(f"❌ Error creating connection: {e}")
        return False

def create_connection_from_config():
    """Create FTP connection from configuration file"""
    config_file = Path("ftp_connection_config.json")
    
    if not config_file.exists():
        print("❌ FTP connection config file not found")
        return False
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        ftp_config = config.get("ftp_connections", {}).get("solter.ftp.1", {})
        
        if not ftp_config:
            print("❌ FTP connection configuration not found in config file")
            return False
        
        # Extract connection details
        host = ftp_config.get("host", "ftp.example.com")
        port = ftp_config.get("port", 21)
        username = ftp_config.get("login", "")
        password = ftp_config.get("password", "")
        extra = ftp_config.get("extra", {})
        
        if not username or not password:
            print("❌ Username and password must be provided in config file")
            return False
        
        # Create the connection
        cmd = [
            "docker-compose", "exec", "-T", "airflow-apiserver",
            "airflow", "connections", "add",
            "solter.ftp.1",
            "--conn-type", "ftp",
            "--conn-host", host,
            "--conn-port", str(port),
            "--conn-login", username,
            "--conn-password", password,
            "--conn-extra", json.dumps(extra)
        ]
        
        print("🔄 Creating FTP connection from config...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ FTP connection created successfully from config")
            return True
        else:
            print(f"❌ Failed to create FTP connection: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error reading config file: {e}")
        return False

def create_config_from_example():
    """Create config file from example if it doesn't exist"""
    config_file = Path("ftp_connection_config.json")
    example_file = Path("ftp_connection_config.example.json")
    
    if config_file.exists():
        return True
    
    if not example_file.exists():
        print("❌ Example config file not found")
        return False
    
    try:
        # Copy example to config file
        with open(example_file, 'r') as f:
            example_config = json.load(f)
        
        with open(config_file, 'w') as f:
            json.dump(example_config, f, indent=4)
        
        print("📁 Created ftp_connection_config.json from example")
        print("💡 Please edit the file with your FTP credentials")
        return True
        
    except Exception as e:
        print(f"❌ Error creating config file: {e}")
        return False

def save_credentials_to_config(host, port, username, password):
    """Save FTP credentials to config file"""
    try:
        config_file = Path("ftp_connection_config.json")
        
        # Load existing config or create new one
        if config_file.exists():
            with open(config_file, 'r') as f:
                config = json.load(f)
        else:
            config = {
                "ftp_connections": {
                    "solter.ftp.1": {}
                }
            }
        
        # Update the FTP connection details
        config["ftp_connections"]["solter.ftp.1"] = {
            "conn_type": "ftp",
            "host": host,
            "port": int(port),
            "login": username,
            "password": password,
            "extra": {
                "passive": True,
                "secure": False
            }
        }
        
        # Save updated config
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
        
        print("💾 FTP credentials saved to config file")
        
    except Exception as e:
        print(f"⚠️ Could not save credentials to config file: {e}")
        print("💡 You can manually edit ftp_connection_config.json if needed")

def test_connection():
    """Test the FTP connection"""
    print("🧪 Testing FTP connection...")
    
    try:
        # Simple test - just check if the connection exists in Airflow
        result = subprocess.run([
            "docker-compose", "exec", "-T", "airflow-apiserver",
            "airflow", "connections", "get", "solter.ftp.1"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ FTP connection exists and is accessible")
            return True
        else:
            print(f"❌ FTP connection test failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Connection test timeout")
        return False
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False

def main():
    """Main function"""
    print("🔧 FTP Connection Configuration")
    print("=" * 40)
    
    # Check for interactive flag
    force_interactive = "--interactive" in sys.argv or "-i" in sys.argv
    print(f"🔍 Interactive mode: {'YES' if force_interactive else 'NO'}")
    
    # Check if we're in the right directory
    if not Path("docker-compose.yml").exists():
        print("❌ Not in project root directory")
        sys.exit(1)
    
    # Wait for Airflow to be ready
    if not wait_for_airflow():
        print("❌ Airflow is not ready. Please start the services first.")
        sys.exit(1)
    
    # Check if connection already exists (skip if interactive mode)
    if not force_interactive and check_connection_exists():
        print("✅ FTP connection is already configured")
        sys.exit(0)
    elif force_interactive and check_connection_exists():
        print("🔄 Connection already exists. Deleting to allow reconfiguration...")
        try:
            subprocess.run([
                "docker-compose", "exec", "-T", "airflow-apiserver",
                "airflow", "connections", "delete", "solter.ftp.1"
            ], capture_output=True, text=True, timeout=30)
            print("✅ Existing connection deleted")
        except Exception as e:
            print(f"⚠️ Could not delete existing connection: {e}")
            print("💡 You may need to delete it manually in Airflow UI")
    
    # Create config file from example if it doesn't exist
    if not Path("ftp_connection_config.json").exists():
        print("📁 Creating config file from example...")
        create_config_from_example()
    
    # Interactive setup (always when --interactive flag is used)
    if force_interactive:
        print("📝 Interactive mode requested - will prompt for credentials")
    else:
        # Try to create from config file first (only if not interactive)
        if Path("ftp_connection_config.json").exists():
            print("📁 Found FTP connection config file")
            if create_connection_from_config():
                if test_connection():
                    print("🎉 FTP connection configured successfully!")
                    sys.exit(0)
                else:
                    print("⚠️ Connection created but test failed")
            else:
                print("⚠️ Failed to create from config, falling back to interactive setup")
        else:
            print("📝 No config file found, using interactive setup")
    
    if create_connection_interactive():
        if test_connection():
            print("🎉 FTP connection configured successfully!")
            sys.exit(0)
        else:
            print("⚠️ Connection created but test failed")
            sys.exit(1)
    else:
        print("❌ Failed to create FTP connection")
        sys.exit(1)

if __name__ == "__main__":
    main()
