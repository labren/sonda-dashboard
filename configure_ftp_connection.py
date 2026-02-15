#!/usr/bin/env python3
"""
Script to configure FTP connection in Airflow.
Sets up the solter.ftp.1 connection required for data download.
"""

import sys
import json
import subprocess
import time
from pathlib import Path

CONN_ID = "solter.ftp.1"
CONFIG_FILE = Path("ftp_connection_config.json")
EXAMPLE_FILE = Path("ftp_connection_config.example.json")
AIRFLOW_CMD = ["docker-compose", "exec", "-T", "airflow-apiserver", "airflow", "connections"]


def run_airflow_conn(*args, timeout=30):
    """Run an airflow connections subcommand. Returns (success, stderr)."""
    try:
        result = subprocess.run([*AIRFLOW_CMD, *args], capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stderr
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except Exception as e:
        return False, str(e)


def wait_for_airflow(max_attempts=30):
    """Wait for Airflow to be ready."""
    print("Waiting for Airflow to be ready...")
    for attempt in range(max_attempts):
        try:
            r = subprocess.run(["curl", "-s", "-f", "http://localhost:8082/api/v2/version"],
                               capture_output=True, timeout=5)
            if r.returncode == 0:
                print("Airflow is ready")
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        print(f"   Attempt {attempt + 1}/{max_attempts}...")
        time.sleep(10)
    return False


def connection_exists():
    """Check if the FTP connection already exists."""
    ok, _ = run_airflow_conn("get", CONN_ID)
    return ok


def add_connection(host, port, login, password, extra='{"passive": true, "secure": false}'):
    """Create the FTP connection in Airflow."""
    print("Creating FTP connection...")
    ok, err = run_airflow_conn(
        "add", CONN_ID,
        "--conn-type", "ftp", "--conn-host", host, "--conn-port", str(port),
        "--conn-login", login, "--conn-password", password, "--conn-extra", extra,
        timeout=60)
    if ok:
        print("FTP connection created successfully")
    else:
        print(f"Failed to create FTP connection: {err}")
    return ok


def delete_connection():
    """Delete existing FTP connection."""
    ok, err = run_airflow_conn("delete", CONN_ID)
    if ok:
        print("Existing connection deleted")
    else:
        print(f"Could not delete existing connection: {err}")


def load_config():
    """Load FTP config from file. Returns dict or None."""
    if not CONFIG_FILE.exists():
        return None
    try:
        cfg = json.loads(CONFIG_FILE.read_text())
        return cfg.get("ftp_connections", {}).get(CONN_ID)
    except Exception as e:
        print(f"Error reading config file: {e}")
        return None


def save_config(host, port, login, password):
    """Save FTP credentials to config file."""
    try:
        config = json.loads(CONFIG_FILE.read_text()) if CONFIG_FILE.exists() else {"ftp_connections": {}}
        config["ftp_connections"][CONN_ID] = {
            "conn_type": "ftp", "host": host, "port": int(port),
            "login": login, "password": password,
            "extra": {"passive": True, "secure": False}
        }
        CONFIG_FILE.write_text(json.dumps(config, indent=4))
        print("FTP credentials saved to config file")
    except Exception as e:
        print(f"Could not save credentials to config file: {e}")


def ensure_config_file():
    """Create config file from example if it doesn't exist."""
    if CONFIG_FILE.exists():
        return
    if not EXAMPLE_FILE.exists():
        return
    try:
        CONFIG_FILE.write_text(EXAMPLE_FILE.read_text())
        print("Created ftp_connection_config.json from example")
        print("Please edit the file with your FTP credentials")
    except Exception as e:
        print(f"Error creating config file: {e}")


def create_from_config():
    """Try creating connection from config file. Returns True on success."""
    cfg = load_config()
    if not cfg:
        return False
    login, password = cfg.get("login", ""), cfg.get("password", "")
    if not login or not password:
        print("Username and password must be provided in config file")
        return False
    return add_connection(cfg.get("host", "ftp.example.com"), cfg.get("port", 21),
                          login, password, json.dumps(cfg.get("extra", {})))


def create_interactive():
    """Prompt user for credentials and create connection."""
    print("\nFTP Connection Setup")
    print("=" * 30)
    print("Please provide the FTP connection details:")
    print("(Press Enter to use default values)\n")
    host = input("FTP Host [ftp.example.com]: ").strip() or "ftp.example.com"
    port = input("FTP Port [21]: ").strip() or "21"
    login = input("FTP Username: ").strip()
    password = input("FTP Password: ").strip()
    if not login or not password:
        print("Username and password are required")
        return False
    if add_connection(host, port, login, password):
        save_config(host, port, login, password)
        return True
    return False


def main():
    print("FTP Connection Configuration")
    print("=" * 40)
    interactive = "--interactive" in sys.argv or "-i" in sys.argv

    if not Path("docker-compose.yml").exists():
        sys.exit("Not in project root directory")
    if not wait_for_airflow():
        sys.exit("Airflow is not ready. Please start the services first.")

    exists = connection_exists()
    if exists and not interactive:
        print("FTP connection is already configured")
        sys.exit(0)
    if exists and interactive:
        delete_connection()

    ensure_config_file()

    if not interactive and create_from_config():
        print("FTP connection configured successfully!")
        sys.exit(0)

    if create_interactive():
        print("FTP connection configured successfully!")
        sys.exit(0)

    sys.exit("Failed to create FTP connection")


if __name__ == "__main__":
    main()
