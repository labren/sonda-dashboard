# Dashboard Refresh Functionality

This document explains how to use the new refresh button functionality in the Solar Data Monitor dashboard.

## Overview

The refresh button allows you to trigger the complete data pipeline from the dashboard:
1. **Download** latest data from FTP servers
2. **Process** the raw data into the interim format
3. **Update** the dashboard with the latest data

## Setup

### 1. Docker Environment (Recommended)

The system is designed to run in Docker containers. Use the provided Docker setup:

```bash
# Start the complete system with Docker
./run_docker.sh

# Or manually with docker-compose
docker-compose up -d
```

This will start:
- **Airflow webserver** on `http://localhost:8082` (credentials: `airflow/airflow`)
- **Streamlit dashboard** on `http://localhost:8501`
- **PostgreSQL database** and **Redis cache**

### 2. Local Development Setup

For local development without Docker:

```bash
# Start Airflow locally
airflow webserver --port 8080
airflow scheduler

# Start the dashboard
./run_dashboard.sh
```

### 3. Configuration

The system automatically detects the environment and uses appropriate settings:

**Docker Environment:**
- Airflow URL: `http://airflow-apiserver:8080` (internal Docker network)
- Credentials: `airflow/airflow`
- Configured via environment variables in `docker-compose.yml`

**Local Environment:**
- Airflow URL: `http://localhost:8080`
- Credentials: `admin/admin` (default)
- Configured via `airflow_config.json`

You can customize settings by modifying `airflow_config.json`:

```json
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
```

### 3. DAG Deployment

Make sure the following DAGs are deployed and enabled in Airflow:
- `ftp_multi_station_download` - Downloads data from FTP servers
- `process_multistation_data` - Processes raw data to interim format
- `header_discovery_dag` - Discovers missing headers (optional)

## Usage

### 1. Access the Dashboard

```bash
streamlit run dashboard.py
```

### 2. Use the Refresh Button

1. Navigate to the dashboard
2. Select a station from the dropdown
3. Click the "🔄 Refresh Latest Data" button
4. Monitor the progress in real-time

### 3. Monitor Progress

The dashboard will show:
- ✅ Connection status to Airflow
- 🔄 Real-time progress bars for each pipeline step
- 📊 Status of recent DAG runs
- ⚠️ Error messages if something goes wrong

## Features

### Real-time Monitoring
- Progress bars for download and processing steps
- Live status updates every 5 seconds
- Automatic timeout handling

### Error Handling
- Connection validation
- DAG availability checks
- Graceful failure handling
- Detailed error messages

### Configuration
- Customizable timeouts
- Configurable DAG names
- Flexible Airflow connection settings

## Troubleshooting

### Docker Environment Issues

**Problem**: "Not connected to Airflow" in Docker
**Solutions**:
1. Check if all services are running: `docker-compose ps`
2. Check Airflow logs: `docker-compose logs airflow-apiserver`
3. Verify network connectivity: `docker-compose exec streamlit curl http://airflow-apiserver:8080/health`
4. Ensure services are on the same network: `docker network ls`

**Problem**: Services won't start
**Solutions**:
1. Check Docker resources: `docker system df`
2. Ensure ports are available: `netstat -tulpn | grep :8082`
3. Check logs: `docker-compose logs`
4. Rebuild if needed: `docker-compose build --no-cache`

### Connection Issues

**Problem**: "Not connected to Airflow"
**Solutions**:
1. **Docker**: Check if Airflow is running: `curl http://localhost:8082/health`
2. **Local**: Check if Airflow is running: `curl http://localhost:8080/health`
3. Verify credentials in `airflow_config.json` or environment variables
4. Ensure Airflow REST API is enabled
5. Check firewall/network settings

### DAG Issues

**Problem**: "DAG not found"
**Solutions**:
1. Verify DAG names in `airflow_config.json`
2. Check if DAGs are deployed in Airflow UI
3. Ensure DAGs are enabled (not paused)
4. Check DAG file syntax

### Timeout Issues

**Problem**: "Monitoring timed out"
**Solutions**:
1. Increase timeout values in `airflow_config.json`
2. Check Airflow scheduler performance
3. Monitor system resources
4. Check for DAG execution bottlenecks

## Testing

Run the test script to verify functionality:

```bash
python test_refresh_functionality.py
```

This will test:
- Airflow connection
- DAG availability
- Pipeline triggering
- Error handling

## API Reference

### AirflowClient Class

```python
client = AirflowClient(config=config_dict)
client.trigger_dag(dag_id, conf=None)
client.get_dag_status(dag_id)
client.get_dag_runs(dag_id, limit=5)
```

### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `base_url` | Airflow webserver URL | `http://localhost:8080` |
| `username` | Airflow username | `admin` |
| `password` | Airflow password | `admin` |
| `timeout_minutes.download` | Download DAG timeout | `5` |
| `timeout_minutes.process` | Processing DAG timeout | `10` |
| `download_dag` | Download DAG ID | `ftp_multi_station_download` |
| `process_dag` | Processing DAG ID | `process_multistation_data` |

## Security Notes

- Store sensitive credentials in environment variables or secure config files
- Use HTTPS for production Airflow deployments
- Implement proper authentication and authorization
- Regularly rotate passwords and API keys

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Airflow logs for detailed error information
3. Verify DAG configurations and dependencies
4. Test with the provided test script
