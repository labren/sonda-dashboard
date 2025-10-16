# Docker Setup for Solar Data Monitor Dashboard

This document explains the Docker-specific changes made to ensure the refresh button functionality works correctly in the containerized environment.

## 🐳 Docker Configuration Changes

### 1. Updated `docker-compose.yml`

**Added environment variables to Streamlit service:**
```yaml
environment:
  # Airflow connection settings for Docker environment
  - AIRFLOW_BASE_URL=http://airflow-apiserver:8080
  - AIRFLOW_USERNAME=airflow
  - AIRFLOW_PASSWORD=airflow
```

**Updated dependencies:**
```yaml
depends_on:
  - airflow-apiserver  # Added this dependency
  - airflow-dag-processor
  - postgres
```

### 2. Updated `airflow_config.json`

**Changed default configuration for Docker:**
```json
{
    "airflow": {
        "base_url": "http://airflow-apiserver:8080",  // Internal Docker network
        "username": "airflow",                        // Docker default credentials
        "password": "airflow"
    }
}
```

### 3. Enhanced `dashboard.py`

**Added environment variable support:**
- Automatically detects Docker environment via `AIRFLOW_BASE_URL` env var
- Falls back to config file for local development
- Uses correct credentials for each environment

**Key changes:**
```python
# Override with environment variables if they exist (for Docker)
airflow_config["base_url"] = os.getenv("AIRFLOW_BASE_URL", airflow_config.get("base_url", "http://localhost:8080"))
airflow_config["username"] = os.getenv("AIRFLOW_USERNAME", airflow_config.get("username", "admin"))
airflow_config["password"] = os.getenv("AIRFLOW_PASSWORD", airflow_config.get("password", "admin"))
```

## 🚀 How to Use

### Start the Complete System

```bash
# Use the Docker startup script (recommended)
./run_docker.sh

# Or manually with docker-compose
docker-compose up -d
```

### Access the Services

- **Dashboard**: http://localhost:8501
- **Airflow UI**: http://localhost:8082 (credentials: `airflow/airflow`)

### Test the Configuration

```bash
# Test Docker configuration
python test_docker_config.py

# Test refresh functionality
python test_refresh_functionality.py
```

## 🔧 Network Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Streamlit     │    │  Airflow API     │    │   PostgreSQL    │
│   Dashboard     │◄──►│  Server          │◄──►│   Database      │
│   :8501         │    │  :8080           │    │   :5432         │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌──────────────────┐
│   Redis Cache   │    │  Airflow Worker  │
│   :6379         │    │  (Celery)        │
└─────────────────┘    └──────────────────┘
```

## 🔍 Key Differences: Docker vs Local

| Aspect | Docker Environment | Local Environment |
|--------|-------------------|-------------------|
| **Airflow URL** | `http://airflow-apiserver:8080` | `http://localhost:8080` |
| **Credentials** | `airflow/airflow` | `admin/admin` |
| **Configuration** | Environment variables | `airflow_config.json` |
| **Network** | Internal Docker network | Localhost |
| **Ports** | 8082 (external), 8080 (internal) | 8080 |

## 🛠️ Troubleshooting

### Common Issues

1. **"Not connected to Airflow"**
   ```bash
   # Check if services are running
   docker-compose ps
   
   # Check Airflow logs
   docker-compose logs airflow-apiserver
   
   # Test connectivity from Streamlit container
   docker-compose exec streamlit curl http://airflow-apiserver:8080/health
   ```

2. **Services won't start**
   ```bash
   # Check Docker resources
   docker system df
   
   # Rebuild containers
   docker-compose build --no-cache
   
   # Check logs
   docker-compose logs
   ```

3. **Port conflicts**
   ```bash
   # Check if ports are in use
   netstat -tulpn | grep :8082
   netstat -tulpn | grep :8501
   ```

### Debug Commands

```bash
# View all service logs
docker-compose logs

# View specific service logs
docker-compose logs streamlit
docker-compose logs airflow-apiserver

# Execute commands in containers
docker-compose exec streamlit bash
docker-compose exec airflow-apiserver bash

# Check network connectivity
docker-compose exec streamlit ping airflow-apiserver
```

## 📋 Environment Variables

The following environment variables are set in `docker-compose.yml`:

| Variable | Value | Purpose |
|----------|-------|---------|
| `AIRFLOW_BASE_URL` | `http://airflow-apiserver:8080` | Internal Airflow API URL |
| `AIRFLOW_USERNAME` | `airflow` | Airflow username |
| `AIRFLOW_PASSWORD` | `airflow` | Airflow password |
| `STREAMLIT_SERVER_PORT` | `8501` | Streamlit port |
| `STREAMLIT_SERVER_ADDRESS` | `0.0.0.0` | Streamlit bind address |
| `PYTHONPATH` | `/opt/airflow` | Python path for imports |

## ✅ Verification Checklist

- [ ] All services start without errors
- [ ] Dashboard accessible at http://localhost:8501
- [ ] Airflow UI accessible at http://localhost:8082
- [ ] Can login to Airflow with `airflow/airflow`
- [ ] Refresh button shows "Connected to Airflow"
- [ ] DAGs are visible in Airflow UI
- [ ] Refresh button triggers DAGs successfully
- [ ] Data updates in dashboard after refresh

## 🔄 Refresh Button Workflow

1. **User clicks refresh button** in Streamlit dashboard
2. **Dashboard connects** to Airflow API via internal Docker network
3. **Triggers download DAG** (`ftp_multi_station_download`)
4. **Monitors progress** with real-time updates
5. **Triggers processing DAG** (`process_multistation_data`)
6. **Monitors processing** with progress bars
7. **Updates dashboard** with latest data
8. **Shows success/failure** status to user

This setup ensures seamless operation in the Docker environment while maintaining compatibility with local development.
