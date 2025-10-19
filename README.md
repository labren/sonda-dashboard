# INPE SONDA Dashboard

Repositório do projeto INPE-SOLTER, onde estarão armazenadas as rotinas necessárias para o funcionamento do dashboard.

## 🚀 Quick Start

### Fresh Installation (First Time Setup)

1. **Clone and start the system:**
   ```bash
   ./setup_fresh_install.sh
   ```
   This will:
   - Start all services (Airflow + Streamlit)
   - Configure FTP connection interactively
   - Provide next steps

2. **Configure FTP connection when prompted:**
   - FTP Host: Your FTP server address
   - FTP Port: 21 (default)
   - Username: Your FTP username
   - Password: Your FTP password

3. **Open the dashboard:** http://localhost:8501

4. **Click "Initialize Data Pipeline"** button in the dashboard

5. **Wait for completion** and refresh the page

### Regular Usage (Existing Installation)

```bash
docker-compose up -d
```

Then open:
- **Dashboard:** http://localhost:8501
- **Airflow UI:** http://localhost:8082 (airflow/airflow)

## 🔧 Reinitialize Existing Installation

If you need to reset or reinitialize an existing installation:

### Option 1: Full Reset
```bash
# Stop services
docker-compose down

# Remove data directories (optional - keeps your data)
# rm -rf data/raw data/interim

# Start fresh
./setup_fresh_install.sh
```

### Option 2: Reconfigure FTP Connection
```bash
python3 configure_ftp_connection.py --interactive
```

### Option 3: Manual Data Refresh
```bash
# Trigger orchestrated refresh pipeline (RECOMMENDED)
docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline

# Or trigger individual DAGs (NOT RECOMMENDED - may cause data issues)
docker-compose exec airflow-apiserver airflow dags trigger ftp_multi_station_download
docker-compose exec airflow-apiserver airflow dags trigger process_multistation_data
```

## 📊 Features

- **Real-time solar radiation data visualization**
- **Multi-station data monitoring** (10+ stations)
- **Orchestrated data pipeline** with proper sequencing
- **Interactive dashboard** with Streamlit
- **Clear sky model calculations**
- **Data quality monitoring**
- **Automatic cache management** for fresh data detection
- **Robust error handling** and retry mechanisms
- **Sequential pipeline execution** (download → process)

## 🆕 Recent Improvements

### Pipeline Orchestration
- **Sequential Execution:** Processing only starts after ALL downloads complete
- **Error Prevention:** Failed downloads prevent processing of old data
- **Robust Monitoring:** REST API-based DAG status checking
- **Automatic Recovery:** System handles download failures gracefully

### Dashboard Enhancements
- **Cache Management:** Automatic cache clearing for new data detection
- **Manual Controls:** Clear Cache and Force Refresh buttons
- **Faster Detection:** New data detected within 15-30 seconds
- **Better UX:** Visual feedback and status indicators

### Technical Fixes
- **Airflow 3.0 Compatibility:** Fixed database access restrictions
- **Task Mapping:** Resolved `.override()` method issues
- **Retry Logic:** Improved FTP download retry mechanisms
- **Error Handling:** Better exception handling and task failure propagation

## 🔧 Troubleshooting

### No Data on Fresh Install
If you see "No stations found":

1. **Configure FTP connection:**
   ```bash
   python3 configure_ftp_connection.py --interactive
   ```

2. **Click "Initialize Data Pipeline"** in the dashboard

3. **Or run manually:**
   ```bash
   docker-compose exec airflow-apiserver airflow dags trigger initial_data_setup
   ```

### Common Issues

**"Connection refused" error:**
- Wait 2-3 minutes for services to start
- Check: `docker-compose ps`

**"FTP connection failed" error:**
- Run: `python3 configure_ftp_connection.py --interactive`
- Check FTP credentials in Airflow UI: http://localhost:8082

**"No data in dashboard" after setup:**
- Click "🧹 Clear Cache" button in dashboard
- Clear browser cache and refresh
- Check if data exists: `ls -la data/interim/`
- Verify DAG completed in Airflow UI
- Use "🔄 Force Refresh" button if needed

**Dashboard cache issues:**
- Cache automatically clears after initial setup completion
- Manual cache clearing available via dashboard buttons
- Cache TTL reduced to 15-30 seconds for faster detection
- Force refresh available for immediate data detection

**Services not starting:**
- Check Docker is running: `docker info`
- Check logs: `docker-compose logs`
- Restart: `docker-compose down && docker-compose up -d`

## 📁 Project Structure

```
├── dags/                           # Airflow DAGs
│   ├── initial_data_setup_dag.py   # Fresh install setup
│   ├── ftp_multi_station_download  # Data download
│   └── process_multistation_data   # Data processing
├── data/                           # Data storage
│   ├── raw/                        # Raw FTP data
│   └── interim/                    # Processed data for dashboard
├── dashboard.py                    # Streamlit dashboard
├── docker-compose.yml              # Docker configuration
├── setup_fresh_install.sh         # Automated setup script
├── configure_ftp_connection.py    # FTP connection setup
└── config_files/                   # Configuration files
```

## 🔄 Data Pipeline

The system uses a sophisticated orchestrated pipeline with proper sequencing:

1. **Download:** FTP data retrieval (daily schedule)
2. **Process:** Data transformation and validation  
3. **Store:** Save to interim format
4. **Visualize:** Display in dashboard

**Pipeline DAGs:**
- `initial_data_setup` - Fresh installation setup (downloads + processes initial data)
- `refresh_data_pipeline` - **NEW!** Orchestrated pipeline that ensures proper sequencing
- `ftp_multi_station_download` - Daily data download (triggered by refresh pipeline)
- `process_multistation_data` - Data processing (triggered after download completion)

**Pipeline Orchestration:**
- **Sequential Execution:** Processing only starts after ALL downloads complete
- **Error Handling:** Failed downloads prevent processing of old data
- **Retry Logic:** Robust retry mechanisms for FTP connections
- **Cache Management:** Dashboard automatically detects new data

## 🚀 Quick Reference

### Recommended Workflow
1. **Fresh Install:** Use `./setup_fresh_install.sh`
2. **Data Refresh:** Use dashboard "🔄 Refresh Latest Data" button
3. **Manual Trigger:** `docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline`
4. **Cache Issues:** Use "🧹 Clear Cache" button in dashboard

### Pipeline DAGs (in order)
1. `refresh_data_pipeline` - **Main orchestration DAG**
2. `ftp_multi_station_download` - Downloads data (triggered by #1)
3. `process_multistation_data` - Processes data (triggered after #2 completes)

## 🛠️ Manual Commands

### Service Management
```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs

# Restart services
docker-compose restart
```

### Airflow Commands
```bash
# List DAGs
docker-compose exec airflow-apiserver airflow dags list

# Trigger orchestrated refresh pipeline (RECOMMENDED)
docker-compose exec airflow-apiserver airflow dags trigger refresh_data_pipeline

# Trigger individual DAGs (use with caution)
docker-compose exec airflow-apiserver airflow dags trigger ftp_multi_station_download
docker-compose exec airflow-apiserver airflow dags trigger process_multistation_data

# Check DAG status
docker-compose exec airflow-apiserver airflow dags state <dag_id>

# View DAG runs
docker-compose exec airflow-apiserver airflow dags list-runs --dag-id <dag_id>

# Clear failed tasks (if needed)
docker-compose exec airflow-apiserver airflow tasks clear refresh_data_pipeline -t wait_for_download_completion
```

### FTP Connection Management
```bash
# Configure FTP connection
python3 configure_ftp_connection.py --interactive

# Test connection
docker-compose exec airflow-apiserver airflow connections get solter.ftp.1

# List connections
docker-compose exec airflow-apiserver airflow connections list
```

## 📋 Configuration Files

- **`ftp_connection_config.json`** - FTP connection settings
- **`airflow_config.json`** - Airflow dashboard configuration
- **`config_files/stations_download_config.json`** - Station configuration

## 🧪 Testing

Test your installation:
```bash
python3 test_fresh_install.py
```

This will check:
- Services are running
- FTP connection is configured
- Data directories exist
- Dashboard can load data

## 📚 URLs

- **Dashboard:** http://localhost:8501
- **Airflow UI:** http://localhost:8082
- **Credentials:** airflow/airflow

## 🆘 Support

If you continue to have issues:

1. **Check service status:** `docker-compose ps`
2. **View logs:** `docker-compose logs`
3. **Test installation:** `python3 test_fresh_install.py`
4. **Reset everything:** `docker-compose down && ./setup_fresh_install.sh`

## 📝 Notes

- **Data retention:** Raw data kept for 1 day, processed data kept indefinitely
- **Scheduling:** DAGs run daily at midnight
- **Stations:** 10+ solar monitoring stations supported
- **Data types:** SD (Solar Data), MD (Meteorological Data), WD (Wind Data)
- **Pipeline orchestration:** Use `refresh_data_pipeline` for proper sequencing
- **Cache management:** Dashboard automatically detects new data within 15-30 seconds
- **Error handling:** Failed downloads prevent processing of old data
- **Retry logic:** FTP downloads have 5 retries with exponential backoff