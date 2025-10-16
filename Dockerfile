# Use the official Airflow image
FROM apache/airflow:latest

# Install any additional packages
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Set the Airflow home directory
ENV AIRFLOW_HOME=/opt/airflow

# Copy the DAGs folder into the container
COPY dags/ $AIRFLOW_HOME/dags/
COPY sonda_translator/ $AIRFLOW_HOME/sonda_translator/

USER airflow

EXPOSE 8501