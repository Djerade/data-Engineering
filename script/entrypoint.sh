#!/bin/bash

# Don't exit on error for some commands, but track failures
set -o pipefail

# Install requirements if file exists (only if network is available)
if [ -e "/opt/airflow/requirements.txt" ]; then
    echo "Installing Python requirements..."
    # Try to ping to check network connectivity
    if ping -c 1 -W 2 8.8.8.8 > /dev/null 2>&1; then
        pip install --no-cache-dir -r /opt/airflow/requirements.txt || {
            echo "Warning: Failed to install some requirements. Continuing anyway..."
        }
    else
        echo "Warning: No internet connectivity. Skipping requirements installation."
        echo "Note: Some packages may already be installed in the base image."
    fi
fi

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
python3 << 'EOF'
import time
import sys

# Try to import psycopg2, install if not available
try:
    import psycopg2
except ImportError:
    print("psycopg2 not found, attempting to install...")
    import subprocess
    try:
        subprocess.check_call(["pip", "install", "--no-cache-dir", "psycopg2-binary"], 
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import psycopg2
        print("psycopg2-binary installed successfully")
    except Exception as e:
        print(f"Failed to install psycopg2: {e}")
        print("Trying to continue without psycopg2 check...")
        sys.exit(0)

max_retries = 30
retry_count = 0

while retry_count < max_retries:
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow",
            connect_timeout=2
        )
        conn.close()
        print("PostgreSQL is up - executing command")
        sys.exit(0)
    except (psycopg2.OperationalError, psycopg2.Error) as e:
        retry_count += 1
        if retry_count < max_retries:
            print(f"PostgreSQL is unavailable - sleeping (attempt {retry_count}/{max_retries}): {e}")
            time.sleep(2)
        else:
            print("Failed to connect to PostgreSQL after maximum retries")
            sys.exit(1)
EOF

# Initialize database if needed
echo "Checking Airflow database..."
if ! airflow db check > /dev/null 2>&1; then
    echo "Initializing Airflow database..."
    airflow db init || echo "Database initialization may have failed, continuing..."
fi

# Create admin user if it doesn't exist
echo "Checking if admin user exists..."
if ! airflow users list 2>/dev/null | grep -q "admin"; then
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com 2>&1 | grep -v "already exists" || true
else
    echo "Admin user already exists"
fi

# Upgrade database
echo "Upgrading Airflow database..."
airflow db upgrade || echo "Database upgrade may have failed, continuing..."

# Execute the command passed as argument
exec "$@" 

