#!/bin/bash
set -e

# -------------------------------
# Activate Superset virtual environment
# -------------------------------
source /app/.venv/bin/activate

# -------------------------------
# Initialize / Upgrade Superset DB
# -------------------------------
echo "Upgrading Superset metadata database..."
superset db upgrade

# -------------------------------
# Create admin user if not exists
# -------------------------------
echo "Creating admin user (if not exists)..."
export FLASK_APP=superset
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@admin.com \
    --password admin || echo "Admin user already exists, skipping..."

# -------------------------------
# Initialize Superset
# -------------------------------
echo "Initializing Superset..."
superset init

# -------------------------------
# Start Superset web server
# -------------------------------
echo "Starting Superset web server on 0.0.0.0:8088..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
