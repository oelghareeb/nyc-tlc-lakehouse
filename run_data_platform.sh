#!/bin/bash
set -e

# ===============================================================
# 0. PREPARE ENVIRONMENT (.env with AIRFLOW_UID)
# ===============================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ ! -f "$SCRIPT_DIR/.env" ]; then
    echo "Creating .env file..."
    echo "AIRFLOW_UID=50000" > "$SCRIPT_DIR/.env"
    echo ".env file created with AIRFLOW_UID=50000"
else
    echo ".env file already exists."
fi

# ===============================================================
# 1. PATHS
# ===============================================================
AIRFLOW_COMPOSE="$SCRIPT_DIR/docker-compose.yml"
LAKEHOUSE_COMPOSE="$SCRIPT_DIR/lakehouse/docker-compose.yml"

NETWORK_NAME="data-platform"

# ===============================================================
# 2. COMMAND CHECK
# ===============================================================
if [ $# -eq 0 ]; then
    echo "Usage: $0 {up|down}"
    exit 1
fi

COMMAND=$1

# ===============================================================
# FUNCTIONS
# ===============================================================
start_services() {
    echo "=================================================="
    echo "Starting Deployment"
    echo "=================================================="

    # Create network if needed
    echo "Checking for Docker network: $NETWORK_NAME"
    if docker network ls --format '{{.Name}}' | grep -w "$NETWORK_NAME" > /dev/null; then
        echo "Network already exists."
    else
        echo "Creating network..."
        docker network create "$NETWORK_NAME"
    fi

    # Start Lakehouse stack
    if [ -f "$LAKEHOUSE_COMPOSE" ]; then
        echo "=================================================="
        echo "Building and starting lakehouse stack"
        echo "=================================================="
        docker compose -f "$LAKEHOUSE_COMPOSE" --project-name lakehouse build
        docker compose -f "$LAKEHOUSE_COMPOSE" --project-name lakehouse up -d
        echo "Lakehouse stack started."
    else
        echo "Lakehouse compose file not found, skipping lakehouse deployment."
    fi

    # Build Airflow images
    echo "=================================================="
    echo "Building Airflow stack"
    echo "=================================================="
    docker compose -f "$AIRFLOW_COMPOSE" --project-name airflow build
    
    # Initialize Airflow
    echo "=================================================="
    echo "Initializing Airflow database (official airflow-init)"
    echo "=================================================="
    docker compose -f "$AIRFLOW_COMPOSE" --project-name airflow up -d airflow-init
    echo "Airflow initialization completed."

    # Start Airflow services
    echo "=================================================="
    echo "Starting Airflow services"
    echo "=================================================="
    docker compose -f "$AIRFLOW_COMPOSE" --project-name airflow up -d

    echo "=================================================="
    echo "All services are up"
    echo "Lakehouse stack: running"
    echo "Airflow webserver: http://localhost:8080"
    echo "=================================================="
}

stop_services() {
    echo "=================================================="
    echo "Stopping and removing all containers and volumes"
    echo "=================================================="

    # Stop and remove Airflow stack with volumes
    if [ -f "$AIRFLOW_COMPOSE" ]; then
        echo "Stopping Airflow stack..."
        docker compose -f "$AIRFLOW_COMPOSE" --project-name airflow down -v
        echo "Airflow stack stopped and volumes removed."
    fi

    # Stop and remove Lakehouse stack with volumes
    if [ -f "$LAKEHOUSE_COMPOSE" ]; then
        echo "Stopping Lakehouse stack..."
        docker compose -f "$LAKEHOUSE_COMPOSE" --project-name lakehouse down -v
        echo "Lakehouse stack stopped and volumes removed."
    fi

    # Optionally remove network
    if docker network ls --format '{{.Name}}' | grep -w "$NETWORK_NAME" > /dev/null; then
        echo "Removing Docker network: $NETWORK_NAME"
        docker network rm "$NETWORK_NAME"
    fi

    echo "=================================================="
    echo "All services stopped and cleaned up"
    echo "=================================================="
}

# ===============================================================
# 3. EXECUTE COMMAND
# ===============================================================
case $COMMAND in
    up)
        start_services
        ;;
    down)
        stop_services
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: $0 {up|down}"
        exit 1
        ;;
esac