# -------------------------------
# Base Superset image
# -------------------------------
FROM apache/superset:latest

# Switch to root to install system dependencies
USER root

# Install packages using uv into the virtual environment
RUN . /app/.venv/bin/activate && \
    uv pip install \
    # install psycopg2 for using PostgreSQL metadata store - could be a MySQL package if using that backend:
    psycopg2-binary \
    # Trino driver
    trino \
    sqlalchemy-trino \
    # package needed for using single-sign on authentication:
    Authlib \
    # openpyxl to be able to upload Excel files
    openpyxl \
    # Pillow for Alerts & Reports to generate PDFs of dashboards
    Pillow \
    # install Playwright for taking screenshots for Alerts & Reports. This assumes the feature flag PLAYWRIGHT_REPORTS_AND_THUMBNAILS is enabled
    # That feature flag will default to True starting in 6.0.0
    # Playwright works only with Chrome.
    # If you are still using Selenium instead of Playwright, you would instead install here the selenium package and a headless browser & webdriver
    playwright \
    && playwright install-deps \
    && PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright-browsers playwright install chromium

# -------------------------------
# Copy Superset init script and config
# -------------------------------
WORKDIR /app
COPY superset-init.sh superset-init.sh
COPY superset_config.py superset_config.py

RUN chmod +x superset-init.sh

# Set Superset config path
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# Switch back to superset user
USER superset

# Entrypoint to initialize and start Superset
ENTRYPOINT ["/app/superset-init.sh"]