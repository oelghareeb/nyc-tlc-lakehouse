# ==========================
# Superset Configuration
# ==========================

ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Disable tracking for better performance
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Secret key for session signing
SECRET_KEY = "supersecretkey123"
# MAPBOX_API_KEY = "pk.eyJ1Ijoib2VsZ2hhcmVlYiIsImEiOiJjbWUxMnU5NjgwOHd6MnFyMGZ1NTZ2aTZhIn0.VRJlEdVgoAPElBkVZkZdxQ"
# # Optional: Enable async query execution (needs Celery & Redis)
# # FEATURE_FLAGS = {
# #     "ASYNC_QUERIES": True,
# # }

