import os
from dotenv import load_dotenv

load_dotenv()

# EPİAŞ credentials
EPIAS_USERNAME = os.getenv("EPIAS_USERNAME")
EPIAS_PASSWORD = os.getenv("EPIAS_PASSWORD")

# EPİAŞ API base URLs
EPIAS_BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"
EPIAS_AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"

# Endpoints
ENDPOINTS = {
    "gop_mcp":        "/v1/markets/dam/data/mcp",
    "gip_price":      "/v1/markets/idm/data/idm-summary",
    "dgp_up":         "/v1/markets/bpm/data/bpm-up-instruction",
    "dgp_down":       "/v1/markets/bpm/data/bpm-down-instruction",
    "production":     "/v1/generation/data/realtime-generation",
    "consumption":    "/v1/consumption/data/realtime-consumption",
    "imbalance":      "/v1/markets/bpm/data/system-direction",
    "capacity_mech":  "/v1/markets/dam/data/side-payments",
}

# Default date range (last 30 days)
import pandas as pd
from datetime import datetime, timedelta

DEFAULT_START = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00+03:00")
DEFAULT_END   = datetime.today().strftime("%Y-%m-%dT23:00:00+03:00")

# Data paths
import pathlib
BASE_DIR      = pathlib.Path(__file__).parent.parent
RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
