import os
from dotenv import load_dotenv

load_dotenv()

# EPİAŞ Transparency Platform
EPIAS_BASE_URL = "https://seffaflik.epias.com.tr/transparency/service"
EPIAS_AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"
EPIAS_USERNAME = os.getenv("EPIAS_USERNAME")
EPIAS_PASSWORD = os.getenv("EPIAS_PASSWORD")

# API Endpoints
ENDPOINTS = {
    # GÖP - Day-Ahead Market
    "gop_mcp":          "/market/day-ahead/prices",          # MCP (PTF)
    "gop_volume":       "/market/day-ahead/clearing-quantity",

    # GİP - Intraday Market
    "gip_prices":       "/market/intra-day/trade-history",
    "gip_volume":       "/market/intra-day/trade-volume",

    # DGP - Balancing Power Market
    "dgp_up":           "/market/bpm/up-regulation-price",    # Artırım (↑)
    "dgp_down":         "/market/bpm/down-regulation-price",  # Azaltım (↓)
    "dgp_smp":          "/market/settlement/system-marginal-price",  # SMF

    # System
    "imbalance":        "/market/settlement/imbalance-quantity",
    "system_direction": "/market/settlement/system-direction",

    # Generation & Consumption
    "realtime_gen":     "/production/realtime-generation",
    "forecast_gen":     "/production/dpp",                    # DPP
    "consumption":      "/consumption/real-time-consumption",
    "consumption_fc":   "/consumption/load-estimation-plan",  # YAP
}

# Data Storage
DATA_PATH = os.getenv("DATA_PATH", "./data")
RAW_PATH  = f"{DATA_PATH}/raw"
PROC_PATH = f"{DATA_PATH}/processed"

# Date format used by EPİAŞ API
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S+03:00"
