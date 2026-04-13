# 🇹🇷 Turkey Power Trading — Market Analysis Dashboard

Professional energy trading analytics for Turkish electricity markets (EPİAŞ).

## Markets Covered
- **GÖP** — Gün Öncesi Piyasası (Day-Ahead Market)
- **GİP** — Gün İçi Piyasası (Intraday Market)
- **DGP** — Dengeleme Güç Piyasası (Balancing Power Market)
- **İkili Anlaşmalar** — Bilateral contracts

## Stack
- **Python 3.10+** — Data fetching, processing, ML models
- **Power BI** — Professional trading dashboard
- **EPİAŞ Transparency Platform** — Primary data source

## Quick Start

```bash
# 1. Clone repo
git clone https://github.com/BRSTKL/turkey-power-trading.git
cd turkey-power-trading

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure credentials
cp .env.example .env
# Edit .env with your EPİAŞ credentials

# 5. Test connection
python fetch/epias_auth.py

# 6. Fetch first data
python fetch/epias_gop.py
```

## Project Structure

```
turkey-power-trading/
├── fetch/                  # EPİAŞ API data fetchers
│   ├── epias_auth.py       # Token management
│   ├── epias_gop.py        # GÖP day-ahead prices (MCP)
│   ├── epias_gip.py        # GİP intraday prices
│   ├── epias_dgp.py        # DGP balancing prices (↑↓)
│   ├── epias_uretim.py     # Generation by source
│   ├── epias_tuketim.py    # Consumption (actual + forecast)
│   └── epias_dengesizlik.py # System imbalance
├── process/
│   ├── clean.py            # Data cleaning & validation
│   ├── indicators.py       # Spark spread, volatility, heatmaps
│   ├── merit_order_tr.py   # Turkish merit order estimation
│   └── export_powerbi.py   # Export to Power BI-ready format
├── models/
│   ├── price_forecast.py   # ML price forecasting (RF/XGBoost)
│   ├── var_cvar.py         # Portfolio risk model
│   └── imbalance_model.py  # Imbalance direction prediction
├── dashboards/
│   └── turkey_trading.pbix # Power BI dashboard
├── data/
│   ├── raw/                # Raw API responses (.json)
│   └── processed/          # Cleaned tables (.csv / .parquet)
├── notebooks/              # EDA & research
├── config/
│   └── settings.py         # API config, parameters
├── scheduler.py            # Daily auto-fetch scheduler
├── .env.example            # Credentials template
└── requirements.txt
```

## Dashboard Pages (Power BI)

| Page | Content |
|------|---------|
| 1. Trading Desk | Daily overview, MCP, system direction |
| 2. GÖP / GİP | Hourly prices, heatmap, peak/off-peak |
| 3. DGP & Balancing | Instruction prices, imbalance volume |
| 4. Portfolio & P&L | Bilateral vs spot, realized P&L |
| 5. Risk (VaR/CVaR) | Price risk, scenario analysis |
