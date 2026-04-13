"""
GÖP — Gün Öncesi Piyasası
Saatlik MCP (Piyasa Takas Fiyatı) verisi çeker ve kaydeder.
"""

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path

from config.settings import EPIAS_BASE_URL, ENDPOINTS, RAW_DIR, PROCESSED_DIR
from fetch.epias_auth import get_token, get_headers


def fetch_gop_mcp(start_date: str, end_date: str, token: str) -> dict:
    """
    GÖP MCP verisini EPİAŞ API'den çeker.

    Args:
        start_date: "2024-01-01T00:00:00+03:00"
        end_date:   "2024-01-31T23:00:00+03:00"
        token:      EPİAŞ auth token

    Returns:
        Ham API yanıtı (dict)
    """
    url = EPIAS_BASE_URL + ENDPOINTS["gop_mcp"]
    payload = {
        "startDate": start_date,
        "endDate":   end_date,
    }

    logger.info(f"GÖP MCP çekiliyor: {start_date[:10]} → {end_date[:10]}")
    response = requests.post(url, headers=get_headers(token), json=payload, timeout=30)

    if response.status_code == 200:
        data = response.json()
        logger.success(f"GÖP MCP alındı: {len(data.get('items', []))} kayıt")
        return data
    else:
        logger.error(f"GÖP MCP hatası: {response.status_code} | {response.text}")
        raise ConnectionError(f"API hatası: {response.status_code}")


def save_raw(data: dict, start_date: str) -> Path:
    """Ham JSON verisini kaydeder."""
    date_str = start_date[:10].replace("-", "")
    path = RAW_DIR / f"gop_mcp_{date_str}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Ham veri kaydedildi: {path}")
    return path


def parse_to_dataframe(data: dict) -> pd.DataFrame:
    """
    API yanıtını pandas DataFrame'e dönüştürür.

    Sütunlar:
        date        : Tarih (datetime)
        hour        : Saat (0-23)
        mcp_tl      : MCP (TL/MWh)
        mcp_usd     : MCP (USD/MWh)
        mcp_eur     : MCP (EUR/MWh)
    """
    items = data.get("items", [])
    if not items:
        logger.warning("Veri boş döndü!")
        return pd.DataFrame()

    rows = []
    for item in items:
        dt = pd.to_datetime(item.get("date"), utc=False)
        rows.append({
            "date":    dt.date(),
            "hour":    dt.hour,
            "datetime": dt,
            "mcp_tl":  item.get("marketTradePrice", None),
            "mcp_usd": item.get("priceUSD", None),
            "mcp_eur": item.get("priceEUR", None),
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("datetime").reset_index(drop=True)

    # Temel türetilen göstergeler
    df["mcp_tl_rolling24h"] = df["mcp_tl"].rolling(24, min_periods=1).mean()
    df["mcp_tl_pct_change"] = df["mcp_tl"].pct_change() * 100
    df["peak_flag"] = df["hour"].apply(lambda h: 1 if 8 <= h <= 20 else 0)

    logger.success(f"DataFrame oluşturuldu: {len(df)} satır, {df.columns.tolist()}")
    return df


def save_processed(df: pd.DataFrame, start_date: str) -> Path:
    """İşlenmiş DataFrame'i CSV olarak kaydeder."""
    date_str = start_date[:10].replace("-", "")
    path = PROCESSED_DIR / f"gop_mcp_{date_str}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.success(f"İşlenmiş veri kaydedildi: {path}")
    return path


def run(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Tam pipeline: token al → veri çek → kaydet → DataFrame döndür.
    """
    if not start_date:
        start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00+03:00")
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%dT23:00:00+03:00")

    token = get_token()
    raw   = fetch_gop_mcp(start_date, end_date, token)
    save_raw(raw, start_date)
    df    = parse_to_dataframe(raw)
    save_processed(df, start_date)
    return df


if __name__ == "__main__":
    df = run()
    print("\n--- GÖP MCP Son 5 Kayıt ---")
    print(df.tail())
    print(f"\nOrtalama MCP: {df['mcp_tl'].mean():.2f} TL/MWh")
    print(f"Min MCP:      {df['mcp_tl'].min():.2f} TL/MWh")
    print(f"Max MCP:      {df['mcp_tl'].max():.2f} TL/MWh")
