"""
DGP — Dengeleme Güç Piyasası
Yük alma (↑) ve yük atma (↓) talimat fiyatlarını çeker.
"""

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path

from config.settings import EPIAS_BASE_URL, ENDPOINTS, RAW_DIR, PROCESSED_DIR
from fetch.epias_auth import get_token, get_headers


def fetch_dgp(direction: str, start_date: str, end_date: str, token: str) -> dict:
    """
    DGP talimat fiyatlarını çeker.

    Args:
        direction: "up" (yük alma) veya "down" (yük atma)
    """
    key = f"dgp_{direction}"
    url = EPIAS_BASE_URL + ENDPOINTS[key]
    payload = {"startDate": start_date, "endDate": end_date}

    logger.info(f"DGP {direction.upper()} çekiliyor: {start_date[:10]} → {end_date[:10]}")
    response = requests.post(url, headers=get_headers(token), json=payload, timeout=30)

    if response.status_code == 200:
        data = response.json()
        logger.success(f"DGP {direction.upper()} alındı: {len(data.get('items', []))} kayıt")
        return data
    else:
        logger.error(f"DGP {direction} hatası: {response.status_code} | {response.text}")
        raise ConnectionError(f"API hatası: {response.status_code}")


def parse_dgp(data_up: dict, data_down: dict) -> pd.DataFrame:
    """
    Yük alma ve yük atma verilerini birleştirir.

    Türetilen göstergeler:
        dgp_spread      : YAL - YAT fiyat farkı
        imbalance_cost  : Dengesizlik maliyeti tahmini
    """
    def extract(data, col_name):
        rows = []
        for item in data.get("items", []):
            dt = pd.to_datetime(item.get("date"), utc=False)
            rows.append({
                "datetime": dt,
                "date":     dt.date(),
                "hour":     dt.hour,
                col_name:   item.get("price", None),
            })
        return pd.DataFrame(rows)

    df_up   = extract(data_up,   "dgp_up_tl")
    df_down = extract(data_down, "dgp_down_tl")

    df = pd.merge(df_up, df_down, on=["datetime", "date", "hour"], how="outer")
    df = df.sort_values("datetime").reset_index(drop=True)

    # Türetilen göstergeler
    df["dgp_spread"]     = df["dgp_up_tl"] - df["dgp_down_tl"]
    df["dgp_mid"]        = (df["dgp_up_tl"] + df["dgp_down_tl"]) / 2
    df["peak_flag"]      = df["hour"].apply(lambda h: 1 if 8 <= h <= 20 else 0)

    logger.success(f"DGP DataFrame: {len(df)} satır")
    return df


def run(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    if not start_date:
        start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00+03:00")
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%dT23:00:00+03:00")

    token    = get_token()
    data_up  = fetch_dgp("up",   start_date, end_date, token)
    data_down= fetch_dgp("down", start_date, end_date, token)

    # Raw kaydet
    date_str = start_date[:10].replace("-", "")
    with open(RAW_DIR / f"dgp_up_{date_str}.json", "w") as f:
        json.dump(data_up, f, ensure_ascii=False, indent=2)
    with open(RAW_DIR / f"dgp_down_{date_str}.json", "w") as f:
        json.dump(data_down, f, ensure_ascii=False, indent=2)

    df = parse_dgp(data_up, data_down)
    path = PROCESSED_DIR / f"dgp_{date_str}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.success(f"DGP kaydedildi: {path}")
    return df


if __name__ == "__main__":
    df = run()
    print("\n--- DGP Son 5 Kayıt ---")
    print(df[["datetime", "dgp_up_tl", "dgp_down_tl", "dgp_spread"]].tail())
