"""
Kaynak bazlı gerçekleşen üretim verisi (UEVM).
Hidro, doğalgaz, rüzgar, güneş, kömür, ithalat/ihracat.
"""

import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger

from config.settings import EPIAS_BASE_URL, ENDPOINTS, RAW_DIR, PROCESSED_DIR
from fetch.epias_auth import get_token, get_headers


def fetch_uretim(start_date: str, end_date: str, token: str) -> dict:
    url = EPIAS_BASE_URL + ENDPOINTS["production"]
    payload = {"startDate": start_date, "endDate": end_date}

    logger.info(f"Üretim verisi çekiliyor: {start_date[:10]} → {end_date[:10]}")
    response = requests.post(url, headers=get_headers(token), json=payload, timeout=30)

    if response.status_code == 200:
        data = response.json()
        logger.success(f"Üretim verisi alındı: {len(data.get('items', []))} kayıt")
        return data
    else:
        logger.error(f"Üretim hatası: {response.status_code}")
        raise ConnectionError(f"API hatası: {response.status_code}")


def parse_uretim(data: dict) -> pd.DataFrame:
    """
    Kaynak bazlı üretimi parse eder.

    Başlıca sütunlar:
        natural_gas, wind, hydro_river, hydro_dam,
        solar, lignite, hard_coal, geothermal,
        biomass, naphtha, waste_heat, import_export
    """
    items = data.get("items", [])
    rows = []
    for item in items:
        dt = pd.to_datetime(item.get("date"), utc=False)
        row = {"datetime": dt, "date": dt.date(), "hour": dt.hour}
        # Tüm üretim kaynaklarını al
        for key, val in item.items():
            if key != "date":
                row[key] = val
        rows.append(row)

    df = pd.DataFrame(rows).sort_values("datetime").reset_index(drop=True)

    # Türetilen göstergeler
    renewable_cols = [c for c in df.columns if any(x in c.lower() for x in ["wind", "solar", "hydro", "geo", "bio"])]
    if renewable_cols:
        df["total_renewable"] = df[renewable_cols].sum(axis=1)
        total_cols = [c for c in df.columns if c not in ["datetime", "date", "hour", "total_renewable"]]
        df["total_generation"] = df[total_cols].sum(axis=1)
        df["renewable_share_pct"] = (df["total_renewable"] / df["total_generation"].replace(0, 1)) * 100

    logger.success(f"Üretim DataFrame: {len(df)} satır, {len(df.columns)} sütun")
    return df


def run(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    if not start_date:
        start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00+03:00")
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%dT23:00:00+03:00")

    token = get_token()
    data  = fetch_uretim(start_date, end_date, token)

    date_str = start_date[:10].replace("-", "")
    with open(RAW_DIR / f"uretim_{date_str}.json", "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    df = parse_uretim(data)
    path = PROCESSED_DIR / f"uretim_{date_str}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.success(f"Üretim kaydedildi: {path}")
    return df


if __name__ == "__main__":
    df = run()
    print("\n--- Üretim Son 5 Kayıt ---")
    print(df[["datetime", "total_generation", "total_renewable", "renewable_share_pct"]].tail())
