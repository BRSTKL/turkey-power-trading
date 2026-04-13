"""
Kaynak bazlı gerçekleşen üretim (UEVM).
eptr2 ile realtime generation verisi.
"""

import os
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
from eptr2 import EPTR2

from config.settings import PROCESSED_DIR

load_dotenv()


def get_client() -> EPTR2:
    return EPTR2(
        username=os.environ["EPIAS_USERNAME"],
        password=os.environ["EPIAS_PASSWORD"]
    )


def fetch_uretim(start_date: str, end_date: str) -> pd.DataFrame:
    """Kaynak bazlı gerçekleşen üretim."""
    logger.info(f"Üretim verisi çekiliyor: {start_date} → {end_date}")
    eptr = get_client()

    res = eptr.call("realtimegeneration", start_date=start_date, end_date=end_date)
    df = res if isinstance(res, pd.DataFrame) else pd.DataFrame(res)
    df.columns = [c.lower() for c in df.columns]

    date_col = next((c for c in df.columns if "date" in c or "tarih" in c), None)
    if date_col:
        df["datetime"] = pd.to_datetime(df[date_col])
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour

    # Yenilenebilir toplam
    renewable_keywords = ["wind", "solar", "hydro", "geo", "bio", "ruzgar", "gunes", "hidro", "jeotermal", "biyokutle"]
    renewable_cols = [c for c in df.columns if any(k in c for k in renewable_keywords)]
    if renewable_cols:
        df[renewable_cols] = df[renewable_cols].apply(pd.to_numeric, errors="coerce")
        df["total_renewable"] = df[renewable_cols].sum(axis=1)
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        df["total_generation"] = df[numeric_cols].sum(axis=1)
        df["renewable_pct"] = (df["total_renewable"] / df["total_generation"].replace(0, 1)) * 100

    df = df.sort_values("datetime").reset_index(drop=True)
    logger.success(f"Üretim: {len(df)} kayıt | {len(df.columns)} sütun")

    path = PROCESSED_DIR / f"uretim_{start_date.replace('-','')}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"Kaydedildi: {path}")
    return df


if __name__ == "__main__":
    from datetime import datetime, timedelta
    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    df = fetch_uretim(start, end)
    print(df[["datetime", "total_generation", "renewable_pct"]].tail())
