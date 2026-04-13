"""
GÖP — Gün Öncesi Piyasası
eptr2 kütüphanesi ile MCP (PTF) verisi çeker.
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


def fetch_gop_mcp(start_date: str, end_date: str) -> pd.DataFrame:
    """
    GÖP MCP (PTF) verisini çeker.
    start_date / end_date format: "2026-04-01"
    """
    logger.info(f"GÖP MCP çekiliyor: {start_date} → {end_date}")
    eptr = get_client()

    res = eptr.call("mcp", start_date=start_date, end_date=end_date)

    # eptr2 liste veya DataFrame döndürür
    if isinstance(res, pd.DataFrame):
        df = res
    else:
        df = pd.DataFrame(res)

    # Sütun isimlerini normalize et
    df.columns = [c.lower() for c in df.columns]

    # Tarih/saat sütununu bul
    date_col = next((c for c in df.columns if "date" in c or "tarih" in c), None)
    price_col = next((c for c in df.columns if "price" in c or "ptf" in c or "mcp" in c), None)

    if date_col:
        df["datetime"] = pd.to_datetime(df[date_col])
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour

    if price_col:
        df = df.rename(columns={price_col: "mcp_tl"})
        df["mcp_tl"] = pd.to_numeric(df["mcp_tl"], errors="coerce")
        df["peak_flag"] = df["hour"].apply(lambda h: 1 if 8 <= h <= 20 else 0)
        df["mcp_rolling24h"] = df["mcp_tl"].rolling(24, min_periods=1).mean()

    df = df.sort_values("datetime").reset_index(drop=True)
    logger.success(f"GÖP MCP: {len(df)} kayıt | Sütunlar: {df.columns.tolist()}")

    # Kaydet
    path = PROCESSED_DIR / f"gop_mcp_{start_date.replace('-','')}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"Kaydedildi: {path}")
    return df


if __name__ == "__main__":
    from datetime import datetime, timedelta
    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    df = fetch_gop_mcp(start, end)
    print(df.tail())
