"""
DGP — Dengeleme Güç Piyasası
eptr2 ile SMP (SMF) ve sistem yönü verisi.
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


def fetch_dgp(start_date: str, end_date: str) -> pd.DataFrame:
    """DGP SMP (SMF) ve sistem yönü verisi."""
    logger.info(f"DGP SMP çekiliyor: {start_date} → {end_date}")
    eptr = get_client()

    # SMP (Sistem Marjinal Fiyatı)
    res_smp = eptr.call("smp", start_date=start_date, end_date=end_date)
    df_smp = res_smp if isinstance(res_smp, pd.DataFrame) else pd.DataFrame(res_smp)
    df_smp.columns = [c.lower() for c in df_smp.columns]

    # Sistem yönü (uzun/kısa)
    try:
        res_dir = eptr.call("systemdirection", start_date=start_date, end_date=end_date)
        df_dir = res_dir if isinstance(res_dir, pd.DataFrame) else pd.DataFrame(res_dir)
        df_dir.columns = [c.lower() for c in df_dir.columns]
    except Exception as e:
        logger.warning(f"Sistem yönü alınamadı: {e}")
        df_dir = pd.DataFrame()

    # Normalize
    date_col = next((c for c in df_smp.columns if "date" in c), None)
    if date_col:
        df_smp["datetime"] = pd.to_datetime(df_smp[date_col])
        df_smp["date"] = df_smp["datetime"].dt.date
        df_smp["hour"] = df_smp["datetime"].dt.hour

    price_col = next((c for c in df_smp.columns if "price" in c or "smf" in c or "smp" in c), None)
    if price_col:
        df_smp = df_smp.rename(columns={price_col: "smp_tl"})
        df_smp["smp_tl"] = pd.to_numeric(df_smp["smp_tl"], errors="coerce")

    df_smp = df_smp.sort_values("datetime").reset_index(drop=True)
    logger.success(f"DGP SMP: {len(df_smp)} kayıt")

    path = PROCESSED_DIR / f"dgp_{start_date.replace('-','')}.csv"
    df_smp.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"Kaydedildi: {path}")
    return df_smp


if __name__ == "__main__":
    from datetime import datetime, timedelta
    end   = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d")
    df = fetch_dgp(start, end)
    print(df.tail())
