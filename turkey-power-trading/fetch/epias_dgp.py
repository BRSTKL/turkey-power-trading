"""
DGP — Dengeleme Güç Piyasası (Balancing Power Market)
------------------------------------------------------
Fetches:
  - Upward regulation price (Artırım talimat fiyatı ↑)
  - Downward regulation price (Azaltım talimat fiyatı ↓)
  - SMF (Sistem Marjinal Fiyatı / System Marginal Price)
  - System direction (long / short)

Key trader insight:
  System SHORT → SMF = DGP_UP   → imbalance buyers pay premium
  System LONG  → SMF = DGP_DOWN → imbalance sellers receive discount
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import ENDPOINTS, RAW_PATH, PROC_PATH
from fetch.epias_auth import epias_get


def fetch_dgp_prices(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch DGP upward/downward regulation prices + SMF.

    Returns:
        DataFrame with: datetime, dgp_up, dgp_down, smf,
                        system_direction, spread_up_down
    """
    params = {
        "startDate": f"{start_date}T00:00:00+03:00",
        "endDate":   f"{end_date}T23:00:00+03:00"
    }

    print(f"[DGP] Fetching regulation prices: {start_date} → {end_date}")

    # Fetch all three endpoints
    raw_up   = epias_get(ENDPOINTS["dgp_up"],   params)
    raw_down = epias_get(ENDPOINTS["dgp_down"], params)
    raw_smf  = epias_get(ENDPOINTS["dgp_smp"],  params)
    raw_dir  = epias_get(ENDPOINTS["system_direction"], params)

    # Save raw
    Path(RAW_PATH).mkdir(parents=True, exist_ok=True)
    for name, data in [("dgp_up", raw_up), ("dgp_down", raw_down),
                       ("smf", raw_smf), ("sys_dir", raw_dir)]:
        with open(f"{RAW_PATH}/{name}_{start_date}_{end_date}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # Parse upward
    up_records   = raw_up.get("body", {}).get("upliftingSettlementPriceList", [])
    down_records = raw_down.get("body", {}).get("downRegulationDeliveryPrice", [])
    smf_records  = raw_smf.get("body", {}).get("systemMarginalPriceList", [])
    dir_records  = raw_dir.get("body", {}).get("systemDirectionList", [])

    def to_df(records, price_col, rename_to):
        df = pd.DataFrame(records)
        if df.empty:
            return df
        df["datetime"] = pd.to_datetime(df["date"])
        df[rename_to] = pd.to_numeric(df[price_col], errors="coerce")
        return df[["datetime", rename_to]]

    df_up   = to_df(up_records,   "price",  "dgp_up")
    df_down = to_df(down_records, "price",  "dgp_down")
    df_smf  = to_df(smf_records,  "systemMarginalPrice", "smf")

    # System direction
    df_dir = pd.DataFrame(dir_records)
    if not df_dir.empty:
        df_dir["datetime"] = pd.to_datetime(df_dir["date"])
        df_dir = df_dir.rename(columns={"systemDirection": "system_direction"})[["datetime", "system_direction"]]

    # Merge all
    df = df_up
    for other in [df_down, df_smf, df_dir]:
        if not other.empty:
            df = df.merge(other, on="datetime", how="outer")

    df = df.sort_values("datetime").reset_index(drop=True)
    df["date"]   = df["datetime"].dt.date
    df["hour"]   = df["datetime"].dt.hour

    # Calculated columns
    df["spread_up_down"] = df["dgp_up"] - df["dgp_down"]   # Width of balancing band
    df["is_short"] = (df["system_direction"] == "SHORT").astype(int)

    print(f"[DGP] Parsed {len(df)} records.")
    if "dgp_up" in df.columns:
        print(f"[DGP] Avg DGP↑: {df['dgp_up'].mean():.2f} | Avg DGP↓: {df['dgp_down'].mean():.2f} TL/MWh")
        short_pct = df["is_short"].mean() * 100 if "is_short" in df.columns else 0
        print(f"[DGP] System SHORT: {short_pct:.1f}% of hours")

    return df


def save_processed(df: pd.DataFrame, append: bool = True):
    Path(PROC_PATH).mkdir(parents=True, exist_ok=True)
    out_file = f"{PROC_PATH}/dgp_prices.parquet"

    if append and Path(out_file).exists():
        existing = pd.read_parquet(out_file)
        df = pd.concat([existing, df]).drop_duplicates(subset=["datetime"]).sort_values("datetime")

    df.to_parquet(out_file, index=False)
    df.to_csv(f"{PROC_PATH}/dgp_prices.csv", index=False, encoding="utf-8-sig")
    print(f"[DGP] Saved → {out_file} ({len(df)} total rows)")


def fetch_last_n_days(n: int = 30):
    end   = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    df = fetch_dgp_prices(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if not df.empty:
        save_processed(df, append=False)
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch DGP prices from EPİAŞ")
    parser.add_argument("--start", type=str)
    parser.add_argument("--end",   type=str)
    parser.add_argument("--days",  type=int, default=30)
    args = parser.parse_args()

    if args.start and args.end:
        df = fetch_dgp_prices(args.start, args.end)
        if not df.empty:
            save_processed(df)
    else:
        df = fetch_last_n_days(args.days)

    if not df.empty:
        print("\n── Sample ───────────────────────────────")
        cols = [c for c in ["datetime", "dgp_up", "dgp_down", "smf", "system_direction", "spread_up_down"] if c in df.columns]
        print(df[cols].head(10).to_string(index=False))
