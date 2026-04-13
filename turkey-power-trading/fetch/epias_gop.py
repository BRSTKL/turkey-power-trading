"""
GÖP — Gün Öncesi Piyasası (Day-Ahead Market)
---------------------------------------------
Fetches hourly MCP (PTF - Piyasa Takas Fiyatı) prices.
Saves to: data/raw/gop_YYYY-MM-DD.json
          data/processed/gop_prices.parquet
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import ENDPOINTS, RAW_PATH, PROC_PATH, DATE_FORMAT
from fetch.epias_auth import epias_get


def fetch_gop_prices(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch GÖP MCP (PTF) prices for a date range.

    Args:
        start_date: "YYYY-MM-DD"
        end_date:   "YYYY-MM-DD"

    Returns:
        DataFrame with columns: datetime, mcp_try, mcp_usd, mcp_eur
    """
    start_dt = f"{start_date}T00:00:00+03:00"
    end_dt   = f"{end_date}T23:00:00+03:00"

    print(f"[GÖP] Fetching MCP prices: {start_date} → {end_date}")

    params = {
        "startDate": start_dt,
        "endDate":   end_dt
    }

    raw = epias_get(ENDPOINTS["gop_mcp"], params)

    # Save raw response
    Path(RAW_PATH).mkdir(parents=True, exist_ok=True)
    raw_file = f"{RAW_PATH}/gop_{start_date}_{end_date}.json"
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)
    print(f"[GÖP] Raw data saved → {raw_file}")

    # Parse response
    # EPİAŞ returns: {"body": {"dayAheadMCPList": [{"date": ..., "price": ..., "priceUsd": ..., "priceEur": ...}]}}
    records = raw.get("body", {}).get("dayAheadMCPList", [])

    if not records:
        print("[GÖP] Warning: No records returned!")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Rename & clean columns
    df = df.rename(columns={
        "date":     "datetime",
        "price":    "mcp_try",
        "priceUsd": "mcp_usd",
        "priceEur": "mcp_eur"
    })

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"]     = df["datetime"].dt.date
    df["hour"]     = df["datetime"].dt.hour
    df["weekday"]  = df["datetime"].dt.day_name()
    df["is_peak"]  = df["hour"].between(8, 20).astype(int)  # Peak: 08-20

    # Derived metrics
    df["mcp_try"] = pd.to_numeric(df["mcp_try"], errors="coerce")
    df["mcp_usd"] = pd.to_numeric(df["mcp_usd"], errors="coerce")
    df["mcp_eur"] = pd.to_numeric(df["mcp_eur"], errors="coerce")

    df = df.sort_values("datetime").reset_index(drop=True)

    print(f"[GÖP] Parsed {len(df)} hourly records.")
    print(f"[GÖP] MCP range: {df['mcp_try'].min():.2f} — {df['mcp_try'].max():.2f} TL/MWh")
    print(f"[GÖP] Avg MCP: {df['mcp_try'].mean():.2f} TL/MWh")

    return df


def save_processed(df: pd.DataFrame, append: bool = True):
    """
    Save processed GÖP data to parquet (append or overwrite).
    """
    Path(PROC_PATH).mkdir(parents=True, exist_ok=True)
    out_file = f"{PROC_PATH}/gop_prices.parquet"

    if append and Path(out_file).exists():
        existing = pd.read_parquet(out_file)
        df = pd.concat([existing, df]).drop_duplicates(subset=["datetime"]).sort_values("datetime")

    df.to_parquet(out_file, index=False)
    print(f"[GÖP] Processed data saved → {out_file} ({len(df)} total rows)")

    # Also save CSV for Power BI direct connection
    csv_file = f"{PROC_PATH}/gop_prices.csv"
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")  # utf-8-sig for Excel/Power BI compatibility
    print(f"[GÖP] CSV saved → {csv_file}")


def fetch_yesterday():
    """Convenience: fetch yesterday's GÖP prices (for daily scheduler)."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    df = fetch_gop_prices(yesterday, yesterday)
    if not df.empty:
        save_processed(df, append=True)
    return df


def fetch_last_n_days(n: int = 30):
    """Convenience: fetch last N days (for initial backfill)."""
    end   = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    df = fetch_gop_prices(
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d")
    )
    if not df.empty:
        save_processed(df, append=False)
    return df


# ── CLI usage ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch GÖP MCP prices from EPİAŞ")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   type=str, help="End date YYYY-MM-DD")
    parser.add_argument("--days",  type=int, default=30, help="Last N days (default: 30)")
    args = parser.parse_args()

    if args.start and args.end:
        df = fetch_gop_prices(args.start, args.end)
        if not df.empty:
            save_processed(df)
    else:
        print(f"[GÖP] Fetching last {args.days} days...")
        df = fetch_last_n_days(args.days)

    if not df.empty:
        print("\n── Sample data ──────────────────────────")
        print(df[["datetime", "mcp_try", "mcp_usd", "hour", "is_peak"]].head(10).to_string(index=False))
