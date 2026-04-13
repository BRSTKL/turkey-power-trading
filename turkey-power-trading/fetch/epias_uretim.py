"""
Üretim & Tüketim — Generation & Consumption
--------------------------------------------
Fetches real-time generation by source + actual consumption.
Critical for: merit order estimation, renewable impact, load forecasting.
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


# Generation source mapping (EPİAŞ field names → clean names)
GEN_COLUMNS = {
    "naturalGas":     "natural_gas",
    "wind":           "wind",
    "lignite":        "lignite",
    "hardCoal":       "hard_coal",
    "importCoal":     "import_coal",
    "fueloil":        "fuel_oil",
    "geothermal":     "geothermal",
    "dammedHydro":    "hydro_dam",
    "riverHydro":     "hydro_river",
    "sun":            "solar",
    "biomass":        "biomass",
    "nuclear":        "nuclear",
    "naphtha":        "naphtha",
    "lng":            "lng",
    "importExport":   "import_export",
}

RENEWABLE_SOURCES = ["wind", "solar", "hydro_dam", "hydro_river", "geothermal", "biomass"]
THERMAL_SOURCES   = ["natural_gas", "lignite", "hard_coal", "import_coal", "fuel_oil", "naphtha", "lng", "nuclear"]


def fetch_generation(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch real-time generation by source (UEVM).
    """
    params = {
        "startDate": f"{start_date}T00:00:00+03:00",
        "endDate":   f"{end_date}T23:00:00+03:00"
    }

    print(f"[ÜRETİM] Fetching generation: {start_date} → {end_date}")
    raw = epias_get(ENDPOINTS["realtime_gen"], params)

    Path(RAW_PATH).mkdir(parents=True, exist_ok=True)
    with open(f"{RAW_PATH}/generation_{start_date}_{end_date}.json", "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, indent=2)

    records = raw.get("body", {}).get("hourlyGenerations", [])
    if not records:
        print("[ÜRETİM] Warning: No records returned!")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["datetime"] = pd.to_datetime(df["date"])
    df["date"]     = df["datetime"].dt.date
    df["hour"]     = df["datetime"].dt.hour

    # Rename generation source columns
    for src, clean in GEN_COLUMNS.items():
        if src in df.columns:
            df[clean] = pd.to_numeric(df[src], errors="coerce").fillna(0)

    # Drop original EPİAŞ column names
    drop_cols = [c for c in GEN_COLUMNS.keys() if c in df.columns]
    df = df.drop(columns=drop_cols + ["date"], errors="ignore")

    # Aggregate totals
    ren_cols   = [c for c in RENEWABLE_SOURCES if c in df.columns]
    therm_cols = [c for c in THERMAL_SOURCES   if c in df.columns]
    all_gen    = ren_cols + therm_cols

    if all_gen:
        df["total_mw"]      = df[all_gen].sum(axis=1)
        df["renewable_mw"]  = df[ren_cols].sum(axis=1)
        df["thermal_mw"]    = df[therm_cols].sum(axis=1)
        df["renewable_pct"] = (df["renewable_mw"] / df["total_mw"].replace(0, 1) * 100).round(1)

    df = df.sort_values("datetime").reset_index(drop=True)
    print(f"[ÜRETİM] {len(df)} records. Avg total: {df['total_mw'].mean():.0f} MW, "
          f"Renewable share: {df['renewable_pct'].mean():.1f}%")
    return df


def fetch_consumption(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch actual consumption (gerçekleşen tüketim).
    """
    params = {
        "startDate": f"{start_date}T00:00:00+03:00",
        "endDate":   f"{end_date}T23:00:00+03:00"
    }

    print(f"[TÜKETİM] Fetching consumption: {start_date} → {end_date}")
    raw = epias_get(ENDPOINTS["consumption"], params)

    records = raw.get("body", {}).get("hourlyConsumptions", [])
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["datetime"]    = pd.to_datetime(df["date"])
    df["hour"]        = df["datetime"].dt.hour
    df["consumption"] = pd.to_numeric(df.get("consumption", df.get("tüketim", 0)), errors="coerce")
    df["is_peak"]     = df["hour"].between(8, 20).astype(int)

    df = df[["datetime", "hour", "consumption", "is_peak"]].sort_values("datetime").reset_index(drop=True)
    print(f"[TÜKETİM] {len(df)} records. Avg: {df['consumption'].mean():.0f} MWh/h")
    return df


def save_processed(df: pd.DataFrame, name: str, append: bool = True):
    Path(PROC_PATH).mkdir(parents=True, exist_ok=True)
    out_file = f"{PROC_PATH}/{name}.parquet"

    if append and Path(out_file).exists():
        existing = pd.read_parquet(out_file)
        df = pd.concat([existing, df]).drop_duplicates(subset=["datetime"]).sort_values("datetime")

    df.to_parquet(out_file, index=False)
    df.to_csv(f"{PROC_PATH}/{name}.csv", index=False, encoding="utf-8-sig")
    print(f"[SAVE] {name} → {out_file} ({len(df)} rows)")


def fetch_last_n_days(n: int = 30):
    end   = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=n - 1)
    s, e  = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    gen = fetch_generation(s, e)
    con = fetch_consumption(s, e)

    if not gen.empty:
        save_processed(gen, "generation", append=False)
    if not con.empty:
        save_processed(con, "consumption", append=False)

    return gen, con


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str)
    parser.add_argument("--end",   type=str)
    parser.add_argument("--days",  type=int, default=30)
    args = parser.parse_args()

    if args.start and args.end:
        gen = fetch_generation(args.start, args.end)
        con = fetch_consumption(args.start, args.end)
        if not gen.empty: save_processed(gen, "generation")
        if not con.empty: save_processed(con, "consumption")
    else:
        gen, con = fetch_last_n_days(args.days)
