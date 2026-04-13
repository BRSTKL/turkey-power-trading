"""
Trading Indicators
------------------
Calculates key metrics a trader monitors daily:
  - PTF volatility (daily, rolling)
  - Peak / off-peak spread
  - DGP spread (up - down regulation band)
  - MCP vs SMF spread (imbalance cost signal)
  - Renewable penetration impact on price
  - Price heatmap matrix (hour × weekday)
  - 7-day / 30-day price percentile
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import PROC_PATH


def load_data() -> dict:
    """Load all processed datasets."""
    data = {}
    files = {
        "gop":  "gop_prices.parquet",
        "dgp":  "dgp_prices.parquet",
        "gen":  "generation.parquet",
        "con":  "consumption.parquet",
    }
    for key, fname in files.items():
        path = f"{PROC_PATH}/{fname}"
        if Path(path).exists():
            data[key] = pd.read_parquet(path)
            data[key]["datetime"] = pd.to_datetime(data[key]["datetime"])
        else:
            print(f"[WARN] Missing: {path}")
    return data


def calc_gop_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add trading indicators to GÖP price DataFrame.
    """
    df = df.copy().sort_values("datetime")
    df["date"] = pd.to_datetime(df["datetime"]).dt.date

    # Rolling volatility (std of last 24h / 168h)
    df["vol_24h"]  = df["mcp_try"].rolling(24,  min_periods=6).std()
    df["vol_168h"] = df["mcp_try"].rolling(168, min_periods=24).std()

    # Price percentile in rolling 30-day window
    df["pct_30d"] = df["mcp_try"].rolling(720, min_periods=48).rank(pct=True).round(2)

    # Daily stats
    daily = df.groupby("date").agg(
        daily_avg   =("mcp_try", "mean"),
        daily_max   =("mcp_try", "max"),
        daily_min   =("mcp_try", "min"),
        daily_std   =("mcp_try", "std"),
        peak_avg    =("mcp_try", lambda x: x[df.loc[x.index, "is_peak"] == 1].mean()),
        offpeak_avg =("mcp_try", lambda x: x[df.loc[x.index, "is_peak"] == 0].mean()),
    ).reset_index()

    daily["peak_offpeak_spread"] = daily["peak_avg"] - daily["offpeak_avg"]
    daily["daily_range"]         = daily["daily_max"] - daily["daily_min"]

    df = df.merge(daily, on="date", how="left")

    print(f"[INDICATORS] GÖP indicators calculated for {len(df)} rows.")
    return df


def calc_dgp_indicators(gop_df: pd.DataFrame, dgp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge GÖP + DGP and calculate key balancing signals.
    """
    df = gop_df[["datetime", "mcp_try"]].merge(
        dgp_df[["datetime", "dgp_up", "dgp_down", "smf", "system_direction", "spread_up_down"]],
        on="datetime", how="inner"
    )

    # MCP vs SMF spread — positive = system pays premium over day-ahead
    df["mcp_smf_spread"]  = df["smf"]    - df["mcp_try"]
    df["mcp_dgpup_spread"]= df["dgp_up"] - df["mcp_try"]

    # Trading signal: if system is SHORT and DGP_UP >> MCP → short sellers lose money
    df["imbalance_risk"] = df["mcp_smf_spread"].abs()

    print(f"[INDICATORS] DGP indicators calculated for {len(df)} rows.")
    return df


def build_price_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build hour × weekday average price matrix for Power BI heatmap visual.
    """
    df = df.copy()
    df["hour"]    = pd.to_datetime(df["datetime"]).dt.hour
    df["weekday"] = pd.to_datetime(df["datetime"]).dt.day_name()

    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap = df.groupby(["hour", "weekday"])["mcp_try"].mean().reset_index()
    heatmap["weekday"] = pd.Categorical(heatmap["weekday"], categories=order, ordered=True)
    heatmap = heatmap.sort_values(["weekday", "hour"]).reset_index(drop=True)
    heatmap["mcp_try"] = heatmap["mcp_try"].round(2)
    heatmap.columns    = ["hour", "weekday", "avg_mcp_try"]

    print(f"[HEATMAP] Built {len(heatmap)}-cell heatmap (hour × weekday).")
    return heatmap


def calc_renewable_price_impact(gop_df: pd.DataFrame, gen_df: pd.DataFrame) -> pd.DataFrame:
    """
    Correlate renewable penetration % with MCP.
    Key insight: high renewable → low MCP (merit order effect).
    """
    if gen_df.empty or "renewable_pct" not in gen_df.columns:
        return pd.DataFrame()

    df = gop_df[["datetime", "mcp_try"]].merge(
        gen_df[["datetime", "renewable_pct", "total_mw", "wind", "solar"]],
        on="datetime", how="inner"
    )

    # Bin renewable penetration
    df["ren_bin"] = pd.cut(df["renewable_pct"], bins=[0, 20, 40, 60, 80, 100],
                           labels=["0-20%", "20-40%", "40-60%", "60-80%", "80-100%"])

    corr = df["renewable_pct"].corr(df["mcp_try"])
    print(f"[RENEWABLE] Renewable-price correlation: {corr:.3f}")

    return df


def run_all():
    """Calculate all indicators and save Power BI-ready outputs."""
    data = load_data()

    if "gop" not in data:
        print("[ERROR] GÖP data missing. Run fetch/epias_gop.py first.")
        return

    # GÖP indicators
    gop = calc_gop_indicators(data["gop"])
    gop.to_csv(f"{PROC_PATH}/gop_indicators.csv", index=False, encoding="utf-8-sig")
    gop.to_parquet(f"{PROC_PATH}/gop_indicators.parquet", index=False)

    # Heatmap
    heatmap = build_price_heatmap(data["gop"])
    heatmap.to_csv(f"{PROC_PATH}/price_heatmap.csv", index=False, encoding="utf-8-sig")

    # DGP indicators
    if "dgp" in data:
        dgp_ind = calc_dgp_indicators(data["gop"], data["dgp"])
        dgp_ind.to_csv(f"{PROC_PATH}/dgp_indicators.csv", index=False, encoding="utf-8-sig")

    # Renewable impact
    if "gen" in data:
        ren = calc_renewable_price_impact(data["gop"], data["gen"])
        if not ren.empty:
            ren.to_csv(f"{PROC_PATH}/renewable_impact.csv", index=False, encoding="utf-8-sig")

    print("\n✅ All indicators calculated and saved to data/processed/")


if __name__ == "__main__":
    run_all()
