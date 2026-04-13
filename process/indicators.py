"""
Trading göstergeleri:
- Volatilite (saatlik, günlük)
- Peak / off-peak spread
- DGP-MCP spread (dengesizlik fırsatı)
- Yenilenebilir baskı göstergesi
- Fiyat heatmap verisi
"""

import pandas as pd
import numpy as np
from loguru import logger
from pathlib import Path

from config.settings import PROCESSED_DIR


def add_volatility(df: pd.DataFrame, price_col: str = "mcp_tl", window: int = 24) -> pd.DataFrame:
    """Saatlik fiyat değişkenliği (rolling std)."""
    df[f"volatility_{window}h"] = df[price_col].rolling(window, min_periods=1).std()
    df[f"volatility_{window}h_pct"] = df[f"volatility_{window}h"] / df[price_col].replace(0, np.nan) * 100
    return df


def add_peak_offpeak_spread(df: pd.DataFrame, price_col: str = "mcp_tl") -> pd.DataFrame:
    """
    Peak (08-20) ve off-peak saatlik ortalama farkı.
    Her güne ait peak/off-peak spread hesaplar.
    """
    df["peak_flag"] = df["hour"].apply(lambda h: 1 if 8 <= h <= 20 else 0)

    daily = df.groupby("date").apply(
        lambda x: pd.Series({
            "peak_avg":     x.loc[x["peak_flag"] == 1, price_col].mean(),
            "offpeak_avg":  x.loc[x["peak_flag"] == 0, price_col].mean(),
        })
    ).reset_index()
    daily["peak_offpeak_spread"] = daily["peak_avg"] - daily["offpeak_avg"]

    df = df.merge(daily[["date", "peak_avg", "offpeak_avg", "peak_offpeak_spread"]], on="date", how="left")
    return df


def add_dgp_mcp_spread(gop_df: pd.DataFrame, dgp_df: pd.DataFrame) -> pd.DataFrame:
    """
    DGP yük alma - MCP farkı: pozitifse DGP'de satmak mantıklı.
    Trader için sinyal: YAL > MCP → sistemde açık var, fiyat yukarı baskılı.
    """
    merged = pd.merge(
        gop_df[["datetime", "mcp_tl"]],
        dgp_df[["datetime", "dgp_up_tl", "dgp_down_tl", "dgp_spread"]],
        on="datetime", how="inner"
    )
    merged["yal_mcp_spread"] = merged["dgp_up_tl"] - merged["mcp_tl"]
    merged["yat_mcp_spread"] = merged["mcp_tl"] - merged["dgp_down_tl"]

    # Sinyal
    merged["signal"] = merged.apply(lambda r:
        "LONG_DGP"  if r["yal_mcp_spread"] > 50 else
        "SHORT_DGP" if r["yat_mcp_spread"] > 50 else
        "NEUTRAL", axis=1
    )
    logger.success(f"DGP-MCP spread hesaplandı: {len(merged)} kayıt")
    return merged


def build_heatmap_data(df: pd.DataFrame, price_col: str = "mcp_tl") -> pd.DataFrame:
    """
    Power BI heatmap için: saat (0-23) x gün (Pzt-Paz) pivot tablosu.
    """
    df["weekday"] = pd.to_datetime(df["date"]).dt.day_name()
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    heatmap = df.groupby(["weekday", "hour"])[price_col].mean().reset_index()
    heatmap["weekday"] = pd.Categorical(heatmap["weekday"], categories=weekday_order, ordered=True)
    heatmap = heatmap.sort_values(["weekday", "hour"])
    heatmap.columns = ["weekday", "hour", "avg_mcp_tl"]

    return heatmap


def run_all(gop_csv: str, dgp_csv: str) -> dict:
    """Tüm göstergeleri hesapla ve kaydet."""
    gop_df = pd.read_csv(gop_csv, parse_dates=["datetime"])
    dgp_df = pd.read_csv(dgp_csv, parse_dates=["datetime"])

    gop_df = add_volatility(gop_df)
    gop_df = add_peak_offpeak_spread(gop_df)

    spread_df = add_dgp_mcp_spread(gop_df, dgp_df)
    heatmap_df = build_heatmap_data(gop_df)

    # Kaydet
    gop_df.to_csv(PROCESSED_DIR / "gop_indicators.csv", index=False, encoding="utf-8-sig")
    spread_df.to_csv(PROCESSED_DIR / "dgp_mcp_spread.csv", index=False, encoding="utf-8-sig")
    heatmap_df.to_csv(PROCESSED_DIR / "price_heatmap.csv", index=False, encoding="utf-8-sig")

    logger.success("Tüm göstergeler hesaplandı ve kaydedildi.")
    return {
        "gop":     gop_df,
        "spread":  spread_df,
        "heatmap": heatmap_df,
    }
