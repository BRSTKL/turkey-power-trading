"""
Initial Setup & Backfill
------------------------
Run this ONCE to set up the project and fetch historical data.
"""

import os
import sys
from pathlib import Path

print("=" * 60)
print("🇹🇷 Turkey Power Trading — Initial Setup")
print("=" * 60)

# Create directory structure
dirs = [
    "data/raw",
    "data/processed",
    "notebooks",
    "fetch",
    "process",
    "models",
    "dashboards",
    "config",
]
for d in dirs:
    Path(d).mkdir(parents=True, exist_ok=True)
    # Create __init__.py for Python packages
    if d in ["fetch", "process", "models", "config"]:
        init = Path(d) / "__init__.py"
        if not init.exists():
            init.touch()

print("✅ Directory structure created.")

# Check .env
if not Path(".env").exists():
    if Path(".env.example").exists():
        import shutil
        shutil.copy(".env.example", ".env")
        print("\n⚠️  .env file created from template.")
        print("   Edit .env and add your EPİAŞ credentials, then run again.")
        sys.exit(0)
    else:
        print("❌ .env.example not found. Please run from project root.")
        sys.exit(1)

# Test credentials
print("\n[1/5] Testing EPİAŞ connection...")
try:
    from fetch.epias_auth import get_tgt
    get_tgt()
    print("✅ Authentication successful!")
except Exception as e:
    print(f"❌ Auth failed: {e}")
    sys.exit(1)

# Backfill: how many days?
days = 90
print(f"\n[2/5] Fetching last {days} days of GÖP prices...")
try:
    from fetch.epias_gop import fetch_last_n_days
    fetch_last_n_days(days)
    print("✅ GÖP data fetched.")
except Exception as e:
    print(f"❌ GÖP fetch error: {e}")

print(f"\n[3/5] Fetching last {days} days of DGP prices...")
try:
    from fetch.epias_dgp import fetch_last_n_days as dgp_backfill
    dgp_backfill(days)
    print("✅ DGP data fetched.")
except Exception as e:
    print(f"❌ DGP fetch error: {e}")

print(f"\n[4/5] Fetching last {days} days of generation & consumption...")
try:
    from fetch.epias_uretim import fetch_last_n_days as gen_backfill
    gen_backfill(days)
    print("✅ Generation & consumption fetched.")
except Exception as e:
    print(f"❌ Generation fetch error: {e}")

print("\n[5/5] Calculating all indicators...")
try:
    from process.indicators import run_all
    run_all()
    print("✅ Indicators calculated.")
except Exception as e:
    print(f"❌ Indicators error: {e}")

print("\n" + "=" * 60)
print("🎉 Setup complete!")
print("   data/processed/ → ready for Power BI")
print("   Run 'python scheduler.py --loop' for daily auto-updates")
print("=" * 60)
