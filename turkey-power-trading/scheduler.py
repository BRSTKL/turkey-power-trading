"""
Daily Scheduler
---------------
Runs every morning at 08:00 to fetch previous day's data.
Usage:
    python scheduler.py          # Run once immediately
    python scheduler.py --loop   # Run daily at 08:00 (keep terminal open)
"""

import schedule
import time
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def run_daily_fetch():
    print("\n" + "=" * 60)
    print(f"🔄 Daily fetch started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        from fetch.epias_gop import fetch_yesterday as gop_yesterday
        print("\n[1/4] Fetching GÖP prices...")
        gop_yesterday()
    except Exception as e:
        print(f"[ERROR] GÖP fetch failed: {e}")

    try:
        from fetch.epias_dgp import fetch_dgp_prices, save_processed as dgp_save
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print("\n[2/4] Fetching DGP prices...")
        df = fetch_dgp_prices(yesterday, yesterday)
        if not df.empty:
            dgp_save(df, append=True)
    except Exception as e:
        print(f"[ERROR] DGP fetch failed: {e}")

    try:
        from fetch.epias_uretim import fetch_generation, fetch_consumption, save_processed as gen_save
        from datetime import timedelta
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print("\n[3/4] Fetching generation & consumption...")
        gen = fetch_generation(yesterday, yesterday)
        con = fetch_consumption(yesterday, yesterday)
        if not gen.empty: gen_save(gen, "generation", append=True)
        if not con.empty: gen_save(con, "consumption", append=True)
    except Exception as e:
        print(f"[ERROR] Generation fetch failed: {e}")

    try:
        from process.indicators import run_all
        print("\n[4/4] Recalculating indicators...")
        run_all()
    except Exception as e:
        print(f"[ERROR] Indicators failed: {e}")

    print(f"\n✅ Daily fetch complete: {datetime.now().strftime('%H:%M:%S')}")
    print("Power BI will pick up changes on next refresh.\n")


if __name__ == "__main__":
    if "--loop" in sys.argv:
        print("📅 Scheduler started. Will run daily at 08:00.")
        print("   Press Ctrl+C to stop.\n")
        schedule.every().day.at("08:00").do(run_daily_fetch)
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        # Run once immediately
        run_daily_fetch()
