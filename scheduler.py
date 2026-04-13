"""
Günlük otomatik veri çekme scheduler'ı.
Her gün sabah 06:30'da çalışır, tüm verileri günceller.

Kullanım:
    python scheduler.py
"""

import schedule
import time
from datetime import datetime, timedelta
from loguru import logger

from fetch.epias_gop    import run as fetch_gop
from fetch.epias_dgp    import run as fetch_dgp
from fetch.epias_uretim import run as fetch_uretim


def daily_pipeline():
    """Günlük veri çekme pipeline'ı."""
    today     = datetime.today()
    yesterday = today - timedelta(days=1)

    start = yesterday.strftime("%Y-%m-%dT00:00:00+03:00")
    end   = yesterday.strftime("%Y-%m-%dT23:00:00+03:00")

    logger.info(f"=== Günlük pipeline başladı: {today.strftime('%Y-%m-%d %H:%M')} ===")

    try:
        logger.info("1/3 GÖP MCP çekiliyor...")
        fetch_gop(start, end)

        logger.info("2/3 DGP çekiliyor...")
        fetch_dgp(start, end)

        logger.info("3/3 Üretim verisi çekiliyor...")
        fetch_uretim(start, end)

        logger.success("=== Günlük pipeline tamamlandı ===")

    except Exception as e:
        logger.error(f"Pipeline hatası: {e}")


def run_now():
    """Manuel tetikleme — son 7 günü çeker."""
    logger.info("Manuel çalıştırma — son 7 gün")
    end   = datetime.today().strftime("%Y-%m-%dT23:00:00+03:00")
    start = (datetime.today() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00+03:00")

    fetch_gop(start, end)
    fetch_dgp(start, end)
    fetch_uretim(start, end)
    logger.success("Tamamlandı!")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        run_now()
    else:
        logger.info("Scheduler başlatıldı. Her gün 06:30'da çalışacak.")
        logger.info("Şimdi çalıştırmak için: python scheduler.py --now")

        schedule.every().day.at("06:30").do(daily_pipeline)

        while True:
            schedule.run_pending()
            time.sleep(60)
