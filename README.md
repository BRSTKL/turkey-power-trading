# 🇹🇷 Turkey Power Trading Dashboard

EPİAŞ Şeffaflık Platformu verileriyle çalışan profesyonel enerji trading analiz sistemi.

## Kapsam
- **GÖP** — Gün Öncesi Piyasası saatlik MCP fiyatları
- **GİP** — Gün İçi Piyasası işlem fiyatları
- **DGP** — Dengeleme Güç Piyasası yük alma / atma talimatları
- **Üretim** — Kaynak bazlı gerçekleşen üretim (UEVM)
- **Power BI** — 5 sayfalı profesyonel trading dashboard

## Kurulum

```bash
git clone https://github.com/BRSTKL/turkey-power-trading.git
cd turkey-power-trading
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

## Konfigürasyon

`.env.example` dosyasını `.env` olarak kopyala ve bilgilerini gir:

```
EPIAS_USERNAME=your_email
EPIAS_PASSWORD=your_password
```

## Kullanım

```bash
# Son 7 günü çek
python scheduler.py --now

# Sürekli çalıştır (her gün 06:30)
python scheduler.py
```

## Veri Akışı

```
EPİAŞ API → fetch/ → data/raw/ → process/ → data/processed/ → Power BI
```

## Power BI Dashboard Sayfaları

| Sayfa | İçerik |
|---|---|
| 1. Trading Desk | Günlük özet, sistem yönü, net pozisyon |
| 2. GÖP / GİP | Saatlik fiyat analizi, heatmap |
| 3. DGP & Dengeleme | Talimat fiyatları, imbalance |
| 4. Portföy & P&L | Gelir analizi, fırsat maliyeti |
| 5. Risk (VaR) | Fiyat riski, senaryo analizi |
