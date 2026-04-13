"""
EPİAŞ Authentication
--------------------
EPİAŞ uses CAS (Central Authentication Service) ticket-based auth.
Flow: credentials -> TGT (Ticket Granting Ticket) -> ST (Service Ticket) -> API calls
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import EPIAS_AUTH_URL, EPIAS_BASE_URL, EPIAS_USERNAME, EPIAS_PASSWORD

load_dotenv()

# In-memory token cache
_tgt_url = None
_tgt_expires = None


def get_tgt() -> str:
    """
    Get or refresh TGT (Ticket Granting Ticket).
    TGTs are valid for ~8 hours — we refresh after 7h to be safe.
    """
    global _tgt_url, _tgt_expires

    if _tgt_url and _tgt_expires and datetime.now() < _tgt_expires:
        return _tgt_url

    print("[AUTH] Requesting new TGT from EPİAŞ...")

    if not EPIAS_USERNAME or not EPIAS_PASSWORD:
        raise ValueError(
            "EPİAŞ credentials not found!\n"
            "Create a .env file with:\n"
            "  EPIAS_USERNAME=your_email\n"
            "  EPIAS_PASSWORD=your_password"
        )

    resp = requests.post(
        EPIAS_AUTH_URL,
        data={"username": EPIAS_USERNAME, "password": EPIAS_PASSWORD},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30
    )

    if resp.status_code == 201:
        # TGT URL is returned in Location header
        _tgt_url = resp.headers.get("Location")
        _tgt_expires = datetime.now() + timedelta(hours=7)
        print(f"[AUTH] TGT obtained successfully. Expires: {_tgt_expires.strftime('%H:%M')}")
        return _tgt_url
    else:
        raise ConnectionError(
            f"[AUTH] Failed to get TGT. Status: {resp.status_code}\n"
            f"Response: {resp.text}"
        )


def get_service_ticket() -> str:
    """
    Get a one-time Service Ticket (ST) from TGT.
    Each ST is valid for a single API call only.
    """
    tgt_url = get_tgt()

    resp = requests.post(
        tgt_url,
        data={"service": EPIAS_BASE_URL},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15
    )

    if resp.status_code == 200:
        return resp.text.strip()
    else:
        raise ConnectionError(
            f"[AUTH] Failed to get Service Ticket. Status: {resp.status_code}"
        )


def epias_get(endpoint: str, params: dict) -> dict:
    """
    Authenticated GET request to EPİAŞ API.
    Automatically handles ticket refresh.
    
    Args:
        endpoint: API path (e.g. "/market/day-ahead/prices")
        params:   Query parameters dict
    
    Returns:
        Parsed JSON response body
    """
    ticket = get_service_ticket()
    url = EPIAS_BASE_URL + endpoint

    headers = {
        "TGT": ticket,
        "Content-Type": "application/json"
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)

    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 401:
        # Token expired mid-session — force refresh
        global _tgt_url, _tgt_expires
        _tgt_url = None
        _tgt_expires = None
        print("[AUTH] Token expired, refreshing...")
        return epias_get(endpoint, params)
    else:
        raise ConnectionError(
            f"[API] Request failed: {resp.status_code}\n"
            f"URL: {url}\n"
            f"Response: {resp.text[:500]}"
        )


# ── Quick connection test ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("EPİAŞ Connection Test")
    print("=" * 50)

    try:
        tgt = get_tgt()
        print(f"TGT URL: {tgt[:60]}...")

        st = get_service_ticket()
        print(f"Service Ticket: {st[:30]}...")

        print("\n✅ Authentication successful! EPİAŞ API is reachable.")

    except ValueError as e:
        print(f"\n❌ Config error: {e}")
    except ConnectionError as e:
        print(f"\n❌ Connection error: {e}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
