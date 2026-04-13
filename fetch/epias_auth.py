"""
EPİAŞ Şeffaflık Platformu — Authentication
Token alıp tüm API çağrılarında header olarak kullanır.
"""

import requests
from loguru import logger
from config.settings import EPIAS_USERNAME, EPIAS_PASSWORD, EPIAS_AUTH_URL


def get_token() -> str:
    """
    EPİAŞ'tan TGT (Ticket Granting Ticket) alır.
    Dönen token tüm API isteklerinde header olarak kullanılır.
    """
    if not EPIAS_USERNAME or not EPIAS_PASSWORD:
        raise ValueError("EPIAS_USERNAME ve EPIAS_PASSWORD .env dosyasında tanımlı olmalı!")

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "username": EPIAS_USERNAME,
        "password": EPIAS_PASSWORD,
    }

    logger.info("EPİAŞ'tan token alınıyor...")
    response = requests.post(EPIAS_AUTH_URL, headers=headers, data=payload, timeout=15)

    if response.status_code == 201:
        tgt_url = response.headers.get("Location", "")
        # Service ticket al
        st_response = requests.post(
            tgt_url,
            headers=headers,
            data={"service": "https://seffaflik.epias.com.tr"},
            timeout=15,
        )
        token = st_response.text.strip()
        logger.success(f"Token başarıyla alındı: {token[:20]}...")
        return token
    else:
        logger.error(f"Token alınamadı! Status: {response.status_code} | {response.text}")
        raise ConnectionError(f"EPİAŞ auth başarısız: {response.status_code}")


def get_headers(token: str) -> dict:
    """API isteklerinde kullanılacak header dict'i döner."""
    return {
        "TGT": token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


if __name__ == "__main__":
    token = get_token()
    print(f"\nToken alındı:\n{token}")
