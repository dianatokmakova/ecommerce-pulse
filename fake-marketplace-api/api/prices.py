"""
Fake Marketplace API — источник синтетических «живых» цен.
Деплоится на Vercel как Python-serverless (FastAPI ASGI app экспортируется как `app`).

GET /api/prices  ->  {"captured_at": ..., "items": [{product_id, category, price, stock}, ...]}

Особенности:
  • базовая цена детерминирована по product_id (стабильна между запросами);
  • цена ДРЕЙФУЕТ во времени: сезонность по часу/дню недели + небольшой шум;
  • с вероятностью ANOMALY_RATE подмешивается АНОМАЛИЯ: скачок +40% или обнуление остатка.
"""
from __future__ import annotations
import hashlib
import math
import random
from datetime import datetime, timezone

from fastapi import FastAPI

app = FastAPI(title="Fake Marketplace API")

N_PRODUCTS = 50
CATEGORIES = ["electronics", "home", "toys", "fashion", "beauty",
              "sports", "garden", "auto", "books", "grocery"]
ANOMALY_RATE = 0.04          # ~4% товаров в запросе получат аномалию


def _seed(product_id: str) -> int:
    return int(hashlib.md5(product_id.encode()).hexdigest(), 16)


def _base_price(product_id: str) -> float:
    rnd = random.Random(_seed(product_id))
    return round(rnd.uniform(20, 500), 2)        # стабильная база 20..500 BRL


def _category(product_id: str) -> str:
    return CATEGORIES[_seed(product_id) % len(CATEGORIES)]


def build_items() -> list[dict]:
    now = datetime.now(timezone.utc)
    hour_factor = math.sin(now.hour / 24 * 2 * math.pi)      # суточная сезонность
    dow_factor = 0.05 if now.weekday() >= 5 else 0.0          # выходные чуть дороже
    items = []
    for i in range(N_PRODUCTS):
        pid = f"SKU-{i:04d}"
        base = _base_price(pid)
        noise = random.uniform(-0.03, 0.03)                  # ±3% шум
        price = base * (1 + 0.08 * hour_factor + dow_factor + noise)
        stock = random.randint(0, 200)

        # подмешиваем аномалию
        if random.random() < ANOMALY_RATE:
            if random.random() < 0.6:
                price *= 1.40                                # резкий скачок цены
            else:
                stock = 0                                    # внезапно out of stock

        items.append({
            "product_id": pid,
            "category": _category(pid),
            "price": round(price, 2),
            "stock": stock,
        })
    return items


@app.get("/api/prices")
def prices():
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": N_PRODUCTS,
        "items": build_items(),
    }


@app.get("/api/health")
def health():
    return {"status": "ok"}
