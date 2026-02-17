import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api-seller.ozon.ru"
TIMEOUT_SEC = 30

CLIENT_ID = os.getenv("OZON_CLIENT_ID")
API_KEY = os.getenv("OZON_API_KEY")
if not CLIENT_ID or not API_KEY:
    raise RuntimeError("В .env не найдены OZON_CLIENT_ID и/или OZON_API_KEY")

HEADERS = {
    "Client-Id": CLIENT_ID,
    "Api-Key": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def post_ozon(path: str, payload: Dict[str, Any], retries: int = 6) -> Any:
    url = f"{BASE_URL}{path}"
    backoff = 1.0

    for _ in range(retries):
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=TIMEOUT_SEC)

        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue

        try:
            data = resp.json()
        except Exception:
            data = resp.text

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code} {path}: {data}")

        return data

    raise RuntimeError(f"Не удалось выполнить запрос {path} после {retries} попыток")


def chunked(lst: List[Any], n: int) -> List[List[Any]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


# --- 1) product_id ---
def get_all_product_ids() -> List[str]:
    product_ids: List[str] = []
    last_id: str = ""

    while True:
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        data = post_ozon("/v3/product/list", payload)

        result = (data or {}).get("result") or {}
        items = result.get("items") or []
        if not items:
            break

        for it in items:
            pid = it.get("product_id")
            if pid is not None:
                product_ids.append(str(pid))

        last_id = str(result.get("last_id") or "")
        if not last_id:
            break

    return product_ids


# --- 2) attributes -> sku + name + offer_id ---
def extract_attributes_items(data: Any) -> List[Dict[str, Any]]:
    # у тебя обычно {"result": [ ... ]}
    if isinstance(data, dict):
        r = data.get("result")
        if isinstance(r, list):
            return [x for x in r if isinstance(x, dict)]
        if isinstance(r, dict) and isinstance(r.get("items"), list):
            return [x for x in r["items"] if isinstance(x, dict)]
        if isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
    elif isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def normalize_sku(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s.isdigit():
        return None
    n = int(s)
    return n if n > 0 else None


def extract_sku_from_item(it: Dict[str, Any]) -> Optional[int]:
    for k in ("sku", "sku_id"):
        n = normalize_sku(it.get(k))
        if n is not None:
            return n
    v = it.get("skus")
    if isinstance(v, list) and v:
        n = normalize_sku(v[0])
        if n is not None:
            return n
    return None


def get_sku_meta_by_product_ids(product_ids: List[str]) -> Dict[int, Dict[str, Any]]:
    """
    Возвращает мапу:
      sku_int -> {"name": ..., "offer_id": ...}
    """
    sku_meta: Dict[int, Dict[str, Any]] = {}

    for batch in chunked(product_ids, 1000):
        payload = {
            "filter": {"product_id": batch, "visibility": "ALL"},
            "limit": len(batch),
            "sort_dir": "ASC",
        }
        data = post_ozon("/v4/product/info/attributes", payload)
        items = extract_attributes_items(data)

        for it in items:
            sku = extract_sku_from_item(it)
            if sku is None:
                continue
            # name/offer_id берём отсюда (если в analytics тоже есть — не страшно)
            sku_meta.setdefault(
                sku,
                {
                    "name": it.get("name"),
                    "offer_id": it.get("offer_id"),
                },
            )

    return sku_meta


# --- 3) analytics/stocks ---
def get_stocks_for_skus(skus: List[int]) -> List[Dict[str, Any]]:
    # API: skus <= 100, мы даём 99
    payload = {"skus": [str(s) for s in skus]}
    data = post_ozon("/v1/analytics/stocks", payload)
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    return []


def compact_row(row: Dict[str, Any], fallback_meta: Dict[str, Any]) -> Dict[str, Any]:
    available = int(row.get("available_stock_count") or 0)
    valid = int(row.get("valid_stock_count") or 0)

    return {
        "sku": row.get("sku"),
        "name": row.get("name") or fallback_meta.get("name"),
        "offer_id": row.get("offer_id") or fallback_meta.get("offer_id"),
        "warehouse_id": row.get("warehouse_id"),
        "warehouse_name": row.get("warehouse_name"),
        "cluster_id": row.get("cluster_id"),
        "cluster_name": row.get("cluster_name"),
        "stock": available + valid,
    }


# --- 4) финальный экспорт ---
def export_stocks_compact(out_path: str = "stocks_compact.json", chunk_size: int = 99) -> None:
    product_ids = get_all_product_ids()
    print(f"Найдено товаров (product_id): {len(product_ids)}")

    sku_meta = get_sku_meta_by_product_ids(product_ids)
    skus = sorted(sku_meta.keys())
    print(f"SKU найдено (уникальных): {len(skus)}")

    # Пишем JSON-массив стримингом
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("[\n")
        first_sku_obj = True

        for idx, sku_batch in enumerate(chunked(skus, chunk_size), start=1):
            items = get_stocks_for_skus(sku_batch)
            print(f"Чанк {idx}: SKU={len(sku_batch)} -> строк items={len(items)}")

            # группируем строки по sku (внутри чанка)
            grouped: Dict[int, List[Dict[str, Any]]] = {s: [] for s in sku_batch}

            for row in items:
                sku_val = row.get("sku")
                if sku_val is None:
                    continue
                try:
                    sku_int = int(sku_val)
                except Exception:
                    continue
                if sku_int not in grouped:
                    # на всякий случай
                    grouped[sku_int] = []

                grouped[sku_int].append(compact_row(row, sku_meta.get(sku_int, {})))

            # записываем объекты {sku, items} по каждому sku из батча
            for s in sku_batch:
                obj = {
                    "sku": str(s),
                    "items": grouped.get(s, []),
                }

                if not first_sku_obj:
                    f.write(",\n")
                f.write("  " + json.dumps(obj, ensure_ascii=False))
                first_sku_obj = False

        f.write("\n]\n")

    print(f"✅ Готово: {out_path}")


if __name__ == "__main__":
    export_stocks_compact()
