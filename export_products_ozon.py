import json
import os
import time
from typing import Any, Dict, List, Optional

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


def post_ozon(path: str, payload: Dict[str, Any], retries: int = 5, debug: bool = False) -> Any:
    url = f"{BASE_URL}{path}"
    backoff = 1.0

    if debug:
        print("\n=== REQUEST ===")
        print(f"POST {url}")
        print(f"payload={payload}")

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

        if debug:
            print("\n=== RESPONSE ===")
            print(f"status={resp.status_code}")
            if isinstance(data, dict):
                print(f"keys={list(data.keys())[:30]}")
            else:
                print(f"type={type(data)}")

        if not resp.ok:
            raise RuntimeError(f"HTTP {resp.status_code} {path}: {data}")

        return data

    raise RuntimeError(f"Не удалось выполнить запрос {path} после {retries} попыток")


def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def to_float_safe(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def extract_items_from_attributes_response(data: Any) -> List[Dict[str, Any]]:
    """
    Для /v4/product/info/attributes у тебя формат:
    {"result": [ {...}, {...} ]}
    Но на всякий случай поддержим и {"result":{"items":[...]}} и {"items":[...]}
    """
    if isinstance(data, dict):
        result = data.get("result")
        if isinstance(result, list):
            return [x for x in result if isinstance(x, dict)]
        if isinstance(result, dict) and isinstance(result.get("items"), list):
            return [x for x in result["items"] if isinstance(x, dict)]
        if isinstance(data.get("items"), list):
            return [x for x in data["items"] if isinstance(x, dict)]
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def calc_volume_m3(depth: Any, width: Any, height: Any, dimension_unit: str) -> Optional[float]:
    """
    Считаем объём по габаритам упаковки.

    Если unit = "mm": depth*width*height -> мм^3, переводим в м^3: / 1e9
    Если unit = "cm": / 1e6
    Если unit = "m":  / 1
    """
    d = to_float_safe(depth)
    w = to_float_safe(width)
    h = to_float_safe(height)
    if d is None or w is None or h is None:
        return None

    unit = (dimension_unit or "").lower().strip()

    v = d * w * h
    if unit == "mm":
        return v / 1_000_000_000.0
    if unit == "cm":
        return v / 1_000_000.0
    if unit == "m":
        return v
    # если вдруг неизвестно — вернём "как есть" (без перевода)
    return v


def get_all_product_ids(debug: bool = False) -> List[str]:
    product_ids: List[str] = []
    last_id: str = ""

    while True:
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        data = post_ozon("/v3/product/list", payload, debug=debug)

        if not isinstance(data, dict):
            break
        result = data.get("result") or {}
        items = result.get("items") or []
        if not items:
            break

        for it in items:
            pid = it.get("product_id")
            if pid is not None:
                product_ids.append(str(pid))

        last_id = str(result.get("last_id") or "")
        if last_id == "":
            break

    return product_ids


def get_attributes_by_product_ids(product_ids: List[str], debug: bool = False) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    for batch in chunked(product_ids, 1000):
        payload = {
            "filter": {
                "product_id": batch,
                "visibility": "ALL",
            },
            "limit": len(batch),
            "sort_dir": "ASC",
        }
        data = post_ozon("/v4/product/info/attributes", payload, debug=debug)
        items = extract_items_from_attributes_response(data)

        if debug and items:
            print(f"attributes example keys={list(items[0].keys())}")

        all_items.extend(items)
    return all_items


def export_products_to_json(out_path: str = "товары.json", debug: bool = False) -> None:
    product_ids = get_all_product_ids(debug=debug)
    print(f"Найдено товаров: {len(product_ids)}")

    attrs = get_attributes_by_product_ids(product_ids, debug=debug)
    print(f"Получено атрибутов: {len(attrs)}")

    товары: List[Dict[str, Any]] = []

    for item in attrs:
        # В твоём примере: длина упаковки = depth
        depth = item.get("depth")
        width = item.get("width")
        height = item.get("height")
        dim_unit = item.get("dimension_unit") or "mm"

        volume_m3 = calc_volume_m3(depth, width, height, dim_unit)

        товар = {
            # ключи под твою таблицу + дополнительные поля
            "id": item.get("id") or item.get("product_id"),
            "sku": item.get("sku"),  # если в ответе будет — отлично, если нет — останется None
            "Штрихкод": item.get("barcode"),
            "Название": item.get("name"),
            "Объем_м3": volume_m3,
            "Артикул": item.get("offer_id"),

            "Длина упаковки": to_float_safe(depth),
            "Ширина упаковки": to_float_safe(width),
            "Высота упаковки": to_float_safe(height),
            "Размерность": dim_unit,

            "Вес упаковки": to_float_safe(item.get("weight")),
            "Ед.веса": item.get("weight_unit"),

            "Категория": item.get("description_category_id"),
        }
        товары.append(товар)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"товары": товары}, f, ensure_ascii=False, indent=2)

    print(f"✅ Сохранено: {len(товары)} товаров → {out_path}")


if __name__ == "__main__":
    export_products_to_json("товары.json", debug=False)
