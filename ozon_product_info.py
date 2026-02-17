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


def to_int_safe(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


def calc_volume(depth: Any, width: Any, height: Any, unit: str) -> Optional[float]:
    """
    Объем = depth*width*height.
    Переводим в м^3:
      mm -> /1e9
      cm -> /1e6
      m  -> как есть
    """
    try:
        d = float(depth)
        w = float(width)
        h = float(height)
    except Exception:
        return None

    u = (unit or "").lower().strip()
    v = d * w * h
    if u == "mm":
        return v / 1_000_000_000.0
    if u == "cm":
        return v / 1_000_000.0
    if u == "m":
        return v
    return v


def extract_result_list(data: Any) -> List[Dict[str, Any]]:
    """
    У тебя /v4/product/info/attributes приходит так:
    {"result": [ {...}, {...} ]}

    На всякий случай поддержим ещё:
    {"result":{"items":[...]}} / {"items":[...]} / просто [...]
    """
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    r = data.get("result")
    if isinstance(r, list):
        return [x for x in r if isinstance(x, dict)]
    if isinstance(r, dict) and isinstance(r.get("items"), list):
        return [x for x in r["items"] if isinstance(x, dict)]
    if isinstance(data.get("items"), list):
        return [x for x in data["items"] if isinstance(x, dict)]
    return []


def get_all_product_ids() -> List[str]:
    product_ids: List[str] = []
    last_id: str = ""

    while True:
        payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
        data = post_ozon("/v3/product/list", payload)

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


def get_attributes(product_ids: List[str], debug: bool = False) -> List[Dict[str, Any]]:
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
        items = extract_result_list(data)

        if debug and items:
            print(f"attributes items[0].keys()={list(items[0].keys())}")

        all_items.extend(items)

    return all_items


def export_characteristics_ozon(items: List[Dict[str, Any]], out_path: str = "характеристики_ozon.json") -> None:
    """
    Формируем "большой" JSON под твою таблицу.
    Поля, которых нет в ответе Ozon прямо сейчас, оставляем null.
    """
    rows: List[Dict[str, Any]] = []

    for it in items:
        depth = it.get("depth")          # Длина упаковки = depth
        width = it.get("width")
        height = it.get("height")
        dim_unit = it.get("dimension_unit") or "mm"

        row = {
            # id в БД автогенерится — в JSON можем не писать (или оставить None)
            "id": None,

            "idtow": it.get("id"),  # product_id из Ozon (по твоему примеру "id")
            "SKU": it.get("sku"),   # если поле есть — будет, если нет — останется null

            "Длина упаковки": to_int_safe(depth),
            "Ширина упаковки": to_int_safe(width),
            "Высота упаковки": to_int_safe(height),

            "Вес с упаковкой": to_int_safe(it.get("weight")),
            "weight_unit": it.get("weight_unit"),

            "ставка НДС": None,  # пока нет в этом ответе
            "idкатегории_товара_oz": it.get("description_category_id"),

            "Наименование товара": it.get("name"),
            "Бренд": None,
            "Наименование модели (Склейка)": None,
            "Кол-во шт": None,
            "Вес самого товара": None,
            "Кол-во товара в УЕИ": None,
            "#": None,
            "Аннотация": None,
            "Названия группы": None,
            "PartNumber": it.get("offer_id"),  # ты пометила PartNumber как артикул
            'Длина "Лента"': None,
            'Ширина "Лента"': None,
            "Крепелние": None,
            "Цвет": None,
            "Материал": None,
            "Вид техники": None,
            "Вид выпуска товара": None,
            "Страна изготовитель": None,
            "Кол-во зав. упаковок": None,
            "ТН ВЭД коды ЕАЭС": None,
            "ОЕМ-номер": None,
            "Алтер. артикул": None,
            "Вид ламп": None,
            'Типоразмер "Цоколь"': None,
            "Кол-во ламп": None,
            "Назначение авто. лампы": None,
            "Питание": None,
            'Мощность "лампы"': None,
            "Комплектация упаковки": None,
            "Вид запчасти": None,
            "Сторона установки": None,
            "Вид спец. техники": None,
            "Кратность покупки": None,
            "Класс опасности товара": None,
            "Место установки": None,

            # Дополнительно: объём (ты просила считать)
            "Объем_м3": calc_volume(depth, width, height, dim_unit),
            "dimension_unit": dim_unit,
        }

        rows.append(row)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"характеристики_ozon": rows}, f, ensure_ascii=False, indent=2)

    print(f"✅ Сохранено: {len(rows)} строк → {out_path}")


def main(debug: bool = False) -> None:
    product_ids = get_all_product_ids()
    print(f"Найдено товаров: {len(product_ids)}")

    items = get_attributes(product_ids, debug=debug)
    print(f"Получено записей attributes: {len(items)}")

    export_characteristics_ozon(items, "характеристики_ozon.json")


if __name__ == "__main__":
    main(debug=False)
