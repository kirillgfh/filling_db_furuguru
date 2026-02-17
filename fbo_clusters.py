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

# Для справочника складов FBO (чтобы маппить warehouse_id -> name)
SUPPLY_TYPES = ["DIRECT", "CROSSDOCK"]


def dump_json(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


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


def _is_list_of_dicts(x: Any) -> bool:
    return isinstance(x, list) and (len(x) == 0 or all(isinstance(i, dict) for i in x))


def find_best_list_of_dicts(obj: Any) -> Tuple[List[Dict[str, Any]], str]:
    """Находим список кластеров в ответе где бы он ни лежал."""
    best: List[Dict[str, Any]] = []
    best_path = ""
    priority_keys = ("clusters", "items", "result", "data")

    def walk(node: Any, path: str) -> None:
        nonlocal best, best_path

        if isinstance(node, dict):
            for k in priority_keys:
                if k in node and _is_list_of_dicts(node[k]):
                    cand = node[k]
                    if len(cand) > len(best):
                        best = cand
                        best_path = f"{path}.{k}" if path else k

            for k, v in node.items():
                walk(v, f"{path}.{k}" if path else k)

        elif isinstance(node, list):
            if _is_list_of_dicts(node):
                if len(node) > len(best):
                    best = node
                    best_path = path or "<root_list>"
            else:
                for i, v in enumerate(node):
                    walk(v, f"{path}[{i}]")

    walk(obj, "")
    return best, best_path


def pick(d: Dict[str, Any], *keys: str) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None


# ---------- 1) Кластеры ----------
def extract_clusters() -> List[Dict[str, Any]]:
    data = post_ozon("/v1/cluster/list", {"cluster_type": "CLUSTER_TYPE_OZON"})
    dump_json("ozon_cluster_list_raw.json", data)

    clusters, path = find_best_list_of_dicts(data)
    if not clusters:
        raise RuntimeError("Не смог найти список кластеров. Открой ozon_cluster_list_raw.json и проверь структуру.")

    dump_json("ozon_clusters_extracted.json", {"extracted_from": path, "clusters_count": len(clusters)})
    return clusters


# ---------- 2) Справочник складов FBO (для маппинга id->name) ----------
def extract_list_any(data: Any) -> List[Dict[str, Any]]:
    """Достаёт список из: {'result':[...]} / {'result':{'items':[...]}} / {'items':[...]} / [...]"""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    r = data.get("result")
    if isinstance(r, list):
        return [x for x in r if isinstance(x, dict)]
    if isinstance(r, dict):
        if isinstance(r.get("items"), list):
            return [x for x in r["items"] if isinstance(x, dict)]
        for _, v in r.items():
            if _is_list_of_dicts(v):
                return v
    if isinstance(data.get("items"), list):
        return [x for x in data["items"] if isinstance(x, dict)]
    return []


def get_fbo_warehouse_map() -> Dict[str, str]:
    """
    Пытаемся получить warehouse_id -> name.
    Если эндпойнт недоступен/ругается — вернём пустую мапу (склады будем брать из cluster/list, если там есть имена).
    """
    try:
        payload = {"filter_by_supply_type": SUPPLY_TYPES, "limit": 1000, "offset": 0}
        data = post_ozon("/v1/warehouse/fbo/list", payload)

        rows = extract_list_any(data)
        mp: Dict[str, str] = {}
        for w in rows:
            wid = pick(w, "warehouse_id", "id")
            nm = pick(w, "name", "warehouse_name", "title")
            if wid is not None and nm:
                mp[str(wid)] = str(nm)
        return mp
    except Exception:
        return {}


# ---------- 3) Рекурсивно вытащить склады из кластера ----------
def collect_warehouse_refs(node: Any) -> List[Tuple[Optional[str], Optional[str]]]:
    """
    Возвращает список (warehouse_id, warehouse_name).
    Ищет внутри структуры любые списки, где ключ содержит 'warehouse'.
    Поддерживает:
      - список словарей (там может быть warehouse_id/name)
      - список id (int/str)
    """
    out: List[Tuple[Optional[str], Optional[str]]] = []

    def add_from_list(lst: list) -> None:
        # список словарей
        if lst and all(isinstance(x, dict) for x in lst):
            for w in lst:
                wid = pick(w, "warehouse_id", "id", "warehouseId")
                nm = pick(w, "name", "warehouse_name", "title", "warehouseName")
                out.append((str(wid) if wid is not None else None, str(nm) if nm is not None else None))
            return

        # список id
        if lst and all(isinstance(x, (int, str)) for x in lst):
            for x in lst:
                out.append((str(x), None))
            return

        # смешанный — рекурсивно
        for x in lst:
            walk(x)

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for k, v in x.items():
                lk = str(k).lower()
                if "warehouse" in lk and isinstance(v, list):
                    add_from_list(v)
                else:
                    walk(v)
        elif isinstance(x, list):
            for i in x:
                walk(i)

    walk(node)
    return out


# ---------- 4) Построение 2 таблиц ----------
def build_regions(clusters: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Регионы:
      id (локальный, для связи внутри json)
      Регион (имя кластера)
      id_cluster_ozon (cluster_id)
    """
    regions: List[Dict[str, Any]] = []
    map_cluster_to_region_id: Dict[str, int] = {}

    next_id = 1
    for c in clusters:
        cid = pick(c, "cluster_id", "id")
        if cid is None:
            continue
        cid_s = str(cid)
        if cid_s in map_cluster_to_region_id:
            continue

        name = pick(c, "cluster_name", "name", "title")
        rid = next_id
        next_id += 1

        map_cluster_to_region_id[cid_s] = rid
        regions.append({
            "id": rid,                 # локальный id (потом в БД будет свой autoincrement)
            "Регион": name,
            "id_cluster_ozon": int(cid) if str(cid).isdigit() else cid,
        })

    return regions, map_cluster_to_region_id


def build_warehouses(
    clusters: List[Dict[str, Any]],
    map_cluster_to_region_id: Dict[str, int],
    warehouse_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Склады:
      id = None (в БД autoincrement)
      Склад = name
      idРегион = локальный id региона
    """
    rows: List[Dict[str, Any]] = []
    seen = set()  # (region_id, warehouse_id or name)

    for c in clusters:
        cid = pick(c, "cluster_id", "id")
        if cid is None:
            continue
        rid = map_cluster_to_region_id.get(str(cid))
        if rid is None:
            continue

        refs = collect_warehouse_refs(c)

        for wid, nm in refs:
            # если имени нет — попробуем по справочнику fbo/list
            if (nm is None or nm == "") and wid is not None and wid in warehouse_map:
                nm = warehouse_map[wid]

            if not nm:
                # если имени нет вообще — пропускаем (ты просишь "Склад" текстом)
                continue

            key = (rid, wid or nm)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "id": None,
                "Склад": nm,
                "idРегион": rid,
                # оставлю полезное поле — можно удалить если не надо
                "warehouse_id_ozon": wid,
            })

    return rows


def main() -> None:
    clusters = extract_clusters()
    warehouse_map = get_fbo_warehouse_map()

    regions, map_cluster_to_region_id = build_regions(clusters)
    warehouses = build_warehouses(clusters, map_cluster_to_region_id, warehouse_map)

    dump_json("регионы_ozon.json", {"регион_ozon": regions})
    dump_json("склады_ozon.json", {"склады_ozon": warehouses})

    print(f"✅ Регионы: {len(regions)} -> регионы_ozon.json")
    print(f"✅ Склады: {len(warehouses)} -> склады_ozon.json")
    if len(warehouses) == 0:
        print("⚠️ Склады не нашлиcь в cluster/list (или нет названий). Открой ozon_cluster_list_raw.json и найди, где лежат склады.")


if __name__ == "__main__":
    main()
