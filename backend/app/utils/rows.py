# backend/app/utils/rows.py
from __future__ import annotations
from typing import Mapping, Iterable, Dict, Any, List

def normalize_keys_upper(mapping: Mapping) -> Dict[str, Any]:
    """
    Devuelve un dict con claves en MAYÚSCULAS (str(k).upper()).
    """
    return {str(k).upper(): v for k, v in dict(mapping).items()}

def normalize_list_upper(rows: Iterable[Mapping]) -> List[Dict[str, Any]]:
    """
    Normaliza una secuencia de mappings a lista de dicts con claves en MAYÚSCULAS.
    """
    return [normalize_keys_upper(r) for r in rows]
