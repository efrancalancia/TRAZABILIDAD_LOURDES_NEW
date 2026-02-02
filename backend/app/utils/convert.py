# backend/app/utils/convert.py
from __future__ import annotations
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional

def to_float(x: Any, default: float = 0.0) -> float:
    """
    Convierte con tolerancia: None, int/float, Decimal, str con . o ,.
    Nunca levanta excepción: cae en `default` si no puede.
    """
    try:
        if x is None:
            return default
        if isinstance(x, float):
            return x
        if isinstance(x, (int, Decimal)):
            return float(x)
        s = str(x).strip()
        # Normaliza "12.345,67" -> "12345.67" sólo si parece formato europeo con miles
        if s.count(",") == 1 and s.count(".") > 1:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return default

def to_int(x: Any) -> Optional[int]:
    """
    Convierte a int cuando es posible. Devuelve None si no puede.
    """
    try:
        if x is None:
            return None
        if isinstance(x, int):
            return x
        if isinstance(x, (float, Decimal)):
            return int(x)
        s = str(x).strip()
        # si llega "43123040001001.0" o similar
        if "." in s or "," in s:
            s = s.replace(",", ".")
            return int(float(s))
        return int(s)
    except Exception:
        return None

def to_iso(dt: Any) -> Optional[str]:
    """
    Devuelve ISO-8601. Acepta datetime/date/str (devuelve el str tal cual).
    """
    try:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            return dt.isoformat().replace("+00:00", "Z")
        if isinstance(dt, date):
            return datetime(dt.year, dt.month, dt.day).isoformat() + "Z"
        return str(dt)
    except Exception:
        return None
