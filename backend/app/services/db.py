# backend/app/services/db.py
from __future__ import annotations
from typing import Optional, Tuple
import os
import json

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ..core.config import settings

_engine: Optional[Engine] = None


def _read_credentials_from_file(path: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Lee credenciales desde DB_CREDENTIALS_PATH soportando:
      1) JSON: {"username":"USR","password":"PWD"}  (o keys USER/DB_USERNAME, PASS/DB_PASSWORD)
      2) INI/ENV: líneas KEY=VAL (DB_USERNAME/DB_PASSWORD, USERNAME/PASSWORD, USER/PASS)
      3) Texto "user:pass" en una línea
      4) **Dos líneas**: línea 1 = user, línea 2 = pass  (FORMATO QUE USA TU PROCESO)
    """
    if not path or not os.path.exists(path):
        return None, None

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

        if not raw:
            return None, None
        raw_stripped = raw.strip()

        # (1) JSON
        try:
            obj = json.loads(raw_stripped)
            u = obj.get("username") or obj.get("user") or obj.get("DB_USERNAME") or obj.get("USERNAME") or obj.get("USER")
            p = obj.get("password") or obj.get("pass") or obj.get("DB_PASSWORD") or obj.get("PASSWORD") or obj.get("PASS")
            if u and p:
                return str(u), str(p)
        except json.JSONDecodeError:
            pass

        # (2) INI/ENV KEY=VAL
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        kv = {}
        for ln in lines:
            if "=" in ln:
                k, v = ln.split("=", 1)
                kv[k.strip()] = v.strip()
        for ukey in ("DB_USERNAME", "USERNAME", "USER"):
            for pkey in ("DB_PASSWORD", "PASSWORD", "PASS"):
                if kv.get(ukey) and kv.get(pkey):
                    return kv[ukey], kv[pkey]

        # (3) Una línea "user:pass"
        if ":" in raw_stripped and "\n" not in raw_stripped:
            u, p = raw_stripped.split(":", 1)
            u, p = u.strip(), p.strip()
            if u and p:
                return u, p

        # (4) **Dos líneas** (user en la 1, pass en la 2) — TU CASO
        if len(lines) >= 2 and "=" not in lines[0] and ":" not in lines[0]:
            u = lines[0].strip()
            p = lines[1].strip()
            if u and p:
                return u, p

    except Exception:
        return None, None

    return None, None


def _build_sqlalchemy_url() -> tuple[str, dict]:
    """
    Construye la URL SQLAlchemy para Oracle (driver 'oracledb') y connect_args.
    Prioridad credenciales:
      A) DB_USERNAME + DB_PASSWORD (en .env)
      B) DB_CREDENTIALS_PATH (archivo con cualquiera de los formatos soportados)
    TNS:
      - ORACLE_TNS_ALIAS obligatorio
      - Si ORACLE_TNS_ADMIN está definido, se pasa como connect_args={'config_dir': ...}
    """
    # Credenciales
    user = settings.db_username or os.getenv("DB_USERNAME")
    pwd = settings.db_password or os.getenv("DB_PASSWORD")

    if not user or not pwd:
        creds_path = settings.db_credentials_path or os.getenv("DB_CREDENTIALS_PATH")
        u2, p2 = _read_credentials_from_file(creds_path) if creds_path else (None, None)
        user = user or u2
        pwd = pwd or p2

    alias = settings.oracle_tns_alias or os.getenv("ORACLE_TNS_ALIAS")
    if not user or not pwd or not alias:
        raise RuntimeError(
            "Faltan credenciales/alias para Oracle. "
            "Defina DB_USERNAME, DB_PASSWORD y ORACLE_TNS_ALIAS en el .env "
            "o proporcione DB_CREDENTIALS_PATH con user/password (2 líneas)."
        )

    url = f"oracle+oracledb://{user}:{pwd}@{alias}"

    # Pasar TNS_ADMIN si está definido (igual que tu proceso de composición)
    connect_args = {}
    tns_admin = settings.oracle_tns_admin or os.getenv("ORACLE_TNS_ADMIN")
    if tns_admin:
        connect_args["config_dir"] = tns_admin

    return url, connect_args


def get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    url, connect_args = _build_sqlalchemy_url()
    _engine = create_engine(
        url,
        connect_args=connect_args,   # 🔧 importante para usar tnsnames.ora
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "5")),
        future=True,
    )
    return _engine


def dispose_engine() -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


def quick_health_check() -> dict:
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1 FROM DUAL"))
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
