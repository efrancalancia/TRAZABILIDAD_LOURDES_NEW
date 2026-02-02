from dataclasses import dataclass
import os
from typing import Optional

# Carga .env si está disponible
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def _getenv(key: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(key)
    return v if (v is not None and v != "") else default


@dataclass
class Settings:
    # API
    api_host: str = _getenv("API_HOST", "0.0.0.0")
    api_port: int = int(_getenv("API_PORT", "8000"))
    log_level: str = _getenv("LOG_LEVEL", "INFO")

    # Oracle
    oracle_tns_alias: Optional[str] = _getenv("ORACLE_TNS_ALIAS")
    oracle_tns_admin: Optional[str] = _getenv("ORACLE_TNS_ADMIN")
    db_credentials_path: Optional[str] = _getenv("DB_CREDENTIALS_PATH")
    db_username: Optional[str] = _getenv("DB_USERNAME")
    db_password: Optional[str] = _getenv("DB_PASSWORD")

    # Límites / constantes
    oracle_in_clause_limit: int = int(_getenv("ORACLE_IN_CLAUSE_LIMIT", "999"))
    max_len_d_deposito: int = int(_getenv("MAX_LEN_D_DEPOSITO", "20"))

    # Salidas
    csv_out_dir: str = _getenv("CSV_OUT_DIR", "./outputs")
    logs_out_dir: str = _getenv("LOGS_OUT_DIR", "./outputs/logs")

    # Módulo de composición (nombre de módulo o ruta .py)
    composicion_module_path: str = _getenv("COMPOSICION_MODULE_PATH", "composicion_enologica")

    # NUEVO: modo de trazabilidad (fake | real)
    trace_mode: str = _getenv("TRACE_MODE", "fake").lower().strip()


settings = Settings()
