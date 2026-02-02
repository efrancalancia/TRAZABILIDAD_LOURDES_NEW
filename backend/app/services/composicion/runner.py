import json
import importlib
import importlib.util
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

from ...core.config import settings


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_comment(msg: str) -> str:
    return f": {msg}\n\n"


def _load_module_from_file(py_path: Path):
    spec = importlib.util.spec_from_file_location(py_path.stem, str(py_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo crear el spec para: {py_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod


def _import_composicion_module():
    """
    Soporta:
      - Nombre de módulo (p.ej. 'composicion_enologica')
      - Ruta absoluta/relativa a un .py
    Debe exponer: ejecutar_proceso_completo(fecha_inicio_str, fecha_fin_str)
    """
    module_ref = settings.composicion_module_path
    if not module_ref:
        raise RuntimeError("COMPOSICION_MODULE_PATH no configurado.")

    p = Path(module_ref)
    if p.suffix.lower() == ".py" or p.exists():
        if not p.exists():
            raise RuntimeError(f"Ruta de módulo no existe: {p}")
        return _load_module_from_file(p)

    return importlib.import_module(module_ref)


def _ensure_logs_dir() -> Path:
    logs_dir = Path(settings.logs_out_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def _open_log_file(fecha_desde: str, fecha_hasta: str) -> Path:
    logs_dir = _ensure_logs_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_desde = fecha_desde.replace("-", "")
    safe_hasta = fecha_hasta.replace("-", "")
    fname = f"composicion_{ts}_{safe_desde}_{safe_hasta}.log"
    return logs_dir / fname


def stream_sse_logs(fecha_desde: str, fecha_hasta: str) -> Generator[str, None, None]:
    print(f"[SSE] inicio stream -> {fecha_desde} .. {fecha_hasta}")

    # Abrir archivo de log
    log_path = _open_log_file(fecha_desde, fecha_hasta)
    f = log_path.open("a", encoding="utf-8", newline="")

    def _write_line(level: str, msg: str):
        ts = _utcnow_iso()
        f.write(f"{ts} [{level}] {msg}\n")
        f.flush()

    # “despertar” al cliente y primer log
    yield _sse_comment("stream-open")
    first_msg = "Iniciando proceso de composición..."
    _write_line("INFO", first_msg)
    yield _sse("log", {"ts": _utcnow_iso(), "level": "INFO", "msg": first_msg})

    # Importar módulo
    try:
        mod = _import_composicion_module()
        msg = f"Módulo importado: {settings.composicion_module_path}"
        _write_line("INFO", msg)
        yield _sse("log", {"ts": _utcnow_iso(), "level": "INFO", "msg": msg})
    except Exception as imp_err:
        err = f"{imp_err}"
        _write_line("ERROR", f"IMPORT_ERROR: {err}")
        yield _sse("error", {"ok": False, "code": "IMPORT_ERROR", "message": err})
        f.close()
        return

    if not hasattr(mod, "ejecutar_proceso_completo"):
        msg = "El módulo no expone 'ejecutar_proceso_completo(fecha_inicio, fecha_fin)'"
        _write_line("ERROR", f"ATTR_ERROR: {msg}")
        yield _sse("error", {"ok": False, "code": "ATTR_ERROR", "message": msg})
        f.close()
        return

    # Ejecutar el generador del proceso y retransmitir logs
    try:
        gen = mod.ejecutar_proceso_completo(fecha_desde, fecha_hasta)
        msg = "Proceso lanzado, leyendo logs..."
        _write_line("INFO", msg)
        yield _sse("log", {"ts": _utcnow_iso(), "level": "INFO", "msg": msg})

        for line in gen:
            msg = str(line).rstrip()
            if not msg:
                continue
            _write_line("INFO", msg)
            yield _sse("log", {"ts": _utcnow_iso(), "level": "INFO", "msg": msg})

    except Exception as run_err:
        err = f"{run_err}"
        _write_line("ERROR", f"RUNTIME_ERROR: {err}")
        yield _sse("error", {"ok": False, "code": "RUNTIME_ERROR", "message": err})
        f.close()
        return

    # Fin correcto
    _write_line("INFO", "Proceso finalizado.")
    yield _sse("done", {"ok": True})
    # comentario final para cerrar prolijo algunos clientes
    yield _sse_comment("stream-close")
    f.close()
    print(f"[SSE] fin stream - log guardado en: {log_path}")
