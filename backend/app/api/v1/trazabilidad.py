from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any, Set, Tuple
from sqlalchemy import text

from ...services import db as db_service
from ...core.config import settings
from ...utils.rows import normalize_list_upper, normalize_keys_upper
from ...utils.convert import to_float, to_int, to_iso

router = APIRouter(tags=["Trazabilidad"])

TIPO_MAP: Dict[int, str] = {
    13: "Compra",
    28: "Descube",
    31: "Ajuste",
    95: "Ajuste",
    43: "Transformación",
    30: "Transformación",
    46: "Transformación",
}

def _tq(table_name: str) -> str:
    schema = getattr(settings, "db_schema", None) or settings.__dict__.get("db_schema")
    return f"{schema}.{table_name}" if schema else table_name

def _tipo_legible(c_tipo: Optional[int]) -> str:
    try:
        return TIPO_MAP.get(int(c_tipo)) if c_tipo is not None else "Movimiento"
    except Exception:
        return "Movimiento"

def _table_exists(conn, name: str) -> bool:
    try:
        conn.execute(text(f"SELECT 1 FROM {_tq(name)} WHERE 1=0"))
        return True
    except Exception:
        return False

# ---------- repos ----------
def _fetch_lote_info(conn, c_lote_num: int) -> Dict[str, Any]:
    if not _table_exists(conn, "LOTES_STOCK"):
        return {"C_LOTE": str(c_lote_num), "D_LOTE": None}
    sql = text(f"""
        SELECT C_LOTE, D_LOTE
        FROM {_tq("LOTES_STOCK")}
        WHERE C_LOTE = :c_lote
    """)
    row = conn.execute(sql, {"c_lote": c_lote_num}).mappings().fetchone()
    if not row:
        return {"C_LOTE": str(c_lote_num), "D_LOTE": None}
    d = normalize_keys_upper(row)
    d["C_LOTE"] = str(d.get("C_LOTE")) if d.get("C_LOTE") is not None else None
    return d

def _fetch_movs_for_dest(conn, c_lote_num: int) -> List[Dict[str, Any]]:
    """
    Movimientos donde el lote aparece como DESTINO → orígenes directos.
    Volumen: CANTIDAD (NUMBER(15,5))
    """
    sql = text(f"""
        SELECT
            C_TIPO_COMPRO,
            F_MOVIMIENTO,
            MOS_ID,
            C_DORIGEN, D_DORIGEN,
            C_DDESTINO, D_DDESTINO,
            C_LOTE_ORIGEN,
            CANTIDAD AS VOL
        FROM {_tq("APX_TRAZA_DETALLE")}
        WHERE C_LOTE = :c_lote
        ORDER BY F_MOVIMIENTO ASC, MOS_ID ASC
    """)
    rows = conn.execute(sql, {"c_lote": c_lote_num}).mappings().all()
    rows = normalize_list_upper(rows)  # ← claves en MAYÚSCULAS
    for d in rows:
        d["VOL"] = to_float(d.get("VOL"))
    return rows

def _sum_destinos_finales(conn, c_lote_num: int) -> float:
    if not _table_exists(conn, "APX_TRAZA_DESTINO_FINAL"):
        return 0.0
    sql = text(f"""
        SELECT COALESCE(SUM(CANTIDAD_USADA), 0)
        FROM {_tq("APX_TRAZA_DESTINO_FINAL")}
        WHERE C_LOTE = :c_lote
    """)
    val = conn.execute(sql, {"c_lote": c_lote_num}).scalar()
    return to_float(val)

def _build_tree(conn, root_lote_num: int, max_depth: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    nodes: List[Dict[str, Any]] = []
    timeline: List[Dict[str, Any]] = []

    root_node_id = f"ROOT-{root_lote_num}"
    nodes.append({
        "node_id": root_node_id,
        "parent_id": None,
        "nivel": 0,
        "tipo": "Lote",
        "fecha": None,
        "ot": None,
        "tk_origen": None,
        "tk_destino": None,
        "lts_in": None,
        "lts_out": None,
        "merma_lts": None,
        "borra_lts": None,
        "otros_uso_lts": None,
        "contrib_pct": None,
        "guia": None,
        "fel": None,
        "observacion": None,
        "c_lote": str(root_lote_num),
        "c_lote_origen": None,
    })

    queue: List[Tuple[int, str, int]] = [(root_lote_num, root_node_id, 0)]
    visited: Set[int] = set([root_lote_num])

    while queue:
        current_lote, parent, level = queue.pop(0)
        if level >= max_depth:
            continue

        movs = _fetch_movs_for_dest(conn, current_lote)
        if not movs:
            continue

        total_lvl = sum(to_float(m.get("VOL")) for m in movs) or 0.0

        for idx, m in enumerate(movs, start=1):
            origen_int = to_int(m.get("C_LOTE_ORIGEN"))
            cantidad = to_float(m.get("VOL"))
            contrib = (cantidad / total_lvl * 100.0) if total_lvl > 0 else None

            nodo_id = f"{level+1}-{current_lote}-{m.get('MOS_ID')}-{idx}"
            node = {
                "node_id": nodo_id,
                "parent_id": parent,
                "nivel": level + 1,
                "tipo": _tipo_legible(m.get("C_TIPO_COMPRO")),
                "fecha": to_iso(m.get("F_MOVIMIENTO")),
                "ot": m.get("MOS_ID"),
                "tk_origen": m.get("D_DORIGEN") or m.get("C_DORIGEN"),
                "tk_destino": m.get("D_DDESTINO") or m.get("C_DDESTINO"),
                "lts_in": cantidad,
                "lts_out": cantidad,
                "merma_lts": None,
                "borra_lts": None,
                "otros_uso_lts": None,
                "contrib_pct": contrib,
                "guia": None,
                "fel": None,
                "observacion": None,
                "c_lote": str(current_lote),
                "c_lote_origen": str(origen_int) if origen_int is not None else None,
            }
            nodes.append(node)

            timeline.append({
                "fecha": node["fecha"],
                "evento": node["tipo"],
                "detalle": f"OT {node['ot']}" if node["ot"] else "",
                "tk_origen": node["tk_origen"],
                "tk_destino": node["tk_destino"],
                "cantidad": cantidad,
            })

            if origen_int is not None and origen_int not in visited:
                visited.add(origen_int)
                queue.append((origen_int, nodo_id, level + 1))

    timeline = sorted([t for t in timeline if t.get("fecha")], key=lambda x: x["fecha"])
    return nodes, timeline

# ---------- endpoint ----------
@router.get("/lote/{c_lote}")
def trazabilidad_lote(
    c_lote: str,
    include: Optional[str] = Query(default="timeline", description="Campos opcionales separados por coma: 'timeline,destinos'"),
    max_depth: int = Query(default=5, ge=1, le=20),
    tolerance: float = Query(default=0.005, ge=0.0, le=0.05),
):
    include_set = set((include or "").lower().split(",")) if include else set()

    c_lote_num = to_int(c_lote)
    if c_lote_num is None:
        raise HTTPException(status_code=422, detail="c_lote debe ser numérico (NUMBER).")

    try:
        engine = db_service.get_engine()
        with engine.connect() as conn:
            if not _table_exists(conn, "APX_TRAZA_DETALLE"):
                raise HTTPException(status_code=501, detail=f"No existe la tabla {_tq('APX_TRAZA_DETALLE')}.")

            info = _fetch_lote_info(conn, c_lote_num)

            movs_lvl1 = _fetch_movs_for_dest(conn, c_lote_num)
            total_in_lvl1 = sum(to_float(m.get("VOL")) for m in movs_lvl1) if movs_lvl1 else 0.0

            total_dest_final = _sum_destinos_finales(conn, c_lote_num)

            nodes, timeline = _build_tree(conn, c_lote_num, max_depth=max_depth)

            fechas = [n.get("fecha") for n in nodes if n.get("fecha")]
            f_ini = min(fechas) if fechas else None
            f_fin = max(fechas) if fechas else None

            lts_origenes = total_in_lvl1
            lts_destino = total_in_lvl1
            diff = abs(lts_origenes - lts_destino)
            ok_balance = (lts_origenes == 0.0) or (diff <= (tolerance * max(lts_origenes, 1.0)))

            resp: Dict[str, Any] = {
                "identificacion": {
                    "c_lote": str(c_lote_num),
                    "producto": None,
                    "tanque_actual": (info.get("D_LOTE") if isinstance(info, dict) else None),
                    "fecha_inicio": f_ini,
                    "fecha_fin": f_fin,
                    "origen_consulta": "C_LOTE",
                },
                "kpis": {
                    "lts_destino": lts_destino,
                    "rendimiento_final_pct": None,
                    "brix_ini": None,
                    "densidad_ini": None,
                    "rendimiento_uva_pct": None,
                },
                "balance": {
                    "ok": ok_balance,
                    "tolerance": tolerance,
                    "lts_origenes": lts_origenes,
                    "lts_destino": lts_destino,
                    "lts_borra": 0.0,
                    "lts_merma": 0.0,
                    "lts_otros_uso": 0.0,
                    "ajuste_lts": 0.0,
                    "lts_destinos_finales": total_dest_final,
                },
                "origenes": nodes,
                "timeline": timeline if "timeline" in include_set else [],
                "destinos": [] if "destinos" in include_set else [],
            }
            return JSONResponse(resp)

    except HTTPException:
        raise
    except Exception as e:
        try:
            import traceback
            print("\n[TRAZA][ERROR] >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            print(traceback.format_exc())
            print("[TRAZA][ERROR] <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"DB_ERROR: {e}")
