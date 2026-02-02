# frontend/streamlit_app/pages/2_Reporte_Trazabilidad.py
import os
import json
from typing import Dict, List, Optional, DefaultDict
from collections import defaultdict

import httpx
import streamlit as st
from datetime import datetime

st.set_page_config(page_title="Reporte de Trazabilidad", page_icon="🧭", layout="wide")

# Configuración: URL base del backend
BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
TRACE_URL_TMPL = f"{BACKEND_BASE_URL}/api/trazabilidad/lote/{{c_lote}}"

def _fmt_date_iso(iso_str: str | None) -> str:
    """Convierte 'YYYY-MM-DD' o 'YYYY-MM-DDTHH:MM:SS' a 'dd-mm-yyyy'."""
    if not iso_str:
        return "—"
    try:
        s = str(iso_str).replace("Z", "")
        # usa sólo la parte de fecha si viene con hora
        s = s.split("T")[0]
        dt = datetime.fromisoformat(s)
        return dt.strftime("%d-%m-%Y")
    except Exception:
        return "—"

# ---------- Utilidades ----------
def fmt(x, nd=3):
    if x is None:
        return "—"
    try:
        return f"{float(x):,.{nd}f}"
    except Exception:
        return str(x)

def pct(x, nd=1):
    if x is None:
        return "—"
    try:
        return f"{float(x):.{nd}f}%"
    except Exception:
        return str(x)

def safe_get(d: dict, path: List[str], default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

# ---------- UI: Encabezado ----------
st.title("🧭 Reporte de Trazabilidad (por C_LOTE)")
st.caption(f"Backend: {BACKEND_BASE_URL}")

with st.container():
    col1, col2, col3, col4 = st.columns([1.4, 1.0, 1.0, 1.2])
    with col1:
        c_lote = st.text_input("C_LOTE a trazar", placeholder="Ej.: 28200001502002").strip()
    with col2:
        max_depth = st.number_input("Profundidad (niveles)", min_value=1, max_value=50, value=10, step=1)
    with col3:
        tolerance_pct = st.number_input("Tolerancia balance (%)", min_value=0.0, max_value=5.0, value=0.5, step=0.1)
    with col4:
        includes = st.multiselect("Incluir secciones", ["timeline", "destinos"], default=[])

    c1, c2 = st.columns([1, 4])
    with c1:
        run_btn = st.button("🔎 Consultar", type="primary", use_container_width=True)
    with c2:
        show_raw = st.checkbox("Ver JSON bruto (debug)")

st.divider()

# ---------- Llamada a la API ----------
@st.cache_data(show_spinner=False, ttl=60)
def fetch_trace(c_lote: str, max_depth: int, tolerance: float, includes: List[str]) -> dict:
    params = {
        "max_depth": max_depth,
        "tolerance": tolerance,
    }
    if includes:
        params["include"] = ",".join(includes)
    url = TRACE_URL_TMPL.format(c_lote=c_lote)
    with httpx.Client(timeout=30.0) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()

def badge_ok(ok: bool) -> str:
    return "🟢 OK" if ok else "🔴 Revisar"

def build_tree(nodes: List[dict]) -> Dict[str, List[dict]]:
    """
    Organiza los nodos por parent_id para recorrer jerárquicamente.
    node_id único, parent_id puede ser None.
    """
    index: Dict[str, dict] = {n["node_id"]: n for n in nodes}
    children: DefaultDict[str, List[dict]] = defaultdict(list)
    roots: List[dict] = []
    for n in nodes:
        pid = n.get("parent_id")
        if pid and pid in index:
            children[pid].append(n)
        else:
            roots.append(n)
    # Ordenar por nivel/fecha dentro de cada lista de hijos (si hay fecha)
    def key_fn(n):
        return (n.get("nivel", 0), n.get("fecha") or "")
    for k in list(children.keys()):
        children[k] = sorted(children[k], key=key_fn)
    roots = sorted(roots, key=key_fn)
    return {"roots": roots, "children": children}

def render_node_line(n: dict, indent: int = 0):
    # Línea compacta por nodo
    pad = "&nbsp;" * (indent * 4)
    tipo = n.get("tipo", "—")
    fecha = n.get("fecha", "—")
    ot = n.get("ot") or "—"
    tk = f'{n.get("tk_origen","—")} → {n.get("tk_destino","—")}'
    l_in = fmt(n.get("lts_in"))
    l_out = fmt(n.get("lts_out"))
    merma = fmt(n.get("merma_lts"))
    borra = fmt(n.get("borra_lts"))
    otros = fmt(n.get("otros_uso_lts"))
    contrib = f'{n.get("contrib_pct"):.1f}%' if n.get("contrib_pct") is not None else "—"
    guia = n.get("guia") or "—"
    fel = n.get("fel") or "—"
    obs = n.get("observacion") or ""
    line = (
        f"{pad}• <b>{tipo}</b> — <i>{fecha}</i> — OT: <code>{ot}</code> — TK: <code>{tk}</code><br>"
        f"{pad}&nbsp;&nbsp;&nbsp;Lts in/out: <b>{l_in}</b>/<b>{l_out}</b> · Merma/Borra/Otros: {merma}/{borra}/{otros} · Contrib: {contrib} · Guía: {guia} · FEL: {fel}"
    )
    if obs:
        line += f"<br>{pad}<span style='color:#666;'>Obs: {obs}</span>"
    st.markdown(line, unsafe_allow_html=True)

def render_tree(nodes: List[dict]):
    tree = build_tree(nodes)
    roots = tree["roots"]
    children = tree["children"]

    if not roots:
        st.info("Sin nodos de trazabilidad para mostrar.")
        return

    # Render recursivo
    def walk(node: dict, level: int = 0):
        render_node_line(node, indent=level)
        for ch in children.get(node["node_id"], []):
            walk(ch, level + 1)

    for r in roots:
        # 🔧 FIX: corregido el f-string (había una comilla extra en r.get("nivel", 0))
        exp_label = (
            f'Nivel {r.get("nivel", 0)} · '
            f'{r.get("tipo", "") or "—"} · '
            f'{r.get("fecha", "—")} · '
            f'TK {r.get("tk_origen","—")}→{r.get("tk_destino","—")}'
        )
        with st.expander(exp_label, expanded=(r.get("nivel", 0) == 0)):
            walk(r, level=0)

# ---------- Ejecución ----------
if run_btn:
    if not c_lote:
        st.warning("Ingresa un **C_LOTE** para consultar.")
        st.stop()

    try:
        data = fetch_trace(
            c_lote=c_lote,
            max_depth=int(max_depth),
            tolerance=float(tolerance_pct) / 100.0,
            includes=includes,
        )
    except httpx.HTTPStatusError as he:
        st.error(f"❌ Error HTTP {he.response.status_code}: {he.response.text[:300]}")
        st.stop()
    except Exception as e:
        st.error(f"❌ Error de conexión: {e}")
        st.stop()

    if show_raw:
        with st.expander("JSON bruto de respuesta", expanded=False):
            st.code(json.dumps(data, ensure_ascii=False, indent=2), language="json")

    # ---------- Sección: Encabezado + KPIs ----------
    #ident = data.get("identificacion", {}) or {}
    ident = data.get("identificacion", {}) if isinstance(data, dict) else {}
    kpis = data.get("kpis", {}) or {}
    balance = data.get("balance", {}) or {}
    origenes = data.get("origenes", []) or []

    st.subheader("🔖 Identificación")
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.1, 1.1, 1.0, 1.0])

    c1.metric("C_LOTE", ident.get("c_lote", "—"))
    c2.metric("Producto", ident.get("producto", "—"))

    # LOTE (mostrar D_LOTE que viene en identificacion.tanque_actual)
    c3.metric("LOTE", ident.get("tanque_actual", "—"))

    # Fechas en dd-mm-yyyy
    c4.metric("Fecha inicio", _fmt_date_iso(ident.get("fecha_inicio")))
    c5.metric("Fecha fin", _fmt_date_iso(ident.get("fecha_fin")))    

    st.subheader("📊 KPIs")
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Lts destino", fmt(kpis.get("lts_destino")))
    k2.metric("# Orígenes (nodos)", f"{len(origenes):,}")
    k3.metric("% Rdto. final", pct(kpis.get("rendimiento_final_pct")))
    k4.metric("Brix ini/fin", f'{fmt(kpis.get("brix_ini"),1)} / {fmt(kpis.get("brix_fin"),1)}')
    k5.metric("Densidad ini/fin", f'{fmt(kpis.get("densidad_ini"),3)} / {fmt(kpis.get("densidad_fin"),3)}')
    k6.metric("% Rdto. uva", pct(kpis.get("rendimiento_uva_pct")))

    # ---------- Sección: Balance de masas ----------
    st.subheader("⚖️ Balance de masas")
    tol = balance.get("tolerance", 0.005)
    ok = bool(balance.get("ok", False))
    b1, b2 = st.columns([1.0, 2.5])
    with b1:
        st.markdown(f"**Estado:** {badge_ok(ok)} &nbsp;&nbsp; | &nbsp;&nbsp; **Tolerancia:** ±{tol*100:.2f}%")
        st.write("")
        st.write("**Conciliación:**")
        st.markdown(
            f"- Σ Orígenes: **{fmt(balance.get('lts_origenes'))} Lts**  \n"
            f"- Destino: **{fmt(balance.get('lts_destino'))} Lts**  \n"
            f"- Pérdidas (Merma/Borra/Otros): **{fmt(balance.get('lts_merma'))}/{fmt(balance.get('lts_borra'))}/{fmt(balance.get('lts_otros_uso'))} Lts**  \n"
            f"- Ajuste: **{fmt(balance.get('ajuste_lts'))} Lts**"
        )
    with b2:
        bal_rows = [
            {"Concepto": "Σ Orígenes", "Litros": balance.get("lts_origenes")},
            {"Concepto": "Destino", "Litros": balance.get("lts_destino")},
            {"Concepto": "Borra", "Litros": balance.get("lts_borra")},
            {"Concepto": "Merma", "Litros": balance.get("lts_merma")},
            {"Concepto": "Otros usos", "Litros": balance.get("lts_otros_uso")},
            {"Concepto": "Ajuste", "Litros": balance.get("ajuste_lts")},
        ]
        st.table([{ "Concepto": r["Concepto"], "Litros": fmt(r["Litros"]) } for r in bal_rows])

    # ---------- Sección: Orígenes (árbol) ----------
    st.subheader("🌳 Orígenes hacia atrás")
    render_tree(origenes)

    # ---------- Sección: Timeline ----------
    if "timeline" in includes and data.get("timeline"):
        st.subheader("🗓️ Línea de tiempo")
        try:
            tl = sorted(data["timeline"], key=lambda x: x.get("fecha") or "")
        except Exception:
            tl = data["timeline"]
        st.table([
            {
                "Fecha": ev.get("fecha", "—"),
                "Tipo": ev.get("tipo", "—"),
                "OT": ev.get("ot", "—") or "—",
                "Volumen (Lts)": fmt(ev.get("volumen_lts")),
                "Nota": ev.get("nota", "") or ""
            }
            for ev in tl
        ])

    # ---------- Sección: Destinos ----------
    if "destinos" in includes and data.get("destinos"):
        st.subheader("📦 Destinos / Despachos")
        dst = data["destinos"]
        st.table([
            {
                "Fecha": d.get("fecha", "—"),
                "Destino": d.get("destino", "—"),
                "Volumen (Lts)": fmt(d.get("volumen_lts")),
                "Guía": d.get("guia", "—") or "—",
                "FEL": d.get("fel", "—") or "—",
            }
            for d in dst
        ])
