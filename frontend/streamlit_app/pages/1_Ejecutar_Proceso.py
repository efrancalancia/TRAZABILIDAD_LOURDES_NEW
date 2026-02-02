import os
import json
import httpx
import streamlit as st
from datetime import date, timedelta

st.set_page_config(page_title="Ejecutar Proceso", page_icon="🛠️", layout="wide")

# Config: URL base del backend
BACKEND_BASE_URL = os.getenv("BACKEND_BASE_URL", "http://localhost:8000")
RUN_URL = f"{BACKEND_BASE_URL}/api/composicion/run"

st.title("🛠️ Ejecutar Proceso de Composición Enológica")

# Estado de la vista
if "running" not in st.session_state:
    st.session_state.running = False
if "logs" not in st.session_state:
    st.session_state.logs = []

col1, col2 = st.columns([1, 1])
with col1:
    f_desde = st.date_input("Fecha desde", value=date.today() - timedelta(days=30), format="YYYY-MM-DD")
with col2:
    f_hasta = st.date_input("Fecha hasta", value=date.today(), format="YYYY-MM-DD")

run_btn = st.button("▶️ Ejecutar", type="primary", disabled=st.session_state.running)

status_placeholder = st.empty()
logs_placeholder = st.empty()


def append_log(line: str):
    st.session_state.logs.append(line)
    logs_placeholder.code("\n".join(st.session_state.logs[-2000:]), language="text", wrap_lines=True)


def parse_sse_stream_line_mode(lines_iter):
    """
    Parser simple de SSE por líneas:
    - Junta pares event + data (JSON)
    - Emite dicts {'event':..., 'data':{...}}
    """
    event = None
    data_lines = []
    for line in lines_iter:  # 'line' es str
        if line is None:
            continue
        if line.startswith(":"):
            continue  # comentario keep-alive
        if line.startswith("event:"):
            event = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].strip())
        elif line.strip() == "":
            if event and data_lines:
                try:
                    payload = json.loads("\n".join(data_lines))
                except Exception:
                    payload = {"raw": "\n".join(data_lines)}
                yield {"event": event, "data": payload}
            event, data_lines = None, []
        else:
            # líneas no esperadas, se ignoran
            pass


if run_btn:
    if f_hasta < f_desde:
        st.error("⚠️ La fecha *hasta* no puede ser anterior a la fecha *desde*.")
    else:
        st.session_state.running = True
        st.session_state.logs = []
        status_placeholder.info("⏳ Ejecutando proceso... (logs en vivo)")

        payload = {"fecha_desde": f_desde.strftime("%Y-%m-%d"),
                   "fecha_hasta": f_hasta.strftime("%Y-%m-%d")}

        try:
            got_done = False
            with httpx.stream(
                "POST",
                RUN_URL,
                json=payload,
                headers={"Accept": "text/event-stream"},
                timeout=None,   # proceso largo
            ) as r:
                if r.status_code != 200:
                    st.error(f"❌ Error HTTP {r.status_code}: {r.text[:300]}")
                else:
                    for evt in parse_sse_stream_line_mode(r.iter_lines()):
                        ev = evt.get("event")
                        data = evt.get("data", {})
                        if ev == "log":
                            msg = data.get("msg", "")
                            ts = data.get("ts", "")
                            append_log(f"{ts} {msg}")
                        elif ev == "error":
                            append_log(f"[ERROR] {data}")
                            status_placeholder.error(f"❌ Proceso con error: {data.get('message','')}")
                            break
                        elif ev == "done":
                            got_done = True
                            append_log("[OK] Proceso finalizado.")
                            status_placeholder.success("✅ Proceso finalizado.")
                            break
        except Exception as e:
            # Tolerar cierre limpio del servidor después de 'done'
            if "incomplete chunked read" in str(e).lower() and 'got_done' in locals() and got_done:
                pass
            else:
                status_placeholder.error(f"❌ Error de conexión: {e}")
        finally:
            st.session_state.running = False

st.caption(f"Backend: {RUN_URL}")
st.caption("Los logs completos se guardan en disco (config `LOGS_OUT_DIR`, por defecto ./outputs/logs).")
