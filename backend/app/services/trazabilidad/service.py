# backend/app/services/trazabilidad/service.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
import re
from datetime import datetime

from ...core.config import settings
from ...models.schemas import (
    TraceResponse, TraceIdentification, TraceKPIs, TraceBalance,
    TraceOriginNode, TraceTimelineEvent, TraceDestination
)


OT_PATTERN = re.compile(r"(?:\bOT[:\s\-]*)(\d+)", re.IGNORECASE)


def _parse_ot(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = OT_PATTERN.search(text)
    return f"OT:{m.group(1)}" if m else None


def _round(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 3)


@dataclass
class TraceQuery:
    c_lote: str
    max_depth: int = 10
    include_timeline: bool = False
    include_destinos: bool = False
    tolerance: float = 0.005


class FakeTraceRepository:
    """
    Repositorio 'fake' para pruebas inmediatas.
    Genera una traza consistente y cercana a la realidad.
    """
    def trace_by_lote(self, q: TraceQuery) -> TraceResponse:
        # Datos de ejemplo (consistentes con el contrato)
        c_lote = q.c_lote
        ident = TraceIdentification(
            c_lote=c_lote,
            producto="Vino Tinto",
            fecha_inicio="2025-01-10",
            fecha_fin="2025-03-15",
            tanque_actual="LO364",
            origen_consulta="C_LOTE",
        )
        kpis = TraceKPIs(
            lts_destino=261735.0,
            kg_destino=0.0,
            rendimiento_uva_pct=72.3,
            rendimiento_final_pct=94.8,
            brix_ini=22.1,
            brix_fin=12.3,
            densidad_ini=1.095,
            densidad_fin=0.993,
        )

        origenes: List[TraceOriginNode] = [
            TraceOriginNode(
                node_id="N0", parent_id=None, nivel=0, tipo="DESTINO",
                fecha="2025-03-15", ot="OT:8700",
                tk_origen="LO200", tk_destino="LO364",
                lts_in=270000.0, lts_out=261735.0,
                kg_in=0.0, kg_out=0.0,
                merma_lts=1500.0, borra_lts=4200.0, otros_uso_lts=1200.0,
                contrib_pct=100.0,
                guia=None, fel=None, observacion="Transformación final. OT:8700"
            ),
            TraceOriginNode(
                node_id="N1", parent_id="N0", nivel=1, tipo="TRANSFORMACION",
                fecha="2025-03-10", ot="OT:8650",
                tk_origen="LO180", tk_destino="LO200",
                lts_in=180000.0, lts_out=175000.0,
                merma_lts=2000.0, borra_lts=2500.0, otros_uso_lts=500.0,
                contrib_pct=66.7,
            ),
            TraceOriginNode(
                node_id="N2", parent_id="N0", nivel=1, tipo="TRANSFORMACION",
                fecha="2025-03-10", ot="OT:8651",
                tk_origen="LO181", tk_destino="LO200",
                lts_in=90000.0, lts_out=88000.0,
                merma_lts=0.0, borra_lts=1700.0, otros_uso_lts=200.0,
                contrib_pct=33.3,
            ),
            TraceOriginNode(
                node_id="N3", parent_id="N1", nivel=2, tipo="DESCUBE",
                fecha="2025-02-15", ot="OT:8123",
                tk_origen="LO050", tk_destino="LO180",
                lts_in=32000.0, lts_out=31500.0,
            ),
            TraceOriginNode(
                node_id="N4", parent_id="N1", nivel=2, tipo="COMPRA",
                fecha="2025-02-05",
                tk_origen=None, tk_destino="LO180",
                lts_in=10000.0, lts_out=10000.0,
                guia="G-123", fel="14909"
            ),
        ]

        # Balance
        lts_origenes = 270000.0
        lts_destino = 261735.0
        lts_borra = 4200.0
        lts_merma = 1500.0
        lts_otros = 1200.0
        ajuste = lts_origenes - (lts_destino + lts_borra + lts_merma + lts_otros)
        ok = abs(ajuste) <= (lts_origenes * q.tolerance)

        balance = TraceBalance(
            lts_origenes=_round(lts_origenes),
            lts_destino=_round(lts_destino),
            lts_borra=_round(lts_borra),
            lts_merma=_round(lts_merma),
            lts_otros_uso=_round(lts_otros),
            ajuste_lts=_round(ajuste),
            ok=bool(ok),
            tolerance=q.tolerance,
        )

        timeline = None
        destinos = None
        if q.include_timeline:
            timeline = [
                TraceTimelineEvent(fecha="2025-01-12", tipo="COMPRA", ot=None, volumen_lts=10000.0, nota="Guía G-123"),
                TraceTimelineEvent(fecha="2025-01-20", tipo="DESCUBE", ot="OT:8123", volumen_lts=32000.0),
                TraceTimelineEvent(fecha="2025-03-10", tipo="TRANSFORMACION", ot="OT:8650", volumen_lts=175000.0),
                TraceTimelineEvent(fecha="2025-03-15", tipo="TRANSFORMACION", ot="OT:8700", volumen_lts=261735.0),
            ]
        if q.include_destinos:
            destinos = [
                TraceDestination(fecha="2025-03-20", destino="Envasado", volumen_lts=120000.0, guia="G-4567", fel="15852")
            ]

        return TraceResponse(
            identificacion=ident,
            kpis=kpis,
            balance=balance,
            origenes=origenes,
            timeline=timeline,
            destinos=destinos,
        )


class RealTraceRepository:
    """
    Esqueleto del repositorio real.
    Aquí conectaremos a Oracle y armaremos la traza por capas (nivel N -> N+1).
    """
    def trace_by_lote(self, q: TraceQuery) -> TraceResponse:
        raise NotImplementedError(
            "Repositorio real no implementado aún. Configura TRACE_MODE=fake (por defecto) "
            "o proporciona las consultas/orígenes de datos para habilitar el modo real."
        )


class TraceService:
    def __init__(self):
        mode = settings.trace_mode
        if mode == "real":
            self.repo = RealTraceRepository()
        else:
            self.repo = FakeTraceRepository()

    def trace_by_lote(self, c_lote: str, max_depth: int, include: List[str], tolerance: float) -> TraceResponse:
        q = TraceQuery(
            c_lote=c_lote,
            max_depth=max_depth if max_depth and max_depth > 0 else 10,
            include_timeline=("timeline" in include),
            include_destinos=("destinos" in include),
            tolerance=tolerance if tolerance is not None else 0.005,
        )
        return self.repo.trace_by_lote(q)
