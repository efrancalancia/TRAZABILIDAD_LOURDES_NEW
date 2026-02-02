from typing import Dict, List, Optional
from pydantic import BaseModel, Field


# ===== HEALTH =====
class HealthResponse(BaseModel):
    status: str = Field(description="Estado global: ok")
    time: str = Field(description="Timestamp UTC ISO-8601 (Z)")


class ErrorItem(BaseModel):
    code: str
    message: str


class DeepHealthResponse(HealthResponse):
    version: str = Field(default="0.1.0")
    dependencies: Dict[str, str] = Field(default_factory=dict, description="oracle: ok/down")
    errors: List[ErrorItem] = Field(default_factory=list)


# ===== COMPOSICIÓN (SSE request) =====
class ComposicionRequest(BaseModel):
    fecha_desde: str = Field(..., description="Fecha inicio (YYYY-MM-DD)")
    fecha_hasta: str = Field(..., description="Fecha fin (YYYY-MM-DD)")


# ===== TRAZABILIDAD (response por C_LOTE) =====
class TraceIdentification(BaseModel):
    c_lote: str
    producto: Optional[str] = None
    fecha_inicio: Optional[str] = None
    fecha_fin: Optional[str] = None
    tanque_actual: Optional[str] = None
    origen_consulta: str = "C_LOTE"


class TraceKPIs(BaseModel):
    lts_destino: Optional[float] = None
    kg_destino: Optional[float] = None
    rendimiento_uva_pct: Optional[float] = None
    rendimiento_final_pct: Optional[float] = None
    brix_ini: Optional[float] = None
    brix_fin: Optional[float] = None
    densidad_ini: Optional[float] = None
    densidad_fin: Optional[float] = None


class TraceBalance(BaseModel):
    lts_origenes: float = 0.0
    lts_destino: float = 0.0
    lts_borra: float = 0.0
    lts_merma: float = 0.0
    lts_otros_uso: float = 0.0
    ajuste_lts: float = 0.0
    ok: bool = True
    tolerance: float = 0.005


class TraceOriginNode(BaseModel):
    node_id: str
    parent_id: Optional[str] = None
    nivel: int
    tipo: str
    fecha: Optional[str] = None
    ot: Optional[str] = None
    tk_origen: Optional[str] = None
    tk_destino: Optional[str] = None
    lts_in: Optional[float] = None
    lts_out: Optional[float] = None
    kg_in: Optional[float] = None
    kg_out: Optional[float] = None
    merma_lts: Optional[float] = None
    borra_lts: Optional[float] = None
    otros_uso_lts: Optional[float] = None
    contrib_pct: Optional[float] = None
    guia: Optional[str] = None
    fel: Optional[str] = None
    observacion: Optional[str] = None


class TraceTimelineEvent(BaseModel):
    fecha: str
    tipo: str
    ot: Optional[str] = None
    volumen_lts: Optional[float] = None
    nota: Optional[str] = None


class TraceDestination(BaseModel):
    fecha: str
    destino: str
    volumen_lts: Optional[float] = None
    guia: Optional[str] = None
    fel: Optional[str] = None


class TraceResponse(BaseModel):
    identificacion: TraceIdentification
    kpis: TraceKPIs
    balance: TraceBalance
    origenes: List[TraceOriginNode]
    timeline: Optional[List[TraceTimelineEvent]] = None
    destinos: Optional[List[TraceDestination]] = None
