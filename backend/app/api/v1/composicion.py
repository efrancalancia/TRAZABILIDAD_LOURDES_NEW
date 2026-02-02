from datetime import datetime
from typing import Iterator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ...models.schemas import ComposicionRequest
from ...services.composicion.runner import stream_sse_logs

router = APIRouter(tags=["composicion"])


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Fecha inválida: '{s}'. Use formato YYYY-MM-DD.")


@router.post("/composicion/run", summary="Ejecutar proceso de composición (SSE)")
def run_composicion(payload: ComposicionRequest):
    # Validar solo el orden (se permite rango > 1 año)
    f_ini = _parse_date(payload.fecha_desde)
    f_fin = _parse_date(payload.fecha_hasta)
    if f_fin < f_ini:
        raise HTTPException(status_code=422, detail="fecha_hasta no puede ser anterior a fecha_desde.")

    def _event_stream() -> Iterator[str]:
        yield from stream_sse_logs(payload.fecha_desde, payload.fecha_hasta)

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
