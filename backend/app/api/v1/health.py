from datetime import datetime, timezone
from typing import List, Union

from fastapi import APIRouter, Response, status
from .. import v1  # noqa: F401  # asegura paquete
from ...models.schemas import HealthResponse, DeepHealthResponse, ErrorItem
from ...services import db

router = APIRouter(tags=["health"])

@router.get(
    "/health",
    response_model=Union[HealthResponse, DeepHealthResponse],
    summary="Liveness (simple) y Readiness (?deep=true) con detalles",
)
def health(response: Response, deep: bool = False) -> Union[HealthResponse, DeepHealthResponse]:
    """
    - Liveness (por defecto): no toca DB. Devuelve {status, time}.
    - Readiness (?deep=true): chequea Oracle y devuelve detalles (version, dependencies, errors).
    """
    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    response.headers["Cache-Control"] = "no-store"

    if not deep:
        # Liveness
        return HealthResponse(status="ok", time=now_utc)

    # Readiness
    errors: List[ErrorItem] = []
    oracle_ok, oracle_err = db.check_oracle_ready(timeout_seconds=3)

    if not oracle_ok:
        errors.append(ErrorItem(code="ORACLE_DOWN", message=oracle_err or "Oracle no disponible"))

    if errors:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return DeepHealthResponse(
            status="down",
            time=now_utc,
            version="0.1.0",
            dependencies={"oracle": "down"},
            errors=errors,
        )

    return DeepHealthResponse(
        status="ok",
        time=now_utc,
        version="0.1.0",
        dependencies={"oracle": "ok"},
        errors=[],
    )
