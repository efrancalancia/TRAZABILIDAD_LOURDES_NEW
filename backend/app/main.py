from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.v1.health import router as health_router
from .api.v1.composicion import router as composicion_router
from .api.v1.trazabilidad import router as trazabilidad_router  # NUEVO



app = FastAPI(
    title="TRAZABILIDAD LOURDES API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# CORS (ajustar orígenes en prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rutas v1
app.include_router(health_router, prefix="/api")
app.include_router(composicion_router, prefix="/api")
app.include_router(trazabilidad_router, prefix="/api")
app.include_router(trazabilidad_router, prefix="/api/trazabilidad", tags=["Trazabilidad"])  # <-- NUEVO
