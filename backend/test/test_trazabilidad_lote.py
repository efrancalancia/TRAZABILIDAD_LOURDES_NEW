from fastapi.testclient import TestClient
from backend.app.main import app

client = TestClient(app)

def test_trazabilidad_lote_ok():
    r = client.get("/api/trazabilidad/lote/TEST123?include=timeline,destinos")
    assert r.status_code == 200
    data = r.json()
    assert data["identificacion"]["c_lote"] == "TEST123"
    assert "origenes" in data and len(data["origenes"]) >= 1
    assert "balance" in data and "ok" in data["balance"]

def test_trazabilidad_lote_bad_depth():
    r = client.get("/api/trazabilidad/lote/TEST123?max_depth=0")
    assert r.status_code == 422  # valida parámetro
