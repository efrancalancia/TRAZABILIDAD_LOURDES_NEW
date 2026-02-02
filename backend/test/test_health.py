from fastapi.testclient import TestClient
from backend.app.main import app

client = TestClient(app)

def test_health_liveness():
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert "time" in data

def test_health_readiness():
    # Readiness puede retornar 200 (ok) o 503 (down) según la config local.
    r = client.get("/api/health?deep=true")
    assert r.status_code in (200, 503)
    data = r.json()
    assert "status" in data
    assert "time" in data
    assert "version" in data
    assert "dependencies" in data
