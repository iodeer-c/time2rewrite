from fastapi.testclient import TestClient

from main import app


def test_root_returns_service_banner():
    client = TestClient(app)
    assert client.get("/").json() == {"message": "time-query-service"}
