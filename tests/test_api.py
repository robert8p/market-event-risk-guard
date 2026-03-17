"""Tests for API endpoints."""
import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

class TestHealth:
    def test_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] in ("healthy", "degraded", "starting")
        assert "sources" in data

class TestEvents:
    def test_returns_200(self, client):
        r = client.get("/api/events")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data["events"], list)

    def test_hours_filter(self, client):
        r = client.get("/api/events?hours=4")
        assert r.status_code == 200
        assert r.json()["window_hours"] == 4

    def test_asset_class_filter(self, client):
        assert client.get("/api/events?asset_class=crypto").status_code == 200

class TestSummary:
    def test_returns_200(self, client):
        r = client.get("/api/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["verdict"] in ("Yes", "Caution", "No")

class TestDashboard:
    def test_homepage(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert "Market Event Risk Guard" in r.text
