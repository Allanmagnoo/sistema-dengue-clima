import os
import sys
from pathlib import Path
import pytest
import requests

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from src.extractors.bulk_municipios_extractor import BulkMunicipiosExtractor


class MockResponse:
    def __init__(self, text: str, status_code: int = 200, headers=None):
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


def sample_csv():
    return (
        "data_iniSE,SE,casos,casos_est,casos_est_min,casos_est_max\n"
        "2024-01-01,1,10,10.5,9.0,12.0\n"
        "2024-01-08,2,5,6.0,3.0,8.0\n"
    )


def test_connection_config_headers():
    ext = BulkMunicipiosExtractor(auth_token="abc123")
    assert ext.session.headers.get("Authorization") == "Bearer abc123"


def test_build_params_merge_defaults():
    ext = BulkMunicipiosExtractor(default_params={"extra": "x"})
    p = ext._build_params(1234567, 2024)
    assert p["geocode"] == 1234567 and p["ey_start"] == 2024
    assert p["extra"] == "x"


def test_parse_and_validate_records():
    ext = BulkMunicipiosExtractor()
    records = ext._parse_csv_records(sample_csv())
    assert len(records) == 2
    ok = ext._validate_and_cache(records, 1234567, 2024)
    assert ok is True
    assert (1234567, 2024) in ext.temp_store


def test_request_csv_with_429_retry(monkeypatch):
    ext = BulkMunicipiosExtractor(backoff_factor=0.01)
    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        if calls["n"] == 1:
            return MockResponse("", status_code=429, headers={"Retry-After": "0.01"})
        return MockResponse(sample_csv(), status_code=200)

    monkeypatch.setattr(ext.session, "get", fake_get)
    csv_text = ext._request_csv(1234567, 2024)
    assert csv_text is not None
    assert ext.metrics["requests"] == 1
    assert ext.metrics["latency_ms_min"] is not None


def test_process_task_saves_file(tmp_path, monkeypatch):
    ext = BulkMunicipiosExtractor()
    ext.connector.output_path = tmp_path / "bronze" / "infodengue" / "municipios"

    def fake_get(url, params=None, timeout=None):
        return MockResponse(sample_csv(), status_code=200)

    monkeypatch.setattr(ext.session, "get", fake_get)

    municipio = {"geocode": 1234567, "nome": "Teste"}
    year = 2024
    total_tasks = 1
    ext.process_task((municipio, year, total_tasks))

    out_file = ext.connector.output_path / f"disease=dengue" / f"year={year}" / f"{municipio['geocode']}.csv"
    assert out_file.exists()
    assert ext.success_count == 1


def test_skip_existing_file(tmp_path, monkeypatch):
    ext = BulkMunicipiosExtractor()
    ext.connector.output_path = tmp_path / "bronze" / "infodengue" / "municipios"
    year = 2024
    geocode = 1234567
    out_file = ext.connector.output_path / f"disease=dengue" / f"year={year}" / f"{geocode}.csv"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text("a,b\n1,2\n")

    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        calls["n"] += 1
        return MockResponse(sample_csv(), status_code=200)

    monkeypatch.setattr(ext.session, "get", fake_get)

    municipio = {"geocode": geocode, "nome": "Teste"}
    ext.process_task((municipio, year, 1))
    assert ext.skip_count == 1
    assert calls["n"] == 0

