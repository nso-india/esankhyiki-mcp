"""Regression checks for CPI data errors and pagination limits (issue #67)."""

import json

import pytest
import requests
from fastmcp import Client

from mospi.client import mospi
from mospi_server import get_data, get_metadata, mcp


def response(status, payload):
    result = requests.Response()
    result.status_code = status
    result.url = "https://api.mospi.gov.in/api/cpi/getCPIData"
    result._content = json.dumps(payload).encode()
    return result


@pytest.mark.parametrize(
    "status,payload,expected",
    [
        (400, {"success": False, "message": "Limit cannot be greater than 200"},
         "Limit cannot be greater than 200"),
        (200, {"error": "Limit parameter too large. Maximum allowed is 100."},
         "Limit parameter too large. Maximum allowed is 100."),
    ],
)
def test_get_data_surfaces_upstream_reason(monkeypatch, status, payload, expected):
    monkeypatch.setattr(mospi.session, "get", lambda *args, **kwargs: response(status, payload))

    result = get_data("CPI_ITEM", {
        "base_year": "2012", "series": "Current", "item_code": 137, "limit": 200,
    })

    assert result["error"] == expected
    assert result["upstream_reason"] == expected
    assert result["http_status"] == status
    assert "troubleshooting" not in result
    assert "suggestion" not in result


def test_get_data_keeps_generic_fallback_without_upstream_reason(monkeypatch):
    monkeypatch.setattr(mospi.session, "get", lambda *args, **kwargs: response(400, {}))

    result = get_data("CPI_GROUP", {"base_year": "2012", "series": "Current"})

    assert "troubleshooting" in result
    assert "suggestion" in result


@pytest.mark.parametrize("limit", [1, 200, 500])
def test_cpi_2024_rejects_out_of_range_limit_before_request(monkeypatch, limit):
    def unexpected_request(*args, **kwargs):
        pytest.fail("Invalid CPI limit reached the upstream API")

    monkeypatch.setattr(mospi.session, "get", unexpected_request)

    result = get_data("CPI_GROUP", {
        "base_year": "2024", "series": "Current", "limit": limit,
    })

    assert result["error"] == "For CPI base_year=2024, limit must be between 10 and 100."


def test_cpi_2024_accepts_limit_100(monkeypatch):
    monkeypatch.setattr(mospi.session, "get", lambda *args, **kwargs: response(200, {
        "statusCode": True, "message": "Data fetched successfully", "data": [1],
    }))

    result = get_data("CPI_GROUP", {
        "base_year": "2024", "series": "Current", "limit": 100,
    })

    assert result["data"] == [1]
    assert "error" not in result


def test_cpi_metadata_exposes_effective_limit(monkeypatch):
    monkeypatch.setattr(mospi, "get_cpi_filters", lambda **kwargs: {"filter_values": {}})

    result = get_metadata("CPI", base_year="2024")

    limit_param = next(param for param in result["api_params"] if param["name"] == "limit")
    assert "10-100" in limit_param["description"]
    assert "10 and 100" in result["parameter_notes"]


@pytest.mark.asyncio
async def test_mcp_tool_returns_upstream_reason(monkeypatch):
    monkeypatch.setattr(mospi.session, "get", lambda *args, **kwargs: response(200, {
        "error": "Limit parameter too large. Maximum allowed is 100.",
    }))

    async with Client(mcp) as client:
        result = await client.call_tool("get_data", {
            "dataset": "CPI_ITEM",
            "filters": {"base_year": "2024", "series": "Current", "item_code": 137,
                        "limit": 100},
        })

    payload = result.structured_content or json.loads(result.content[0].text)
    assert payload["error"] == "Limit parameter too large. Maximum allowed is 100."
    assert "troubleshooting" not in payload
