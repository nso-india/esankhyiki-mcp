"""Exercise feedback through MCP and the authenticated Sheets HTTP request."""

import json
from datetime import datetime
from unittest.mock import Mock
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import pytest
import requests
from fastmcp import Client
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials

from mospi_server import mcp, service_account, submit_feedback


@pytest.fixture
def sheets(monkeypatch):
    monkeypatch.setenv("FEEDBACK_SPREADSHEET_ID", "test-sheet")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/test-key.json")
    load = Mock(return_value=Credentials(token="test-token"))
    monkeypatch.setattr(service_account.Credentials, "from_service_account_file", load)
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"updates":{"updatedRows":1}}'
    send = Mock(return_value=response)
    monkeypatch.setattr(requests.Session, "send", send)
    return load, send, response


async def test_feedback_saved_through_mcp(sheets):
    load, send, _ = sheets
    async with Client(mcp) as client:
        tool = next(t for t in await client.list_tools() if t.name == "submit_feedback")
        assert tool.annotations.readOnlyHint is False
        assert tool.annotations.destructiveHint is False
        assert tool.annotations.idempotentHint is False
        assert tool.annotations.openWorldHint is True
        result = await client.call_tool("submit_feedback", {
            "feedback": "  =unexpected result\nExpected CPI data.  ",
            "tool_name": "get_data", "dataset": "CPI",
        })

    saved = result.structured_content
    assert saved["status"] == "saved"
    UUID(saved["feedback_id"])
    load.assert_called_once_with(
        "/secrets/test-key.json", scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    send.assert_called_once()
    request = send.call_args.args[0]
    url = urlparse(request.url)
    assert request.method == "POST"
    assert url.netloc == "sheets.googleapis.com"
    assert url.path == "/v4/spreadsheets/test-sheet/values/Feedback!A:E:append"
    assert parse_qs(url.query) == {
        "valueInputOption": ["RAW"], "insertDataOption": ["INSERT_ROWS"],
    }
    assert request.headers["Authorization"] == "Bearer test-token"
    assert send.call_args.kwargs["timeout"] == 10
    row, = json.loads(request.body)["values"]
    assert row[0] == saved["feedback_id"]
    assert datetime.fromisoformat(row[1]).utcoffset().total_seconds() == 0
    assert row[2:] == ["get_data", "CPI", "=unexpected result\nExpected CPI data."]


@pytest.mark.parametrize("arguments", [
    {"feedback": ""},
    {"feedback": " "},
    {"feedback": "x" * 5001},
    {"feedback": "issue", "tool_name": "x" * 101},
    {"feedback": "issue", "dataset": "x" * 51},
])
async def test_invalid_feedback_does_not_write(sheets, arguments):
    _, send, _ = sheets
    async with Client(mcp) as client:
        result = await client.call_tool("submit_feedback", arguments, raise_on_error=False)
    assert result.is_error or "error" in result.structured_content
    send.assert_not_called()


@pytest.mark.parametrize("missing", ["FEEDBACK_SPREADSHEET_ID", "GOOGLE_APPLICATION_CREDENTIALS"])
def test_missing_configuration(sheets, monkeypatch, missing):
    load, send, _ = sheets
    monkeypatch.delenv(missing)
    assert "not configured" in submit_feedback("issue")["error"]
    load.assert_not_called()
    send.assert_not_called()


@pytest.mark.parametrize("error", [OSError("secret path"), ValueError("secret key")])
def test_bad_credentials_do_not_leak_details(sheets, error):
    load, send, _ = sheets
    load.side_effect = error
    result = submit_feedback("issue")
    assert "credentials could not be loaded" in result["error"]
    assert "secret" not in json.dumps(result)
    send.assert_not_called()


@pytest.mark.parametrize("failure", [
    requests.Timeout("secret token"),
    RefreshError("secret key"),
    403,
    429,
    500,
    b"not JSON",
    b'{"updates":{"updatedRows":0}}',
])
def test_failed_save_is_unconfirmed_and_not_retried(sheets, failure):
    _, send, response = sheets
    if isinstance(failure, Exception):
        send.side_effect = failure
    elif isinstance(failure, int):
        response.status_code = failure
    else:
        response._content = failure
    result = submit_feedback("issue")
    assert result["status"] == "unconfirmed"
    UUID(result["feedback_id"])
    assert "Do not retry automatically" in result["error"]
    assert "secret" not in json.dumps(result)
    send.assert_called_once()
