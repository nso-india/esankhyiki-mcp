import os

import pytest
from fastmcp.server.middleware import MiddlewareContext

from observability.telemetry import TelemetryMiddleware, extract_client_ip, redact_sensitive


def test_redact_sensitive_nested_payload():
    payload = {
        "authorization": "Bearer secret",
        "nested": {
            "token": "abc",
            "normal": "ok",
        },
    }
    redacted = redact_sensitive(payload, {"authorization", "token"})
    assert redacted["authorization"] == "[REDACTED]"
    assert redacted["nested"]["token"] == "[REDACTED]"
    assert redacted["nested"]["normal"] == "ok"


def test_extract_client_ip_trust_boundary():
    headers = {"x-forwarded-for": "198.51.100.25, 10.0.0.2"}
    client_ip, source = extract_client_ip(
        headers=headers,
        peer_ip="203.0.113.10",
        trusted_proxy_ips=set(),
        trusted_proxy_cidrs=(),
    )
    assert client_ip == "203.0.113.10"
    assert source == "direct"

    client_ip, source = extract_client_ip(
        headers=headers,
        peer_ip="203.0.113.10",
        trusted_proxy_ips={"203.0.113.10"},
        trusted_proxy_cidrs=(),
    )
    assert client_ip == "198.51.100.25"
    assert source == "x-forwarded-for"


@pytest.mark.asyncio
async def test_telemetry_middleware_no_nameerror(monkeypatch):
    monkeypatch.setenv("TELEMETRY_CAPTURE_TOOL_OUTPUT", "true")
    monkeypatch.setenv("TELEMETRY_LOG_FULL_OUTPUT", "true")

    middleware = TelemetryMiddleware()

    class Message:
        name = "demo_tool"
        arguments = {"token": "secret"}

    context = MiddlewareContext(message=Message(), method="tools/call")

    async def call_next(_):
        return {"result": "ok"}

    result = await middleware.on_call_tool(context, call_next)
    assert result == {"result": "ok"}
