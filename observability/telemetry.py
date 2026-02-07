"""
Telemetry middleware for MoSPI MCP Server.

This middleware captures request metadata and tool execution traces with
defensive defaults:
1) input/output payload tracing is disabled by default
2) sensitive keys are redacted before serialization
3) forwarded IP headers are only trusted behind configured proxies
"""

from __future__ import annotations

import contextlib
import ipaddress
import json
import logging
import os
import sys
from typing import Any

from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.telemetry import get_tracer

logger = logging.getLogger(__name__)

DEFAULT_MAX_ATTRIBUTE_SIZE = 4096
DEFAULT_REDACT_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "access_token",
    "refresh_token",
    "secret",
    "password",
    "passcode",
    "x-api-key",
}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


def _env_csv(name: str) -> set[str]:
    raw = os.getenv(name, "")
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def truncate_json(value: Any, max_size: int = DEFAULT_MAX_ATTRIBUTE_SIZE) -> tuple[str, int]:
    """
    Serialize value to JSON and truncate if necessary.

    Returns:
        Tuple (serialized_or_truncated, original_size_bytes)
    """
    try:
        serialized = json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        serialized = str(value)

    original_size = len(serialized.encode("utf-8"))
    if original_size > max_size:
        truncated = serialized[: max_size - 50] + f"... [truncated, full size: {original_size} bytes]"
        return truncated, original_size
    return serialized, original_size


def redact_sensitive(value: Any, sensitive_keys: set[str]) -> Any:
    """
    Recursively redact known sensitive keys in dict/list structures.
    """
    if isinstance(value, dict):
        redacted: dict[str, Any] = {}
        for key, item in value.items():
            if str(key).lower() in sensitive_keys:
                redacted[key] = "[REDACTED]"
            else:
                redacted[key] = redact_sensitive(item, sensitive_keys)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive(item, sensitive_keys) for item in value]
    return value


def _is_trusted_proxy(
    peer_ip: str | None,
    trusted_proxy_ips: set[str],
    trusted_proxy_cidrs: tuple[ipaddress._BaseNetwork, ...],  # type: ignore[attr-defined]
) -> bool:
    if not peer_ip:
        return False
    if peer_ip in trusted_proxy_ips:
        return True
    try:
        parsed_ip = ipaddress.ip_address(peer_ip)
    except ValueError:
        return False
    return any(parsed_ip in cidr for cidr in trusted_proxy_cidrs)


def extract_client_ip(
    headers: dict[str, str],
    peer_ip: str | None,
    trusted_proxy_ips: set[str],
    trusted_proxy_cidrs: tuple[ipaddress._BaseNetwork, ...],  # type: ignore[attr-defined]
) -> tuple[str, str]:
    """
    Resolve client IP with trust boundaries.

    Returns:
        Tuple (client_ip, source) where source is one of: direct, x-forwarded-for,
        x-real-ip, unknown.
    """
    if _is_trusted_proxy(peer_ip, trusted_proxy_ips, trusted_proxy_cidrs):
        x_forwarded_for = headers.get("x-forwarded-for", "")
        if x_forwarded_for:
            return x_forwarded_for.split(",")[0].strip(), "x-forwarded-for"

        x_real_ip = headers.get("x-real-ip", "")
        if x_real_ip:
            return x_real_ip.strip(), "x-real-ip"

    if peer_ip:
        return peer_ip, "direct"
    return "unknown", "unknown"


class TelemetryMiddleware(Middleware):
    """
    FastMCP middleware that captures telemetry data in OpenTelemetry spans.
    """

    def __init__(self) -> None:
        super().__init__()
        self._tracer = get_tracer()
        self._max_attr_size = _env_int("TELEMETRY_MAX_ATTR_BYTES", DEFAULT_MAX_ATTRIBUTE_SIZE)
        self._capture_input = _env_bool("TELEMETRY_CAPTURE_TOOL_INPUT", False)
        self._capture_output = _env_bool("TELEMETRY_CAPTURE_TOOL_OUTPUT", False)
        self._log_full_output = _env_bool("TELEMETRY_LOG_FULL_OUTPUT", False)
        self._sensitive_keys = DEFAULT_REDACT_KEYS | {item.lower() for item in _env_csv("TELEMETRY_REDACT_KEYS")}
        self._trusted_proxy_ips = _env_csv("TRUSTED_PROXY_IPS")
        cidr_values = []
        for cidr in _env_csv("TRUSTED_PROXY_CIDRS"):
            with contextlib.suppress(ValueError):
                cidr_values.append(ipaddress.ip_network(cidr, strict=False))
        self._trusted_proxy_cidrs = tuple(cidr_values)

    async def on_call_tool(self, context: MiddlewareContext, call_next):
        """Hook that intercepts all tool calls."""
        tool_name = getattr(context.message, "name", "unknown")
        tool_args = getattr(context.message, "arguments", None)

        with self._tracer.start_as_current_span(f"tool.{tool_name}") as span:
            span.set_attribute("tool.name", tool_name)

            if self._capture_input and tool_args is not None:
                safe_args = redact_sensitive(tool_args, self._sensitive_keys)
                input_str, _ = truncate_json(safe_args, self._max_attr_size)
                span.set_attribute("tool.input", input_str)

            self._add_client_info_to_span(context, span)
            result = await call_next(context)

            if self._capture_output:
                output_data = getattr(result, "structured_content", result)
                if output_data is not None:
                    safe_output = redact_sensitive(output_data, self._sensitive_keys)
                    output_str, output_size = truncate_json(safe_output, self._max_attr_size)
                    span.set_attribute("tool.output", output_str)
                    span.set_attribute("tool.output_size", output_size)

                    if self._log_full_output:
                        try:
                            full_output = json.dumps(safe_output, default=str, ensure_ascii=False)
                        except (TypeError, ValueError):
                            full_output = str(safe_output)
                        print(
                            f"[TELEMETRY] Output ({output_size} bytes): {full_output}",
                            file=sys.stderr,
                        )

        return result

    def _add_client_info_to_span(self, context: MiddlewareContext, span) -> None:
        """Extract and add client metadata attributes to the span."""
        try:
            request = None
            fastmcp_ctx = context.fastmcp_context
            if fastmcp_ctx and fastmcp_ctx.request_context and fastmcp_ctx.request_context.request:
                request = fastmcp_ctx.request_context.request
            else:
                with contextlib.suppress(RuntimeError):
                    request = get_http_request()

            if request is None:
                return

            headers_dict = {k.lower(): v for k, v in request.headers.items()}
            peer_ip = None
            if getattr(request, "client", None) and getattr(request.client, "host", None):
                peer_ip = request.client.host

            client_ip, source = extract_client_ip(
                headers_dict,
                peer_ip=peer_ip,
                trusted_proxy_ips=self._trusted_proxy_ips,
                trusted_proxy_cidrs=self._trusted_proxy_cidrs,
            )
            span.set_attribute("client.ip", client_ip)
            span.set_attribute("client.ip_source", source)
            span.set_attribute("client.user_agent", headers_dict.get("user-agent", "unknown"))
        except Exception as exc:
            logger.debug("Failed to capture request telemetry fields: %s", exc)
