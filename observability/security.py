"""
Security middleware for MoSPI MCP Server.

Includes:
1) API-key based request authentication for HTTP transports
2) In-memory per-client rate limiting
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import ipaddress
import os
import time
from collections import defaultdict, deque

from fastmcp.exceptions import AuthorizationError
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import Middleware, MiddlewareContext

from observability.telemetry import extract_client_ip


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
        return value if value >= 0 else default
    except ValueError:
        return default


def _env_csv(name: str) -> set[str]:
    raw = os.getenv(name, "")
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def _build_trusted_proxy_cidrs() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    cidrs: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for cidr in _env_csv("TRUSTED_PROXY_CIDRS"):
        with contextlib.suppress(ValueError):
            cidrs.append(ipaddress.ip_network(cidr, strict=False))
    return tuple(cidrs)


class AuthMiddleware(Middleware):
    """
    Optional API-key authentication for HTTP requests.

    Configuration:
    1) MCP_AUTH_MODE: required | disabled (default: required)
    2) MCP_API_KEYS: comma-separated API keys (required when MCP_AUTH_MODE=required)
    """

    def __init__(self) -> None:
        super().__init__()
        self._auth_mode = os.getenv("MCP_AUTH_MODE", "required").strip().lower()
        if self._auth_mode not in {"required", "disabled"}:
            raise ValueError("MCP_AUTH_MODE must be 'required' or 'disabled'")

        self._api_keys = _env_csv("MCP_API_KEYS")
        if self._auth_mode == "required" and not self._api_keys:
            raise ValueError(
                "MCP_AUTH_MODE is 'required' but MCP_API_KEYS is empty. "
                "Set MCP_API_KEYS or explicitly set MCP_AUTH_MODE=disabled for local-only use."
            )

        self._allow_initialize_without_auth = _env_bool("MCP_ALLOW_INIT_WITHOUT_AUTH", False)

    def _extract_token(self, headers: dict[str, str]) -> str | None:
        api_key = headers.get("x-api-key")
        if api_key:
            return api_key.strip()

        auth_header = headers.get("authorization", "")
        if auth_header.lower().startswith("bearer "):
            return auth_header[7:].strip()
        return None

    def _is_authorized(self, headers: dict[str, str]) -> bool:
        if self._auth_mode == "disabled":
            return True
        token = self._extract_token(headers)
        if not token:
            return False
        return any(hmac.compare_digest(token, key) for key in self._api_keys)

    async def on_request(self, context: MiddlewareContext, call_next):
        if self._auth_mode == "disabled":
            return await call_next(context)

        if self._allow_initialize_without_auth and context.method == "initialize":
            return await call_next(context)

        with contextlib.suppress(RuntimeError):
            request = get_http_request()
            headers = {k.lower(): v for k, v in request.headers.items()}
            if not self._is_authorized(headers):
                raise AuthorizationError("Unauthorized request. Provide a valid API key.")

        return await call_next(context)


class RateLimitMiddleware(Middleware):
    """
    Basic in-memory rate limiter.

    Configuration:
    1) MCP_RATE_LIMIT_PER_MINUTE: allowed requests per client IP per minute (default: 120)
       Use 0 to disable.
    """

    def __init__(self) -> None:
        super().__init__()
        self._limit_per_minute = _env_int("MCP_RATE_LIMIT_PER_MINUTE", 120)
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._trusted_proxy_ips = _env_csv("TRUSTED_PROXY_IPS")
        self._trusted_proxy_cidrs = _build_trusted_proxy_cidrs()

    async def on_request(self, context: MiddlewareContext, call_next):
        if self._limit_per_minute == 0:
            return await call_next(context)

        with contextlib.suppress(RuntimeError):
            request = get_http_request()
            headers = {k.lower(): v for k, v in request.headers.items()}
            peer_ip = getattr(getattr(request, "client", None), "host", None)
            client_ip, _ = extract_client_ip(
                headers=headers,
                peer_ip=peer_ip,
                trusted_proxy_ips=self._trusted_proxy_ips,
                trusted_proxy_cidrs=self._trusted_proxy_cidrs,
            )
            await self._check_limit(client_ip)

        return await call_next(context)

    async def _check_limit(self, client_ip: str) -> None:
        now = time.monotonic()
        window_start = now - 60.0
        async with self._lock:
            events = self._events[client_ip]
            while events and events[0] < window_start:
                events.popleft()

            if len(events) >= self._limit_per_minute:
                raise AuthorizationError(
                    f"Rate limit exceeded: {self._limit_per_minute} requests/minute for client '{client_ip}'."
                )

            events.append(now)
