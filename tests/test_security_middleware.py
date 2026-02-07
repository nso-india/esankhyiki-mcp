import pytest
from fastmcp.exceptions import AuthorizationError

from observability.security import AuthMiddleware, RateLimitMiddleware


def test_auth_middleware_requires_keys_when_required(monkeypatch):
    monkeypatch.setenv("MCP_AUTH_MODE", "required")
    monkeypatch.delenv("MCP_API_KEYS", raising=False)
    with pytest.raises(ValueError):
        AuthMiddleware()


def test_auth_middleware_accepts_bearer_and_x_api_key(monkeypatch):
    monkeypatch.setenv("MCP_AUTH_MODE", "required")
    monkeypatch.setenv("MCP_API_KEYS", "key-1,key-2")
    middleware = AuthMiddleware()

    assert middleware._is_authorized({"authorization": "Bearer key-1"}) is True
    assert middleware._is_authorized({"x-api-key": "key-2"}) is True
    assert middleware._is_authorized({"authorization": "Bearer wrong"}) is False


@pytest.mark.asyncio
async def test_rate_limit_blocks_excess_requests(monkeypatch):
    monkeypatch.setenv("MCP_RATE_LIMIT_PER_MINUTE", "1")
    middleware = RateLimitMiddleware()

    await middleware._check_limit("127.0.0.1")
    with pytest.raises(AuthorizationError):
        await middleware._check_limit("127.0.0.1")
