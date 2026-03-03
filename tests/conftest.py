"""Shared pytest fixtures for MCP server health-check tests."""

import os

import pytest


SERVER_URL = os.environ.get("MCP_SERVER_URL")


@pytest.fixture(scope="session")
def mcp_target():
    """Return the FastMCP server object (in-process) or URL string (HTTP).

    - Default (no env var): imports ``mcp`` from ``mospi_server.py`` for in-process testing.
    - ``MCP_SERVER_URL=https://…``: returns the URL for HTTP testing against a deployed server.
    """
    if SERVER_URL:
        return SERVER_URL
    from mospi_server import mcp
    return mcp
