"""MCP Server health-check tests — all 4 tools across all 19 datasets.

Uses FastMCP Client (in-process by default, HTTP via MCP_SERVER_URL env var).
Run:  pytest tests/ -v
"""

import asyncio
import json
import os

import pytest
from fastmcp import Client

# The MoSPI backend rate-limits rapid requests. In-process tests hit the real
# API with zero MCP overhead, so calls arrive much faster than via HTTP.
# A small delay between calls prevents transient 500s.
THROTTLE = float(os.environ.get("MCP_TEST_THROTTLE", "0.5"))


def parse_tool_result(result) -> dict:
    """Extract and parse the JSON text from a CallToolResult."""
    assert result.content, "Tool returned empty content"
    text = result.content[0].text
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return {"raw": text}


async def call(mcp_target, tool_name: str, arguments: dict, retries: int = 2) -> dict:
    """Open a Client, call a tool, parse and return the result dict.

    Retries on transient backend errors (MoSPI 500s from rate-limiting).
    """
    for attempt in range(1, retries + 2):
        await asyncio.sleep(THROTTLE)
        async with Client(mcp_target) as c:
            result = await c.call_tool(tool_name, arguments)
        data = parse_tool_result(result)
        if "error" not in data or attempt > retries:
            return data
        # Back off before retry
        await asyncio.sleep(THROTTLE * attempt)
    return data  # unreachable, but keeps linters happy


# ---------------------------------------------------------------------------
# Dataset definitions: each tuple is
#   (dataset, step3_kwargs, step4_filters)
#
# step3_kwargs  — extra keyword args for get_metadata (beyond 'dataset')
# step4_filters — the 'filters' dict for get_data
# ---------------------------------------------------------------------------

DATASETS = [
    pytest.param(
        "PLFS",
        {"indicator_code": 1, "frequency_code": 1},
        {"indicator_code": "1", "frequency_code": "1", "limit": "1"},
        id="PLFS",
    ),
    pytest.param(
        "CPI",
        {"base_year": "2024", "level": "Group"},
        {"base_year": "2024", "series": "Current", "limit": "1"},
        id="CPI",
    ),
    pytest.param(
        "IIP",
        {"base_year": "2011-12", "frequency": "Annually"},
        {"base_year": "2011-12", "type": "All", "limit": "1"},
        id="IIP",
    ),
    pytest.param(
        "ASI",
        {"classification_year": "2008"},
        {"classification_year": "2008", "sector_code": "Combined", "nic_type": "All", "limit": "1"},
        id="ASI",
    ),
    pytest.param(
        "NAS",
        {"indicator_code": 1, "base_year": "2011-12"},
        {"indicator_code": "1", "base_year": "2011-12", "series": "Current", "frequency_code": "1", "limit": "1"},
        id="NAS",
    ),
    pytest.param(
        "WPI",
        {},
        {"limit": "1"},
        id="WPI",
    ),
    pytest.param(
        "ENERGY",
        {"indicator_code": 1, "use_of_energy_balance_code": 1},
        {"indicator_code": "1", "use_of_energy_balance_code": "1", "limit": "1"},
        id="ENERGY",
    ),
    pytest.param(
        "AISHE",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="AISHE",
    ),
    pytest.param(
        "ASUSE",
        {"indicator_code": 1, "frequency_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="ASUSE",
    ),
    pytest.param(
        "GENDER",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="GENDER",
    ),
    pytest.param(
        "NFHS",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="NFHS",
    ),
    pytest.param(
        "ENVSTATS",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="ENVSTATS",
    ),
    pytest.param(
        "RBI",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="RBI",
    ),
    pytest.param(
        "NSS77",
        {"indicator_code": 16},
        {"indicator_code": "16", "limit": "1"},
        id="NSS77",
    ),
    pytest.param(
        "NSS78",
        {"indicator_code": 2},
        {"Indicator": "Access to Improved Source of Drinking Water", "limit": "1"},
        id="NSS78",
    ),
    pytest.param(
        "CPIALRL",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="CPIALRL",
    ),
    pytest.param(
        "HCES",
        {"indicator_code": 1},
        {"indicator_code": "1", "limit": "1"},
        id="HCES",
    ),
    pytest.param(
        "TUS",
        {"indicator_code": 4},
        {"indicator_code": "4", "limit": "1"},
        id="TUS",
    ),
    pytest.param(
        "EC",
        {"indicator_code": 1},
        {"indicator_code": "1", "state": "01", "top5opt": "2"},
        id="EC",
    ),
]

EXPECTED_TOOLS = {
    "list_datasets",
    "get_indicators",
    "get_metadata",
    "get_data",
}

EXPECTED_DATASETS = {
    "PLFS", "CPI", "IIP", "ASI", "NAS", "WPI", "ENERGY",
    "AISHE", "ASUSE", "GENDER", "NFHS", "ENVSTATS", "RBI",
    "NSS77", "NSS78", "CPIALRL", "HCES", "TUS", "EC",
}

# Internal keys injected by the server (not dataset-specific content)
_INTERNAL_KEYS = {"user_query", "next_step", "related_datasets"}


# ---------------------------------------------------------------------------
# Tool registration — verify the server exposes exactly 4 tools
# ---------------------------------------------------------------------------


async def test_list_tools(mcp_target):
    """Server exposes exactly 4 tools with the expected names."""
    async with Client(mcp_target) as c:
        tools = await c.list_tools()
    names = {t.name for t in tools}
    assert names == EXPECTED_TOOLS, f"Expected {EXPECTED_TOOLS}, got {names}"


# ---------------------------------------------------------------------------
# Step 1: API overview (single test, not parametrized)
# ---------------------------------------------------------------------------


async def test_list_datasets(mcp_target):
    """list_datasets: API overview returns all 19 datasets and workflow instructions."""
    data = await call(mcp_target, "list_datasets", {})
    assert isinstance(data, dict)
    assert "datasets" in data
    assert set(data["datasets"].keys()) == EXPECTED_DATASETS
    assert "workflow" in data
    assert "next_step" in data


# ---------------------------------------------------------------------------
# get_indicators — one test per dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset,step3_kwargs,step4_filters", DATASETS)
async def test_get_indicators(mcp_target, dataset, step3_kwargs, step4_filters):
    """get_indicators: returns non-empty indicator data for each dataset."""
    data = await call(
        mcp_target,
        "get_indicators",
        {"dataset": dataset, "user_query": "health check"},
    )
    assert isinstance(data, dict)
    assert "error" not in data
    # Response should contain dataset-specific content beyond internal keys
    content_keys = set(data.keys()) - _INTERNAL_KEYS
    assert content_keys, f"{dataset}: get_indicators returned only internal keys, no indicator data"


# ---------------------------------------------------------------------------
# get_metadata — one test per dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset,step3_kwargs,step4_filters", DATASETS)
async def test_get_metadata(mcp_target, dataset, step3_kwargs, step4_filters):
    """get_metadata: returns filter options and API param definitions."""
    data = await call(
        mcp_target,
        "get_metadata",
        {"dataset": dataset, **step3_kwargs},
    )
    assert isinstance(data, dict)
    assert "error" not in data
    # Every get_metadata response should include swagger param definitions
    assert "api_params" in data, f"{dataset}: get_metadata missing 'api_params'"
    assert isinstance(data["api_params"], list)
    assert len(data["api_params"]) > 0, f"{dataset}: api_params is empty"


# ---------------------------------------------------------------------------
# get_data — one test per dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset,step3_kwargs,step4_filters", DATASETS)
async def test_get_data(mcp_target, dataset, step3_kwargs, step4_filters):
    """get_data: returns records (not 'No Data Found') for each dataset."""
    data = await call(
        mcp_target,
        "get_data",
        {"dataset": dataset, "filters": step4_filters},
    )
    assert isinstance(data, dict)
    assert "error" not in data
    # Ensure the API actually returned data, not a "No Data Found" response
    assert data.get("msg") != "No Data Found", f"{dataset}: API returned 'No Data Found'"


# ---------------------------------------------------------------------------
# Negative / error-path tests
# ---------------------------------------------------------------------------


async def test_get_indicators_invalid_dataset(mcp_target):
    """get_indicators: invalid dataset name returns an error with valid_datasets hint."""
    data = await call(
        mcp_target,
        "get_indicators",
        {"dataset": "NONEXISTENT", "user_query": "test"},
    )
    assert "error" in data
    assert "valid_datasets" in data


async def test_get_metadata_missing_required_indicator(mcp_target):
    """get_metadata: omitting indicator_code for a dataset that requires it returns an error."""
    data = await call(
        mcp_target,
        "get_metadata",
        {"dataset": "PLFS"},  # PLFS requires indicator_code
    )
    assert "error" in data


async def test_get_data_unknown_filter_param(mcp_target):
    """get_data: passing an unknown filter param returns a validation error."""
    data = await call(
        mcp_target,
        "get_data",
        {"dataset": "WPI", "filters": {"totally_bogus_param": "1"}},
    )
    assert "error" in data or "invalid_params" in data


# ---------------------------------------------------------------------------
# Input type validation tests (Issue 3)
# ---------------------------------------------------------------------------


async def test_get_metadata_invalid_int(mcp_target):
    """get_metadata: non-integer indicator_code raises a validation error, not 500."""
    from fastmcp.exceptions import ToolError
    with pytest.raises(ToolError):
        await asyncio.sleep(THROTTLE)
        async with Client(mcp_target) as c:
            await c.call_tool("get_metadata", {"dataset": "PLFS", "indicator_code": "abc", "frequency_code": 1})


async def test_get_indicators_invalid_frequency(mcp_target):
    """get_indicators: non-integer frequency_code raises a validation error, not 500."""
    from fastmcp.exceptions import ToolError
    with pytest.raises(ToolError):
        await asyncio.sleep(THROTTLE)
        async with Client(mcp_target) as c:
            await c.call_tool("get_indicators", {"dataset": "PLFS", "frequency_code": "abc"})


async def test_get_data_ec_invalid_indicator(mcp_target):
    """get_data: non-integer indicator_code for EC returns a validation error, not 500."""
    data = await call(
        mcp_target,
        "get_data",
        {"dataset": "EC", "filters": {"indicator_code": "abc"}},
    )
    assert "error" in data
