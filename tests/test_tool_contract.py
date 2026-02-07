import asyncio

import mospi_server


def test_mcp_exposes_expected_tools():
    async def _list():
        tools = await mospi_server.mcp.list_tools()
        return sorted(tool.name for tool in tools)

    names = asyncio.run(_list())
    assert names == [
        "1_know_about_mospi_api",
        "2_get_indicators",
        "3_get_metadata",
        "4_get_data",
    ]
