"""
Test client for MoSPI MCP Server
Tests HTTP transport with new 2-tool design
"""
import asyncio
from fastmcp import Client

async def test_server():
    """Test the MoSPI MCP server"""

    # Connect to the HTTP server
    async with Client("http://localhost:8000/mcp") as client:
        print("Connected to MoSPI MCP Server\n")

        # Test 1: List available tools
        print("Available tools:")
        tools = await client.list_tools()
        for tool in tools:
            print(f"   - {tool.name}")
        print(f"\n   Total: {len(tools)} tools\n")

        # Test 2: describe_dataset — PLFS search
        print("Testing describe_dataset (PLFS)...")
        result = await client.call_tool("describe_dataset", {
            "dataset": "PLFS",
            "search_terms": ["unemployment", "maharashtra", "2022"]
        })
        print(f"   Result: {str(result)[:200]}...\n")

        # Test 3: describe_dataset — WPI search
        print("Testing describe_dataset (WPI)...")
        result = await client.call_tool("describe_dataset", {
            "dataset": "WPI",
            "search_terms": ["rice", "paddy"]
        })
        print(f"   Result: {str(result)[:200]}...\n")

        # Test 4: describe_dataset — CPI search
        print("Testing describe_dataset (CPI)...")
        result = await client.call_tool("describe_dataset", {
            "dataset": "CPI",
            "search_terms": ["food", "delhi"]
        })
        print(f"   Result: {str(result)[:200]}...\n")

        # Test 5: get_data — PLFS unemployment rate
        print("Testing get_data (PLFS)...")
        result = await client.call_tool("get_data", {
            "dataset": "PLFS",
            "filters": {
                "indicator_code": "3",
                "frequency_code": "1",
                "state_code": "16",
                "year": "2022-23",
                "gender_code": "3",
                "sector_code": "3",
            }
        })
        print(f"   Result: {str(result)[:200]}...\n")

        print("All tests passed!")

if __name__ == "__main__":
    print("Testing MoSPI MCP Server (HTTP Transport)\n")
    print("Make sure the server is running:")
    print("  python mospi_server.py\n")
    print("-" * 60 + "\n")

    asyncio.run(test_server())
