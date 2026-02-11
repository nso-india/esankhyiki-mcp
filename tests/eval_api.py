"""
Programmatic eval for MoSPI MCP 2-tool design.

Sends natural language queries to Claude API with tool definitions,
executes tools locally (no MCP server needed), and checks whether
Claude's final response contains the expected golden value.

Usage:
    python tests/eval_api.py                    # Run all golden queries
    python tests/eval_api.py --query 3          # Run a single query by number
"""

import argparse
import json
import sys
import time
from pathlib import Path

import anthropic

# Add project root to path so we can import mospi modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from mospi.search import search_dataset
from mospi.client import mospi

# ---------------------------------------------------------------------------
# Golden eval set — query + expected value
# ---------------------------------------------------------------------------
# Expected values were computed by running the correct API queries manually.
# The eval checks whether Claude's final response contains this value.

GOLDEN_QUERIES = [
    {
        "dataset": "PLFS",
        "query": "What is the unemployment rate for rural males in Bihar during 2022-23?",
        "expected": "4.40",
        "note": "UR, rural, male, Bihar, 2022-23, PS+SS, 15+",
    },
    {
        "dataset": "PLFS",
        "query": "What percentage of workers are in the manufacturing sector in Gujarat for females 2021-22?",
        "expected": "17.71",
        "note": "Pct distribution of workers, manufacturing, Gujarat, female, PS+SS, all ages",
    },
    {
        "dataset": "CPI",
        "query": "What is the CPI for food and beverages in rural India for January 2023?",
        "expected": "175.0",
        "note": "CPI group Food+Bev overall, rural, All India, Jan 2023, base 2012",
    },
    {
        "dataset": "CPI",
        "query": "CPI fuel and light combined sector delhi 2019 march",
        "expected": "111.9",
        "note": "CPI group Fuel+Light, combined, Delhi, Mar 2019, base 2012",
    },
    {
        "dataset": "WPI",
        "query": "What is the wholesale price index for primary articles in January 2023?",
        "expected": "174.3",
        "note": "WPI major_group primary articles, Jan 2023",
    },
    {
        "dataset": "WPI",
        "query": "What is the WPI for fuel and power in June 2022?",
        "expected": "167.1",
        "note": "WPI major_group fuel & power, Jun 2022",
    },
    {
        "dataset": "IIP",
        "query": "What is the index of industrial production for mining sector in 2022-23?",
        "expected": "119.9",
        "note": "IIP sectoral, mining, 2022-23, base 2011-12",
    },
    {
        "dataset": "ASI",
        "query": "How many textile factories were operating in Maharashtra in 2022-23?",
        "expected": "1221",
        "note": "ASI indicator=2 (factories), NIC 13 (textiles), Maharashtra, 2022-23",
    },
    {
        "dataset": "ASI",
        "query": "show me gross value added for textile industry across all states 2020-21",
        "expected": "7135367",
        "note": "ASI indicator=19 (GVA), NIC 13 (textiles), 2020-21, SUM across states",
    },
    {
        "dataset": "NAS",
        "query": "What was India's GDP at market prices in 2023-24 at current prices, current series, in crores?",
        "expected": "296,57,744.74",
        "note": "NAS GDP at market prices, current series, annual, 2023-24, current_price in Rs Crore",
    },
    {
        "dataset": "ENERGY",
        "query": "What was India's total coal production in 2023-24 in KToE?",
        "expected": "403799.46",
        "note": "Energy balance KToE, supply, coal, production, 2023-24",
    },
    {
        "dataset": "ENERGY",
        "query": "Diesel consumption by transport sector in petajoules for 2022-23",
        "expected": "113.33",
        "note": "Energy balance PJ, consumption, diesel, transport sector total, 2022-23",
    },
]

# ---------------------------------------------------------------------------
# Tool definitions (matching @mcp.tool schemas from mospi_server.py)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "describe_dataset",
        "description": (
            "Search a MoSPI dataset for indicators and filter values.\n\n"
            "Datasets: PLFS (employment), CPI (inflation), IIP (industrial production),\n"
            "ASI (factory data), NAS (GDP), WPI (wholesale prices), ENERGY.\n\n"
            "search_terms: case-insensitive search across all indicators and filters.\n"
            "Be liberal — include synonyms, abbreviations, and related terms.\n"
            "e.g., for \"unemployment in Maharashtra\":\n"
            '  search_terms=["unemployment", "UR", "maharashtra", "2022", "2023"]\n\n'
            "Returns matching codes to use in get_data(), plus any required params\n"
            "you didn't search for with their full option lists."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string"},
                "search_terms": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["dataset", "search_terms"],
        },
    },
    {
        "name": "get_data",
        "description": (
            "Fetch data from a MoSPI dataset. Use codes from describe_dataset().\n"
            'Pass limit (e.g., "50", "100") if you expect many records.'
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string"},
                "filters": {
                    "type": "object",
                    "additionalProperties": {"type": "string"},
                },
            },
            "required": ["dataset", "filters"],
        },
    },
]

# ---------------------------------------------------------------------------
# Tool execution (replicates mospi_server.py routing logic)
# ---------------------------------------------------------------------------

DATASET_MAP = {
    "CPI_GROUP": "CPI_Group",
    "CPI_ITEM": "CPI_Item",
    "IIP_ANNUAL": "IIP_Annual",
    "IIP_MONTHLY": "IIP_Monthly",
    "PLFS": "PLFS",
    "ASI": "ASI",
    "NAS": "NAS",
    "WPI": "WPI",
    "ENERGY": "Energy",
}


def execute_tool(name: str, args: dict) -> dict:
    """Execute a tool call by dispatching to our Python functions."""
    if name == "describe_dataset":
        return search_dataset(args["dataset"], args["search_terms"])

    elif name == "get_data":
        dataset = args["dataset"].upper()
        filters = {k: str(v) for k, v in args.get("filters", {}).items() if v is not None}

        if dataset == "CPI":
            dataset = "CPI_ITEM" if "item_code" in filters else "CPI_GROUP"
        if dataset == "IIP":
            dataset = "IIP_MONTHLY" if "month_code" in filters else "IIP_ANNUAL"

        api_dataset = DATASET_MAP.get(dataset)
        if not api_dataset:
            return {"error": f"Unknown dataset: {dataset}"}

        result = mospi.get_data(api_dataset, filters)

        if isinstance(result, dict) and result.get("msg") == "No Data Found":
            result["hint"] = (
                "No data for this filter combination. Try: "
                "1) Remove optional filters one at a time. "
                "2) Use describe_dataset() to verify your codes are correct. "
                "3) Try a broader filter (e.g., group level instead of item level)."
            )
        return result

    return {"error": f"Unknown tool: {name}"}


# ---------------------------------------------------------------------------
# Claude API tool-use loop
# ---------------------------------------------------------------------------

def run_query(client: anthropic.Anthropic, query: str, max_turns: int = 10) -> dict:
    """Send a query to Claude with tools, run the tool loop, return results."""
    messages = [{"role": "user", "content": query}]
    tool_calls = []

    for _ in range(max_turns):
        try:
            response = client.messages.create(
                model="claude-sonnet-4-5-20250929",
                max_tokens=4096,
                tools=TOOLS,
                messages=messages,
            )
        except Exception as e:
            return {"text": "", "tool_calls": tool_calls, "error": str(e)}

        assistant_text = ""
        tool_use_blocks = []
        for block in response.content:
            if block.type == "text":
                assistant_text += block.text
            elif block.type == "tool_use":
                tool_use_blocks.append(block)

        if response.stop_reason == "end_turn":
            return {"text": assistant_text, "tool_calls": tool_calls}

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in tool_use_blocks:
                print(f"      -> {block.name}({json.dumps(block.input, default=str)[:120]})")
                try:
                    result = execute_tool(block.name, block.input)
                except Exception as e:
                    result = {"error": str(e)}

                tool_calls.append({
                    "name": block.name,
                    "input": block.input,
                    "output": result,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

    return {"text": "", "tool_calls": tool_calls, "error": "max_turns_exceeded"}


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def normalize_number(s: str) -> float:
    """Parse a number string, stripping commas and whitespace."""
    return float(s.replace(",", "").strip())


def value_in_response(expected: str, response_text: str) -> bool:
    """Check if the expected value appears in Claude's response."""
    if not response_text:
        return False

    response_clean = response_text.replace(",", "")

    try:
        expected_float = normalize_number(expected)
    except ValueError:
        return expected in response_text

    # Try common formats of the number
    formats = [
        str(expected_float),                                    # 4.4
        f"{expected_float:.1f}",                                # 4.4
        f"{expected_float:.2f}",                                # 4.40
        str(int(expected_float)) if expected_float == int(expected_float) else None,  # 4
        f"{expected_float:,.0f}",                               # 25,822
        f"{expected_float:,.2f}",                               # 403,799.46
    ]

    for fmt in formats:
        if fmt and fmt in response_clean:
            return True

    # Also try the raw expected string
    if expected.replace(",", "") in response_clean:
        return True

    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MoSPI MCP eval via Claude API")
    parser.add_argument("--query", type=int, help="Run a single query by number (1-indexed)")
    args = parser.parse_args()

    client = anthropic.Anthropic()

    queries = GOLDEN_QUERIES
    if args.query:
        if 1 <= args.query <= len(queries):
            queries = [queries[args.query - 1]]
        else:
            print(f"Query number must be 1-{len(GOLDEN_QUERIES)}")
            sys.exit(1)

    print(f"Running {len(queries)} golden queries against Claude API (Sonnet 4.5)")
    print(f"Scoring: PASS = expected value appears in Claude's response\n")

    results = []
    for i, q in enumerate(queries):
        ds = q["dataset"]
        query_text = q["query"]
        expected = q["expected"]

        print(f"  [{i+1}/{len(queries)}] [{ds}] {query_text[:65]}...")
        print(f"    expected: {expected}")

        t0 = time.time()
        result = run_query(client, query_text)
        elapsed = time.time() - t0

        response_text = result.get("text", "")
        passed = value_in_response(expected, response_text)

        results.append({
            "dataset": ds,
            "query": query_text,
            "expected": expected,
            "passed": passed,
            "num_calls": len(result.get("tool_calls", [])),
            "elapsed": round(elapsed, 1),
            "response_preview": response_text[:150].replace("\n", " ") if response_text else "(empty)",
        })

        status = "PASS" if passed else "FAIL"
        print(f"    {status}  [{results[-1]['num_calls']} calls, {elapsed:.1f}s]")
        if response_text:
            print(f"    response: {response_text[:120].replace(chr(10), ' ')}...")
        print()

    # Summary
    print("=" * 70)
    total = len(results)
    passed = sum(1 for r in results if r["passed"])

    print(f"\n{'#':<4} {'Dataset':<8} {'Expected':<16} {'Result':<6} {'Calls':<6}")
    print("-" * 50)
    for i, r in enumerate(results):
        status = "PASS" if r["passed"] else "FAIL"
        print(f"{i+1:<4} {r['dataset']:<8} {r['expected']:<16} {status:<6} {r['num_calls']:<6}")

    print("-" * 50)
    print(f"\nPASSED: {passed}/{total} ({100*passed/total:.0f}%)")


if __name__ == "__main__":
    main()
