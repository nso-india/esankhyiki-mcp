import sys
import os
import yaml
from typing import Dict, Any, List
from fastmcp import FastMCP
from mospi.client import mospi
from mospi.search import search_dataset
from observability.telemetry import TelemetryMiddleware

SWAGGER_DIR = os.path.join(os.path.dirname(__file__), "swagger")


def log(msg: str):
    """Print to stderr to avoid interfering with stdio transport"""
    print(msg, file=sys.stderr)

# Initialize FastMCP server
mcp = FastMCP("MoSPI Data Server")

# Add telemetry middleware for IP tracking and input/output capture
mcp.add_middleware(TelemetryMiddleware())


VALID_DATASETS = [
    "PLFS", "CPI", "IIP", "ASI", "NAS", "WPI", "ENERGY",
]

# Maps dataset key -> (swagger_yaml_file, endpoint_path)
DATASET_SWAGGER = {
    "PLFS": ("swagger_user_plfs.yaml", "/api/plfs/getData"),
    "CPI": ("swagger_user_cpi.yaml", "/api/cpi/getCPIIndex"),
    "CPI_GROUP": ("swagger_user_cpi.yaml", "/api/cpi/getCPIIndex"),
    "CPI_ITEM": ("swagger_user_cpi.yaml", "/api/cpi/getItemIndex"),
    "IIP": ("swagger_user_iip.yaml", "/api/iip/getIIPAnnual"),
    "IIP_ANNUAL": ("swagger_user_iip.yaml", "/api/iip/getIIPAnnual"),
    "IIP_MONTHLY": ("swagger_user_iip.yaml", "/api/iip/getIIPMonthly"),
    "ASI": ("swagger_user_asi.yaml", "/api/asi/getASIData"),
    "NAS": ("swagger_user_nas.yaml", "/api/nas/getNASData"),
    "WPI": ("swagger_user_wpi.yaml", "/api/wpi/getWpiRecords"),
    "ENERGY": ("swagger_user_energy.yaml", "/api/energy/getEnergyRecords"),
}


def get_swagger_param_definitions(dataset: str) -> list:
    """Load full param definitions from swagger spec for a dataset."""
    dataset_upper = dataset.upper()
    if dataset_upper not in DATASET_SWAGGER:
        return []
    yaml_file, endpoint_path = DATASET_SWAGGER[dataset_upper]
    swagger_path = os.path.join(SWAGGER_DIR, yaml_file)
    if not os.path.exists(swagger_path):
        return []
    with open(swagger_path, 'r') as f:
        spec = yaml.safe_load(f)
    return spec.get("paths", {}).get(endpoint_path, {}).get("get", {}).get("parameters", [])


def validate_filters(dataset: str, filters: Dict[str, str]) -> Dict[str, Any]:
    """
    Validate filters against swagger spec for a dataset.
    Checks for unknown params and missing required params.
    """
    param_defs = get_swagger_param_definitions(dataset)
    if not param_defs:
        return {"valid": True}

    valid_params = [p["name"] for p in param_defs]

    # Check for unknown params
    invalid = [k for k in filters.keys() if k not in valid_params]
    if invalid:
        return {
            "valid": False,
            "invalid_params": invalid,
            "valid_params": valid_params,
            "hint": f"Invalid params: {invalid}. Use describe_dataset() to find valid param names."
        }

    # Check for missing required params (exclude Format — auto-handled by client)
    missing = [
        p["name"] for p in param_defs
        if p.get("required") and p["name"] != "Format" and p["name"] not in filters
    ]
    if missing:
        return {
            "valid": False,
            "missing_required": missing,
            "hint": f"Missing required params: {missing}. Use describe_dataset() to find valid values."
        }

    return {"valid": True}


def transform_filters(filters: Dict[str, str]) -> Dict[str, str]:
    """Transform filters: skip None values and convert all values to strings."""
    return {k: str(v) for k, v in filters.items() if v is not None}


# =============================================================================
# Tool 1: describe_dataset — search metadata for indicators and filter values
# =============================================================================

@mcp.tool(name="describe_dataset")
def describe_dataset(dataset: str, search_terms: List[str]) -> Dict[str, Any]:
    """
    Search a MoSPI dataset for indicators and filter values.

    Datasets: PLFS (employment), CPI (inflation), IIP (industrial production),
    ASI (factory data), NAS (GDP), WPI (wholesale prices), ENERGY.

    search_terms: case-insensitive search across all indicators and filters.
    Be liberal — include synonyms, abbreviations, and related terms.
    e.g., for "unemployment in Maharashtra":
      search_terms=["unemployment", "UR", "maharashtra", "2022", "2023"]

    Returns matching codes to use in get_data(), plus any required params
    you didn't search for with their full option lists.
    """
    return search_dataset(dataset, search_terms)


# =============================================================================
# Tool 2: get_data — fetch data using codes from describe_dataset
# =============================================================================

@mcp.tool(name="get_data")
def get_data(dataset: str, filters: Dict[str, str]) -> Dict[str, Any]:
    """
    Fetch data from a MoSPI dataset. Use codes from describe_dataset().
    Pass limit (e.g., "50", "100") if you expect many records.
    """
    dataset = dataset.upper()

    # Auto-route CPI and IIP based on filters provided
    if dataset == "CPI":
        if "item_code" in filters:
            dataset = "CPI_ITEM"
        else:
            dataset = "CPI_GROUP"

    if dataset == "IIP":
        if "month_code" in filters:
            dataset = "IIP_MONTHLY"
        else:
            dataset = "IIP_ANNUAL"

    # Map to API dataset keys
    dataset_map = {
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

    api_dataset = dataset_map.get(dataset)
    if not api_dataset:
        return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS}

    # Transform filters: skip None values and convert to strings
    transformed_filters = transform_filters(filters)

    # Validate params against swagger spec
    validation = validate_filters(dataset, transformed_filters)
    if not validation["valid"]:
        return {"error": "Invalid parameters", **validation}

    result = mospi.get_data(api_dataset, transformed_filters)

    # If no data found, provide guidance
    if isinstance(result, dict) and result.get("msg") == "No Data Found":
        result["hint"] = (
            "No data for this filter combination. Try: "
            "1) Remove optional filters one at a time. "
            "2) Use describe_dataset() to verify your codes are correct. "
            "3) Try a broader filter (e.g., group level instead of item level)."
        )

    return result


if __name__ == "__main__":
    log("\n" + "="*75)
    log("MoSPI MCP Server - Starting...")
    log("="*75)
    log("Serving Indian Government Statistical Data")
    log("Framework: FastMCP 3.0 with OpenTelemetry")
    log("Tools: describe_dataset, get_data")
    log("Datasets: 7 (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY)")
    log("="*75)
    log("Server will be available at http://localhost:8000/mcp")
    log("="*75 + "\n")

    mcp.run(transport="http", port=8000)
