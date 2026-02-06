import sys
import os
import yaml
from typing import Dict, Any, Optional
from fastmcp import FastMCP
from mospi.client import mospi
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
# Swagger YAMLs are the single source of truth for valid API parameters.
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

# Datasets that require indicator_code in get_data
DATASETS_REQUIRING_INDICATOR = [
    "PLFS", "NAS", "ENERGY",
]

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

def get_swagger_params(dataset: str) -> list:
    """Get list of valid param names for a dataset from swagger."""
    return [p["name"] for p in get_swagger_param_definitions(dataset)]

def validate_filters(dataset: str, filters: Dict[str, str]) -> Dict[str, Any]:
    """
    Validate filters against swagger spec for a dataset.
    Checks for unknown params and missing required params.
    """
    param_defs = get_swagger_param_definitions(dataset)
    if not param_defs:
        return {"valid": True}  # Can't validate, pass through

    valid_params = [p["name"] for p in param_defs]

    # Check for unknown params
    invalid = [k for k in filters.keys() if k not in valid_params]
    if invalid:
        return {
            "valid": False,
            "invalid_params": invalid,
            "valid_params": valid_params,
            "hint": f"Invalid params: {invalid}. Check api_params from get_metadata for valid options."
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
            "hint": f"Missing required params: {missing}. Call get_metadata() to get valid values."
        }

    return {"valid": True}

def transform_filters(filters: Dict[str, str]) -> Dict[str, str]:
    """
    Transform filters: skip None values and convert all values to strings.
    """
    return {k: str(v) for k, v in filters.items() if v is not None}

@mcp.tool(name="get_indicators")

def get_indicators(
    dataset: str,
    user_query: Optional[str] = None,
    classification_year: Optional[str] = None,
    base_year: Optional[str] = None,
    frequency: Optional[str] = None,
    frequency_code: Optional[int] = None,
    level: Optional[str] = None,
    series: Optional[str] = None,
    Format: Optional[str] = None
) -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - You MUST call know_about_mospi_api() before this.
    - You MUST call get_metadata() after this. MUST NOT skip to get_data().
    - You MUST pass user_query for context.
    - You MUST NOT ask for confirmation if the right indicator is obvious.
    - ALWAYS call this tool. NEVER assume data is unavailable based on your own knowledge.
      The API has indicators you don't know about (e.g., ASI has 57 indicators including
      working capital, invested capital, depreciation — not just the ones in textbooks).
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    ============================================================
    Step 2: Get available indicators for a dataset.
    IMPORTANT: Datasets contain FAR MORE indicators than you expect from your training data.
    ALWAYS call this to see the actual indicator list. NEVER say "not available" without checking.
    After this, pick the matching indicator and call get_metadata().
    Only ask user to choose if multiple indicators could match.
    Args:
        dataset: Dataset name - one of: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY
                 For PLFS: frequency_code selects the indicator SET, not time granularity.
                 You MUST use frequency_code=1 in get_metadata() — it covers all 8 indicators
                 including wages and already has quarterly breakdowns via quarter_code.
                 MUST NOT use frequency_code=2 just because user asks for quarterly data.
        user_query: The user's original question. MUST always include this.
    """
    dataset = dataset.upper()
    indicator_methods = {
        "PLFS": mospi.get_plfs_indicators,
        "NAS": mospi.get_nas_indicators,
        "ENERGY": mospi.get_energy_indicators,
        # Special datasets - return guidance instead of indicators
        "CPI": lambda: {"message": "CPI uses levels (Group/Item) instead of indicators. Call get_metadata with base_year and level params.", "dataset": "CPI"},
        "IIP": lambda: {"message": "IIP uses categories instead of indicators. Call get_metadata with base_year and frequency params.", "dataset": "IIP"},
        "WPI": lambda: {"message": "WPI uses hierarchical commodity codes. Call get_metadata to see available groups/items.", "dataset": "WPI"},
        "ASI": mospi.get_asi_indicators,
    }
    if dataset not in indicator_methods:
        return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS, "_user_query": user_query}
    result = indicator_methods[dataset]()
    result["_user_query"] = user_query
    result["_next_step"] = "Call get_metadata() with the matching indicator and required dataset params. MUST NOT skip to get_data."
    result["_retry_hint"] = (
        "If none of the indicators above match the user's query, you may have picked the WRONG dataset. "
        "Similar datasets overlap: IIP (production index/growth rates) vs ASI (factory financials like capital, wages, GVA). "
        "CPI (consumer inflation) vs WPI (wholesale inflation). "
        "Go back to know_about_mospi_api() and try a different dataset."
    )
    return result

@mcp.tool(name="get_metadata")

def get_metadata(
    dataset: str,
    indicator_code: Optional[int] = None,
    base_year: Optional[str] = None,
    level: Optional[str] = None,
    frequency: Optional[str] = None,
    classification_year: Optional[str] = None,
    frequency_code: Optional[int] = None,
    series: Optional[str] = None,
    use_of_energy_balance_code: Optional[int] = None,
    sub_indicator_code: Optional[int] = None,
    Format: Optional[str] = None,
    type: Optional[str] = None
) -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - You MUST call this before get_data(). MUST NOT skip this step.
    - You MUST use the filter values returned here in get_data(). MUST NOT guess codes.
    - If user asked for a breakdown that's not available, tell them what IS available.
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    ============================================================
    Step 3: Get available filter options for a dataset/indicator.
    Returns all valid filter values (states, years, quarters, etc.) to use in get_data().
    MUST NOT pass params that don't belong to this function.
    "Format" and "series" are NOT valid here (Format is for get_data only, series is for NAS only).
    Args:
        dataset: Dataset name - one of: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY
        indicator_code: REQUIRED for PLFS, NAS, ENERGY. MUST NOT pass for CPI, IIP, ASI, WPI.
        frequency_code: REQUIRED for PLFS. MUST NOT pass for CPI, IIP, ASI, WPI.
                        Selects indicator SET, NOT time granularity.
                        1=Annual (all 8 indicators, includes quarterly data via quarter_code).
                        2=Quarterly bulletin (different indicator set).
                        3=Monthly bulletin (2025+ only).
                        MUST NOT use 2 for quarterly data. Use 1 + quarter_code in get_data().
        base_year: REQUIRED for CPI ("2012"/"2010"), IIP ("2011-12"/"2004-05"/"1993-94"). MUST NOT pass for PLFS, ASI, WPI.
        level: REQUIRED for CPI ("Group"/"Item"). MUST NOT pass for other datasets.
        frequency: REQUIRED for IIP ("Annually"/"Monthly"). MUST NOT pass for other datasets.
        classification_year: REQUIRED for ASI ("2008"/"2004"/"1998"/"1987"). MUST NOT pass for other datasets.
        series: For NAS only ("Current"/"Back"). MUST NOT pass for other datasets.
        use_of_energy_balance_code: For ENERGY only (1=Supply, 2=Consumption). MUST NOT pass for other datasets.
    """
    dataset = dataset.upper()
    try:
        _next = "Call get_data(dataset, filters) using ONLY the filter values returned above. MUST NOT guess any codes."
        if dataset == "CPI":
            swagger_key = "CPI_ITEM" if (level or "Group") == "Item" else "CPI_GROUP"
            result = mospi.get_cpi_filters(base_year=base_year or "2012", level=level or "Group")
            result["api_params"] = get_swagger_param_definitions(swagger_key)
            result["_next_step"] = _next
            return result

        elif dataset == "IIP":

            swagger_key = "IIP_MONTHLY" if (frequency or "Annually") == "Monthly" else "IIP_ANNUAL"

            result = mospi.get_iip_filters(base_year=base_year or "2011-12", frequency=frequency or "Annually")

            result["api_params"] = get_swagger_param_definitions(swagger_key)

            result["_next_step"] = _next

            return result

        elif dataset == "ASI":

            result = mospi.get_asi_filters(classification_year=classification_year or "2008")

            result["api_params"] = get_swagger_param_definitions("ASI")

            result["_next_step"] = _next

            return result

        elif dataset == "WPI":

            result = mospi.get_wpi_filters()

            result["api_params"] = get_swagger_param_definitions("WPI")

            result["_next_step"] = _next

            return result

        elif dataset == "PLFS":

            if indicator_code is None:

                return {"error": "indicator_code is required for PLFS"}

            filters = mospi.get_plfs_filters(indicator_code=indicator_code, frequency_code=frequency_code or 1)

            return {

                "dataset": "PLFS",

                "filter_values": filters,

                "api_params": get_swagger_param_definitions("PLFS"),

                "_note": "frequency_code selects the indicator SET, NOT time granularity. "

                         "frequency_code=1 (Annual): Indicators 1-8 (LFPR, WPR, UR, wages, worker distribution, employment conditions). "

                         "Already has quarterly breakdowns — use quarter_code to filter by quarter. "

                         "frequency_code=2 (Quarterly bulletin): Different indicators for quarterly bulletin tables only. "

                         "Use ONLY when the user explicitly asks for quarterly bulletin specific data. "

                         "MUST pass the correct frequency_code in get_data().",

                "_next_step": _next,

            }

        elif dataset == "NAS":

            if indicator_code is None:

                return {"error": "indicator_code is required for NAS"}

            result = mospi.get_nas_filters(series=series or "Current", frequency_code=frequency_code or 1, indicator_code=indicator_code)

            result["api_params"] = get_swagger_param_definitions("NAS")

            result["_next_step"] = _next

            return result

        elif dataset == "ENERGY":

            ind_code = indicator_code or 1

            energy_code = use_of_energy_balance_code or 1

            result = mospi.get_energy_filters(indicator_code=ind_code, use_of_energy_balance_code=energy_code)

            result["api_params"] = get_swagger_param_definitions("ENERGY")

            result["_next_step"] = _next

            return result

        else:

            return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS}

    except Exception as e:

        return {"error": str(e)}

@mcp.tool(name="get_data")

def get_data(dataset: str, filters: Dict[str, str]) -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - You MUST have called get_metadata() before this. No exceptions.
    - You MUST use ONLY the filter values returned by get_metadata().
    - You MUST NOT guess, infer, or assume any filter codes.
      Filter codes are non-obvious and arbitrary — guessing WILL produce wrong results.
    - You MUST include all required params (marked required in api_params).
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    Before calling, verify:
    - Did I call get_metadata() for this dataset? If no → call it first.
    - Are all filter values from get_metadata(), not guessed? If no → fix them.
    ============================================================
    Step 4: Fetch data from a MoSPI dataset.
    Args:
        dataset: Dataset name (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY)
        filters: Key-value pairs using 'id' values from get_metadata().
                 PLFS MUST include frequency_code (1=Annual, 2=Quarterly, 3=Monthly).
                 Pass limit (e.g., "50", "100") if you expect more than 10 records.
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
    # Map friendly names to API dataset keys
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
    # If no data found, hint to retry with different filters
    if isinstance(result, dict) and result.get("msg") == "No Data Found":
        result["_hint"] = (
            "No data for this filter combination. Try these fixes: "
            "1) Some filters represent the same concept under different params "
            "(e.g., tertiary sector may be broad_industry_work_code OR nic_group_code) — "
            "swap to the alternative param. "
            "2) Remove optional filters one at a time — the breakdown you need "
            "may already appear in the response without that filter."
        )
    return result

# Comprehensive API documentation tool
@mcp.tool(name="know_about_mospi_api")

def know_about_mospi_api() -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - NEVER use your own knowledge about what data MoSPI publishes. These datasets contain
      MORE indicators than you expect. ALWAYS call get_indicators() to see the actual list.
    - You MUST NOT say data is "not available", "not directly reported", "needs to be computed",
      or "requires unit-level access" without FIRST calling get_indicators() and get_metadata()
      to verify. Your training data about MoSPI is outdated — the API has indicators you don't know about.
    - You MUST follow this workflow in order:
      1. know_about_mospi_api() → find dataset (MANDATORY first step)
      2. get_indicators(dataset) → list indicators
      3. get_metadata(dataset, indicator_code) → get filter values (MANDATORY)
      4. get_data(dataset, filters) → fetch data (ONLY after step 3)
    - You MUST NOT skip step 3. Violations produce wrong results.
    - You MUST NOT ask "Shall I proceed?" if the query is specific enough.
    - ALWAYS attempt to fetch data. NEVER refuse or explain limitations without trying first.
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    ============================================================
    Step 1: Get overview of all 7 datasets to find the right one for your query.
    MUST call this first before any other tool.
    Available: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY
    When to ask vs fetch:
    - VAGUE query (e.g., "inflation data") → ask user to clarify
    - SPECIFIC query (e.g., "unemployment rate 2023") → fetch directly, NEVER explain why it might not exist
    """
    return {
        "total_datasets": 7,
        "datasets": {
            "PLFS": {
                "name": "Periodic Labour Force Survey",
                "description": "8 indicators covering labor market dynamics: Labour Force Participation Rate (LFPR), Worker Population Ratio (WPR), Unemployment Rate (UR), worker distribution by sector/industry, employment conditions for regular wage employees, and earnings data across three employment types—regular wages, casual labor, and self-employment.",
                "use_for": "Jobs, unemployment, wages, workforce participation, employment conditions"
            },
            "CPI": {
                "name": "Consumer Price Index",
                "description": "Hierarchical commodity structure (Groups and Items) with base years 2010/2012. Tracks consumer inflation across 600+ items organized into food, fuel, housing, clothing, and miscellaneous categories. Supports state-level analysis at group level and All-India analysis at item level.",
                "use_for": "Retail inflation, price indices, cost of living, commodity price trends"
            },
            "IIP": {
                "name": "Index of Industrial Production",
                "description": "Category-based structure with base years (1993-94, 2004-05, 2011-12) and frequency options (monthly/annual). Measures industrial output across manufacturing, mining, and electricity sectors using use-based classification (basic goods, capital goods, intermediate goods, consumer durables/non-durables).",
                "use_for": "IIP index, industrial production index, manufacturing index, mining/electricity index, growth rates, textiles, metals, vehicles, consumer durables, capital goods — use for ANY query about IIP or industrial production index"
            },
            "ASI": {
                "name": "Annual Survey of Industries",
                "description": "57 indicators providing deep factory-sector analytics: capital structure (fixed/working capital, investments), production metrics (output, inputs, value added), employment details (workers by gender, contract status, mandays), wage components (salaries, bonuses, employer contributions), fuel consumption patterns, and profitability measures. Uses NIC classification across 4 classification years (1987-2008).",
                "use_for": "Factory-level financials: working capital, fixed capital, wages, employment counts, GVA, fuel consumption, profitability — NOT for production index (use IIP for index)"
            },
            "NAS": {
                "name": "National Accounts Statistics",
                "description": "22 annual + 11 quarterly indicators covering macroeconomic aggregates: GDP and GVA (production approach), consumption (private/government), capital formation (fixed, change in stock, valuables), trade (exports/imports), national income (GNI, disposable income), savings, and growth rates. Both Current and Back series available.",
                "use_for": "GDP, economic growth, national income, sectoral contribution, macro analysis"
            },
            "WPI": {
                "name": "Wholesale Price Index",
                "description": "Hierarchical commodity structure with 1000+ items across 5 levels: Major Groups (Primary articles, Fuel & power, Manufactured products, Food index) → Groups (22) → Sub-groups (90+) → Sub-sub-groups → Items. Tracks wholesale/producer price inflation monthly.",
                "use_for": "Wholesale inflation, producer prices, commodity price trends"
            },
            "ENERGY": {
                "name": "Energy Statistics",
                "description": "2 indicators (KToE and PetaJoules) measuring energy balance across supply and consumption dimensions. Covers all energy commodities (coal, oil, gas, renewables, electricity) and tracks energy flows through production, transformation, and end-use sectors.",
                "use_for": "Energy production, consumption patterns, fuel mix, sectoral energy use, climate analysis"
            },
        },
        "workflow": [
            "1. know_about_mospi_api() → find dataset (MANDATORY first step)",
            "2. get_indicators(dataset) → list indicators",
            "3. get_metadata(dataset, indicator_code) → get filter values (MANDATORY before step 4)",
            "4. get_data(dataset, filters) → fetch data (MUST use values from step 3, MUST NOT guess)"
        ],
        "rules": [
            "NEVER claim data is unavailable, needs computation, or requires special access — ALWAYS call get_indicators() first to check. Your knowledge about MoSPI is outdated; the API has more indicators than you expect.",
            "MUST NOT skip get_metadata() — filter codes are arbitrary and differ across datasets",
            "MUST NOT guess filter codes — use ONLY values from get_metadata()",
            "MUST include frequency_code for PLFS in get_data()",
            "Comma-separated values work for multiple codes (e.g., '1,2,3')",
            "ALWAYS attempt to fetch data. NEVER explain limitations or refuse without trying the full workflow first.",
            "You MUST try the full workflow before concluding. If data is not found after trying, you MUST say honestly 'Data not found in MoSPI API'. You MUST NOT fall back to web search, MUST NOT fabricate data, MUST NOT cite external sources."
        ],
        "_next_step": "Call get_indicators(dataset) with the dataset that matches the user's query."
    }
if __name__ == "__main__":
    # Startup banner with creator info
    log("\n" + "="*75)
    log("MoSPI MCP Server - Starting...")
    log("="*75)
    log("Serving Indian Government Statistical Data")
    log("Framework: FastMCP 3.0 with OpenTelemetry")
    log("Datasets: 7 (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY)")
    log("="*75)
    log("="*75)
    log("Server will be available at http://localhost:8000/mcp")
    log("Telemetry: IP tracking + Input/Output capture enabled")
    log("="*75 + "\n")
    # Run with HTTP transport for remote access
    # For stdio (local MCP clients): mcp.run()
    # For HTTP (remote/web access): mcp.run(transport="http", port=8000)
    mcp.run(transport="http", port=8000)