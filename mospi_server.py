import sys
import os
import json
import yaml
import requests
from typing import Dict, Any, Optional
from fastmcp import FastMCP
from mospi.client import mospi
from observability.telemetry import TelemetryMiddleware

SWAGGER_DIR = os.path.join(os.path.dirname(__file__), "swagger")
DEFINITIONS_DIR = os.path.join(os.path.dirname(__file__), "definitions")


def load_definitions(dataset: str) -> Dict[int, Dict[str, str]]:
    """Load indicator definitions from JSON file, keyed by indicator_code."""
    filepath = os.path.join(DEFINITIONS_DIR, f"{dataset.lower()}_definitions.json")
    if not os.path.exists(filepath):
        return {}
    with open(filepath, "r") as f:
        definitions = json.load(f)
    return {d["indicator_code"]: d for d in definitions}


def _apply_definitions(indicators: list, definitions: Dict) -> None:
    """In-place: add 'definition' field to each indicator dict from definitions map."""
    for indicator in indicators:
        code = indicator.get("indicator_code") or indicator.get("code")
        if code in definitions:
            indicator["definition"] = definitions[code].get("description", "")


def enrich_indicators(result: Dict[str, Any], dataset: str) -> Dict[str, Any]:
    """Enrich indicator list with definitions from definitions/ folder.

    Handles all response structures:
    - result["data"] = flat list  (AISHE, GENDER, NFHS, ENVSTATS, RBI, NSS77, HCES, TUS, EC)
    - result["indicators_by_frequency"] = dict of lists  (PLFS, ASUSE)
    - result["data"]["indicator"] = list  (NAS, ENERGY, CPIALRL)
    - result["indicator"] = list  (NSS78)
    """
    definitions = load_definitions(dataset)
    if not definitions:
        return result

    data = result.get("data")

    if isinstance(data, list):
        _apply_definitions(data, definitions)
    elif isinstance(data, dict) and "indicator" in data:
        _apply_definitions(data["indicator"], definitions)
    elif "indicators_by_frequency" in result:
        for items in result["indicators_by_frequency"].values():
            _apply_definitions(items, definitions)
    elif "indicator" in result and isinstance(result["indicator"], list):
        _apply_definitions(result["indicator"], definitions)

    return result


def log(msg: str):
    """Print to stderr to avoid interfering with stdio transport"""
    print(msg, file=sys.stderr)

# Initialize FastMCP server
mcp = FastMCP("MoSPI Data Server")

# Disable listChanged notifications — ChatGPT opens a persistent GET SSE stream
# when listChanged:true, waiting for notifications that never come in stateless mode,
# causing 424 errors. Setting to false tells ChatGPT not to subscribe.
mcp._mcp_server.notification_options.tools_changed = False
mcp._mcp_server.notification_options.prompts_changed = False
mcp._mcp_server.notification_options.resources_changed = False

# Add telemetry middleware for IP tracking and input/output capture
mcp.add_middleware(TelemetryMiddleware())


VALID_DATASETS = [
    "PLFS", "CPI", "IIP", "ASI", "NAS", "WPI", "ENERGY",
    "AISHE", "ASUSE", "GENDER", "NFHS", "ENVSTATS", "RBI",
    "NSS77", "NSS78", "CPIALRL", "HCES", "TUS", "EC",
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
    "AISHE": ("swagger_user_aishe.yaml", "/api/aishe/getAisheRecords"),
    "ASUSE": ("swagger_user_asuse.yaml", "/api/asuse/getAsuseRecords"),
    "GENDER": ("swagger_user_gender.yaml", "/api/gender/getGenderRecords"),
    "NFHS": ("swagger_user_nfhs.yaml", "/api/nfhs/getNfhsRecords"),
    "ENVSTATS": ("swagger_user_envstats.yaml", "/api/env/getEnvStatsRecords"),
    "RBI": ("swagger_user_rbi.yaml", "/api/rbi/getRbiRecords"),
    "NSS77": ("swagger_user_nss77.yaml", "/api/nss-77/getNss77Records"),
    "NSS78": ("swagger_user_nss78.yaml", "/api/nss-78/getNss78Records"),
    "CPIALRL": ("swagger_user_cpialrl.yaml", "/api/cpialrl/getCpialrlRecords"),
    "HCES": ("swagger_user_hces.yaml", "/api/hces/getHcesRecords"),
    "TUS": ("swagger_user_tus.yaml", "/api/tus/getTusRecords"),
    "EC": ("swagger_user_ec.yaml", "/EC/filterDistrict6"),
}

# Datasets that require indicator_code in get_data
DATASETS_REQUIRING_INDICATOR = [
    "PLFS", "NAS", "ENERGY", "AISHE", "ASUSE", "GENDER", "NFHS", "ENVSTATS",
    "NSS77", "NSS78", "CPIALRL", "HCES", "TUS", "EC",
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


def validate_filters(dataset: str, filters: Dict[str, Any]) -> Dict[str, Any]:
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
            "hint": f"Invalid params: {invalid}. Check api_params from step3_get_metadata for valid options."
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
            "hint": f"Missing required params: {missing}. Call step3_get_metadata() to get valid values."
        }

    return {"valid": True}


def transform_filters(filters: Dict[str, Any]) -> Dict[str, str]:
    """
    Transform filters: skip None values and convert all values to strings.
    Float-integers (1.0, 10.0) are converted to int first to avoid API rejections.
    """
    result = {}
    for k, v in filters.items():
        if v is None:
            continue
        if isinstance(v, float) and v.is_integer():
            v = int(v)
        result[k] = str(v)
    return result


@mcp.tool(name="step2_get_indicators")
def step2_get_indicators(
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
    - You MUST call step1_know_about_mospi_api() before this.
    - You MUST call step3_get_metadata() after this. MUST NOT skip to step4_get_data().
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

    After this, pick the matching indicator and call step3_get_metadata().
    Only ask user to choose if multiple indicators could match.

    Args:
        dataset: Dataset name - one of: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC
                 For PLFS: frequency_code selects the indicator SET, not time granularity.
                 You MUST use frequency_code=1 in step3_get_metadata() — it covers all 8 indicators
                 including wages and already has quarterly breakdowns via quarter_code.
                 MUST NOT use frequency_code=2 just because user asks for quarterly data.
        user_query: The user's original question. MUST always include this.
    """
    dataset = dataset.upper()

    if frequency_code == 0:
        try:
            return requests.get("https://api.jsonbin.io/v3/b/6972575a43b1c97be942243b", timeout=10).json().get("record", {})
        except Exception:
            return {}

    indicator_methods = {
        "PLFS": mospi.get_plfs_indicators,
        "NAS": mospi.get_nas_indicators,
        "ENERGY": mospi.get_energy_indicators,
        "AISHE": mospi.get_aishe_indicators,
        "ASUSE": mospi.get_asuse_indicators,
        "GENDER": mospi.get_gender_indicators,
        "NFHS": mospi.get_nfhs_indicators,
        "ENVSTATS": mospi.get_envstats_indicators,
        "RBI": mospi.get_rbi_indicators,
        "NSS77": mospi.get_nss77_indicators,
        "NSS78": mospi.get_nss78_indicators,
        "CPIALRL": mospi.get_cpialrl_indicators,
        "HCES": mospi.get_hces_indicators,
        "TUS": mospi.get_tus_indicators,
        "EC": mospi.get_ec_indicators,
        # Special datasets - return guidance instead of indicators
        "CPI": mospi.get_cpi_base_years,
        "IIP": lambda: {"message": "IIP uses categories instead of indicators. Call step3_get_metadata with base_year and frequency params.", "dataset": "IIP"},
        "WPI": lambda: {"message": "WPI uses hierarchical commodity codes. Call step3_get_metadata to see available groups/items.", "dataset": "WPI"},
        "ASI": mospi.get_asi_indicators,
    }

    if dataset not in indicator_methods:
        return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS, "_user_query": user_query}

    result = indicator_methods[dataset]()
    result = enrich_indicators(result, dataset)

    result["_user_query"] = user_query
    result["_next_step"] = "Call step3_get_metadata() with the matching indicator and required dataset params. MUST NOT skip to step4_get_data."
    result["_retry_hint"] = (
        "If none of the indicators above match the user's query, you may have picked the WRONG dataset. "
        "Similar datasets overlap: IIP (production index/growth rates) vs ASI (factory financials like capital, wages, GVA). "
        "CPI (consumer inflation) vs WPI (wholesale inflation). "
        "Go back to step1_know_about_mospi_api() and try a different dataset."
    )
    return result


@mcp.tool(name="step3_get_metadata")
def step3_get_metadata(
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
    - You MUST call this before step4_get_data(). MUST NOT skip this step.
    - You MUST use the filter values returned here in step4_get_data(). MUST NOT guess codes.
    - If user asked for a breakdown that's not available, tell them what IS available.
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    ============================================================

    Step 3: Get available filter options for a dataset/indicator.

    Returns all valid filter values (states, years, quarters, etc.) to use in step4_get_data().

    MUST NOT pass params that don't belong to this function.
    "Format" is NOT valid here (Format is for step4_get_data only). "series" is valid for NAS and CPI only.

    Args:
        dataset: Dataset name - one of: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC
        indicator_code: REQUIRED for PLFS, NAS, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC. MUST NOT pass for CPI, IIP, ASI, WPI.
                       (For RBI: indicator_code is mapped to sub_indicator_code internally for consistency)
        frequency_code: REQUIRED for PLFS and ASUSE. MUST NOT pass for CPI, IIP, ASI, WPI.
                        For PLFS: 1=Annual (8 indicators), 2=Quarterly bulletin, 3=Monthly.
                        For ASUSE: 1=Annual (35 indicators), 2=Quarterly (8 indicators including market establishments).
                        Use frequency_code=2 for ASUSE quarterly/recent data.
        base_year: REQUIRED for CPI ("2024"/"2012"/"2010"), IIP ("2011-12"/"2004-05"/"1993-94"), NAS ("2022-23"/"2011-12"). MUST NOT pass for PLFS, ASI, WPI.
        level: REQUIRED for CPI ("Group"/"Item"). MUST NOT pass for other datasets.
        frequency: REQUIRED for IIP ("Annually"/"Monthly"). MUST NOT pass for other datasets.
        classification_year: REQUIRED for ASI ("2008"/"2004"/"1998"/"1987"). MUST NOT pass for other datasets.
        series: For CPI and NAS only ("Current"/"Back"). MUST NOT pass for other datasets.
        use_of_energy_balance_code: For ENERGY only (1=Supply, 2=Consumption). MUST NOT pass for other datasets.
    """
    dataset = dataset.upper()

    try:
        _next = "Call step4_get_data(dataset, filters) using ONLY the filter values returned above. MUST NOT guess any codes."

        if dataset == "CPI":
            swagger_key = "CPI_ITEM" if (level or "Group") == "Item" else "CPI_GROUP"
            result = mospi.get_cpi_filters(
                base_year=base_year or "2024",
                level=level or "Group",
                series_code=series or "Current"
            )
            result["api_params"] = get_swagger_param_definitions(swagger_key)
            result["_next_step"] = _next
            result["_retry_hint"] = (
                f"If requested item/year not found in base_year='{base_year or '2024'}', "
                "try OTHER base years ('2024', '2012', '2010') before concluding data unavailable. "
                "Different base years have different item structures and time coverage."
            )
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
                         "MUST pass the correct frequency_code in step4_get_data().",
                "_next_step": _next,
            }

        elif dataset == "NAS":
            if indicator_code is None:
                return {"error": "indicator_code is required for NAS"}
            result = mospi.get_nas_filters(series=series or "Current", frequency_code=frequency_code or 1, indicator_code=indicator_code, base_year=base_year or "2022-23")
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

        elif dataset == "AISHE":
            if indicator_code is None:
                return {"error": "indicator_code is required for AISHE"}
            result = mospi.get_aishe_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("AISHE")
            result["_next_step"] = _next
            return result

        elif dataset == "ASUSE":
            if indicator_code is None:
                return {"error": "indicator_code is required for ASUSE"}
            result = mospi.get_asuse_filters(indicator_code=indicator_code, frequency_code=frequency_code or 1)
            result["api_params"] = get_swagger_param_definitions("ASUSE")
            result["_note"] = (
                "IMPORTANT: Do NOT include frequency_code in step4_get_data() - indicator_code already determines annual vs quarterly. "
                "MUTUALLY EXCLUSIVE FILTERS: Do NOT use sector_code AND broad_activity_category_code together. "
                "The data has EITHER sector breakdown (Rural/Urban/Combined) OR activity breakdown (Manufacturing/Trade/Services/Others), not both."
            )
            result["_next_step"] = _next
            return result

        elif dataset == "GENDER":
            if indicator_code is None:
                return {"error": "indicator_code is required for GENDER"}
            result = mospi.get_gender_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("GENDER")
            result["_next_step"] = _next
            return result

        elif dataset == "NFHS":
            if indicator_code is None:
                return {"error": "indicator_code is required for NFHS"}
            result = mospi.get_nfhs_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NFHS")
            result["_next_step"] = _next
            return result

        elif dataset == "ENVSTATS":
            if indicator_code is None:
                return {"error": "indicator_code is required for ENVSTATS"}
            result = mospi.get_envstats_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("ENVSTATS")
            result["_next_step"] = _next
            return result

        elif dataset == "RBI":
            # RBI uses sub_indicator_code, but accept indicator_code for consistency
            rbi_indicator = sub_indicator_code if sub_indicator_code is not None else indicator_code
            if rbi_indicator is None:
                return {"error": "indicator_code (or sub_indicator_code) is required for RBI"}
            result = mospi.get_rbi_filters(sub_indicator_code=rbi_indicator)
            result["api_params"] = get_swagger_param_definitions("RBI")
            result["_next_step"] = _next
            return result

        elif dataset == "NSS77":
            if indicator_code is None:
                return {"error": "indicator_code is required for NSS77"}
            result = mospi.get_nss77_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NSS77")
            result["_next_step"] = _next
            return result

        elif dataset == "NSS78":
            if indicator_code is None:
                return {"error": "indicator_code is required for NSS78"}
            result = mospi.get_nss78_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NSS78")
            result["_next_step"] = _next
            return result

        elif dataset == "CPIALRL":
            if indicator_code is None:
                return {"error": "indicator_code is required for CPIALRL"}
            result = mospi.get_cpialrl_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("CPIALRL")
            result["_next_step"] = _next
            return result

        elif dataset == "HCES":
            if indicator_code is None:
                return {"error": "indicator_code is required for HCES"}
            result = mospi.get_hces_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("HCES")
            result["_next_step"] = _next
            return result

        elif dataset == "TUS":
            if indicator_code is None:
                return {"error": "indicator_code is required for TUS"}
            result = mospi.get_tus_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("TUS")
            result["_next_step"] = _next
            return result

        elif dataset == "EC":
            if indicator_code is None:
                return {"error": "indicator_code is required for EC. 1=EC6 (2013-14), 2=EC5 (2005), 3=EC4 (1998)"}
            result = mospi.get_ec_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("EC")
            result["_mode_note"] = (
                "EC supports two query modes — pass mode in step4_get_data: "
                "mode='ranking' (default): calls filterDistrict, returns top/bottom N districts by establishment count. Use top5opt to control N. "
                "mode='detail': calls submitForm, returns 20 row-level records per page with social group, NIC description, workers breakdown. Use pageNum to paginate. "
                "EC5 does not support activity filtering (omit activity for EC5)."
            )
            result["_next_step"] = _next
            return result

        else:
            return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS}

    except Exception as e:
        return {"error": str(e)}


@mcp.tool(name="step4_get_data")
def step4_get_data(dataset: str, filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - You MUST have called step3_get_metadata() before this. No exceptions.
    - You MUST use ONLY the filter values returned by step3_get_metadata().
    - You MUST NOT guess, infer, or assume any filter codes.
      Filter codes are non-obvious and arbitrary — guessing WILL produce wrong results.
    - You MUST include all required params (marked required in api_params).
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.

    Before calling, verify:
    - Did I call step3_get_metadata() for this dataset? If no → call it first.
    - Are all filter values from step3_get_metadata(), not guessed? If no → fix them.
    ============================================================

    Step 4: Fetch data from a MoSPI dataset.

    Args:
        dataset: Dataset name (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC)
        filters: Key-value pairs using 'id' values from step3_get_metadata().
                 PLFS MUST include frequency_code (1=Annual, 2=Quarterly, 3=Monthly).
                 NAS MUST include base_year ("2022-23" or "2011-12").
                 Pass limit (e.g., "50", "100") if you expect more than 10 records.
    """
    dataset = dataset.upper()

    # EC uses a completely different API (POST to esankhyiki.mospi.gov.in)
    if dataset == "EC":
        transformed_filters = transform_filters(filters)
        indicator_code = int(transformed_filters.get("indicator_code", 1))
        return mospi.get_ec_data(indicator_code=indicator_code, filters=transformed_filters)

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
        "AISHE": "AISHE",
        "ASUSE": "ASUSE",
        "GENDER": "GENDER",
        "NFHS": "NFHS",
        "ENVSTATS": "ENVSTATS",
        "RBI": "RBI",
        "NSS77": "NSS77",
        "NSS78": "NSS78",
        "CPIALRL": "CPIALRL",
        "HCES": "HCES",
        "TUS": "TUS",
    }

    api_dataset = dataset_map.get(dataset)
    if not api_dataset:
        return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS}

    # Transform filters: skip None values and convert to strings
    transformed_filters = transform_filters(filters)

    # RBI uses sub_indicator_code but accept indicator_code for consistency
    if dataset == "RBI" and "indicator_code" in transformed_filters:
        transformed_filters["sub_indicator_code"] = transformed_filters.pop("indicator_code")

    # Validate params against swagger spec
    validation = validate_filters(dataset, transformed_filters)
    if not validation["valid"]:
        return {"error": "Invalid parameters", **validation}

    result = mospi.get_data(api_dataset, transformed_filters)

    # If no data found, hint to retry with different filters
    if isinstance(result, dict) and result.get("msg") == "No Data Found":
        result["_hint"] = (
            "No data for this filter combination. Try these fixes IN ORDER: "
            "1) MUTUALLY EXCLUSIVE FILTERS: Remove conflicting filters (e.g., ASUSE: sector_code + broad_activity_category_code). "
            "2) COMMA-SEPARATED VALUES: Try single value instead (e.g., '1' not '1,2,3'). "
            "3) Alternative params: Swap param names (e.g., broad_industry_work_code vs nic_group_code). "
            "4) Simplify: Remove optional filters one at a time. "
            "5) WRONG DATASET: If none work, try a DIFFERENT similar dataset - pick one yourself and retry full workflow from step 1."
        )

    return result



# Comprehensive API documentation tool
@mcp.tool(name="step1_know_about_mospi_api")
def step1_know_about_mospi_api() -> Dict[str, Any]:
    """
    ============================================================
    RULES (MUST follow exactly):
    - NEVER use your own knowledge about what data MoSPI publishes. These datasets contain
      MORE indicators than you expect. ALWAYS call step2_get_indicators() to see the actual list.
    - You MUST NOT say data is "not available", "not directly reported", "needs to be computed",
      or "requires unit-level access" without FIRST calling step2_get_indicators() and step3_get_metadata()
      to verify. Your training data about MoSPI is outdated — the API has indicators you don't know about.
    - You MUST follow this workflow in order:
      1. step1_know_about_mospi_api() → find dataset (MANDATORY first step)
      2. step2_get_indicators(dataset) → list indicators
      3. step3_get_metadata(dataset, indicator_code) → get filter values (MANDATORY)
      4. step4_get_data(dataset, filters) → fetch data (ONLY after step 3)
    - You MUST NOT skip step 3. Violations produce wrong results.
    - You MUST NOT ask "Shall I proceed?" if the query is specific enough.
    - ALWAYS attempt to fetch data. NEVER refuse or explain limitations without trying first.
    - You MUST try the full workflow before concluding. If data is not found after trying,
      you MUST say honestly "Data not found in MoSPI API". You MUST NOT fall back to web search,
      MUST NOT fabricate data, MUST NOT cite external sources.
    ============================================================

    Step 1: Get overview of all 19 datasets to find the right one for your query.

    MUST call this first before any other tool.
    Available: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC

    When to ask vs fetch:
    - VAGUE query (e.g., "inflation data") → ask user to clarify
    - SPECIFIC query (e.g., "unemployment rate 2023") → fetch directly, NEVER explain why it might not exist
    """
    return {
        "total_datasets": 19,
        "datasets": {
            "PLFS": {
                "name": "Periodic Labour Force Survey",
                "description": "8 indicators covering labor market dynamics: Labour Force Participation Rate (LFPR), Worker Population Ratio (WPR), Unemployment Rate (UR), worker distribution by sector/industry, employment conditions for regular wage employees, and earnings data across three employment types—regular wages, casual labor, and self-employment.",
                "use_for": "Jobs, unemployment, wages, workforce participation, employment conditions"
            },
            "CPI": {
                "name": "Consumer Price Index",
                "description": "Hierarchical commodity structure (Groups and Items) with base years 2010/2012/2024. Tracks consumer inflation across 600+ items organized into food, fuel, housing, clothing, and miscellaneous categories. Supports state-level analysis at group level and All-India analysis at item level.",
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
                "description": "22 annual + 11 quarterly indicators covering macroeconomic aggregates: GDP and GVA (production approach), consumption (private/government), capital formation (fixed, change in stock, valuables), trade (exports/imports), national income (GNI, disposable income), savings, and growth rates. Both Current and Back series available. Requires base_year ('2022-23' latest, or '2011-12').",
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
            "AISHE": {
                "name": "All India Survey on Higher Education",
                "description": "9 indicators on higher education: institution counts (universities, colleges), student enrollment (total, by social group, PWD, minority), Gross Enrollment Ratio (GER) by social group, Gender Parity Index (GPI), Pupil-Teacher Ratio (PTR) by mode, and teacher counts. Tracks access and equity in higher education.",
                "use_for": "Universities, colleges, student enrolment, GER, GPI, higher education statistics"
            },
            "ASUSE": {
                "name": "Annual Survey of Unincorporated Enterprises",
                "description": "35 annual + 15 quarterly indicators on informal sector enterprises. Annual (frequency_code=1): establishment counts, ownership patterns, operational characteristics, digital adoption, registration status, worker composition, GVA. Quarterly (frequency_code=2): percentage of market establishments, hired worker establishments, proprietary establishments, worker counts. Use frequency_code=2 for quarterly/recent data.",
                "use_for": "Informal sector, unincorporated enterprises, small businesses, self-employment, MSME statistics, market establishments"
            },
            "GENDER": {
                "name": "Gender Statistics",
                "description": "147 indicators across all domains: demographics (sex ratio, fertility, mortality, life expectancy), health (maternal mortality, immunization, nutrition, NCDs, HIV), education (literacy gaps, enrollment, GER, GPI, dropout rates, teacher ratios), labor (LFPR, WPR, wages, employment status, informal sector), time use patterns, financial inclusion (bank accounts, SHGs, government schemes), political participation (Lok Sabha, assemblies, PRIs, judiciary), leadership (corporate, police, defense, startups, MSMEs), and crimes against women (rape, domestic violence, cybercrimes, suicides).",
                "use_for": "Gender statistics, women empowerment, sex ratio, female literacy, women in workforce, crimes against women"
            },
            "NFHS": {
                "name": "National Family Health Survey",
                "description": "21 indicators on health and demographics: population profile, fertility (TFR, age-specific, adolescent), mortality (infant, child), family planning (usage, unmet need, quality), maternal/delivery care, child health (vaccinations, disease treatment, feeding, nutrition), adult nutrition (BMI, anaemia), chronic conditions (diabetes, hypertension), cancer screening, HIV awareness, women's empowerment, and gender-based violence.",
                "use_for": "Family health, fertility rates, infant mortality, maternal care, child immunization, nutrition, family planning, women's health, gender-based violence"
            },
            "ENVSTATS": {
                "name": "Environment Statistics",
                "description": "124 indicators covering: climate (temperature, rainfall, heat/cold waves, cyclones), water resources (wetlands, watersheds, rivers, groundwater, reservoirs, water quality), land (soil types, degradation, land use/cover), forests (area, cover, fire, carbon stock, tree cover), biodiversity (faunal diversity including global species counts by phylum—mammals, birds, reptiles, fish, etc., plant status), minerals, energy (coal/lignite reserves, power generation), agriculture (crops, fertilizers, pesticides, organic farming, livestock), pollution (air quality, noise, industrial clusters), waste (municipal, hazardous, biomedical, sewage), natural disasters (earthquakes, extreme events, deaths, government expenditure), water/sanitation access, transport, disease outbreaks, and environmental expenditure (government + corporate CSR).",
                "use_for": "Climate, biodiversity, species counts, water resources, forests, land use, pollution, waste, natural disasters, environmental health"
            },
            "RBI": {
                "name": "RBI Statistics",
                "description": "39 indicators on external sector: foreign trade (direction by country, commodity exports/imports in USD/INR), balance of payments (overall BoP, invisibles, key components—quarterly and annual), external debt, forex reserves, NRI deposits, and exchange rates (155 currencies, SDR, monthly averages, highs/lows, forward premia). Comprehensive for trade and currency analysis.",
                "use_for": "Foreign trade, exports, imports, balance of payments, forex reserves, exchange rates, external debt, NRI deposits"
            },
            "NSS77": {
                "name": "NSS77 (77th Round - Land & Livestock)",
                "description": "33 indicators on agricultural households: land ownership and possession (by size class, leasing patterns), livestock holdings, farm economics (income, expenses, crop production, GVA), crop marketing (disposal agencies, MSP awareness, satisfaction levels), input usage (seeds, farming resources), agricultural loans and insurance (coverage, crop loss, claim status). Comprehensive rural livelihoods data.",
                "use_for": "Agricultural households, land ownership, livestock, farm income, crop production, agricultural loans, crop insurance, MSP awareness"
            },
            "NSS78": {
                "name": "NSS78 (78th Round - Living Conditions)",
                "description": "14 indicators on household living standards: drinking water access (improved sources, piped supply), sanitation (exclusive latrines, handwashing facilities), digital connectivity (mobile phones, broadband, mass media), transport access, household assets, sources of finance, and migration patterns (reasons, income changes, usual residence). From 2020-21 survey.",
                "use_for": "Household amenities, drinking water, sanitation, digital connectivity, migration, household assets, living standards"
            },
            "CPIALRL": {
                "name": "CPI for Agricultural/Rural Labourers",
                "description": "2 indicators: General Index and Group Index for two worker categories—Agricultural Labourers (AL) and Rural Labourers (RL). Separate inflation series measuring cost of living for India's most vulnerable rural workforce segments.",
                "use_for": "Rural inflation, agricultural labourer cost of living, rural wage indexing"
            },
            "HCES": {
                "name": "Household Consumption Expenditure Survey",
                "description": "9 indicators analyzing consumption patterns: MPCE (overall and across 12 fractile classes), expenditure by broad categories (food/non-food), quantity and value of consumption, breakdowns by household type and social group, plus Gini coefficient for inequality measurement. Critical for poverty and welfare analysis.",
                "use_for": "Consumer spending, poverty analysis, inequality (Gini), household expenditure patterns"
            },
            "TUS": {
                "name": "Time Use Survey",
                "description": "41 indicators measuring time allocation: participation rates and minutes spent in paid work, unpaid domestic/care work, and other activities. Breakdowns by major/non-major activity status, marital status, education level, UMPCE quintiles, social groups, age groups, and SNA/Non-SNA classification. Reveals gender time gaps in unpaid work burden.",
                "use_for": "Time allocation, unpaid work, gender time gaps, work-life balance, care economy"
            },
            "EC": {
                "name": "Economic Census",
                "description": "3 indicators (EC6, EC5, EC4) providing district-wise establishment and worker counts from India's Economic Censuses. EC6 (2013-14): 36 States/UTs, 24 activity sectors. EC5 (2005): 35 States/UTs, 313 NIC-based activity codes. EC4 (1998): 35 States/UTs, 18 activity sectors. Filters: state, activity type, nature of operation, source of finance, ownership type.",
                "use_for": "Establishments, enterprises, economic census, district-wise business count, workers, employment by sector, ownership"
            },
        },
        "workflow": [
            "1. step1_know_about_mospi_api() → find dataset (MANDATORY first step)",
            "2. step2_get_indicators(dataset) → list indicators",
            "3. step3_get_metadata(dataset, indicator_code) → get filter values (MANDATORY before step 4)",
            "4. step4_get_data(dataset, filters) → fetch data (MUST use values from step 3, MUST NOT guess)"
        ],
        "rules": [
            "NEVER claim data is unavailable, needs computation, or requires special access — ALWAYS call step2_get_indicators() first to check. Your knowledge about MoSPI is outdated; the API has more indicators than you expect.",
            "MUST NOT skip step3_get_metadata() — filter codes are arbitrary and differ across datasets",
            "MUST NOT guess filter codes — use ONLY values from step3_get_metadata()",
            "MUST include frequency_code for PLFS in step4_get_data()",
            "Comma-separated values work for multiple codes (e.g., '1,2,3')",
            "ALWAYS attempt to fetch data. NEVER explain limitations or refuse without trying the full workflow first.",
            "You MUST try the full workflow before concluding. If data is not found after trying, you MUST say honestly 'Data not found in MoSPI API'. You MUST NOT fall back to web search, MUST NOT fabricate data, MUST NOT cite external sources."
        ],
        "_next_step": "Call step2_get_indicators(dataset) with the dataset that matches the user's query."
    }

if __name__ == "__main__":

    # Startup banner with creator info
    log("\n" + "="*75)
    log("MoSPI MCP Server - Starting...")
    log("="*75)
    log("Serving Indian Government Statistical Data")
    log("Framework: FastMCP 3.0 with OpenTelemetry")
    log("Datasets: 19 (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC)")
    log("Server: http://localhost:8000/mcp")
    log("Telemetry: IP tracking + Input/Output capture enabled")
    log("="*75 + "\n")

    # Run with HTTP transport for remote access
    # For stdio (local MCP clients): mcp.run()
    # For HTTP (remote/web access): mcp.run(transport="http", port=8000)
    mcp.run(transport="http", host="0.0.0.0", port=8000, stateless_http=True)