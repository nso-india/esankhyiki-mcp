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
    "NSS77", "NSS78", "NSS79", "CPIALRL", "HCES", "TUS", "EC",
]

# Maps dataset key -> (swagger_yaml_file, endpoint_path)
# Swagger YAMLs are the single source of truth for valid API parameters.
DATASET_SWAGGER = {
    "PLFS": ("swagger_user_plfs.yaml", "/api/plfs/getData"),
    "CPI": ("swagger_user_cpi.yaml", "/api/cpi/getCPIIndex"),
    "CPI_GROUP": ("swagger_user_cpi.yaml", "/api/cpi/getCPIIndex"),
    "CPI_ITEM": ("swagger_user_cpi.yaml", "/api/cpi/getItemIndex"),
    "IIP": ("swagger_user_iip.yaml", "/api/iip/getIipData"),
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
    "NSS79": ("swagger_user_nss79.yaml", "/api/nss-79/getNSS79Records"),
    "CPIALRL": ("swagger_user_cpialrl.yaml", "/api/cpialrl/getCpialrlRecords"),
    "HCES": ("swagger_user_hces.yaml", "/api/hces/getHcesRecords"),
    "TUS": ("swagger_user_tus.yaml", "/api/tus/getTusRecords"),
    "EC": ("swagger_user_ec.yaml", "/EC/filterDistrict6"),
}

# Datasets that require indicator_code in get_data
DATASETS_REQUIRING_INDICATOR = [
    "PLFS", "NAS", "ENERGY", "AISHE", "ASUSE", "GENDER", "NFHS", "ENVSTATS",
    "NSS77", "NSS78", "NSS79", "CPIALRL", "HCES", "TUS", "EC",
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
            "hint": f"Invalid params: {invalid}. Valid params: {valid_params}."
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
            "hint": f"Missing required params: {missing}."
        }

    return {"valid": True}


def _check_empty_metadata(result, dataset, **params):
    """Annotate metadata result if upstream returned empty filter values."""
    data = result.get("filter_values", result.get("data", {}))
    if isinstance(data, dict):
        inner = data.get("data", data)
        if isinstance(inner, dict):
            values = [v for v in inner.values() if isinstance(v, list)]
            is_empty = (not inner) or (values and all(len(v) == 0 for v in values))
        else:
            is_empty = False
    else:
        is_empty = False

    if is_empty:
        param_str = ", ".join(f"{k}={v}" for k, v in params.items() if v is not None)
        result["troubleshooting"] = (
            f"The upstream API returned empty filter values for {dataset} "
            f"with {param_str}. This usually means the parameter values are "
            "out of range. Check get_indicators() for valid codes."
        )
        result["suggestion"] = f"Call get_indicators(dataset='{dataset}') to verify valid indicator codes."
    return result


def _safe_int(value, param_name: str):
    """Validate and coerce a value to int. Returns (int_value, None) or (None, error_dict)."""
    if value is None:
        return None, None
    try:
        return int(value), None
    except (ValueError, TypeError):
        return None, {"error": f"{param_name} must be an integer, got: {value!r}"}


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


@mcp.tool(name="get_indicators", title="Browse Dataset Indicators", annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True})
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
) -> dict:
    """
    Returns the full list of available indicators for a given dataset.

    Datasets often have broader coverage than expected — for example, ASI covers
    57 indicators (capital structure, wages, employment, GVA, fuel consumption),
    and GENDER covers 147 indicators across health, education, labor, and crime.

    For PLFS and ASUSE, indicators are grouped by frequency_code:
      - PLFS frequency_code=1 (Annual): all 8 indicators including wages
      - PLFS frequency_code=2 (Quarterly): indicators 1-3 only
      - PLFS frequency_code=3 (Monthly): indicators 1-3 only
      frequency_code selects the indicator set, not time granularity.

    Step 2 of: list_datasets → get_indicators → get_metadata → get_data

    Args:
        dataset: Dataset name — one of: PLFS, CPI, IIP, ASI, NAS, WPI,
                 ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI,
                 NSS77, NSS78, NSS79, CPIALRL, HCES, TUS, EC.
                 For CPI, IIP, WPI: returns available base years and frequencies.
        user_query: The user's original question, used for context.

    Returns:
        dict with indicator list (codes, names, definitions where available).
        For frequency-based datasets (PLFS, ASUSE), indicators are grouped
        by frequency_code.
    """
    dataset = dataset.upper()

    frequency_code, err = _safe_int(frequency_code, "frequency_code")
    if err:
        return err

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
        "NSS79": mospi.get_nss79_indicators,
        "CPIALRL": mospi.get_cpialrl_indicators,
        "HCES": mospi.get_hces_indicators,
        "TUS": mospi.get_tus_indicators,
        "EC": mospi.get_ec_indicators,
        # Special datasets - return guidance instead of indicators
        "CPI": mospi.get_cpi_base_years,
        "IIP": mospi.get_iip_base_years,
        "WPI": mospi.get_wpi_base_years,
        "ASI": mospi.get_asi_indicators,
    }

    if dataset not in indicator_methods:
        return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS, "user_query": user_query}

    result = indicator_methods[dataset]()
    result = enrich_indicators(result, dataset)

    result["user_query"] = user_query
    result["next_step"] = "get_metadata(dataset, indicator_code) to retrieve valid filter values."
    result["related_datasets"] = (
        "Datasets with overlapping coverage: "
        "IIP (production index, growth rates) vs ASI (factory financials: capital, wages, GVA). "
        "CPI (consumer inflation) vs WPI (wholesale inflation)."
    )
    return result


@mcp.tool(name="get_metadata", title="Get Filter Options & Parameters", annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True})
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
) -> dict:
    """
    Returns the valid filter values (states, years, quarters, etc.) for a
    given dataset and indicator.

    Filter codes are arbitrary and dataset-specific — for example, PLFS
    state_code 99 means "All India", and NAS frequency_code 1 means "Annual".
    These values cannot be inferred or guessed from parameter names alone.

    The returned filter_values and api_params should be used as-is when
    calling get_data.

    Step 3 of: list_datasets → get_indicators → get_metadata → get_data

    Args:
        dataset: Dataset name (same values as get_indicators).
        indicator_code: Required for: PLFS, NAS, ENERGY, AISHE, ASUSE, GENDER,
                        NFHS, ENVSTATS, RBI, NSS77, NSS78, NSS79, CPIALRL, HCES, TUS, EC.
                        Not applicable for: CPI, IIP, ASI, WPI.
                        For RBI, this maps to sub_indicator_code internally.
        frequency_code: Required for PLFS and ASUSE.
                        PLFS: 1=Annual, 2=Quarterly bulletin, 3=Monthly.
                        ASUSE: 1=Annual, 2=Quarterly.
        base_year: Required for CPI ("2024"/"2012"/"2010"),
                   IIP ("2011-12"/"2004-05"/"1993-94"),
                   NAS ("2022-23"/"2011-12"),
                   WPI ("2011-12"/"2004-05"/"1993-94").
                   Not applicable for PLFS, ASI.
        level: Required for CPI ("Group"/"Item").
        frequency: Required for IIP ("Annually"/"Monthly").
        classification_year: Required for ASI ("2008"/"2004"/"1998"/"1987").
        series: For CPI and NAS only ("Current"/"Back").
        use_of_energy_balance_code: For ENERGY only (1=Supply, 2=Consumption).

    Returns:
        dict with 'filter_values' (valid codes for each parameter),
        'api_params' (parameter definitions), and dataset-specific
        parameter documentation.
    """
    dataset = dataset.upper()

    # Validate numeric params — FastMCP doesn't enforce type hints
    indicator_code, err = _safe_int(indicator_code, "indicator_code")
    if err: return err
    frequency_code, err = _safe_int(frequency_code, "frequency_code")
    if err: return err
    use_of_energy_balance_code, err = _safe_int(use_of_energy_balance_code, "use_of_energy_balance_code")
    if err: return err
    sub_indicator_code, err = _safe_int(sub_indicator_code, "sub_indicator_code")
    if err: return err

    try:
        _next = "get_data(dataset, filters) using the filter values returned above."

        if dataset == "CPI":
            swagger_key = "CPI_ITEM" if (level or "Group") == "Item" else "CPI_GROUP"
            result = mospi.get_cpi_filters(
                base_year=base_year or "2024",
                level=level or "Group",
                series_code=series or "Current"
            )
            result["api_params"] = get_swagger_param_definitions(swagger_key)
            result["next_step"] = _next
            result["base_year_coverage"] = (
                f"Current base_year='{base_year or '2024'}'. "
                "Other available base years: '2024', '2012', '2010'. "
                "Each base year has different item structures and time coverage."
            )
            return _check_empty_metadata(result, dataset, base_year=base_year, level=level, series=series)

        elif dataset == "IIP":
            result = mospi.get_iip_filters(base_year=base_year or "2011-12", frequency=frequency or "Annually")
            result["api_params"] = get_swagger_param_definitions("IIP")
            result["next_step"] = _next
            result["base_year_coverage"] = (
                f"Current base_year='{base_year or '2011-12'}', frequency='{frequency or 'Annually'}'. "
                "Other available base years: '2011-12', '2004-05', '1993-94'. "
                "frequency='Annually': use financial_year param (YYYY-YY). "
                "frequency='Monthly': use year (YYYY) and month_code params."
            )
            return _check_empty_metadata(result, dataset, base_year=base_year, frequency=frequency)

        elif dataset == "ASI":
            result = mospi.get_asi_filters(classification_year=classification_year or "2008")
            result["api_params"] = get_swagger_param_definitions("ASI")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, classification_year=classification_year)

        elif dataset == "WPI":
            result = mospi.get_wpi_filters(base_year=base_year or "2011-12")
            result["api_params"] = get_swagger_param_definitions("WPI")
            result["next_step"] = _next
            result["base_year_coverage"] = (
                f"Current base_year='{base_year or '2011-12'}'. "
                "Other available base years: '2011-12', '2004-05', '1993-94'. "
                "Each base year has different commodity structures and time coverage."
            )
            return _check_empty_metadata(result, dataset, base_year=base_year)

        elif dataset == "PLFS":
            if indicator_code is None:
                return {"error": "indicator_code is required for PLFS"}

            filters = mospi.get_plfs_filters(indicator_code=indicator_code, frequency_code=frequency_code or 1)

            result = {
                "dataset": "PLFS",
                "filter_values": filters,
                "api_params": get_swagger_param_definitions("PLFS"),
                "parameter_notes": "frequency_code selects the indicator set, not time granularity. "
                         "indicator_code accepts a single integer only; comma-separated values cause a 500 error. "
                         "frequency_code=1 (Annual, 2017-18 to 2023-24): indicators 1=LFPR, 2=WPR, 3=UR, "
                         "4=Worker distribution, 5=Employment conditions, 6=Salaried wages, 7=Casual wages, "
                         "8=Self-employment earnings. Year format: YYYY-YY (e.g. 2023-24). "
                         "frequency_code=2 (Quarterly bulletin, 2017-18 to 2025-26): indicators 1-3 only. "
                         "Year format: YYYY-YY. quarter_code: 2=JUL-SEP, 3=OCT-DEC, 4=JAN-MAR, 5=APR-JUN. "
                         "frequency_code=3 (Monthly, 2025 onwards): indicators 1-3 only. "
                         "Year format: YYYY (e.g. 2025). "
                         "year_type_code: 1=Agriculture Year (financial year YYYY-YY), 2=Calendar Year (YYYY). "
                         "state_code=99 for All India; omitting state_code returns all states.",
                "next_step": _next,
            }
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code, frequency_code=frequency_code)

        elif dataset == "NAS":
            if indicator_code is None:
                return {"error": "indicator_code is required for NAS"}
            result = mospi.get_nas_filters(series=series or "Current", frequency_code=frequency_code or 1, indicator_code=indicator_code, base_year=base_year or "2022-23")
            result["api_params"] = get_swagger_param_definitions("NAS")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code, base_year=base_year, frequency_code=frequency_code)

        elif dataset == "ENERGY":
            ind_code = indicator_code or 1
            energy_code = use_of_energy_balance_code or 1
            result = mospi.get_energy_filters(indicator_code=ind_code, use_of_energy_balance_code=energy_code)
            result["api_params"] = get_swagger_param_definitions("ENERGY")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code, use_of_energy_balance_code=use_of_energy_balance_code)

        elif dataset == "AISHE":
            if indicator_code is None:
                return {"error": "indicator_code is required for AISHE"}
            result = mospi.get_aishe_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("AISHE")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "ASUSE":
            if indicator_code is None:
                return {"error": "indicator_code is required for ASUSE"}
            result = mospi.get_asuse_filters(indicator_code=indicator_code, frequency_code=frequency_code or 1)
            result["api_params"] = get_swagger_param_definitions("ASUSE")
            result["parameter_notes"] = (
                "frequency_code is not a parameter for get_data; the indicator_code "
                "already determines annual vs quarterly data. "
                "sector_code and broad_activity_category_code are mutually exclusive: "
                "data has either sector breakdown (Rural/Urban/Combined) or activity "
                "breakdown (Manufacturing/Trade/Services/Others), not both."
            )
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code, frequency_code=frequency_code)

        elif dataset == "GENDER":
            if indicator_code is None:
                return {"error": "indicator_code is required for GENDER"}
            result = mospi.get_gender_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("GENDER")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "NFHS":
            if indicator_code is None:
                return {"error": "indicator_code is required for NFHS"}
            result = mospi.get_nfhs_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NFHS")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "ENVSTATS":
            if indicator_code is None:
                return {"error": "indicator_code is required for ENVSTATS"}
            result = mospi.get_envstats_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("ENVSTATS")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "RBI":
            # RBI uses sub_indicator_code, but accept indicator_code for consistency
            rbi_indicator = sub_indicator_code if sub_indicator_code is not None else indicator_code
            if rbi_indicator is None:
                return {"error": "indicator_code (or sub_indicator_code) is required for RBI"}
            result = mospi.get_rbi_filters(sub_indicator_code=rbi_indicator)
            result["api_params"] = get_swagger_param_definitions("RBI")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "NSS77":
            if indicator_code is None:
                return {"error": "indicator_code is required for NSS77"}
            result = mospi.get_nss77_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NSS77")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "NSS78":
            if indicator_code is None:
                return {"error": "indicator_code is required for NSS78"}
            result = mospi.get_nss78_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NSS78")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "NSS79":
            if indicator_code is None:
                return {"error": "indicator_code is required for NSS79"}
            result = mospi.get_nss79_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("NSS79")
            result["_next_step"] = _next
            return result

        elif dataset == "CPIALRL":
            if indicator_code is None:
                return {"error": "indicator_code is required for CPIALRL"}
            result = mospi.get_cpialrl_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("CPIALRL")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "HCES":
            if indicator_code is None:
                return {"error": "indicator_code is required for HCES"}
            result = mospi.get_hces_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("HCES")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "TUS":
            if indicator_code is None:
                return {"error": "indicator_code is required for TUS"}
            result = mospi.get_tus_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("TUS")
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        elif dataset == "EC":
            if indicator_code is None:
                return {"error": "indicator_code is required for EC. 1=EC6 (2013-14), 2=EC5 (2005), 3=EC4 (1998)"}
            result = mospi.get_ec_filters(indicator_code=indicator_code)
            result["api_params"] = get_swagger_param_definitions("EC")
            result["query_modes"] = (
                "EC supports two query modes via the 'mode' parameter in get_data: "
                "mode='ranking' (default): returns top/bottom N districts by establishment count. "
                "Control N with top5opt parameter. "
                "mode='detail': returns 20 row-level records per page with social group, NIC "
                "description, and workers breakdown. Use pageNum to paginate. "
                "EC5 (indicator_code=2) does not support activity filtering."
            )
            result["next_step"] = _next
            return _check_empty_metadata(result, dataset, indicator_code=indicator_code)

        else:
            return {"error": f"Unknown dataset: {dataset}", "valid_datasets": VALID_DATASETS}

    except Exception as e:
        return {"error": str(e)}


@mcp.tool(name="get_data", title="Fetch Statistical Data", annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": True})
def get_data(dataset: str, filters: Dict[str, Any]) -> dict:
    """
    Fetches statistical data from a MoSPI dataset.

    This is the final step of the workflow. It requires filter values from
    get_metadata — filter codes are arbitrary (e.g., indicator_code=3 means
    "Unemployment Rate" in PLFS but something different in other datasets).

    All filter parameters including limit and page go inside the filters dict,
    not as top-level arguments.

    Step 4 of: list_datasets → get_indicators → get_metadata → get_data

    Args:
        dataset: Dataset name (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY,
                 AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77,
                 NSS78, NSS79, CPIALRL, HCES, TUS, EC).
                 CPI auto-routes to Group or Item endpoint based on
                 whether filters contain item_code.
                 IIP uses a single endpoint; pass frequency="Annually" or
                 frequency="Monthly" in filters.
        filters: Key-value pairs from get_metadata filter_values.
                 PLFS requires frequency_code (1=Annual, 2=Quarterly, 3=Monthly).
                 NAS requires base_year ("2022-23" or "2011-12").
                 Pass limit (e.g., "50") to retrieve more than 10 records.

    Returns:
        dict with statistical records, or an error/validation message
        if parameters are invalid.
    """
    dataset = dataset.upper()

    # EC uses a completely different API (POST to esankhyiki.mospi.gov.in)
    if dataset == "EC":
        transformed_filters = transform_filters(filters)
        ic, err = _safe_int(transformed_filters.get("indicator_code", 1), "indicator_code")
        if err: return err
        return mospi.get_ec_data(indicator_code=ic, filters=transformed_filters)

    # Auto-route CPI and IIP based on filters provided
    if dataset == "CPI":
        if "item_code" in filters:
            dataset = "CPI_ITEM"
        else:
            dataset = "CPI_GROUP"

    # Map friendly names to API dataset keys
    dataset_map = {
        "CPI_GROUP": "CPI_Group",
        "CPI_ITEM": "CPI_Item",
        "IIP": "IIP",
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
        "NSS79": "NSS79",
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

    # Upstream error (500s, timeouts, etc.)
    if isinstance(result, dict) and "error" in result and "msg" not in result:
        filter_str = ", ".join(f"{k}={v}" for k, v in transformed_filters.items() if k not in ("Format", "limit", "page"))
        result["troubleshooting"] = (
            f"The upstream API returned an error for dataset '{dataset}' "
            f"with filters: {filter_str}. "
            "Common causes: 1) indicator_code or other numeric codes are out of range. "
            "2) Non-integer values like '1.0', 'abc', or empty strings in numeric fields. "
            "3) Comma-separated values where only single values are accepted (e.g. NAS indicator_code)."
        )
        result["suggestion"] = f"Call get_indicators(dataset='{dataset}') to check valid codes, then get_metadata() for filter values."

    # If no data found, hint to retry with different filters
    if isinstance(result, dict) and result.get("msg") == "No Data Found":
        result["troubleshooting"] = (
            f"No data found for dataset '{dataset}' with the given filters. "
            "Most common causes: "
            "1) Out-of-range codes — verify indicator_code and other numeric codes "
            "match values from get_metadata(). "
            "2) Incompatible filter combination — some filters are mutually exclusive. "
            "3) Comma-separated values where only single values are accepted. "
            "4) Optional filters narrowing results too much — try removing optional params."
        )
        result["suggestion"] = f"Call get_metadata(dataset='{dataset}') to verify valid filter values."

    return result



# Comprehensive API documentation tool
@mcp.tool(name="list_datasets", title="Discover Available Datasets", annotations={"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False})
def list_datasets() -> dict:
    """
    Returns an overview of all 19 MoSPI statistical datasets with descriptions and coverage.
    This is the starting point — call this first to identify the right dataset.

    The API covers 500+ indicators across employment, prices, industry, national
    accounts, health, education, environment, trade, and more. Each dataset has
    its own indicator codes, filter parameters, and valid values — these are not
    standardized and cannot be inferred or guessed from parameter names alone.

    Four-step workflow (each step depends on the previous):
      1. list_datasets() — identify the dataset
      2. get_indicators(dataset) — list available indicators
      3. get_metadata(dataset, indicator_code) — retrieve valid filter values
      4. get_data(dataset, filters) — fetch the data

    Returns:
        dict with 'datasets' (name, description, use_for for each dataset)
        and 'workflow' (the four-step sequence).
    """
    return {
        "total_datasets": 20,
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
                "use_for": "IIP index, industrial production index, manufacturing index, mining/electricity index, growth rates, textiles, metals, vehicles, consumer durables, capital goods"
            },
            "ASI": {
                "name": "Annual Survey of Industries",
                "description": "57 indicators providing deep factory-sector analytics: capital structure (fixed/working capital, investments), production metrics (output, inputs, value added), employment details (workers by gender, contract status, mandays), wage components (salaries, bonuses, employer contributions), fuel consumption patterns, and profitability measures. Uses NIC classification across 4 classification years (1987-2008).",
                "use_for": "Factory-level financials: working capital, fixed capital, wages, employment counts, GVA, fuel consumption, profitability (distinct from IIP which covers production indices)"
            },
            "NAS": {
                "name": "National Accounts Statistics",
                "description": "22 annual + 11 quarterly indicators covering macroeconomic aggregates: GDP and GVA (production approach), consumption (private/government), capital formation (fixed, change in stock, valuables), trade (exports/imports), national income (GNI, disposable income), savings, and growth rates. Both Current and Back series available. Requires base_year ('2022-23' latest, or '2011-12').",
                "use_for": "GDP, economic growth, national income, sectoral contribution, macro analysis"
            },
            "WPI": {
                "name": "Wholesale Price Index",
                "description": "Hierarchical commodity structure with 1000+ items across 5 levels: Major Groups (Primary articles, Fuel & power, Manufactured products, Food index) → Groups (22) → Sub-groups (90+) → Sub-sub-groups → Items. Tracks wholesale/producer price inflation monthly. Three base years available: 2011-12 (latest, default), 2004-05, and 1993-94.",
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
            "NSS79": {
                "name": "NSS79 (79th Round - CAMS)",
                "description": "28 indicators from the Comprehensive Annual Modular Survey (CAMS). Education (indicators 1-9): literacy rates, numeracy, mean years of schooling, primary enrolment, secondary education attainment, out-of-school children, science & technology graduates, youth in training, and NEET youth (not in education, employment or training). Health expenditure (10-13): average and out-of-pocket medical expenditure for both hospitalised and non-hospitalised treatment. Financial inclusion (14-15): bank/financial account ownership and number of borrowers per 1 lakh persons. Digital literacy & connectivity (16-23): mobile usage ability and actual usage, internet ability and actual usage, 4G coverage, and advanced digital skills (file sharing, copy-paste, online search/email/banking). Household living conditions (24-28): asset possession, transport access, birth registration, clean cooking fuel, and access to safe drinking water and improved sanitation.",
                "use_for": "Literacy, numeracy, school enrolment, NEET youth, health expenditure, out-of-pocket medical costs, financial inclusion, mobile/internet usage, digital skills, 4G coverage, household assets, clean fuel, drinking water, sanitation, birth registration, CAMS survey"
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
            "1. list_datasets() — identify the relevant dataset",
            "2. get_indicators(dataset) — list available indicators",
            "3. get_metadata(dataset, indicator_code) — retrieve valid filter values",
            "4. get_data(dataset, filters) — fetch the data using filter values from step 3"
        ],
        "next_step": "get_indicators(dataset) to list available indicators for the chosen dataset."
    }

if __name__ == "__main__":

    # Startup banner with creator info
    log("\n" + "="*75)
    log("MoSPI MCP Server - Starting...")
    log("="*75)
    log("Serving Indian Government Statistical Data")
    log("Framework: FastMCP 3.0 with OpenTelemetry")
    log("Datasets: 20 (PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY, AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, NSS79, CPIALRL, HCES, TUS, EC)")
    log("Server: http://localhost:8000/mcp")
    log("Telemetry: IP tracking + Input/Output capture enabled")
    log("="*75 + "\n")

    # Run with HTTP transport for remote access
    # For stdio (local MCP clients): mcp.run()
    # For HTTP (remote/web access): mcp.run(transport="http", port=8000)
    mcp.run(transport="http", host="0.0.0.0", port=8000, stateless_http=True)