"""
Search module for MoSPI metadata.
Flattens all indicators and filter dimensions into a searchable list,
performs case-insensitive substring matching, and returns matches with
API param names and hierarchy context.
"""

import os
import yaml
import requests
from typing import Any

from mospi.cache import metadata_cache

BASE_URL = "https://api.mospi.gov.in"
SWAGGER_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "swagger")

# Dataset descriptions for the response
DATASET_DESCRIPTIONS = {
    "PLFS": "Periodic Labour Force Survey — employment, unemployment, wages, workforce participation",
    "CPI": "Consumer Price Index — retail inflation, cost of living, commodity prices",
    "IIP": "Index of Industrial Production — manufacturing, mining, electricity output",
    "ASI": "Annual Survey of Industries — factory financials, capital, wages, employment",
    "NAS": "National Accounts Statistics — GDP, economic growth, national income",
    "WPI": "Wholesale Price Index — wholesale/producer price inflation, commodity prices",
    "ENERGY": "Energy Statistics — energy production, consumption, fuel mix",
}

# Required params per dataset from swagger specs
# (param_name, swagger_file, endpoint_path)
DATASET_SWAGGER = {
    "PLFS": ("swagger_user_plfs.yaml", "/api/plfs/getData"),
    "CPI": ("swagger_user_cpi.yaml", "/api/cpi/getCPIIndex"),
    "IIP": ("swagger_user_iip.yaml", "/api/iip/getIIPAnnual"),
    "ASI": ("swagger_user_asi.yaml", "/api/asi/getASIData"),
    "NAS": ("swagger_user_nas.yaml", "/api/nas/getNASData"),
    "WPI": ("swagger_user_wpi.yaml", "/api/wpi/getWpiRecords"),
    "ENERGY": ("swagger_user_energy.yaml", "/api/energy/getEnergyRecords"),
}

# Hierarchical dimension mappings (parent -> child relationships)
# Used to build context strings for search results
HIERARCHY_MAPS = {
    "WPI": {
        "dimensions": ["major_group", "group", "sub_group", "sub_sub_group", "item"],
        "code_keys": ["major_group_code", "group_code", "sub_group_code", "sub_sub_group_code", "item_code"],
        "name_keys": ["major_group_name", "group_name", "sub_group_name", "sub_sub_group_name", "item_name"],
    },
    "ASI": {
        "dimensions": ["nic_2_digit", "nic_3_digit", "nic_4_digit"],
        "code_keys": ["nic_code", "nic_code", "nic_code"],
        "name_keys": ["nic_name", "nic_name", "nic_name"],
        "parent_key": "parent_nic_code",
    },
    "IIP": {
        "dimensions": ["category", "subcategory"],
        "code_keys": ["category_code", "subcategory_code"],
        "name_keys": ["category_name", "subcategory_name"],
    },
    "CPI": {
        "dimensions": ["group", "subgroup"],
        "code_keys": ["group_code", "subgroup_code"],
        "name_keys": ["group_name", "subgroup_name"],
    },
    "NAS": {
        "dimensions": ["industry", "subindustry"],
        "code_keys": ["industry_code", "subindustry_code"],
        "name_keys": ["industry_name", "subindustry_name"],
    },
}


def _fetch_json(url: str, params: dict = None) -> dict:
    """Fetch JSON from a URL with caching."""
    cache_key = f"{url}:{params}"
    cached = metadata_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        metadata_cache.set(cache_key, data)
        return data
    except Exception as e:
        return {"error": str(e)}


def _get_required_params(dataset: str) -> list[dict]:
    """Get required params from swagger spec for a dataset."""
    if dataset not in DATASET_SWAGGER:
        return []
    yaml_file, endpoint_path = DATASET_SWAGGER[dataset]
    swagger_path = os.path.join(SWAGGER_DIR, yaml_file)
    if not os.path.exists(swagger_path):
        return []
    with open(swagger_path, "r") as f:
        spec = yaml.safe_load(f)
    params = spec.get("paths", {}).get(endpoint_path, {}).get("get", {}).get("parameters", [])
    return [p for p in params if p.get("required") and p["name"] != "Format"]


def _load_all_metadata(dataset: str) -> list[dict]:
    """
    Load all indicators and filter values for a dataset.
    Returns a flat list of searchable entries:
    [{"param": "indicator_code", "code": 3, "name": "Unemployment Rate", "context": ""}, ...]
    """
    entries = []
    dataset = dataset.upper()

    if dataset == "PLFS":
        # Indicators (fetch all frequency codes)
        for fc in [1, 2, 3]:
            freq_label = {1: "Annual", 2: "Quarterly", 3: "Monthly"}[fc]
            data = _fetch_json(f"{BASE_URL}/api/plfs/getIndicatorListByFrequency", {"frequency_code": fc})
            for ind in data.get("data", []):
                entries.append({
                    "param": "indicator_code",
                    "code": ind["indicator_code"],
                    "name": ind["description"],
                    "context": f"frequency_code={fc} ({freq_label})",
                })

        # Filters (use indicator_code=3 as representative — filters are same across indicators)
        data = _fetch_json(f"{BASE_URL}/api/plfs/getFilterByIndicatorId", {"indicator_code": 3, "frequency_code": 1})
        filters = data.get("data", {})
        for dim_name, values in filters.items():
            if not isinstance(values, list):
                continue
            for item in values:
                code_key = next((k for k in item.keys() if k.endswith("_code") or k == "year"), None)
                name_key = next((k for k in item.keys() if k in ("description", "name") or k == "year"), None)
                if code_key:
                    param_name = code_key if code_key != "year" else "year"
                    entries.append({
                        "param": param_name,
                        "code": item[code_key],
                        "name": item.get(name_key, str(item[code_key])) if name_key else str(item[code_key]),
                    })

        # Add frequency_code options
        for fc, label in [(1, "Annual"), (2, "Quarterly"), (3, "Monthly")]:
            entries.append({"param": "frequency_code", "code": fc, "name": label})

    elif dataset == "CPI":
        data = _fetch_json(f"{BASE_URL}/api/cpi/getCpiFilterByLevelAndBaseYear", {"base_year": "2012", "level": "Group"})
        filters = data.get("data", {})

        # Add base_year and level options (not in filter response)
        for by in ["2012", "2010"]:
            entries.append({"param": "base_year", "code": by, "name": f"Base Year {by}"})
        for lvl in ["Group", "Item"]:
            entries.append({"param": "level", "code": lvl, "name": f"Level: {lvl}"})

        _flatten_filters(entries, filters, dataset)

    elif dataset == "IIP":
        data = _fetch_json(f"{BASE_URL}/api/iip/getIipFilter", {"base_year": "2011-12", "frequency": "Annually"})
        filters = data.get("data", {})

        # Add base_year and frequency options
        for by in ["2011-12", "2004-05", "1993-94"]:
            entries.append({"param": "base_year", "code": by, "name": f"Base Year {by}"})
        for freq in ["Annually", "Monthly"]:
            entries.append({"param": "frequency", "code": freq, "name": freq})

        _flatten_filters(entries, filters, dataset)

    elif dataset == "ASI":
        data = _fetch_json(f"{BASE_URL}/api/asi/getAsiFilter", {"classification_year": "2008"})
        filters = data.get("data", {})

        # Add classification_year options
        for cy in ["2008", "2004", "1998", "1987"]:
            entries.append({"param": "classification_year", "code": cy, "name": f"NIC Classification {cy}"})

        _flatten_filters(entries, filters, dataset)

    elif dataset == "NAS":
        # Indicators
        data = _fetch_json(f"{BASE_URL}/api/nas/getNasIndicatorList")
        ind_data = data.get("data", {})

        # Series options
        for s in ind_data.get("series", []):
            entries.append({"param": "series", "code": s["series"], "name": f"Series: {s['series']}"})

        # Frequency options
        for f_item in ind_data.get("frequency", []):
            entries.append({
                "param": "frequency_code",
                "code": f_item["frequency_code"],
                "name": f_item["description"],
                "context": f"series: {f_item.get('series', '')}",
            })

        # Annual indicators
        for ind in ind_data.get("indicator", []):
            entries.append({
                "param": "indicator_code",
                "code": ind["indicator_code"],
                "name": ind["description"],
                "context": "Annual",
            })

        # Quarterly indicators
        for ind in ind_data.get("quarter_indicator", []):
            entries.append({
                "param": "indicator_code",
                "code": ind["indicator_code"],
                "name": ind["description"],
                "context": "Quarterly",
            })

        # Filters
        filters_data = _fetch_json(f"{BASE_URL}/api/nas/getNasFilterByIndicatorId",
                                   {"series": "Current", "frequency_code": 1, "indicator_code": 1})
        _flatten_filters(entries, filters_data.get("data", {}), dataset)

    elif dataset == "WPI":
        data = _fetch_json(f"{BASE_URL}/api/wpi/getWpiData")
        _flatten_filters(entries, data.get("data", {}), dataset)

    elif dataset == "ENERGY":
        # Indicators
        data = _fetch_json(f"{BASE_URL}/api/energy/getEnergyIndicatorList")
        ind_data = data.get("data", {})

        for ind in ind_data.get("indicator", []):
            entries.append({
                "param": "indicator_code",
                "code": ind["indicator_code"],
                "name": ind["description"],
            })

        for balance in ind_data.get("use_of_energy_balance", []):
            entries.append({
                "param": "use_of_energy_balance_code",
                "code": balance["use_of_energy_balance_code"],
                "name": balance["use_of_energy_balance_name"],
            })

        # Filters — load all indicator × balance combinations so sub-commodities
        # and sectors are searchable regardless of which combo the query needs.
        # Merge filter dicts to deduplicate entries that appear in multiple combos.
        merged_filters = {}
        for ind_code in [ind["indicator_code"] for ind in ind_data.get("indicator", [])]:
            for bal_code in [b["use_of_energy_balance_code"] for b in ind_data.get("use_of_energy_balance", [])]:
                filters_data = _fetch_json(f"{BASE_URL}/api/energy/getEnergyFilterByIndicatorId",
                                           {"indicator_code": ind_code, "use_of_energy_balance_code": bal_code})
                for dim_name, values in filters_data.get("data", {}).items():
                    if not isinstance(values, list):
                        continue
                    existing = {str(v): v for v in merged_filters.get(dim_name, [])}
                    for item in values:
                        key = str(item)
                        if key not in existing:
                            existing[key] = item
                    merged_filters[dim_name] = list(existing.values())
        _flatten_filters(entries, merged_filters, dataset)

    return entries


def _flatten_filters(entries: list, filters: dict, dataset: str) -> None:
    """Flatten filter dimension values into searchable entries."""
    hierarchy = HIERARCHY_MAPS.get(dataset)

    # Build parent lookup for hierarchical dimensions
    parent_lookup = {}
    if hierarchy:
        for dim_name in hierarchy["dimensions"]:
            values = filters.get(dim_name, [])
            for item in values:
                code_key = next((k for k in item.keys() if k.endswith("_code")), None)
                name_key = next((k for k in item.keys() if k.endswith("_name")), None)
                if code_key:
                    parent_lookup[str(item[code_key])] = item.get(name_key, "")

    for dim_name, values in filters.items():
        if not isinstance(values, list):
            continue

        for item in values:
            # Find the code and name keys
            code_key = next((k for k in item.keys() if k.endswith("_code") or k == "year"), None)
            name_key = next((k for k in item.keys()
                            if k.endswith("_name") or k == "description" or k == "year"
                            ), None)

            if code_key is None:
                # Handle simple entries like {"year": "2017-18"}, {"series": "Current"}
                for k, v in item.items():
                    if k not in ("viz",):
                        entries.append({"param": k, "code": v, "name": str(v)})
                continue

            param_name = code_key
            code_val = item[code_key]
            name_val = str(item.get(name_key, code_val)) if name_key else str(code_val)

            entry = {"param": param_name, "code": code_val, "name": name_val}

            # Add hierarchy context if applicable
            if hierarchy and dim_name in hierarchy["dimensions"]:
                # Add dimension level (e.g., "2-digit" from "nic_2_digit")
                level_label = dim_name  # default
                for part in dim_name.split("_"):
                    if "digit" in part or part.isdigit():
                        level_label = dim_name.replace("_", " ")
                        break

                parent_code_key = hierarchy.get("parent_key")
                if parent_code_key and parent_code_key in item:
                    parent_code = str(item[parent_code_key])
                    parent_name = parent_lookup.get(parent_code, parent_code)
                    entry["context"] = f"{level_label}, parent: {parent_name}"
                elif "parent_nic_code" in item:
                    parent_code = str(item["parent_nic_code"])
                    parent_name = parent_lookup.get(parent_code, parent_code)
                    entry["context"] = f"{level_label}, parent: {parent_name}"
                elif dim_name in ("subgroup", "subcategory", "subindustry"):
                    for pk in item.keys():
                        if pk.endswith("_code") and pk != code_key:
                            parent_code = str(item[pk])
                            parent_name = parent_lookup.get(parent_code, parent_code)
                            entry["context"] = f"{dim_name}, parent: {parent_name}"
                            break
                else:
                    # Top-level dimension (no parent) — still label it
                    entry["context"] = f"{level_label} (top-level)"

            entries.append(entry)


def search_dataset(dataset: str, search_terms: list[str]) -> dict[str, Any]:
    """
    Search a dataset's metadata for matching indicators and filter values.

    Returns:
        {
            "dataset": "PLFS",
            "description": "...",
            "matches": [{"param": "indicator_code", "code": 3, "name": "...", "context": "..."}],
            "unmatched_required_params": {"frequency_code": {"description": "...", "options": [...]}}
        }
    """
    dataset = dataset.upper()

    if dataset not in DATASET_DESCRIPTIONS:
        return {
            "error": f"Unknown dataset: {dataset}",
            "valid_datasets": list(DATASET_DESCRIPTIONS.keys()),
        }

    # Load all metadata entries
    all_entries = _load_all_metadata(dataset)

    if not all_entries:
        return {
            "dataset": dataset,
            "description": DATASET_DESCRIPTIONS[dataset],
            "matches": [],
            "error": "Could not load metadata for this dataset",
        }

    # Search: case-insensitive substring match, OR across terms
    matches = []
    matched_params = set()

    for entry in all_entries:
        searchable = str(entry["name"]).lower()
        context_str = str(entry.get("context", "")).lower()
        for term in search_terms:
            term_lower = term.lower()
            if term_lower in searchable or term_lower in context_str or term_lower in str(entry["code"]).lower():
                matches.append(entry)
                matched_params.add(entry["param"])
                break  # Don't double-add if multiple terms match same entry

    # Determine unmatched required params
    required_params = _get_required_params(dataset)
    unmatched_required = {}

    for param_def in required_params:
        param_name = param_def["name"]
        if param_name not in matched_params:
            # Find all options for this param from the full entries list
            options = [
                {"code": e["code"], "name": e["name"]}
                for e in all_entries
                if e["param"] == param_name
            ]
            # Dedupe
            seen = set()
            unique_options = []
            for opt in options:
                key = (str(opt["code"]), opt["name"])
                if key not in seen:
                    seen.add(key)
                    unique_options.append(opt)

            if unique_options:
                unmatched_required[param_name] = {
                    "description": param_def.get("description", param_name),
                    "options": unique_options,
                }

    return {
        "dataset": dataset,
        "description": DATASET_DESCRIPTIONS[dataset],
        "matches": matches,
        "unmatched_required_params": unmatched_required if unmatched_required else None,
    }
