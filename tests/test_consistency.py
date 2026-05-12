"""Deterministic checks that dataset registration is internally consistent.

The MCP server has many places that must agree on the dataset list and its
total count: VALID_DATASETS, DATASET_SWAGGER, EXPECTED_DATASETS, the
startup banner, total_datasets in list_datasets, README, CONTRIBUTING, and
the per-dataset swagger YAML files. These tests catch any drift across
those locations, replacing the most common class of bug we've shipped
(counts saying 19/20/21/22 in different files) with a hard CI failure.

LLM-based PR review explicitly does NOT duplicate these checks — see
.github/workflows/ai-review.yml.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# CPI has two internal endpoint aliases (Group/Item) that exist in
# DATASET_SWAGGER but are not user-facing datasets in VALID_DATASETS.
INTERNAL_SWAGGER_ALIASES = {"CPI_GROUP", "CPI_ITEM"}


def _python_constant(file_path: str, name: str):
    """Pull a top-level constant via AST. Returns list / set / dict-keys."""
    tree = ast.parse((ROOT / file_path).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == name:
                v = node.value
                if isinstance(v, ast.List):
                    return [e.value for e in v.elts if isinstance(e, ast.Constant)]
                if isinstance(v, ast.Set):
                    return {e.value for e in v.elts if isinstance(e, ast.Constant)}
                if isinstance(v, ast.Dict):
                    return [k.value for k in v.keys if isinstance(k, ast.Constant)]
    raise AssertionError(f"{name} not found as a top-level assignment in {file_path}")


# ---- module-scoped fixtures so we don't reparse files for each test ----

@pytest.fixture(scope="module")
def valid_datasets() -> list[str]:
    return _python_constant("mospi_server.py", "VALID_DATASETS")


@pytest.fixture(scope="module")
def server_text() -> str:
    return (ROOT / "mospi_server.py").read_text()


@pytest.fixture(scope="module")
def readme_text() -> str:
    return (ROOT / "README.md").read_text()


@pytest.fixture(scope="module")
def contributing_text() -> str:
    return (ROOT / "CONTRIBUTING.md").read_text()


# ---- the checks ----

def test_dataset_swagger_keys_match_valid_datasets(valid_datasets):
    """DATASET_SWAGGER keys (minus internal aliases) must equal VALID_DATASETS."""
    swagger_keys = set(_python_constant("mospi_server.py", "DATASET_SWAGGER")) - INTERNAL_SWAGGER_ALIASES
    valid = set(valid_datasets)
    assert swagger_keys == valid, (
        f"VALID_DATASETS and DATASET_SWAGGER disagree.\n"
        f"  In DATASET_SWAGGER but not VALID_DATASETS: {sorted(swagger_keys - valid)}\n"
        f"  In VALID_DATASETS but not DATASET_SWAGGER: {sorted(valid - swagger_keys)}"
    )


def test_expected_datasets_matches_valid_datasets(valid_datasets):
    """tests EXPECTED_DATASETS must equal server VALID_DATASETS."""
    expected = _python_constant("tests/test_mcp_server.py", "EXPECTED_DATASETS")
    valid = set(valid_datasets)
    assert expected == valid, (
        f"EXPECTED_DATASETS does not match VALID_DATASETS. Diff: {expected ^ valid}"
    )


def test_total_datasets_field_matches_count(server_text, valid_datasets):
    """list_datasets() must return total_datasets == len(VALID_DATASETS)."""
    m = re.search(r'"total_datasets":\s*(\d+)', server_text)
    assert m, 'Could not find `"total_datasets": N` in mospi_server.py'
    declared = int(m.group(1))
    assert declared == len(valid_datasets), (
        f'total_datasets={declared} but VALID_DATASETS has {len(valid_datasets)} entries'
    )


def test_startup_banner_matches_valid_datasets(server_text, valid_datasets):
    """Startup banner 'Datasets: N (A, B, C, ...)' must agree with VALID_DATASETS."""
    m = re.search(r'Datasets:\s*(\d+)\s*\(([^)]+)\)', server_text)
    assert m, 'Startup banner pattern not found in mospi_server.py'
    banner_count = int(m.group(1))
    banner_names = {s.strip() for s in m.group(2).split(",")}
    valid = set(valid_datasets)
    assert banner_count == len(valid_datasets), (
        f'Banner says {banner_count} datasets, VALID_DATASETS has {len(valid_datasets)}'
    )
    assert banner_names == valid, f'Banner names differ from VALID_DATASETS: {banner_names ^ valid}'


def test_readme_feature_count_matches(readme_text, valid_datasets):
    """README 'N statistical datasets' line must match VALID_DATASETS count."""
    m = re.search(r'(\d+)\s+statistical\s+datasets', readme_text)
    assert m, "Could not find 'N statistical datasets' in README.md"
    assert int(m.group(1)) == len(valid_datasets), (
        f'README features says {m.group(1)} datasets, VALID_DATASETS has {len(valid_datasets)}'
    )


def test_readme_dataset_table_rows_match(readme_text, valid_datasets):
    """README dataset table must have one row per dataset in VALID_DATASETS."""
    rows = re.findall(r'^\|\s*\*\*([A-Z][A-Z0-9]+)\*\*\s*\|', readme_text, re.MULTILINE)
    table = set(rows)
    valid = set(valid_datasets)
    assert table == valid, (
        f"README dataset table rows disagree with VALID_DATASETS.\n"
        f"  In table but not VALID_DATASETS: {sorted(table - valid)}\n"
        f"  In VALID_DATASETS but not table: {sorted(valid - table)}"
    )


def test_contributing_counts_match(contributing_text, valid_datasets):
    """Any 'N datasets' line in CONTRIBUTING.md must match VALID_DATASETS."""
    n = len(valid_datasets)
    for pattern, label in [
        (r'supports\s+(\d+)\s+datasets', "supports N datasets"),
        (r'covering\s+all\s+(\d+)\s+datasets', "covering all N datasets"),
    ]:
        m = re.search(pattern, contributing_text)
        if m is None:
            continue  # not all repos have both lines
        assert int(m.group(1)) == n, (
            f"CONTRIBUTING '{label}' says {m.group(1)}, VALID_DATASETS has {n}"
        )


def test_swagger_files_exist_for_each_dataset(valid_datasets):
    """Every dataset must have a swagger_user_<name>.yaml file."""
    swagger_dir = ROOT / "swagger"
    present = {p.name for p in swagger_dir.iterdir() if p.is_file()}
    missing = []
    for ds in valid_datasets:
        if ds in INTERNAL_SWAGGER_ALIASES:
            continue
        expected = f"swagger_user_{ds.lower()}.yaml"
        if expected not in present:
            missing.append(expected)
    assert not missing, f"Missing swagger files: {missing}"


def test_indicator_methods_dict_has_entry_per_dataset(server_text, valid_datasets):
    """get_indicators's indicator_methods dict must have a key for every dataset."""
    m = re.search(r'indicator_methods\s*=\s*\{(.*?)\n\s*\}', server_text, re.DOTALL)
    assert m, "indicator_methods dict not found in mospi_server.py"
    keys = set(re.findall(r'"([A-Z][A-Z0-9_]+)"', m.group(1)))
    missing = set(valid_datasets) - keys
    assert not missing, f"indicator_methods missing entries for: {sorted(missing)}"


def test_dataset_map_in_get_data_has_entry_per_dataset(server_text, valid_datasets):
    """get_data's dataset_map must cover every dataset, with documented exceptions."""
    # Architectural exceptions:
    # - CPI auto-routes to CPI_GROUP / CPI_ITEM inside get_data, so either
    #   form is acceptable.
    # - EC uses a completely separate POST endpoint and is short-circuited
    #   BEFORE dataset_map is consulted; it doesn't need to be in the map.
    SPECIAL_CASED = {"EC"}
    m = re.search(r'dataset_map\s*=\s*\{(.*?)\n\s*\}', server_text, re.DOTALL)
    assert m, "dataset_map dict not found in mospi_server.py"
    keys = set(re.findall(r'"([A-Z][A-Z0-9_]+)"', m.group(1)))
    missing = []
    for ds in valid_datasets:
        if ds in SPECIAL_CASED:
            continue
        if ds == "CPI":
            if not ({"CPI", "CPI_GROUP", "CPI_ITEM"} & keys):
                missing.append(ds)
        elif ds not in keys:
            missing.append(ds)
    assert not missing, f"dataset_map missing entries for: {sorted(missing)}"
