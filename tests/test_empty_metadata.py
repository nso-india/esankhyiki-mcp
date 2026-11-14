"""Offline checks for the empty-filter-values warning on get_metadata results.

Upstream filter endpoints come in two shapes: wrapped under "data" or
"filter_values" (most datasets), or flat with filter lists at the top level
(NSS78). The warning must fire only when the filter values are really empty,
in either shape. No network calls here.
"""

import pytest

from mospi_server import _check_empty_metadata


API_PARAMS = [{"name": "indicator_code", "required": True}]


@pytest.mark.parametrize(
    "result",
    [
        pytest.param(
            {"data": {"state": [{"state_code": 1}], "sector": []}, "api_params": API_PARAMS},
            id="wrapped-data",
        ),
        pytest.param(
            {"filter_values": {"state": [{"state_code": 1}]}, "api_params": API_PARAMS},
            id="wrapped-filter-values",
        ),
        pytest.param(
            {"state": [{"code": 1, "name": "Andhra Pradesh"}], "sector": [], "api_params": API_PARAMS},
            id="flat-nss78",
        ),
    ],
)
def test_populated_filters_are_not_flagged(result):
    checked = _check_empty_metadata(result, "NSS78", indicator_code=2)
    assert "troubleshooting" not in checked
    assert "suggestion" not in checked


@pytest.mark.parametrize(
    "result",
    [
        pytest.param({"data": {"state": [], "sector": []}, "api_params": API_PARAMS}, id="wrapped-empty-lists"),
        pytest.param({"data": {}, "api_params": API_PARAMS}, id="wrapped-empty-dict"),
        pytest.param({"state": [], "sector": [], "api_params": API_PARAMS}, id="flat-empty-lists"),
        pytest.param({"api_params": API_PARAMS, "next_step": "..."}, id="flat-nothing-upstream"),
    ],
)
def test_empty_filters_are_flagged(result):
    checked = _check_empty_metadata(result, "NSS78", indicator_code=99)
    assert "indicator_code=99" in checked["troubleshooting"]
    assert "get_indicators" in checked["suggestion"]
