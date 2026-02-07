import mospi_server


def test_plfs_metadata_requires_indicator_code():
    result = mospi_server.get_metadata("PLFS")
    assert result["error"] == "indicator_code is required for PLFS"


def test_get_data_rejects_unknown_dataset():
    result = mospi_server.get_data("UNKNOWN", {})
    assert "error" in result
    assert "Unknown dataset" in result["error"]


def test_get_data_rejects_unknown_filter_param():
    result = mospi_server.get_data(
        "PLFS",
        {
            "indicator_code": "3",
            "frequency_code": "1",
            "invalid_param": "oops",
        },
    )
    assert result["error"] == "Invalid parameters"
    assert "invalid_param" in result["invalid_params"]


def test_get_metadata_error_is_sanitized(monkeypatch):
    def _boom():
        raise RuntimeError("sensitive backend details")

    monkeypatch.setattr(mospi_server.mospi, "get_wpi_filters", _boom)
    result = mospi_server.get_metadata("WPI")
    assert result["statusCode"] is False
    assert result["error"] == "Metadata lookup failed due to an internal server error."
