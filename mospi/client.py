"""
MoSPI API Client
Handles all API calls to the MoSPI data portal
"""

import logging
import requests
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def _upstream_error(operation: str, exc: Exception) -> Dict[str, Any]:
    """Return a user-safe error while logging internal details server-side."""
    logger.warning("%s failed: %s", operation, exc)
    return {
        "error": f"{operation} failed due to an upstream API error.",
        "statusCode": False,
    }


class MoSPI:
    """
    A unified class to interact with various MoSPI APIs.
    """

    def __init__(self, base_url: str = "https://api.mospi.gov.in"):
        self.base_url = base_url
        self.api_endpoints = {
            "PLFS": "/api/plfs/getData",
            "CPI_Group": "/api/cpi/getCPIIndex",
            "CPI_Item": "/api/cpi/getItemIndex",
            "IIP_Annual": "/api/iip/getIIPAnnual",
            "IIP_Monthly": "/api/iip/getIIPMonthly",
            "ASI": "/api/asi/getASIData",
            "NAS": "/api/nas/getNASData",
            "WPI": "/api/wpi/getWpiRecords",
            "Energy": "/api/energy/getEnergyRecords",
        }

    def get_data(self, dataset_name: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Fetches data from a specified MoSPI dataset.
        """
        endpoint_path = self.api_endpoints.get(dataset_name)
        if not endpoint_path:
            return {"error": f"Dataset '{dataset_name}' not found."}

        full_url = f"{self.base_url}{endpoint_path}"

        # Clean up params - remove None values
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        try:
            response = requests.get(full_url, params=params, timeout=30)
            response.raise_for_status()

            # Check if CSV format was requested
            format_param = params.get("Format", "JSON") if params else "JSON"
            if format_param == "CSV":
                return {"data": response.text, "format": "CSV"}
            else:
                return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_data", e)
        except ValueError as e:
            logger.warning("get_data returned invalid JSON: %s", e)
            return {"error": "get_data failed due to invalid upstream response.", "statusCode": False}

    # =========================================================================
    # PLFS Metadata Methods
    # =========================================================================

    def get_plfs_indicators(self) -> Dict[str, Any]:
        """Fetch PLFS indicators grouped by frequency_code."""
        url = f"{self.base_url}/api/plfs/getIndicatorListByFrequency"
        result = {}
        try:
            for fc, label in [(1, "Annual"), (2, "Quarterly"), (3, "Monthly")]:
                response = requests.get(url, params={"frequency_code": fc}, timeout=30)
                response.raise_for_status()
                data = response.json()
                result[f"frequency_code_{fc}_{label}"] = data.get("data", [])
            return {
                "indicators_by_frequency": result,
                "_note": "frequency_code=1 (Annual) has 8 indicators including all wages. "
                         "It already contains quarterly breakdowns — use quarter_code to filter. "
                         "frequency_code=2 (Quarterly) has 4 indicators for quarterly bulletin tables. "
                         "frequency_code=3 (Monthly) has 3 indicators (2025+ data only). "
                         "Pick the frequency_code whose indicator set matches the query.",
                "statusCode": True,
            }
        except requests.RequestException as e:
            return _upstream_error("get_plfs_indicators", e)

    def get_plfs_filters(
        self,
        indicator_code: int,
        frequency_code: int = 1,
        year: Optional[str] = None,
        month_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch available PLFS filters for given indicator/frequency/year/month."""
        params = {
            "indicator_code": indicator_code,
            "frequency_code": frequency_code,
        }
        if year:
            params["year"] = year
        if month_code:
            params["month_code"] = month_code

        try:
            response = requests.get(
                f"{self.base_url}/api/plfs/getFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_plfs_filters", e)

    # =========================================================================
    # CPI Metadata Methods
    # =========================================================================

    def get_cpi_filters(
        self,
        base_year: str = "2012",
        level: str = "Group"
    ) -> Dict[str, Any]:
        """Fetch available CPI filters for given base year and level.

        Args:
            base_year: "2012" or "2010"
            level: "Group" or "Item"
        """
        params = {
            "base_year": base_year,
            "level": level,
        }

        try:
            response = requests.get(
                f"{self.base_url}/api/cpi/getCpiFilterByLevelAndBaseYear",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_cpi_filters", e)

    # =========================================================================
    # IIP Metadata Methods
    # =========================================================================

    def get_iip_filters(
        self,
        base_year: str = "2011-12",
        frequency: str = "Annually"
    ) -> Dict[str, Any]:
        """Fetch available IIP filters for given base year and frequency.

        Args:
            base_year: "2011-12", "2004-05", or "1993-94"
            frequency: "Annually" or "Monthly"
        """
        params = {
            "base_year": base_year,
            "frequency": frequency,
        }

        try:
            response = requests.get(
                f"{self.base_url}/api/iip/getIipFilter",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_iip_filters", e)

    # =========================================================================
    # ASI Metadata Methods
    # =========================================================================

    def get_asi_classification_years(self) -> Dict[str, Any]:
        """Fetch list of available NIC classification years from MoSPI API."""
        try:
            response = requests.get(
                f"{self.base_url}/api/asi/getNicClassificationYear",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_asi_classification_years", e)

    def get_asi_filters(
        self,
        classification_year: str = "2008"
    ) -> Dict[str, Any]:
        """Fetch available ASI filters for given classification year.

        Args:
            classification_year: "2008", "2004", "1998", or "1987"
        """
        params = {
            "classification_year": classification_year,
        }

        try:
            response = requests.get(
                f"{self.base_url}/api/asi/getAsiFilter",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_asi_filters", e)

    def get_asi_indicators(self) -> Dict[str, Any]:
        """Fetch ASI indicator list from the filter endpoint (using classification_year=2008).

        Returns indicators plus classification year info so the LLM knows
        it must pass classification_year in get_metadata/get_data.
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/asi/getAsiFilter",
                params={"classification_year": "2008"},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            filter_data = data.get("data", data)
            # Extract indicator list if present
            indicators = None
            if isinstance(filter_data, dict):
                indicators = filter_data.get("indicator", filter_data.get("indicators", None))
            result = {
                "dataset": "ASI",
                "classification_years": ["2008", "2004", "1998", "1987"],
                "_note": "classification_year is REQUIRED for ASI. It is the NIC classification version, NOT the data year. "
                         "Pick based on which data year you need: "
                         "'1987' → 1992-93 to 1997-98 | "
                         "'1998' → 1998-99 to 2003-04 | "
                         "'2004' → 2004-05 to 2007-08 | "
                         "'2008' → 2008-09 to 2023-24. "
                         "Pass classification_year in 3_get_metadata() and 4_get_data().",
                "statusCode": True,
            }
            if indicators:
                result["indicators"] = indicators
            else:
                result["filters"] = filter_data
            return result
        except requests.RequestException as e:
            return _upstream_error("get_asi_indicators", e)

    # =========================================================================
    # NAS Metadata Methods
    # =========================================================================

    def get_nas_indicators(self) -> Dict[str, Any]:
        """Fetch list of all NAS indicators from MoSPI API."""
        try:
            response = requests.get(
                f"{self.base_url}/api/nas/getNasIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_nas_indicators", e)

    def get_nas_filters(
        self,
        series: str = "Current",
        frequency_code: int = 1,
        indicator_code: int = 1
    ) -> Dict[str, Any]:
        """Fetch available NAS filters for given series/frequency/indicator.

        Args:
            series: "Current" or "Back"
            frequency_code: 1 (Annually) or 2 (Quarterly, Current series only)
            indicator_code: Indicator code (1-22 for Annual, 1-11 for Quarterly)
        """
        params = {
            "series": series,
            "frequency_code": frequency_code,
            "indicator_code": indicator_code,
        }

        try:
            response = requests.get(
                f"{self.base_url}/api/nas/getNasFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_nas_filters", e)

    # =========================================================================
    # WPI Metadata Methods
    # =========================================================================

    def get_wpi_filters(self) -> Dict[str, Any]:
        """Fetch available WPI filters from MoSPI API.

        Returns:
            Available filters: year, month, major_group, group, sub_group, sub_sub_group, item
        """
        try:
            response = requests.get(
                f"{self.base_url}/api/wpi/getWpiData",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_wpi_filters", e)

    # =========================================================================
    # Energy Metadata Methods
    # =========================================================================

    def get_energy_indicators(self) -> Dict[str, Any]:
        """Fetch list of Energy indicators from MoSPI API."""
        try:
            response = requests.get(
                f"{self.base_url}/api/energy/getEnergyIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_energy_indicators", e)

    def get_energy_filters(
        self,
        indicator_code: int = 1,
        use_of_energy_balance_code: int = 1
    ) -> Dict[str, Any]:
        """Fetch available Energy filters for given indicator and balance type.

        Args:
            indicator_code: 1 (KToE) or 2 (PetaJoules)
            use_of_energy_balance_code: 1 (Supply) or 2 (Consumption)
        """
        params = {
            "indicator_code": indicator_code,
            "use_of_energy_balance_code": use_of_energy_balance_code,
        }

        try:
            response = requests.get(
                f"{self.base_url}/api/energy/getEnergyFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return _upstream_error("get_energy_filters", e)



# Global instance
mospi = MoSPI()
