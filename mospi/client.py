"""
MoSPI API Client
Handles all API calls to the MoSPI data portal
"""

import ssl
import requests
import yaml, os
import math, random
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3 import PoolManager
from urllib3.util.retry import Retry


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class LegacyRenegotiationAdapter(HTTPAdapter):
    """HTTP adapter that enables OpenSSL legacy server connect for MoSPI."""

    def __init__(self, ssl_context: ssl.SSLContext, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = self.ssl_context
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )


class MoSPI:
    """
    A unified class to interact with various MoSPI APIs.
    """

    def __init__(self, base_url: str = "https://api.mospi.gov.in"):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        ssl_context = ssl.create_default_context()
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        legacy_server_connect = getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        ssl_context.options |= legacy_server_connect
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        retry = Retry(
            total=3,
            connect=2,
            read=2,
            status=3,
            backoff_factor=0.6,
            status_forcelist=sorted(RETRYABLE_STATUS_CODES),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        adapter = LegacyRenegotiationAdapter(ssl_context=ssl_context, max_retries=retry)
        self.session.verify = False
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.api_endpoints = {
            "PLFS": "/api/plfs/getData",
            "CPI_Group": "/api/cpi/getCPIIndex",
            "CPI_Item": "/api/cpi/getItemIndex",
            "IIP": "/api/iip/getIipData",
            "ASI": "/api/asi/getASIData",
            "NAS": "/api/nas/getNASData",
            "WPI": "/api/wpi/getWpiRecords",
            "Energy": "/api/energy/getEnergyRecords",
            "AISHE": "/api/aishe/getAisheRecords",
            "ASUSE": "/api/asuse/getAsuseRecords",
            "GENDER": "/api/gender/getGenderRecords",
            "NFHS": "/api/nfhs/getNfhsRecords",
            "ENVSTATS": "/api/env/getEnvStatsRecords",
            "RBI": "/api/rbi/getRbiRecords",
            "NSS77": "/api/nss-77/getNss77Records",
            "NSS78": "/api/nss-78/getNss78Records",
            "NSS79": "/api/nss-79/getNSS79Records",
            "CPIALRL": "/api/cpialrl/getCpialrlRecords",
            "HCES": "/api/hces/getHcesRecords",
            "TUS": "/api/tus/getTusRecords",
            "UDISE": "/api/udise/getUdiseRecords",
            "MNRE": "/api/mnre/getDataByEnergy",
        }

    def get_data(self, dataset_name: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Fetches data from a specified MoSPI dataset.
        """
        # Clean up params - remove None values
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        # Special handling for CPI with base_year 2024 (unified endpoint)
        if dataset_name in ["CPI_Group", "CPI_Item"] and params and params.get("base_year") == "2024":
            full_url = f"{self.base_url}/api/cpi/getCPIData"
        else:
            endpoint_path = self.api_endpoints.get(dataset_name)
            if not endpoint_path:
                return {"error": f"Dataset '{dataset_name}' not found."}
            full_url = f"{self.base_url}{endpoint_path}"

        try:
            response = self.session.get(full_url, params=params, timeout=30)
            response.raise_for_status()

            # Check if CSV format was requested
            format_param = params.get("Format", "JSON") if params else "JSON"
            if format_param == "CSV":
                return {"data": response.text, "format": "CSV"}
            else:
                return response.json()
        except Exception as e:
            return {"error": f"An error occurred: {e}"}

    # =========================================================================
    # PLFS Metadata Methods
    # =========================================================================

    def get_plfs_indicators(self) -> Dict[str, Any]:
        """Fetch PLFS indicators grouped by frequency_code."""
        url = f"{self.base_url}/api/plfs/getIndicatorListByFrequency"
        result = {}
        try:
            for fc, label in [(1, "Annual"), (2, "Quarterly"), (3, "Monthly")]:
                response = self.session.get(url, params={"frequency_code": fc}, timeout=30)
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
            return {"error": str(e), "statusCode": False}

    def get_plfs_filters(
        self,
        indicator_code: int,
        frequency_code: int = 1,
        year: Optional[str] = None,
        month_code: Optional[str] = None,
        year_type_code: Optional[int] = None
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
        if year_type_code is not None:
            params["year_type_code"] = year_type_code

        try:
            response = self.session.get(
                f"{self.base_url}/api/plfs/getFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # CPI Metadata Methods
    # =========================================================================

    def get_cpi_filters(
        self,
        base_year: str = "2024",
        level: str = "Group",
        series_code: str = "Current"
    ) -> Dict[str, Any]:
        """Fetch available CPI filters for given base year and level.

        Args:
            base_year: "2012", "2010", or "2024"
            level: "Group" or "Item" (can be "null" for base_year 2024)
            series_code: "Current" or "Back" (for base_year 2024)
        """
        params = {
            "base_year": base_year,
            "level": level if level else "null",
            "series_code": series_code,
        }

        try:
            response = self.session.get(
                f"{self.base_url}/api/cpi/getCpiFilterByLevelAndBaseYear",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_cpi_base_years(self) -> Dict[str, Any]:
        """Fetch available CPI base years and levels.

        Returns:
            Dictionary with available base_year and level options
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/cpi/getCpiBaseYear",
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            # Add guidance about base years
            result["_note"] = (
                "CPI has multiple base years with different data coverage. "
                "Latest base_year is '2024'. "
                "base_year='2024': Latest data (2026+), new hierarchical structure (division/class/sub_class). "
                "base_year='2012': Data up to 2025. base_year='2010': Historical data. "
                "Each base year covers a different time period."
            )
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # IIP Metadata Methods
    # =========================================================================

    def get_iip_base_years(self) -> Dict[str, Any]:
        """Fetch available IIP base years and frequencies from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/iip/getIipBaseYear",
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            result["_note"] = (
                "IIP has multiple base years with different commodity structures and time coverage. "
                "Latest base_year is '2011-12'. "
                "base_year='2011-12': Data from 2012-13 onwards. "
                "base_year='2004-05': Data from 2005-06 to 2016-17. "
                "base_year='1993-94': Historical data. "
                "frequency='Annually': annual data using financial_year param (YYYY-YY). "
                "frequency='Monthly': monthly data using year and month_code params."
            )
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

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
            response = self.session.get(
                f"{self.base_url}/api/iip/getIipFilter",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # ASI Metadata Methods
    # =========================================================================

    def get_asi_classification_years(self) -> Dict[str, Any]:
        """Fetch list of available NIC classification years from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/asi/getNicClassificationYear",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

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
            response = self.session.get(
                f"{self.base_url}/api/asi/getAsiFilter",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_asi_indicators(self) -> Dict[str, Any]:
        """Fetch ASI indicator list from the filter endpoint (using classification_year=2008).

        Returns indicators plus classification year info so the LLM knows
        it must pass classification_year in get_metadata/get_data.
        """
        try:
            response = self.session.get(
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
                         "Pass classification_year in get_metadata() and get_data().",
                "statusCode": True,
            }
            if indicators:
                result["indicators"] = indicators
            else:
                result["filters"] = filter_data
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # NAS Metadata Methods
    # =========================================================================

    def get_nas_indicators(self) -> Dict[str, Any]:
        """Fetch list of all NAS indicators from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/nas/getNasIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            result = response.json()

            # Add base_year info for consistency with CPI workflow
            if "data" in result and isinstance(result["data"], dict):
                result["data"]["base_year"] = [
                    {"base_year": "2022-23"},
                    {"base_year": "2011-12"},
                ]
            result["_note"] = (
                "NAS requires base_year in get_metadata and get_data. "
                "Available base years: '2022-23' (latest) and '2011-12'. "
                "Latest base_year is '2022-23'. "
                "Pass base_year along with series, frequency_code, and indicator_code."
            )
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nas_filters(
        self,
        series: str = "Current",
        frequency_code: int = 1,
        indicator_code: int = 1,
        base_year: str = "2022-23"
    ) -> Dict[str, Any]:
        """Fetch available NAS filters for given series/frequency/indicator.

        Args:
            series: "Current" or "Back"
            frequency_code: 1 (Annually) or 2 (Quarterly, Current series only)
            indicator_code: Indicator code (1-22 for Annual, 1-11 for Quarterly)
            base_year: Base year - "2022-23" (latest) or "2011-12"
        """
        params = {
            "base_year": base_year,
            "series": series,
            "frequency_code": frequency_code,
            "indicator_code": indicator_code,
        }

        try:
            response = self.session.get(
                f"{self.base_url}/api/nas/getNasFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # WPI Metadata Methods
    # =========================================================================

    def get_wpi_base_years(self) -> Dict[str, Any]:
        """Fetch available WPI base years from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/wpi/getWpiBaseYear",
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            result["_note"] = (
                "WPI has multiple base years with different commodity structures and time coverage. "
                "Latest base_year is '2011-12'. "
                "base_year='2011-12': Data from 2012 onwards. "
                "base_year='2004-05': Data from 2005 to 2017. "
                "base_year='1993-94': Historical data from 1995 to 2010."
            )
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_wpi_filters(self, base_year: str = "2011-12") -> Dict[str, Any]:
        """Fetch available WPI filters for a given base year.

        Args:
            base_year: "2011-12" (default/latest), "2004-05", or "1993-94"

        Returns:
            Available filters: year, month, major_group, group, sub_group, sub_sub_group, item
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/wpi/getWpiData",
                params={"base_year": base_year},
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # Energy Metadata Methods
    # =========================================================================

    def get_energy_indicators(self) -> Dict[str, Any]:
        """Fetch list of Energy indicators from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/energy/getEnergyIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

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
            response = self.session.get(
                f"{self.base_url}/api/energy/getEnergyFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # AISHE (All India Survey on Higher Education) Methods
    # =========================================================================

    def get_aishe_indicators(self) -> Dict[str, Any]:
        """Fetch list of AISHE indicators from MoSPI API.

        Returns 9 indicators covering:
        - Number of Universities
        - Number of Colleges
        - Student Enrolment
        - Social Group-wise Enrolment
        - PWD & Minority Enrolment
        - Gross Enrolment Ratio (GER)
        - Gender Parity Index (GPI)
        - Pupil Teacher Ratio
        - Number of Teachers
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/aishe/getAisheIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_aishe_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available AISHE filters for given indicator.

        Args:
            indicator_code: Indicator code (1-9)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/aishe/getAisheFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # ASUSE (Annual Survey of Unincorporated Sector Enterprises) Methods
    # =========================================================================

    def get_asuse_frequencies(self) -> Dict[str, Any]:
        """Fetch list of ASUSE frequencies from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/asuse/getAsuseFrequencyList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_asuse_indicators(self, frequency_code: int = 1) -> Dict[str, Any]:
        """Fetch list of ASUSE indicators grouped by frequency_code.

        Args:
            frequency_code: 1=Annually, 2=Quarterly (ignored - fetches both)
        """
        url = f"{self.base_url}/api/asuse/getAsuseIndicatorListByFrequency"
        result = {}
        try:
            for fc, label in [(1, "Annual"), (2, "Quarterly")]:
                response = self.session.get(url, params={"frequency_code": fc}, timeout=30)
                response.raise_for_status()
                data = response.json()
                result[f"frequency_code_{fc}_{label}"] = data.get("data", [])
            return {
                "indicators_by_frequency": result,
                "_note": "frequency_code=1 (Annual) has 35 indicators on establishment details, ownership, workers, GVA. "
                         "frequency_code=2 (Quarterly) has 15 indicators including market establishments, worker counts. "
                         "For RECENT data or quarterly breakdowns (Jan-Mar, Apr-Jun, etc.), use frequency_code=2. "
                         "Pass the correct frequency_code in get_metadata() and get_data().",
                "statusCode": True,
            }
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_asuse_filters(self, indicator_code: int, frequency_code: int = 1) -> Dict[str, Any]:
        """Fetch available ASUSE filters for given indicator.

        Args:
            indicator_code: Indicator code
            frequency_code: 1=Annually, 2=Quarterly
        """
        params = {
            "indicator_code": indicator_code,
            "frequency_code": frequency_code
        }

        try:
            response = self.session.get(
                f"{self.base_url}/api/asuse/getAsuseFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # Gender Statistics Methods
    # =========================================================================

    def get_gender_indicators(self) -> Dict[str, Any]:
        """Fetch list of Gender indicators from MoSPI API.

        Returns 157 indicators covering demographics, health, education,
        labour, time use, financial inclusion, political participation,
        crimes against women, and more.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/gender/getGenderIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_gender_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available Gender filters for given indicator.

        Args:
            indicator_code: Indicator code (1-157)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/gender/getGenderFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # NFHS (National Family Health Survey) Metadata Methods
    # =========================================================================

    def get_nfhs_indicators(self) -> Dict[str, Any]:
        """Fetch list of NFHS indicators from MoSPI API."""
        try:
            response = self.session.get(
                f"{self.base_url}/api/nfhs/getNfhsIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nfhs_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available NFHS filters for given indicator.

        Args:
            indicator_code: Indicator code
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/nfhs/getNfhsFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # Environment Statistics Methods
    # =========================================================================

    def get_envstats_indicators(self) -> Dict[str, Any]:
        """Fetch list of Environment Statistics indicators from MoSPI API.

        Returns 124 indicators covering climate, biodiversity, pollution,
        resources, disasters, health, and environmental expenditure.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/env/getEnvStatsIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_envstats_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available Environment Statistics filters for given indicator.

        Args:
            indicator_code: Indicator code (1-130)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/env/getEnvStatsFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # RBI (Reserve Bank of India) Statistics Methods
    # =========================================================================

    def get_rbi_indicators(self) -> Dict[str, Any]:
        """Fetch list of RBI indicators from MoSPI API.

        Returns 39 indicators covering foreign trade, balance of payments,
        forex rates, external debt, and NRI deposits.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/rbi/getRbiIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_rbi_filters(self, sub_indicator_code: int) -> Dict[str, Any]:
        """Fetch available RBI filters for given indicator.

        Args:
            sub_indicator_code: Indicator code (1-48)
        """
        params = {"sub_indicator_code": sub_indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/rbi/getRbiMetaData",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nss77_indicators(self) -> Dict[str, Any]:
        """Fetch list of NSS77 indicators from MoSPI API.

        Returns indicators from NSS 77th Round (Situation Assessment Survey of Agricultural Households).
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/nss-77/getIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nss77_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available NSS77 filters for given indicator.

        Args:
            indicator_code: Indicator code (16-51)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/nss-77/getFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nss78_indicators(self) -> Dict[str, Any]:
        """Fetch list of NSS78 indicators from MoSPI API.

        Returns indicators from NSS 78th Round (Living Conditions - drinking water,
        sanitation, digital connectivity, migration, household assets).
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/nss-78/getIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nss78_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available NSS78 filters for given indicator.

        Args:
            indicator_code: Indicator code (2-15)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/nss-78/getFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # NSS79 (NSS 79th Round - CAMS + AYUSH) Methods
    # =========================================================================

    def get_nss79_indicators(self) -> Dict[str, Any]:
        """Fetch list of NSS79 indicators from MoSPI API.

        Returns all 35 indicators from NSS 79th Round — two survey modules combined:
        - survey_code=1 (CAMS): 28 indicators on education, health expenditure,
          financial inclusion, digital literacy, and household living conditions.
        - survey_code=2 (AYUSH): 7 indicators on AYUSH awareness, usage,
          treatment types, therapy knowledge, and expenditure.
        """
        try:
            resp1 = self.session.get(
                f"{self.base_url}/api/nss-79/getNSS79IndicatorList",
                params={"survey_code": 1},
                timeout=30
            )
            resp1.raise_for_status()
            result = resp1.json()

            resp2 = self.session.get(
                f"{self.base_url}/api/nss-79/getNSS79IndicatorList",
                params={"survey_code": 2},
                timeout=30
            )
            resp2.raise_for_status()
            ayush = resp2.json().get("data", [])

            result["data"] = result.get("data", []) + ayush
            result["count"] = len(result["data"])
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_nss79_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available NSS79 filters for given indicator.

        Args:
            indicator_code: Indicator code (1-35). 1-28 = CAMS module, 29-35 = AYUSH module.
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/nss-79/getNSS79FilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # CPIALRL (CPI for Agricultural Labourers and Rural Labourers) Methods
    # =========================================================================

    def get_cpialrl_indicators(self) -> Dict[str, Any]:
        """Fetch list of CPIALRL indicators from MoSPI API.

        Returns 2 indicators: General Index and Group Index.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/cpialrl/getCpialrlIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_cpialrl_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available CPIALRL filters for given indicator.

        Args:
            indicator_code: Indicator code (1-2)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/cpialrl/getCpialrlFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # HCES (Household Consumption Expenditure Survey) Methods
    # =========================================================================

    def get_hces_indicators(self) -> Dict[str, Any]:
        """Fetch list of HCES indicators from MoSPI API.

        Returns 9 indicators covering MPCE, consumption patterns,
        Gini coefficient, and expenditure by household type/social group.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/hces/getHcesIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_hces_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available HCES filters for given indicator.

        Args:
            indicator_code: Indicator code (1-9)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/hces/getHcesFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # TUS (Time Use Survey) Methods
    # =========================================================================

    def get_tus_indicators(self) -> Dict[str, Any]:
        """Fetch list of TUS indicators from MoSPI API.

        Returns 41 indicators covering time spent on paid/unpaid activities,
        SNA/non-SNA activities, by gender, age, education, marital status, etc.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/tus/getTusIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_tus_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available TUS filters for given indicator.

        Args:
            indicator_code: Indicator code (4-44)
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/tus/getTusFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # EC (Economic Census) Methods
    # =========================================================================

    _EC_URLS = {
        1: "https://esankhyiki.mospi.gov.in/EC/filterDistrict6",
        2: "https://esankhyiki.mospi.gov.in/EC/filterDistrict5",
        3: "https://esankhyiki.mospi.gov.in/EC/filterDistrict4",
    }
    _EC_SUBMIT_URLS = {
        1: "https://esankhyiki.mospi.gov.in/dashboard/EC/submitForm6",
        2: "https://esankhyiki.mospi.gov.in/dashboard/EC/submitForm5",
        3: "https://esankhyiki.mospi.gov.in/dashboard/EC/submitForm4",
    }
    _EC_VERSION_MAP = {1: "6", 2: "5", 3: "4"}
    _EC_SWAGGER_PATHS = {
        1: "/EC/filterDistrict6",
        2: "/EC/filterDistrict5",
        3: "/EC/filterDistrict4",
    }
    _EC_SUBMIT_SWAGGER_PATHS = {
        1: "/EC/submitForm6",
        2: "/EC/submitForm5",
        3: "/EC/submitForm4",
    }

    def get_ec_indicators(self) -> Dict[str, Any]:
        """Return available Economic Census indicators (EC4, EC5, EC6)."""
        return {
            "data": [
                {"indicator_code": 1, "indicator_name": "Sixth Economic Census (EC6) - 2013-14",
                 "description": "District-wise establishment and worker counts. 36 States/UTs, 24 activity sectors, source of finance, ownership type."},
                {"indicator_code": 2, "indicator_name": "Fifth Economic Census (EC5) - 2005",
                 "description": "District-wise establishment and worker counts. 35 States/UTs, 313 NIC-based activity codes, nature of operation, source of finance, ownership type."},
                {"indicator_code": 3, "indicator_name": "Fourth Economic Census (EC4) - 1998",
                 "description": "District-wise establishment and worker counts. 35 States/UTs, 18 activity sectors, nature of operation, source of finance, ownership type."},
            ],
            "statusCode": True,
        }

    def get_ec_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Read EC filter values from swagger YAML for given EC version.

        Args:
            indicator_code: 1=EC6, 2=EC5, 3=EC4
        """
        
        swagger_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "swagger", "swagger_user_ec.yaml"
        )
        ranking_path = self._EC_SWAGGER_PATHS.get(indicator_code)
        detail_path = self._EC_SUBMIT_SWAGGER_PATHS.get(indicator_code)
        if not ranking_path:
            return {"error": f"Invalid indicator_code {indicator_code}. Use 1 (EC6), 2 (EC5), or 3 (EC4).", "statusCode": False}

        try:
            with open(swagger_path, 'r') as f:
                spec = yaml.safe_load(f)

            # Read filter enum values from filterDistrict path
            ranking_params = spec["paths"][ranking_path]["get"]["parameters"]
            data = {}
            for p in ranking_params:
                name = p["name"]
                enum_names = p.get("x-enum-names", {})
                if enum_names:
                    data[name] = [{"id": code, "name": desc} for code, desc in sorted(enum_names.items())]

            # Read detail-mode param names from submitForm path
            detail_params = spec["paths"][detail_path]["get"]["parameters"]
            detail_param_names = [p["name"] for p in detail_params]

            return {
                "data": data,
                "ranking_mode_params": [p["name"] for p in ranking_params],
                "detail_mode_params": detail_param_names,
                "statusCode": True,
                "_note": (
                    "state is required. All other filters are optional. "
                    "In get_data, pass mode='ranking' for top/bottom N districts (uses top5opt). "
                    "Pass mode='detail' for row-level data with social group, NIC description, workers (uses pageNum, 20 rows/page)."
                ),
            }
        except Exception as e:
            return {"error": str(e), "statusCode": False}

    def get_ec_data(self, indicator_code: int, filters: Dict[str, str]) -> Dict[str, Any]:
        """Fetch Economic Census data via POST to esankhyiki.mospi.gov.in.

        Args:
            indicator_code: 1=EC6, 2=EC5, 3=EC4
            filters: Dict with keys: state (required), activity, nop, sof, ownership (optional)
        """
        if filters.get("mode") == "detail":
            return self.get_ec_detail_data(indicator_code=indicator_code, filters=filters)

        url = self._EC_URLS.get(indicator_code)
        ec_num = self._EC_VERSION_MAP.get(indicator_code)
        if not url:
            return {"error": f"Invalid indicator_code {indicator_code}. Use 1 (EC6), 2 (EC5), or 3 (EC4).", "statusCode": False}

        state = filters.get("state", "")
        if not state:
            return {"error": "state is required for EC queries.", "statusCode": False}

        form_data = {
            "ec": ec_num,
            "state": state,
            "param1": "val1",
            "top5opt": filters.get("top5opt", "2"),
            "nop": filters.get("nop", ""),
            "sof": filters.get("sof", ""),
            "activity": filters.get("activity", ""),
            "ownership": filters.get("ownership", ""),
            "sector": filters.get("sector", ""),
        }

        try:
            response = self.session.post(
                url, data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=30,
            )
            response.raise_for_status()
            raw = response.json()

            # Parse HTML table from "code" field
            districts = []
            html = raw.get("code", "")
            if html:
                soup = BeautifulSoup(f"<table>{html}</table>", "html.parser")
                for row in soup.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) == 3:
                        districts.append({
                            "rank": cols[0].get_text(strip=True),
                            "district": cols[1].get_text(strip=True),
                            "establishments": cols[2].get_text(strip=True),
                        })

            return {
                "data": districts,
                "summary": {
                    "total_establishments": raw.get("counter"),
                    "total_workers": raw.get("wcounter"),
                    "max_establishments": raw.get("max_ent"),
                    "min_establishments": raw.get("min_ent"),
                    "max_workers": raw.get("max_workers"),
                    "min_workers": raw.get("min_workers"),
                },
                "description": raw.get("msgText", ""),
                "statusCode": True,
            }
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_ec_detail_data(self, indicator_code: int, filters: Dict[str, str]) -> Dict[str, Any]:
        """Fetch row-level EC data via POST to dashboard/EC/submitForm{4,5,6}.

        Returns 20 rows per page with full breakdown: sector, district, household type,
        activity, NOP, SOF, ownership, social group, NIC description, workers.

        Args:
            indicator_code: 1=EC6, 2=EC5, 3=EC4
            filters: state (required), activity, nop, sof, ownership, sector, pageNum (optional)
        """

        url = self._EC_SUBMIT_URLS.get(indicator_code)
        ec_num = self._EC_VERSION_MAP.get(indicator_code)
        if not url:
            return {"error": f"Invalid indicator_code {indicator_code}.", "statusCode": False}

        page_num = filters.get("pageNum", "1")
        form_data = {
            "ec": ec_num,
            "state": filters.get("state", ""),
            "nop": filters.get("nop", ""),
            "sof": filters.get("sof", ""),
            "activity": filters.get("activity", ""),
            "randomnum": str(random.random()),
            "pageNum": str(page_num),
            "ownership": filters.get("ownership", ""),
            "sector": filters.get("sector", ""),
        }

        try:
            response = self.session.post(
                url, data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=30,
            )
            response.raise_for_status()
            raw = response.json()

            # Column positions differ per EC version
            # EC6 column names sourced directly from portal headers
            col_maps = {
                1: {  # EC6: 20 cols
                    "sl_no": 0,
                    "state": 1,
                    "sector": 2,
                    "district": 3,
                    "household_type": 4,
                    "activity": 5,
                    "nature_of_operation": 6,
                    "source_of_finance": 7,
                    "ownership": 8,
                    "social_group": 9,
                    "establishments_in_household": 10,
                    "nic_description": 11,
                    "handloom_activity": 12,
                    "gender_code": 13,
                    "religion_code": 14,
                    "hired_male_workers": 15,
                    "hired_female_workers": 16,
                    "unpaid_male_workers": 17,
                    "unpaid_female_workers": 18,
                    "total_workers": 19,
                },
                2: {  # EC5: 22 cols - sourced from EC5 portal headers
                    "sl_no": 0,
                    "state": 1,
                    "sector": 2,
                    "district": 3,
                    "activity": 4,                       # NIC description
                    "enterprise_classification": 5,       # Agriculture / Non-Agriculture
                    "nature_of_operation": 6,
                    "ownership": 7,
                    "source_of_finance": 8,
                    "social_group": 9,
                    "power_fuel_usage": 10,
                    "hired_male_workers": 11,
                    "hired_female_workers": 12,
                    "male_child_workers": 13,
                    "female_child_workers": 14,
                    "unpaid_male_child_workers": 15,
                    "unpaid_female_child_workers": 16,
                    "unpaid_male_workers": 17,
                    "unpaid_female_workers": 18,
                    "total_workers": 19,
                    "registration_code1": 20,
                    "registration_code2": 21,
                },
                3: {  # EC4: 18 cols - sourced from EC4 portal headers + API cross-check
                    "sl_no": 0,
                    "state": 1,
                    "sector": 2,
                    "district": 3,
                    "activity": 4,
                    "enterprise_classification": 5,       # Agriculture / Non-Agriculture
                    "nature_of_operation": 6,
                    "ownership": 7,
                    "source_of_finance": 8,
                    "social_group": 9,
                    "power_fuel_usage": 10,
                    "nic_description": 11,
                    "male_workers": 12,
                    "female_workers": 13,
                    "male_child_workers": 14,
                    "female_child_workers": 15,
                    "total_workers": 16,
                    "enterprise_type": 17,               # OAE / NDE / DE
                },
            }
            col_map = col_maps.get(indicator_code, col_maps[1])

            rows = []
            html = raw.get("code", "")
            if html and "No Record" not in html:
                soup = BeautifulSoup(f"<table>{html}</table>", "html.parser")
                for row in soup.find_all("tr"):
                    cols = row.find_all("td")
                    if len(cols) >= 10:
                        row_data = {}
                        for field, idx in col_map.items():
                            row_data[field] = cols[idx].get_text(strip=True) if idx < len(cols) else ""
                        rows.append(row_data)

            total_records = int(str(raw.get("counter", 0) or 0).replace(",", ""))
            total_pages = math.ceil(total_records / 20) if total_records else 0

            return {
                "data": rows,
                "page": int(page_num),
                "total_pages": total_pages,
                "total_records": total_records,
                "statusCode": True,
                "_note": f"Showing page {page_num} of {total_pages} ({total_records} total records). Pass pageNum to fetch next page." if total_pages > 1 else "",
            }
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # UDISE (Unified District Information System for Education) Methods
    # =========================================================================

    def get_udise_indicators(self) -> Dict[str, Any]:
        """Fetch list of UDISE indicators from MoSPI API.

        Returns 46 active indicators covering:
        - Schools: total count, infrastructure, management type, level, AWC sections
        - Teachers: total, by management, by gender/class, trained, professionally qualified
        - Enrolment: total, CWSN, pre-school experience, GER, NER, ANER, ASER, GPI
        - Social groups: OBC, Muslim minority, all minority enrolment percentages
        - Transition metrics: promotion, repetition, dropout, transition, retention rates
        - Ratios: pupil-teacher ratio, average teachers/enrolments per school
        - Special focus: zero-enrolment schools, single-teacher schools, enrolment brackets
        - Facilities: drinking water, ICT labs, computers, digital initiatives, library
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/udise/getIndicatorList",
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_udise_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available UDISE filters for given indicator.

        Args:
            indicator_code: Indicator code
        """
        params = {"indicator_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/udise/getUdiseFilterByIndicatorId",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    # =========================================================================
    # MNRE (Ministry of New and Renewable Energy) Methods
    # =========================================================================

    def get_mnre_indicators(self) -> Dict[str, Any]:
        """Fetch list of MNRE renewable energy types.

        Returns 5 types of renewable energy: Solar Power, Wind Power, Hydro Power,
        Bio Power, Total Power. The type_of_renewable_energy_code field is exposed
        as indicator_code for consistency with other datasets.
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/mnre/getTypeOfRenewableEnergy",
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            for item in result.get("data", []):
                if "type_of_renewable_energy_code" in item:
                    item["indicator_code"] = item["type_of_renewable_energy_code"]
            return result
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}

    def get_mnre_filters(self, indicator_code: int) -> Dict[str, Any]:
        """Fetch available MNRE filters for given energy type.

        Args:
            indicator_code: Renewable energy type code (1=Solar, 2=Wind, 3=Hydro,
                            4=Bio, 5=Total). Solar (1), Hydro (3), and Bio (4)
                            have categories; Wind (2) and Total (5) have none.
        """
        params = {"type_of_renewable_energy_code": indicator_code}

        try:
            response = self.session.get(
                f"{self.base_url}/api/mnre/getFilterByEnergy",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e), "statusCode": False}


# Global instance
mospi = MoSPI()
