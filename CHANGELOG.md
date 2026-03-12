# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- NSS79 (79th Round - Comprehensive Annual Modular Survey / CAMS) with 28 indicators: literacy, numeracy, school enrolment, NEET youth, health expenditure (hospitalised and non-hospitalised, out-of-pocket), financial inclusion (bank accounts, borrowers), digital literacy (mobile/internet usage, 4G coverage, online banking, file sharing), and household living conditions (assets, clean fuel, drinking water, sanitation, birth registration, transport access)
- Indicator definitions (definitions/) for all 15 datasets with indicator_code support — enriches step2_get_indicators responses with human-readable descriptions

### Changed
- Total datasets: 19 → 20
- step1 updated to reflect 20 datasets and NSS79 description

---

## [2.0.0] - 2026-02-22

### Added
- 12 new datasets: AISHE, ASUSE, GENDER, NFHS, ENVSTATS, RBI, NSS77, NSS78, CPIALRL, HCES, TUS, EC
- CPI base year 2024 support with unified endpoint
- Economic Census (EC) integration with two query modes:
  - Ranking mode (filterDistrict): top/bottom N districts by establishment count
  - Detail mode (submitForm): row-level paginated records with full worker breakdown
- EC supports 3 census versions: EC6 (2013-14), EC5 (2005), EC4 (1998)
- EC column maps verified against portal headers for all 3 versions
- Per-version state code handling (EC6: Census 2011, EC5: Census 2001, EC4: old MoSPI codes)
- NAS base_year support (constant price base year selection)
- Swagger specs for all new datasets

### Changed
- Total datasets: 7 → 19
- Tool names: `1_know_about_mospi_api` → `step1_know_about_mospi_api` → `list_datasets` (all 4 tools renamed)
- list_datasets now returns overview of all 19 datasets
- get_indicators/get_metadata/get_data updated to route all new datasets
- Swagger specs corrected for existing datasets (ASI, IIP, PLFS, WPI, Energy, NAS)
- README updated with full dataset table

## [1.0.0] - 2026-02-06

### Added
- Initial public release
- 7 datasets: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY
- 4-step MCP tool workflow (`list_datasets`, `get_indicators`, `get_metadata`, `get_data`)
- Swagger-driven parameter validation
- OpenTelemetry integration for observability
- Docker and docker-compose deployment
- GitHub Actions CI/CD pipeline
- MIT License

### Architecture
- FastMCP 3.0 server framework
- Single-file server design (`mospi_server.py`)
- Unified API client (`mospi/client.py`)
- YAML-based Swagger specs as source of truth for API parameters

---

## Version History

| Version | Date | Description |
|---------|------|-------------|
| 2.0.0 | 2026-02-22 | 12 new datasets (19 total), EC integration, CPI base year 2024 |
| 1.0.0 | 2026-02-06 | Initial public release with 7 datasets |
