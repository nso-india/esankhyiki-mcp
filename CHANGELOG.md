# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-02-06

### Added
- Initial public release
- 7 datasets: PLFS, CPI, IIP, ASI, NAS, WPI, ENERGY
- 4-step MCP tool workflow (`know_about_mospi_api`, `get_indicators`, `get_metadata`, `get_data`)
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
| 1.0.0 | 2025-02-06 | Initial public release with 7 datasets |

