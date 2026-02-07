# MoSPI MCP Repository Security Audit Report

## Executive Summary

This report documents a full security and reliability review of the repository:

- Repository: `nso-india/esankhyiki-mcp`
- Audited commit: `bbcacb8be1456b175ee1937ce77ada7fbb5b7855`
- Commit message: `Revise MCP server connection instructions`
- Audit timestamp (UTC): `2026-02-07 02:26:40 UTC`
- Auditor workspace: `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan`

Overall risk posture: **Moderate**

Key outcomes:

1. No known dependency CVEs were found from `pip-audit`.
2. One confirmed runtime failure was found in telemetry middleware.
3. Multiple medium-risk operational/security issues were identified (unauthenticated exposure, telemetry privacy leakage, spoofable client-IP attribution, and missing CI quality gates).

---

## Scope

In-scope files and runtime paths:

1. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi_server.py`
2. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi/client.py`
3. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py`
4. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/Dockerfile`
5. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/docker-compose.yml`
6. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/.github/workflows/deploy.yml`
7. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/swagger/*.yaml`
8. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/tests/*.py`

Out-of-scope:

1. MoSPI upstream API implementation at `https://api.mospi.gov.in`
2. Infrastructure controls outside this repository (WAF, ingress, secrets manager, host hardening)

---

## Methodology

### 1) Static code review

Manual review of server, client, telemetry, deployment, and CI configuration with focus on:

1. Authentication and network exposure
2. Input handling and validation
3. Error handling and information leakage
4. Logging/telemetry privacy and abuse risk
5. Test and CI assurance coverage

### 2) Automated SAST

Tool: `bandit`

Command executed:

```bash
.audit-venv/bin/bandit -r mospi_server.py mospi observability -f json -o audit_artifacts/bandit.json
```

Result:

1. 1 finding (LOW): B110 (`try/except/pass`) in telemetry middleware.

### 3) Dependency vulnerability audit

Tool: `pip-audit`

Commands executed:

```bash
XDG_CACHE_HOME=/tmp PIP_AUDIT_CACHE_DIR=/tmp/pip-audit .audit-venv/bin/pip-audit -r requirements.txt -f json -o audit_artifacts/pip-audit-requirements.json
XDG_CACHE_HOME=/tmp PIP_AUDIT_CACHE_DIR=/tmp/pip-audit .audit-venv/bin/pip-audit -f json -o audit_artifacts/pip-audit-env.json
```

Result:

1. No known CVEs found in requirements-resolved dependencies.
2. No known CVEs found in the installed audit environment.

### 4) Runtime validation

Performed targeted runtime checks in a virtual environment:

1. Verified server module imports successfully.
2. Verified exposed MCP tool names via `await mcp.list_tools()`.
3. Reproduced telemetry middleware runtime failure (`NameError`) due to missing `sys` import.

---

## System Architecture (Observed)

### Request flow

1. Client invokes MCP tool over HTTP endpoint (`/mcp`).
2. `FastMCP` routes calls to one of four tools in `mospi_server.py`:
   `1_know_about_mospi_api`, `2_get_indicators`, `3_get_metadata`, `4_get_data`.
3. `mospi/client.py` issues outbound `requests.get(...)` calls to `https://api.mospi.gov.in`.
4. Telemetry middleware records tool input/output and client metadata to OpenTelemetry spans and stderr.

### Data validation model

1. Parameter names and required fields are validated against local Swagger YAML specs.
2. No in-repo authentication/authorization layer is enforced for incoming MCP requests.

---

## Findings

## F-01: Telemetry middleware runtime failure (`NameError`)

- Severity: **High**
- Confidence: **High**
- CWE: `CWE-248` (uncaught exception leading to availability issues)
- Affected file:
  `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:110`

### Evidence

At line 110:

```python
print(f"[TELEMETRY] Output ({output_size} bytes): {full_output}", file=sys.stderr)
```

`sys` is not imported in this module.

Runtime reproduction returned:

```text
NameError name 'sys' is not defined
```

### Impact

Tool call processing can fail when telemetry logging executes, causing service instability or request failure.

### Recommendation

1. Add `import sys` at top of telemetry module.
2. Add regression test that executes `TelemetryMiddleware.on_call_tool(...)` and asserts no exception.
3. Add runtime smoke test in CI.

---

## F-02: Sensitive data exposure and log amplification via telemetry

- Severity: **High**
- Confidence: **High**
- CWE: `CWE-532` (sensitive information in logs), `CWE-400` (resource exhaustion)
- Affected files:
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:89`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:102`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:105`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:140`

### Evidence

1. Tool inputs are stored in traces (`tool.input`).
2. Tool outputs are stored in traces (`tool.output` + size).
3. Full serialized output is printed to stderr.
4. Client IP and user-agent are captured.

### Impact

1. Potential collection of user query content and other potentially sensitive payloads.
2. Large responses can inflate logs and observability storage.
3. Compliance/privacy risk in production environments.

### Recommendation

1. Disable full payload stderr logging by default.
2. Redact or hash sensitive fields before tracing.
3. Keep strict byte limits per trace attribute and per request.
4. Introduce config flags:
   `TELEMETRY_CAPTURE_INPUT=false`, `TELEMETRY_CAPTURE_OUTPUT=false` by default in production.

---

## F-03: Unauthenticated HTTP service exposure by default deployment path

- Severity: **Medium**
- Confidence: **High**
- CWE: `CWE-306` (missing authentication for critical function)
- Affected files:
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi_server.py:497`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/Dockerfile:39`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/docker-compose.yml:7`

### Evidence

1. Server runs in HTTP transport mode.
2. Container command binds to `0.0.0.0`.
3. Compose publishes port `8000:8000`.
4. No in-app authN/authZ checks are implemented.

### Impact

If deployed with public network reachability, endpoint abuse and denial-of-service risk increase significantly.

### Recommendation

1. Enforce auth/rate limiting at gateway or reverse proxy.
2. Restrict network exposure with private ingress and IP allowlists.
3. Add server-side request throttling and request size limits.

---

## F-04: Client IP attribution spoofable via forwarding headers

- Severity: **Medium**
- Confidence: **High**
- CWE: `CWE-345` (insufficient verification of data authenticity)
- Affected file:
  `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/observability/telemetry.py:50`

### Evidence

`X-Forwarded-For` is trusted directly and first hop is accepted without proxy trust verification.

### Impact

Audit trails can be manipulated by malicious clients setting forged headers.

### Recommendation

1. Trust `X-Forwarded-For` only when request originates from approved proxy IPs.
2. Otherwise use transport socket peer address.
3. Add clear provenance flag in telemetry (`ip_source=proxy|socket|unknown`).

---

## F-05: Test suite and CI mismatch reduce security assurance

- Severity: **Medium**
- Confidence: **High**
- CWE: `CWE-1120` (insufficient test coverage of security-relevant behavior)
- Affected files:
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi_server.py`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/tests/test_client.py:24`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/.github/workflows/deploy.yml:40`

### Evidence

1. Server exposes only these tools:
   `1_know_about_mospi_api`, `2_get_indicators`, `3_get_metadata`, `4_get_data`.
2. Tests invoke old/non-existent tool names, including:
   `get_plfs_data`, `get_nas_data`, `lookup_mospi_codes`, `know_about_mospi_api` (without numeric prefix), etc.
3. Deploy workflow builds/pushes image but does not run tests or security gates before publish.

### Impact

Broken behavior and security regressions can ship undetected.

### Recommendation

1. Update tests to current 4-tool contract.
2. Add CI gates: lint, tests, bandit, pip-audit, and blocking policy on high severity findings.
3. Enforce deploy only after successful validation.

---

## F-06: Raw exception text returned to callers

- Severity: **Low**
- Confidence: **High**
- CWE: `CWE-209` (information exposure through an error message)
- Affected files:
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi/client.py:53`
  - `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/mospi_server.py:300`

### Evidence

Errors are returned directly as strings from caught exceptions.

### Impact

Can reveal internals (network errors, endpoint details) to clients.

### Recommendation

1. Return generic user-safe error messages.
2. Log detailed exceptions server-side with correlation IDs.

---

## Additional Observations (Non-vuln but important)

1. README and tests reference old tool names and clone path examples that no longer match current code behavior.
2. Swagger specs describe bearer auth sections, while runtime calls currently use no auth headers for upstream API requests.
3. FastMCP pinned to beta (`fastmcp==3.0.0b1`), increasing change risk; consider release pinning strategy.

---

## Strengths

1. Clear workflow separation between dataset discovery, metadata retrieval, and data query.
2. Good parameter validation pattern via local Swagger specs.
3. Outbound HTTP timeouts are present (`timeout=30`) across client calls.
4. Dependencies currently have no known CVEs in audit results.

---

## Priority Remediation Plan

### P0 (Immediate)

1. Fix `sys` import in telemetry middleware.
2. Disable full output logging in telemetry by default.
3. Add minimal auth/rate limiting at deployment boundary before public exposure.

### P1 (Short term, 1-2 sprints)

1. Introduce telemetry redaction strategy and strict payload limits.
2. Fix IP attribution trust model behind known proxies only.
3. Update and re-enable test suite against current tool API.
4. Add CI quality/security gates before image publish.

### P2 (Hardening)

1. Standardize secure error envelope with trace IDs.
2. Review and enforce retention policies for logs/traces.
3. Revisit dependency pinning strategy for beta framework components.

---

## Reproducibility Artifacts

Generated artifacts are stored in:

1. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/audit_artifacts/bandit.json`
2. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/audit_artifacts/bandit.txt`
3. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/audit_artifacts/pip-audit-requirements.json`
4. `/Users/rishabh/Desktop/test_new_things/mospi_gov_in/repo_scan/audit_artifacts/pip-audit-env.json`

---

## Tooling Versions

1. Python: `3.14.0`
2. bandit: `1.9.3`
3. pip-audit: `2.10.0`
4. fastmcp (project requirement): `3.0.0b1`

