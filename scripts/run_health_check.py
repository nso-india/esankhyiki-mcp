#!/usr/bin/env python3
"""MoSPI MCP Server Health Check — text report for cron/email.

Uses the same dataset config as tests/test_mcp_server.py (fixed NSS77 module, NAS retries).

Examples:
    MCP_SERVER_URL=https://mcp.mospi.gov.in python scripts/run_health_check.py
    python scripts/run_health_check.py   # defaults to production URL
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.test_mcp_server import DATASETS, call  # noqa: E402

TZ_IST = timezone(timedelta(hours=5, minutes=30))
# Empty/unset MCP_SERVER_URL → in-process local server (includes NAS fallback).
# Set MCP_SERVER_URL=https://mcp.mospi.gov.in/ to test production.
if os.environ.get("MCP_SERVER_URL"):
    SERVER = os.environ["MCP_SERVER_URL"].rstrip("/")
else:
    from mospi_server import mcp as _local_mcp  # noqa: E402

    SERVER = _local_mcp
_INTERNAL = {"user_query", "next_step", "related_datasets"}


def _cases():
    for param in DATASETS:
        dataset, step3, step4 = param.values
        yield param.id, dataset, step3, step4


async def _check(name: str, fn) -> tuple[bool, str]:
    try:
        data = await fn()
        if not isinstance(data, dict):
            return False, f"unexpected response type: {type(data).__name__}"
        if "error" in data:
            return False, json.dumps(data, ensure_ascii=False)[:900]
        return True, ""
    except Exception as e:
        return False, str(e)


async def main() -> int:
    started = time.perf_counter()
    stamp = datetime.now(TZ_IST).strftime("%Y-%m-%d_%H-%M-%S")
    results: list[tuple[str, str, bool, str]] = []

    for ds_id, dataset, step3, step4 in _cases():
        retries = 6 if dataset == "NAS" else None

        async def step2(retries=retries, dataset=dataset):
            data = await call(
                SERVER,
                "get_indicators",
                {"dataset": dataset, "user_query": "health check"},
                retries=retries,
            )
            if isinstance(data, dict) and "error" not in data:
                if not (set(data.keys()) - _INTERNAL):
                    return {"error": "empty indicator payload"}
            return data

        ok, err = await _check(f"{ds_id.lower()}_step2", step2)
        results.append((f"{ds_id.lower()}_step2", f"{dataset} step2: get indicators", ok, err))

    for ds_id, dataset, step3, step4 in _cases():
        ok, err = await _check(
            f"{ds_id.lower()}_step3",
            lambda dataset=dataset, step3=step3: call(
                SERVER, "get_metadata", {"dataset": dataset, **step3}
            ),
        )
        results.append((f"{ds_id.lower()}_step3", f"{dataset} step3: get metadata", ok, err))

    for ds_id, dataset, step3, step4 in _cases():
        ok, err = await _check(
            f"{ds_id.lower()}_step4",
            lambda dataset=dataset, step4=step4: call(
                SERVER, "get_data", {"dataset": dataset, "filters": step4}
            ),
        )
        results.append((f"{ds_id.lower()}_step4", f"{dataset} step4: get data", ok, err))

    elapsed = time.perf_counter() - started
    passed = sum(1 for *_, ok, _ in results if ok)
    failed = len(results) - passed

    print("MoSPI MCP Server Health Check Results")
    print(f"Server: {SERVER}")
    print(f"Timestamp: {stamp}")
    print(f"Time: {len(results)} tests in {elapsed:.1f}s")
    print(f"Total Tests: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print()

    if failed:
        print("Failed Tests:")
        for test_id, label, ok, err in results:
            if ok:
                continue
            print(f"  {test_id} ({label})")
            if err:
                print(f"    {err}")
        print()

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
