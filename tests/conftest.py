import os
import pathlib
import sys

# Ensure local/unit tests run without requiring HTTP auth headers.
os.environ.setdefault("MCP_AUTH_MODE", "disabled")
os.environ.setdefault("TELEMETRY_CAPTURE_TOOL_INPUT", "false")
os.environ.setdefault("TELEMETRY_CAPTURE_TOOL_OUTPUT", "false")
os.environ.setdefault("TELEMETRY_LOG_FULL_OUTPUT", "false")

# Ensure project root is importable when pytest uses importlib mode.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
