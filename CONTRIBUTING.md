# Contributing

## Adding a New Dataset

The server currently supports 19 datasets. To add a new one:

### 1. Get the Swagger spec

Fetch the swagger YAML for the dataset from the MoSPI API portal. Save it to `swagger/swagger_user_<dataset>.yaml`.

### 2. Add the swagger mapping

In `mospi_server.py`, add an entry to `DATASET_SWAGGER`:

```python
DATASET_SWAGGER = {
    ...
    "YOUR_DATASET": ("swagger_user_yourdataset.yaml", "/api/yourdataset/getData"),
}
```

If the dataset has multiple endpoints (like CPI has Group and Item), add separate keys:

```python
"YOUR_DATASET_VARIANT1": ("swagger_user_yourdataset.yaml", "/api/yourdataset/endpointA"),
"YOUR_DATASET_VARIANT2": ("swagger_user_yourdataset.yaml", "/api/yourdataset/endpointB"),
```

### 3. Add API client methods

In `mospi/client.py`, add methods for the dataset:

- `get_<dataset>_indicators()` - fetch indicator list
- `get_<dataset>_filters(...)` - fetch filter/metadata values
- The generic `get_data()` method handles data fetching if you add the dataset to `api_endpoints`

### 4. Wire up the tools

In `mospi_server.py`:

- Add dataset name to `VALID_DATASETS`
- Add to `DATASETS_REQUIRING_INDICATOR` if it uses indicator codes
- Add indicator method to `get_indicators()`
- Add metadata branch to `get_metadata()`
- Add dataset mapping to `get_data()` → `dataset_map`
- Add dataset description to `know_about_mospi_api()`

### 5. Update docstrings

- Add dataset-specific params to `get_metadata` docstring
- Add dataset to tool argument descriptions

## Swagger as Source of Truth

Parameter validation is driven by swagger YAML files, not hardcoded lists. When `get_metadata` is called, it returns `api_params` from the swagger spec so LLMs know exactly which params to send to `get_data`.

The validation flow:
1. `get_swagger_param_definitions(dataset)` loads params from the YAML
2. `validate_filters(dataset, filters)` checks user-supplied filters against swagger
3. Invalid params are rejected with a clear error listing valid options

If the MoSPI API adds or changes params, update the swagger YAML and the server picks it up automatically.

## Project Structure

```
mospi_server.py       # All MCP tools + validation logic (single file server)
mospi/client.py       # HTTP client for MoSPI API calls
swagger/*.yaml        # Swagger specs per dataset (param source of truth)
observability/        # OpenTelemetry middleware (telemetry.py)
```

## Development Setup

```bash
# Create environment
conda create -n mospi python=3.11
conda activate mospi

# Install dependencies
pip install -r requirements.txt

# Run server
python mospi_server.py
```

## Testing

Test against the live MoSPI API by running the server and making tool calls:

```bash
# Run server
python mospi_server.py

# Test with FastMCP client
python -c "
import asyncio
from fastmcp import Client

async def test():
    async with Client('http://localhost:8000/mcp') as c:
        r = await c.call_tool('step2_get_indicators', {'dataset': 'PLFS', 'user_query': 'test'})
        print(r)

asyncio.run(test())
"
```

## Code Style

- Keep `mospi_server.py` as the single server file. All tools, validation, and routing live here.
- Use swagger YAMLs for param definitions. Don't hardcode param lists.
- Tool docstrings are LLM-facing. Write them as instructions, not developer docs.
- Keep the codebase clean. Don't leave commented-out code blocks.

