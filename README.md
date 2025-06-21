# 6ix-mcp

this provides access to the Toronto Open Data portal


## installing
clone this repo and update your mcp configuration as follows

```json

"mcpServers": {
    "6ix-mcp": {
      "command": "uv",
      "args": [
        "--directory",
        "/path/to/6ix-mcp",
        "run",
        "main.py"
      ]
    }
}
```
