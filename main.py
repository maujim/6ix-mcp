from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP, Context
from async_lru import alru_cache

import json, time
from urllib.parse import urljoin
import requests
import functools


base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/"


@alru_cache(maxsize=100)
async def api(path: str, params: dict = {}) -> dict:
    """
    Makes a cached GET request to the full URL composed of base_url + path,
    with params converted from frozenset to dict.
    """

    # Construct the full URL
    url = urljoin(base_url, path)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        print(response.url)
        return response.json()


# Initialize FastMCP server
mcp = FastMCP("6ix-mcp")


async def all_packages() -> list[str]:
    response = await api("/api/3/action/package_list")

    return response["result"]


@mcp.tool()
async def list_datasets() -> list[str]:
    """list all available datasets"""

    return await all_packages()


@mcp.tool()
async def search_datasets(queries: list[str]) -> list[str]:
    """a simple string search across all available datasets names. multiple
    queries can be provided"""

    names = await all_packages()

    resp = []
    for query in queries:
        resp.extend([nn for nn in names if query.lower() in nn.lower()])

    return resp


@mcp.tool()
async def get_dataset_columns(dataset_name: str) -> list[str]:
    """This returns a description of the columns used in this data set"""

    resp = await api(
        "/api/3/action/current_package_list_with_resources", {"limit": 999}
    )

    data = [x for x in resp["result"] if x["name"] == dataset_name]

    if not data:
        return ["no matching dataset found"]

    data = data[0]

    good_resources = [
        resource for resource in data["resources"] if resource["datastore_active"]
    ]
    if not good_resources:
        return ["we cannot check db columns via the api for this dataset"]

    resp = await api(
        "/api/3/action/datastore_search", {"resource_id": good_resources[0]["id"]}
    )
    fields: list[str] = list(map(json.dumps, resp["result"]["fields"]))
    return fields


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting MCP server...")
    mcp.run(transport="stdio")
