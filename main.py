from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP

import json
from urllib.parse import urljoin
import requests


base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/"


def api(path: str, params={}):
    """
    Makes a cached GET request to the full URL composed of base_url + path,
    with params converted from frozenset to dict.
    """
    # Convert params back to a dictionary
    assert isinstance(params, dict)

    # Construct the full URL
    url = urljoin(base_url, path)

    # Make the request
    response = requests.get(url, params=params)
    response.raise_for_status()

    print(response.url)
    return response.json()


# Initialize FastMCP server
mcp = FastMCP("6ix-mcp")


@mcp.tool()
async def my_tool() -> str:
    """My awesome tool."""
    print("I am going to do something awesome today!")


def prep_datasets() -> str:
    valid_names = []

    with open("datasets/package_index.json", "r") as fp:
        data = json.load(fp)
        for result in data["result"]["results"]:
            if result["resources"] is not None:
                for resource in result["resources"]:
                    if resource["datastore_active"] == True:
                        valid_names.append(result["name"])
                        break

    return valid_names


valid_dataset_names = prep_datasets()


@mcp.tool()
async def list_datasets() -> str:
    """list all available datasets

    caveat: only datasets where we can access the db columns via the api
    """

    return valid_dataset_names


@mcp.tool()
async def get_dataset_columns(dataset_name: str) -> str:
    """This returns a description of the columns used in this data set"""

    with open("datasets/package_index.json", "r") as fp:
        data = json.load(fp)
        target = [
            result
            for result in data["result"]["results"]
            if result["name"] == dataset_name
        ]
        if target is None:
            return "no matching dataset found"
        target = target[0]

    good_resources = [
        resource for resource in target["resources"] if resource["datastore_active"]
    ]
    if good_resources is None:
        return "unknown error occurred"

    resp = api(
        "/api/3/action/datastore_search", {"resource_id": good_resources[0]["id"]}
    )
    description = resp["result"]["fields"]
    return description


if __name__ == "__main__":
    # Initialize and run the server
    print("Starting MCP server...")
    mcp.run(transport="stdio")
