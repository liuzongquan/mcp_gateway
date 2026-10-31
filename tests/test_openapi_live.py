"""Real HTTP OpenAPI adapter tests through official MCP clients, without mocks."""
import json

import httpx
import pytest
from mcp import ClientSession, McpError
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client
from starlette.applications import Starlette
from starlette.responses import JSONResponse, RedirectResponse
from starlette.routing import Route
from test_sdk_acceptance import live_server, local_http_client_factory

from mcp_gateway.app import create_app
from mcp_gateway.config import GatewayConfig, OpenAPIServiceConfig, PrincipalConfig

pytestmark = pytest.mark.functional


def document():
    return {
        "openapi": "3.1.0", "info": {"title": "live", "version": "1"},
        "paths": {
            "/items/{item_id}": {"post": {
                "operationId": "save", "description": "Save a catalog item",
                "parameters": [
                    {"name": "item_id", "in": "path", "required": True, "schema": {"type": "string"}},
                    {"name": "tag", "in": "query", "schema": {"type": "array", "items": {"type": "string"}}},
                    {"name": "X-Trace", "in": "header", "schema": {"type": "string"}},
                ],
                "requestBody": {"required": True, "content": {"application/json": {
                    "schema": {"type": "object", "properties": {"value": {"type": "integer"}},
                               "required": ["value"], "additionalProperties": False}
                }}},
                "responses": {"200": {"description": "saved"}},
            }},
            "/redirect": {"get": {"operationId": "redirect", "responses": {"302": {"description": "redirect"}}}},
            "/fail": {"get": {"operationId": "fail", "responses": {"503": {"description": "failed"}}}},
        },
    }


@pytest.mark.parametrize("transport", ["streamable-http", "sse"])
async def test_real_openapi_mapping_auth_schema_and_redirects(transport, monkeypatch):
    recorded = []
    redirects_followed = []

    async def save(request):
        value = {"item_id": request.path_params["item_id"], "tags": request.query_params.getlist("tag"),
                 "trace": request.headers.get("x-trace"), "auth": request.headers.get("authorization"),
                 "hash": request.headers.get("routing-hash"), "body": await request.json()}
        recorded.append(value)
        return JSONResponse(value)

    async def redirect(request):
        return RedirectResponse("/forbidden-target", status_code=302)

    async def forbidden(request):
        redirects_followed.append(True)
        return JSONResponse({"leaked": True})

    async def fail(request):
        return JSONResponse({"private": "do not expose"}, status_code=503)

    upstream = Starlette(routes=[Route("/items/{item_id}", save, methods=["POST"]),
                                Route("/redirect", redirect), Route("/forbidden-target", forbidden),
                                Route("/fail", fail)])
    monkeypatch.setenv("TEST_REST_EGRESS", "Bearer separate-backend-secret")
    async with live_server(upstream) as backend:
        config = GatewayConfig(
            principals=[PrincipalConfig(subject="alice", token="client-token-only", allow=["rest__*"])],
            openapi_services=[OpenAPIServiceConfig(name="rest", document=document(), base_url=backend,
                                                   headers_env={"Authorization": "TEST_REST_EGRESS"})],
        )
        async with live_server(create_app(config)) as gateway:
            headers = {"Authorization": "Bearer client-token-only"}
            client = httpx.AsyncClient(headers=headers, trust_env=False)
            connection = (sse_client(gateway + "/sse", headers=headers, httpx_client_factory=local_http_client_factory) if transport == "sse"
                          else streamable_http_client(gateway + "/mcp", http_client=client))
            async with client, connection as streams, ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                tools = await session.list_tools()
                assert {"rest__save", "gateway.search"} <= {tool.name for tool in tools.tools}
                result = await session.call_tool("rest__save", {"item_id": "A B", "tag": ["x", "y"],
                    "X-Trace": "trace-123", "body": {"value": 7}})
                assert not result.isError
                assert result.structuredContent["item_id"] == "A B"
                assert recorded == [result.structuredContent]
                assert recorded[0]["tags"] == ["x", "y"]
                assert recorded[0]["trace"] == "trace-123"
                assert recorded[0]["auth"] == "Bearer separate-backend-secret"
                assert recorded[0]["body"] == {"value": 7}
                assert len(recorded[0]["hash"]) == 64
                for tool in ("rest__redirect", "rest__fail"):
                    response = await session.call_tool(tool, {})
                    assert response.isError
                    assert "private" not in json.dumps(response.model_dump())
                assert not redirects_followed
                # Schema failure must be rejected before it reaches the legacy API.
                with pytest.raises(McpError):
                    await session.call_tool("rest__save", {"item_id": "x", "body": {"value": "bad"}})
                assert len(recorded) == 1
                search = await session.call_tool("gateway.search", {"query": "save catalog item"})
                assert "rest__save" in {tool["name"] for tool in search.structuredContent["tools"]}
