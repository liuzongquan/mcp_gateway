"""Real HTTP legacy API -> gateway -> official MCP SDK functional tests."""

import asyncio
import json
import socket
import time
from contextlib import asynccontextmanager
from datetime import timedelta

import httpx
import pytest
import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import McpError
from starlette.applications import Starlette
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from mcp_gateway.app import create_app
from mcp_gateway.config import GatewayConfig

pytestmark = pytest.mark.functional
INGRESS_TOKEN = "functional-ingress-token"
EGRESS_SECRET = "Bearer private-upstream-token"


@asynccontextmanager
async def listening(app):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", lifespan="on", timeout_graceful_shutdown=1)
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(8):
            while not server.started:
                if task.done():
                    await task
                    raise AssertionError("HTTP server stopped before readiness")
                await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        except TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        sock.close()


def legacy_service(state):
    async def inspect_request(request):
        state["calls"].append("inspect")
        return JSONResponse(
            {
                "item": request.path_params["item"],
                "raw_path": request.scope["raw_path"].decode("ascii"),
                "query": list(request.query_params.multi_items()),
                "body": await request.json(),
                "authorization": request.headers.get("authorization"),
                "region": request.headers.get("x-region"),
                "routing_hash": request.headers.get("routing-hash"),
            }
        )

    async def denied(request):
        state["calls"].append("denied")
        return JSONResponse({"unreachable": True})

    async def failure(request):
        state["calls"].append("failure")
        return Response("internal stack trace: " + EGRESS_SECRET, status_code=500)

    async def redirect(request):
        state["calls"].append("redirect")
        return RedirectResponse(str(request.url_for("target")), status_code=307)

    async def target(request):
        state["calls"].append("redirect-target")
        return JSONResponse({"leaked_credential": request.headers.get("authorization")})

    async def slow_mutation(request):
        state["calls"].append("mutation")
        state["effects"] += 1
        await asyncio.sleep(0.5)
        return JSONResponse({"effects": state["effects"]})

    return Starlette(
        routes=[
            Route("/inspect/{item:path}", inspect_request, methods=["POST"]),
            Route("/denied", denied),
            Route("/failure", failure),
            Route("/redirect", redirect),
            Route("/target", target, name="target"),
            Route("/mutation", slow_mutation, methods=["POST"]),
        ]
    )


def document():
    return {
        "openapi": "3.1.0",
        "info": {"title": "legacy", "version": "1"},
        "paths": {
            "/inspect/{item}": {
                "post": {
                    "operationId": "inspect",
                    "parameters": [
                        {"name": "item", "in": "path", "required": True, "schema": {"type": "string"}},
                        {
                            "name": "tag",
                            "in": "query",
                            "schema": {"type": "array", "items": {"type": "string"}},
                        },
                        {"name": "active", "in": "query", "schema": {"type": "boolean"}},
                        {"name": "X-Region", "in": "header", "schema": {"type": "string"}},
                    ],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}},
                                    "required": ["name"],
                                    "additionalProperties": False,
                                }
                            }
                        },
                    },
                }
            },
            "/denied": {"get": {"operationId": "denied"}},
            "/failure": {"get": {"operationId": "failure"}},
            "/redirect": {"get": {"operationId": "redirect"}},
            "/mutation": {"post": {"operationId": "mutation"}},
        },
    }


@asynccontextmanager
async def environment(monkeypatch, request_timeout=2):
    monkeypatch.setenv("FUNCTIONAL_EGRESS_TOKEN", EGRESS_SECRET)
    state = {"calls": [], "effects": 0}
    async with listening(legacy_service(state)) as backend:
        config = GatewayConfig.model_validate(
            {
                "instance_id": "functional-openapi",
                "request_timeout": request_timeout,
                "connect_timeout": 2,
                "principals": [
                    {
                        "subject": "alice",
                        "token": INGRESS_TOKEN,
                        "allow": ["api__inspect", "api__failure", "api__redirect", "api__mutation"],
                    }
                ],
                "openapi_services": [
                    {
                        "name": "api",
                        "document": document(),
                        "base_url": backend,
                        "headers_env": {"Authorization": "FUNCTIONAL_EGRESS_TOKEN"},
                    }
                ],
            }
        )
        async with listening(create_app(config)) as gateway:
            async with httpx.AsyncClient(
                headers={"Authorization": f"Bearer {INGRESS_TOKEN}"}, trust_env=False, timeout=5
            ) as http:
                async with streamable_http_client(gateway + "/mcp", http_client=http) as (read, write, _):
                    async with ClientSession(
                        read, write, read_timeout_seconds=timedelta(seconds=5)
                    ) as client:
                        await client.initialize()
                        yield client, state


def payload(result):
    assert not result.isError, result
    return result.structuredContent or json.loads(result.content[0].text)


async def test_real_http_parameters_json_body_and_separate_egress_credentials(monkeypatch):
    async with environment(monkeypatch) as (client, state):
        data = payload(
            await client.call_tool(
                "api__inspect",
                {
                    "item": "a/b%?#中",
                    "tag": ["red blue", "two"],
                    "active": False,
                    "X-Region": "eu-west",
                    "body": {"name": "πip"},
                },
            )
        )
        assert data["item"] == "a/b%?#中"
        assert data["raw_path"] == "/inspect/a%2Fb%25%3F%23%E4%B8%AD"
        assert data["query"] == [["tag", "red blue"], ["tag", "two"], ["active", "false"]]
        assert data["body"] == {"name": "πip"}
        assert data["region"] == "eu-west"
        assert data["authorization"] == EGRESS_SECRET
        assert INGRESS_TOKEN not in data["authorization"]
        assert len(data["routing_hash"]) == 64
        assert state["calls"] == ["inspect"]


async def test_acl_hides_and_blocks_tools_before_any_legacy_http_request(monkeypatch):
    async with environment(monkeypatch) as (client, state):
        listed = await client.list_tools()
        assert "api__denied" not in {tool.name for tool in listed.tools}
        search = payload(await client.call_tool("gateway.search", {"query": "denied"}))
        assert "api__denied" not in {tool["name"] for tool in search["tools"]}
        with pytest.raises(McpError, match="not found or not permitted"):
            await client.call_tool("api__denied", {})
        assert state["calls"] == []


async def test_invalid_arguments_never_reach_legacy_backend(monkeypatch):
    async with environment(monkeypatch) as (client, state):
        for arguments in (
            {"item": "x"},
            {"item": "x", "body": {"name": 7}},
            {"item": "x", "body": {"name": "ok"}, "Authorization": "attacker"},
            {"item": "x", "body": {"name": "ok"}, "X-Region": "x\r\nHost: attacker"},
        ):
            with pytest.raises(McpError, match="Invalid parameters"):
                await client.call_tool("api__inspect", arguments)
        assert state["calls"] == []


async def test_backend_errors_are_sanitized_and_redirects_never_followed(monkeypatch):
    async with environment(monkeypatch) as (client, state):
        failure = await client.call_tool("api__failure", {})
        assert failure.isError
        assert failure.content[0].text == "Backend returned HTTP 500"
        assert EGRESS_SECRET not in failure.model_dump_json()
        redirected = await client.call_tool("api__redirect", {})
        assert redirected.isError
        assert redirected.content[0].text == "Backend returned HTTP 307"
        assert state["calls"] == ["failure", "redirect"]


async def test_timed_out_mutation_has_one_effect_and_is_never_retried(monkeypatch):
    async with environment(monkeypatch, request_timeout=0.1) as (client, state):
        started = time.monotonic()
        result = await client.call_tool("api__mutation", {})
        assert result.isError
        assert "not retried" in result.content[0].text
        assert time.monotonic() - started < 1
        await asyncio.sleep(0.6)
        assert state["effects"] == 1
        assert state["calls"] == ["mutation"]
