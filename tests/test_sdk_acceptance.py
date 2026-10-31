"""Real-socket, official-SDK, real-Redis system acceptance tests.

No fake MCP client, ASGI shortcut, fakeredis, or silently skipped integration tests.
Set REDIS_SERVER to an installed redis-server executable when it is not on PATH.
"""

import asyncio
import json
import os
import shutil
import socket
import sys
import tempfile
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
import redis.asyncio as redis
import uvicorn
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client
from mcp.server.fastmcp import Context, FastMCP

from mcp_gateway.app import create_app
from mcp_gateway.config import GatewayConfig, MCPBackendConfig, PrincipalConfig

ROOT = Path(__file__).resolve().parents[1]
TOKEN = "sdk-acceptance-token"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json, text/event-stream"}
TOOL = {
    "name": "record",
    "description": "Append a record and report connection-local sequence",
    "inputSchema": {
        "type": "object",
        "properties": {
            "message": {"type": "string"},
            "delay": {"type": "number", "default": 0},
        },
        "required": ["message"],
    },
}


@asynccontextmanager
async def live_server(app):
    """Start Uvicorn on a reserved loopback socket and guarantee cleanup."""
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=port, log_level="error", lifespan="on", timeout_graceful_shutdown=1
        )
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        deadline = time.monotonic() + 8
        while not server.started:
            if task.done():
                await task
                raise AssertionError("Uvicorn exited before startup")
            if time.monotonic() >= deadline:
                raise AssertionError("Uvicorn failed to become ready")
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


@asynccontextmanager
async def gateway_process(config):
    """Use an independent interpreter: Redis is the only gateway-shared state."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with tempfile.TemporaryDirectory(prefix="mcp-gateway-process-") as directory:
        path = Path(directory) / "config.json"
        path.write_text(config.model_dump_json())
        log_path = Path(directory) / "gateway.log"
        command = (
            "import sys,uvicorn; from mcp_gateway.app import create_app; "
            "from mcp_gateway.config import load_config; "
            "uvicorn.run(create_app(load_config(sys.argv[1])),host='127.0.0.1',"
            "port=int(sys.argv[2]),log_level='error',timeout_graceful_shutdown=1)"
        )
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
        with log_path.open("wb") as log:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                command,
                str(path),
                str(port),
                cwd=ROOT,
                env=env,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
            )
            url = f"http://127.0.0.1:{port}"
            try:
                deadline = time.monotonic() + 12
                async with httpx.AsyncClient(trust_env=False, timeout=0.5) as client:
                    while True:
                        assert process.returncode is None, log_path.read_text()
                        try:
                            response = await client.get(url + "/healthz")
                            if response.status_code == 200:
                                break
                        except httpx.HTTPError:
                            pass
                        assert time.monotonic() < deadline, log_path.read_text()
                        await asyncio.sleep(0.05)
                yield url
            finally:
                if process.returncode is None:
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except TimeoutError:
                    process.kill()
                    await asyncio.wait_for(process.wait(), 5)


@asynccontextmanager
async def real_redis():
    executable = os.getenv("REDIS_SERVER") or shutil.which("redis-server")
    assert executable and os.access(executable, os.X_OK), (
        "Install redis-server or set REDIS_SERVER; Redis integration must run"
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    with tempfile.TemporaryDirectory(prefix="mcp-gateway-acceptance-") as directory:
        process = await asyncio.create_subprocess_exec(
            executable,
            "--bind",
            "127.0.0.1",
            "--port",
            str(port),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
            directory,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        url = f"redis://127.0.0.1:{port}/0"
        client = redis.from_url(url)
        try:
            deadline = time.monotonic() + 8
            while True:
                if process.returncode is not None:
                    raise AssertionError(f"Redis exited: {(await process.stderr.read()).decode()}")
                try:
                    if await client.ping():
                        break
                except redis.ConnectionError:
                    pass
                if time.monotonic() >= deadline:
                    raise AssertionError("Redis did not become ready")
                await asyncio.sleep(0.02)
            yield url
        finally:
            await client.aclose()
            if process.returncode is None:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                process.kill()
                await asyncio.wait_for(process.wait(), 5)


def upstream_app(transport, calls, json_response=True):
    upstream = FastMCP("acceptance-upstream", json_response=json_response)
    counts = defaultdict(int)

    @upstream.tool()
    async def record(message: str, ctx: Context, delay: float = 0) -> dict:
        key = str(id(ctx.request_context.session))
        calls.append(message)
        counts[key] += 1
        sequence = counts[key]
        if delay:
            await asyncio.sleep(delay)
        return {
            "message": message,
            "sequence": sequence,
            "upstream_session": key,
            "upstream_session_id": ctx.request_context.request.headers.get("mcp-session-id")
            if ctx.request_context.request
            else None,
        }

    return upstream.sse_app() if transport == "sse" else upstream.streamable_http_app()


def configuration(
    upstream_url,
    upstream_transport="streamable-http",
    instance="acceptance-a",
    redis_url=None,
    request_timeout=3,
):
    return GatewayConfig(
        instance_id=instance,
        redis_url=redis_url,
        request_timeout=request_timeout,
        session_ttl=60,
        lease_ttl=5,
        principals=[PrincipalConfig(subject="tester", token=TOKEN, allow=["*"])],
        mcp_backends=[
            MCPBackendConfig(name="backend", url=upstream_url, transport=upstream_transport, tools=[TOOL])
        ],
    )


def local_http_client_factory(**kwargs):
    return httpx.AsyncClient(trust_env=False, **kwargs)


@asynccontextmanager
async def sdk_session(base_url, ingress, client=None):
    if ingress == "sse":
        async with sse_client(
            base_url + "/sse",
            headers=HEADERS,
            timeout=4,
            sse_read_timeout=5,
            httpx_client_factory=local_http_client_factory,
        ) as streams:
            async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=5)) as session:
                yield session
    else:
        async with (
            httpx.AsyncClient(trust_env=False, headers=HEADERS, timeout=5)
            if client is None
            else _borrow(client) as http_client
        ):
            async with streamable_http_client(base_url + "/mcp", http_client=http_client) as (read, write, _):
                async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=5)) as session:
                    yield session


@asynccontextmanager
async def _borrow(value):
    yield value


def unpack(result):
    assert not result.isError, result
    if result.structuredContent is not None:
        return result.structuredContent
    return json.loads(result.content[0].text)


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.parametrize("ingress", ["streamable-http", "sse"])
@pytest.mark.parametrize("backend_transport", ["streamable-http", "sse"])
async def test_official_sdk_all_transport_bridges_and_connection_reuse(ingress, backend_transport):
    calls = []
    async with live_server(upstream_app(backend_transport, calls)) as upstream:
        suffix = "/sse" if backend_transport == "sse" else "/mcp"
        app = create_app(configuration(upstream + suffix, backend_transport))
        async with live_server(app) as gateway:
            async with sdk_session(gateway, ingress) as client:
                initialized = await client.initialize()
                assert initialized.capabilities.tools is not None
                listed = await client.list_tools()
                assert {"backend__record", "gateway.search"} <= {tool.name for tool in listed.tools}
                first = unpack(await client.call_tool("backend__record", {"message": "first"}))
                second = unpack(await client.call_tool("backend__record", {"message": "second"}))
                assert first["sequence"] == 1 and second["sequence"] == 2
                assert first["upstream_session"] == second["upstream_session"]
    assert calls == ["first", "second"]


class AlternateGatewayTransport(httpx.AsyncBaseTransport):
    """Send initialization to A and every session request to B without changing the SDK."""

    def __init__(self, second_url):
        self.second = httpx.URL(second_url)
        self.transport = httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request):
        if request.headers.get("MCP-Session-Id"):
            request.url = request.url.copy_with(
                scheme=self.second.scheme, host=self.second.host, port=self.second.port
            )
        return await self.transport.handle_async_request(request)

    async def aclose(self):
        await self.transport.aclose()


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.parametrize("backend_transport", ["streamable-http", "sse"])
async def test_real_redis_two_gateways_preserve_upstream_session(backend_transport):
    calls = []
    async with real_redis() as redis_url, live_server(upstream_app(backend_transport, calls)) as upstream:
        suffix = "/sse" if backend_transport == "sse" else "/mcp"
        config_a = configuration(upstream + suffix, backend_transport, "redis-a", redis_url)
        config_b = configuration(upstream + suffix, backend_transport, "redis-b", redis_url)
        async with gateway_process(config_a) as a, gateway_process(config_b) as b:
            async with httpx.AsyncClient(
                trust_env=False, headers=HEADERS, timeout=5, transport=AlternateGatewayTransport(b)
            ) as http:
                async with sdk_session(a, "streamable-http", http) as client:
                    await client.initialize()
                    first = unpack(
                        await client.call_tool("backend__record", {"message": "via-second-gateway"})
                    )
                    second = unpack(
                        await client.call_tool("backend__record", {"message": "same-upstream-session"})
                    )
                    assert (first["sequence"], second["sequence"]) == (1, 2)
                    assert first["upstream_session"] == second["upstream_session"]
    assert calls == ["via-second-gateway", "same-upstream-session"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_upstream_timeout_does_not_replay_mutating_call():
    calls = []
    async with live_server(upstream_app("streamable-http", calls)) as upstream:
        app = create_app(configuration(upstream + "/mcp", request_timeout=0.15))
        async with live_server(app) as gateway:
            async with sdk_session(gateway, "streamable-http") as client:
                await client.initialize()
                # Warm the backend connection so timeout is about the mutation, not initialization.
                unpack(await client.call_tool("backend__record", {"message": "warmup"}))
                try:
                    result = await client.call_tool(
                        "backend__record", {"message": "mutate-once", "delay": 0.6}
                    )
                except Exception as exc:
                    assert "timeout" in str(exc).lower() or "timed out" in str(exc).lower(), str(exc)
                else:
                    assert result.isError is True
                await asyncio.sleep(0.7)
    assert calls.count("mutate-once") == 1, calls


class LegacyPostsToSecondGateway(AlternateGatewayTransport):
    async def handle_async_request(self, request):
        if request.method == "POST" and request.url.path.rstrip("/") == "/messages":
            request.url = request.url.copy_with(
                scheme=self.second.scheme, host=self.second.host, port=self.second.port
            )
        return await self.transport.handle_async_request(request)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_real_redis_legacy_sse_request_and_response_owner_routing():
    calls = []
    async with real_redis() as redis_url, live_server(upstream_app("sse", calls)) as upstream:
        config_a = configuration(upstream + "/sse", "sse", "legacy-owner", redis_url)
        config_b = configuration(upstream + "/sse", "sse", "legacy-forwarder", redis_url)
        async with gateway_process(config_a) as a, gateway_process(config_b) as b:

            def factory(**kwargs):
                return httpx.AsyncClient(trust_env=False, **kwargs, transport=LegacyPostsToSecondGateway(b))

            async with sse_client(
                a + "/sse", headers=HEADERS, timeout=4, sse_read_timeout=5, httpx_client_factory=factory
            ) as streams:
                async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=5)) as client:
                    await client.initialize()
                    first = unpack(
                        await client.call_tool("backend__record", {"message": "sse-cross-gateway-first"})
                    )
                    second = unpack(
                        await client.call_tool("backend__record", {"message": "sse-cross-gateway-second"})
                    )
                    assert (first["sequence"], second["sequence"]) == (1, 2)
                    assert first["upstream_session"] == second["upstream_session"]
    assert calls == ["sse-cross-gateway-first", "sse-cross-gateway-second"]


async def raw_initialize(client):
    response = await client.post(
        "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "raw-acceptance", "version": "1"},
            },
        },
    )
    assert response.status_code == 200, response.text
    headers = {"MCP-Session-Id": response.headers["MCP-Session-Id"], "MCP-Protocol-Version": "2025-11-25"}
    initialized = await client.post(
        "/mcp", headers=headers, json={"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    assert initialized.status_code == 202
    return headers


async def wait_for_dispatch(calls, message):
    deadline = time.monotonic() + 3
    while message not in calls:
        assert time.monotonic() < deadline, f"Upstream did not receive {message}: {calls}"
        await asyncio.sleep(0.01)


def record_request(request_id, message, delay=0):
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": "backend__record",
            "arguments": {"message": message, "delay": delay},
        },
    }


@pytest.mark.asyncio
@pytest.mark.integration
async def test_duplicate_inflight_id_does_not_execute_mutation_twice():
    calls = []
    async with live_server(upstream_app("streamable-http", calls)) as upstream:
        async with live_server(create_app(configuration(upstream + "/mcp"))) as gateway:
            async with httpx.AsyncClient(
                trust_env=False, base_url=gateway, headers=HEADERS, timeout=5
            ) as client:
                headers = await raw_initialize(client)
                first = asyncio.create_task(
                    client.post("/mcp", headers=headers, json=record_request(42, "first", 0.4))
                )
                try:
                    await wait_for_dispatch(calls, "first")
                    duplicate = await client.post(
                        "/mcp", headers=headers, json=record_request(42, "duplicate", 0)
                    )
                    assert duplicate.status_code in (200, 400, 409), duplicate.text
                    assert "error" in duplicate.json(), duplicate.text
                    original = await first
                    assert original.status_code == 200 and "result" in original.json()
                finally:
                    if not first.done():
                        first.cancel()
                    await asyncio.gather(first, return_exceptions=True)
    assert calls == ["first"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cancellation_is_scoped_to_session_and_does_not_replay():
    calls = []
    async with live_server(upstream_app("streamable-http", calls)) as upstream:
        async with live_server(create_app(configuration(upstream + "/mcp"))) as gateway:
            async with httpx.AsyncClient(
                trust_env=False, base_url=gateway, headers=HEADERS, timeout=5
            ) as client:
                owner = await raw_initialize(client)
                other = await raw_initialize(client)
                request = asyncio.create_task(
                    client.post("/mcp", headers=owner, json=record_request(42, "cancel-me", 1))
                )
                notification = {
                    "jsonrpc": "2.0",
                    "method": "notifications/cancelled",
                    "params": {"requestId": 42},
                }
                try:
                    await wait_for_dispatch(calls, "cancel-me")
                    wrong_session = await client.post("/mcp", headers=other, json=notification)
                    assert wrong_session.status_code == 202
                    await asyncio.sleep(0.05)
                    assert not request.done(), "Another session cancelled this session's request"
                    cancelled = await client.post("/mcp", headers=owner, json=notification)
                    assert cancelled.status_code == 202
                    stopped = await asyncio.wait_for(request, timeout=0.5)
                    # Servers may acknowledge without an RPC body or return a documented cancellation error.
                    assert stopped.status_code in (200, 202, 204), stopped.text
                    if stopped.content:
                        body = stopped.json()
                        assert "error" in body or body.get("result", {}).get("isError") is True, body
                    alive = await client.post(
                        "/mcp", headers=owner, json={"jsonrpc": "2.0", "id": 43, "method": "ping"}
                    )
                    assert alive.status_code == 200
                finally:
                    if not request.done():
                        request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
    assert calls == ["cancel-me"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_lost_backend_owner_fails_closed_without_recreating_or_replaying():
    calls = []
    async with real_redis() as redis_url, live_server(upstream_app("sse", calls)) as upstream:
        config_a = configuration(upstream + "/sse", "sse", "departing-owner", redis_url)
        config_b = configuration(upstream + "/sse", "sse", "survivor", redis_url)
        async with gateway_process(config_b) as b:
            async with gateway_process(config_a) as a:
                async with httpx.AsyncClient(
                    trust_env=False, base_url=a, headers=HEADERS, timeout=5
                ) as client:
                    headers = await raw_initialize(client)
                    result = await client.post(
                        "/mcp", headers=headers, json=record_request(2, "before-owner-stop")
                    )
                    assert "result" in result.json() and not result.json()["result"].get("isError"), (
                        result.text
                    )
            # The first gateway and its TCP/SSE backend connection no longer exist.
            async with httpx.AsyncClient(trust_env=False, base_url=b, headers=HEADERS, timeout=5) as client:
                unavailable = await client.post(
                    "/mcp", headers=headers, json=record_request(3, "after-owner-stop")
                )
                assert unavailable.status_code in (200, 400, 404, 503), unavailable.text
                if unavailable.status_code == 200:
                    body = unavailable.json()
                    assert "error" in body or body.get("result", {}).get("isError") is True, body
                fresh = await raw_initialize(client)
                recovered = await client.post(
                    "/mcp", headers=fresh, json=record_request(2, "explicit-new-session")
                )
                assert "result" in recovered.json() and not recovered.json()["result"].get("isError"), (
                    recovered.text
                )
    assert calls == ["before-owner-stop", "explicit-new-session"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_streamable_backend_session_survives_initializer_gateway_exit():
    calls = []
    async with real_redis() as redis_url, live_server(upstream_app("streamable-http", calls)) as upstream:
        config_a = configuration(upstream + "/mcp", "streamable-http", "streamable-initializer", redis_url)
        config_b = configuration(upstream + "/mcp", "streamable-http", "streamable-survivor", redis_url)
        async with gateway_process(config_b) as b:
            async with gateway_process(config_a) as a:
                async with httpx.AsyncClient(
                    trust_env=False, base_url=a, headers=HEADERS, timeout=5
                ) as client:
                    headers = await raw_initialize(client)
                    before = await client.post("/mcp", headers=headers, json=record_request(2, "before-exit"))
                    assert "result" in before.json() and not before.json()["result"].get("isError"), (
                        before.text
                    )
                    first = before.json()["result"].get("structuredContent") or json.loads(
                        before.json()["result"]["content"][0]["text"]
                    )
            # No SDK DELETE was sent: only A's process and connection pool were stopped.
            async with httpx.AsyncClient(trust_env=False, base_url=b, headers=HEADERS, timeout=5) as client:
                after = await client.post("/mcp", headers=headers, json=record_request(3, "after-exit"))
                assert "result" in after.json() and not after.json()["result"].get("isError"), after.text
                second = after.json()["result"].get("structuredContent") or json.loads(
                    after.json()["result"]["content"][0]["text"]
                )
                assert (first["sequence"], second["sequence"]) == (1, 2)
                assert first["upstream_session"] == second["upstream_session"]
                deleted = await client.delete("/mcp", headers=headers)
                assert deleted.status_code == 204
            assert first["upstream_session_id"], first
            async with httpx.AsyncClient(trust_env=False, timeout=3) as upstream_client:
                expired = await upstream_client.post(
                    upstream + "/mcp",
                    headers={
                        "Accept": "application/json, text/event-stream",
                        "MCP-Session-Id": first["upstream_session_id"],
                        "MCP-Protocol-Version": "2025-11-25",
                    },
                    json={"jsonrpc": "2.0", "id": "after-delete", "method": "ping"},
                )
                assert expired.status_code == 404, expired.text
    assert calls == ["before-exit", "after-exit"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_concurrent_legacy_initialization_negotiates_exactly_once():
    async with real_redis() as redis_url:
        config = GatewayConfig(
            instance_id="init-race",
            redis_url=redis_url,
            principals=[PrincipalConfig(subject="tester", token=TOKEN, allow=["*"])],
        )
        async with live_server(create_app(config)) as gateway:
            async with httpx.AsyncClient(
                trust_env=False, base_url=gateway, headers=HEADERS, timeout=5
            ) as client:
                async with client.stream("GET", "/sse") as stream:
                    lines = stream.aiter_lines()
                    endpoint = None
                    async with asyncio.timeout(3):
                        async for line in lines:
                            if line.startswith("data: "):
                                endpoint = line[6:]
                                break
                    assert endpoint and endpoint.startswith("/messages?")
                    requests = [
                        {
                            "jsonrpc": "2.0",
                            "id": i,
                            "method": "initialize",
                            "params": {
                                "protocolVersion": "2025-11-25",
                                "capabilities": {},
                                "clientInfo": {"name": "concurrent-init", "version": "1"},
                            },
                        }
                        for i in range(1, 9)
                    ]
                    accepted = await asyncio.gather(
                        *(client.post(endpoint, json=message) for message in requests)
                    )
                    assert all(response.status_code == 202 for response in accepted)
                    replies = []
                    async with asyncio.timeout(5):
                        async for line in lines:
                            if line.startswith("data: "):
                                replies.append(json.loads(line[6:]))
                                if len(replies) == len(requests):
                                    break
                    assert sum("result" in reply for reply in replies) == 1, replies
                    assert sum("error" in reply for reply in replies) == len(requests) - 1, replies


@pytest.mark.asyncio
@pytest.mark.integration
async def test_streamable_backend_can_return_sse_instead_of_json():
    calls = []
    async with live_server(upstream_app("streamable-http", calls, json_response=False)) as upstream:
        async with live_server(create_app(configuration(upstream + "/mcp"))) as gateway:
            async with sdk_session(gateway, "streamable-http") as client:
                await client.initialize()
                first = unpack(await client.call_tool("backend__record", {"message": "sse-response"}))
                assert first["message"] == "sse-response"
    assert calls == ["sse-response"]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_legacy_sse_post_endpoint_is_bound_to_authenticated_subject():
    config = GatewayConfig(
        instance_id="sse-auth",
        principals=[
            PrincipalConfig(subject="owner", token=TOKEN, allow=["*"]),
            PrincipalConfig(subject="intruder", token="other-acceptance-token", allow=["*"]),
        ],
    )
    async with live_server(create_app(config)) as gateway:
        async with httpx.AsyncClient(trust_env=False, base_url=gateway, headers=HEADERS, timeout=5) as client:
            async with client.stream("GET", "/sse") as stream:
                lines = stream.aiter_lines()
                endpoint = None
                async with asyncio.timeout(3):
                    async for line in lines:
                        if line.startswith("data: "):
                            endpoint = line[6:]
                            break
                assert endpoint
                message = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "sse-auth", "version": "1"},
                    },
                }
                stolen = await client.post(
                    endpoint, json=message, headers={"Authorization": "Bearer other-acceptance-token"}
                )
                assert stolen.status_code in (403, 404)
                rightful = await client.post(endpoint, json=message)
                assert rightful.status_code == 202
                async with asyncio.timeout(3):
                    async for line in lines:
                        if line.startswith("data: "):
                            assert "result" in json.loads(line[6:])
                            break
