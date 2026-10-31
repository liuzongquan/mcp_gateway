"""System tests: independent gateway, official MCP backend and real Redis processes.

No gateway or backend objects are shared with the test runner. Session state can
cross process boundaries only through Redis and actual HTTP requests.
"""

import asyncio
import json
import os
import shutil
import socket
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import redis.asyncio as redis

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
TOKEN = "process-cluster-alice-token"
OTHER_TOKEN = "process-cluster-bob-token"
PREFIX = "process-cluster:"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json, text/event-stream"}
PROTOCOL = "2025-11-25"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@dataclass
class ServiceProcess:
    process: asyncio.subprocess.Process
    url: str
    log: Path

    async def crash(self):
        assert self.process.returncode is None
        self.process.kill()
        await asyncio.wait_for(self.process.wait(), 5)
        assert self.process.returncode != 0


@asynccontextmanager
async def subprocess_service(command, *, role, directory, url):
    log_path = directory / f"{role}.log"
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src") + os.pathsep + str(ROOT), PYTHONUNBUFFERED="1")
    with log_path.open("wb") as log:
        process = await asyncio.create_subprocess_exec(
            *command, cwd=ROOT, env=env, stdout=log, stderr=asyncio.subprocess.STDOUT
        )
        service = ServiceProcess(process, url, log_path)
        try:
            yield service
        finally:
            if process.returncode is None:
                process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                process.kill()
                await asyncio.wait_for(process.wait(), 5)
            assert process.returncode is not None, f"Leaked {role} process {process.pid}"


async def ready_http(service):
    deadline = time.monotonic() + 15
    async with httpx.AsyncClient(trust_env=False, timeout=0.4) as client:
        while True:
            assert service.process.returncode is None, service.log.read_text()
            try:
                response = await client.get(service.url + "/healthz")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            assert time.monotonic() < deadline, service.log.read_text()
            await asyncio.sleep(0.03)


@pytest.fixture
async def cluster(tmp_path):
    executable = os.getenv("REDIS_SERVER") or shutil.which("redis-server")
    assert executable and os.access(executable, os.X_OK), (
        "Install Redis or set REDIS_SERVER; system tests must run"
    )
    redis_port, backend_port, a_port, b_port = [free_port() for _ in range(4)]
    redis_url = f"redis://127.0.0.1:{redis_port}/0"
    backend_url = f"http://127.0.0.1:{backend_port}"
    async with AsyncExitStack() as stack:
        redis_process = await stack.enter_async_context(
            subprocess_service(
                [
                    executable,
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(redis_port),
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--dir",
                    str(tmp_path),
                ],
                role="redis",
                directory=tmp_path,
                url=redis_url,
            )
        )
        storage = redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
        stack.push_async_callback(storage.aclose)
        deadline = time.monotonic() + 10
        while True:
            assert redis_process.process.returncode is None, redis_process.log.read_text()
            try:
                if await storage.ping():
                    break
            except redis.ConnectionError:
                pass
            assert time.monotonic() < deadline, "Redis readiness timeout"
            await asyncio.sleep(0.02)
        backend = await stack.enter_async_context(
            subprocess_service(
                [
                    sys.executable,
                    "-m",
                    "uvicorn",
                    "examples.demo_backend:app",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(backend_port),
                    "--log-level",
                    "error",
                    "--no-access-log",
                ],
                role="backend",
                directory=tmp_path,
                url=backend_url,
            )
        )
        await ready_http(backend)
        nodes = {}
        for name, port in (("a", a_port), ("b", b_port)):
            config = {
                "instance_id": "node-" + name,
                "redis_url": redis_url,
                "redis_prefix": PREFIX,
                "session_ttl": 60,
                "request_timeout": 2,
                "connect_timeout": 3,
                "lease_ttl": 1,
                "principals": [
                    {"subject": "alice", "token": TOKEN, "allow": ["demo__*"]},
                    {"subject": "bob", "token": OTHER_TOKEN, "allow": ["demo__*"]},
                ],
                "mcp_backends": [
                    {
                        "name": "demo",
                        "url": backend_url + "/mcp",
                        "transport": "streamable-http",
                        "tools": [
                            {
                                "name": "increment",
                                "description": "Session-local counter",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {"amount": {"type": "integer", "default": 1}},
                                    "additionalProperties": False,
                                },
                            }
                        ],
                    }
                ],
            }
            path = tmp_path / f"gateway-{name}.json"
            path.write_text(json.dumps(config))
            nodes[name] = await stack.enter_async_context(
                subprocess_service(
                    [
                        sys.executable,
                        "-m",
                        "mcp_gateway",
                        "--config",
                        str(path),
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                        "--log-level",
                        "error",
                    ],
                    role="gateway-" + name,
                    directory=tmp_path,
                    url=f"http://127.0.0.1:{port}",
                )
            )
        await asyncio.gather(*(ready_http(node) for node in nodes.values()))
        assert (
            len(
                {
                    os.getpid(),
                    backend.process.pid,
                    redis_process.process.pid,
                    nodes["a"].process.pid,
                    nodes["b"].process.pid,
                }
            )
            == 5
        )
        async with httpx.AsyncClient(trust_env=False, timeout=7, headers=HEADERS) as http:
            yield {"nodes": nodes, "storage": storage, "http": http, "backend": backend}


async def initialize(http, url):
    response = await http.post(
        url + "/mcp",
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL,
                "capabilities": {},
                "clientInfo": {"name": "process-test", "version": "1"},
            },
        },
    )
    assert response.status_code == 200, response.text
    assert "result" in response.json(), response.text
    assert response.json()["result"]["protocolVersion"] == PROTOCOL
    sid = response.headers["mcp-session-id"]
    initialized = await http.post(
        url + "/mcp",
        headers=session_headers(sid),
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert initialized.status_code == 202, initialized.text
    return sid


def session_headers(sid):
    return {"MCP-Session-Id": sid, "MCP-Protocol-Version": PROTOCOL}


async def increment(http, url, sid, amount, request_id):
    response = await http.post(
        url + "/mcp",
        headers=session_headers(sid),
        json={
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": "demo__increment", "arguments": {"amount": amount}},
        },
    )
    assert response.status_code == 200, response.text
    message = response.json()
    assert "result" in message, message
    assert not message["result"].get("isError"), message
    result = message["result"]
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


async def test_cross_process_sessions_affinity_metadata_and_identity_isolation(cluster):
    http, storage = cluster["http"], cluster["storage"]
    a, b = cluster["nodes"]["a"].url, cluster["nodes"]["b"].url
    sid = await initialize(http, a)
    assert await storage.get(PREFIX + f"backend:{sid}:demo") is None, "Backend must initialize lazily"
    first = await increment(http, a, sid, 1, 10)
    second = await increment(http, b, sid, 2, 11)
    third = await increment(http, a, sid, 3, 12)
    assert [first["count"], second["count"], third["count"]] == [1, 3, 6]
    session = json.loads(await storage.get(PREFIX + f"session:{sid}"))
    mapping = json.loads(await storage.get(PREFIX + f"backend:{sid}:demo"))
    assert mapping["backend_session_id"] and mapping["backend_session_id"] != sid
    assert mapping["routing_hash"] == session["routing_hash"] == first["routing_hash"]
    assert first["routing_hash"] == second["routing_hash"] == third["routing_hash"]
    # Prove that the recorded ID is the actual official SDK backend session ID,
    # not a fabricated surrogate, by issuing a read-only increment of zero.
    upstream = await http.post(
        cluster["backend"].url + "/mcp",
        headers={
            **session_headers(mapping["backend_session_id"]),
            "Routing-Hash": mapping["routing_hash"],
        },
        json={
            "jsonrpc": "2.0",
            "id": 700,
            "method": "tools/call",
            "params": {"name": "increment", "arguments": {"amount": 0}},
        },
    )
    assert upstream.status_code == 200, upstream.text
    upstream_result = upstream.json()["result"]
    assert not upstream_result.get("isError"), upstream_result
    upstream_data = upstream_result.get("structuredContent") or json.loads(
        upstream_result["content"][0]["text"]
    )
    assert upstream_data["count"] == 6
    independent = await initialize(http, b)
    separate = await increment(http, a, independent, 1, 13)
    assert separate["count"] == 1
    assert separate["routing_hash"] != first["routing_hash"]
    forbidden = await http.post(
        b + "/mcp",
        headers={**session_headers(sid), "Authorization": f"Bearer {OTHER_TOKEN}"},
        json={"jsonrpc": "2.0", "id": 14, "method": "tools/list"},
    )
    assert forbidden.status_code == 403
    assert (await increment(http, b, sid, 0, 15))["count"] == 6
    deleted = await http.delete(b + "/mcp", headers=session_headers(sid))
    assert deleted.status_code == 204
    assert await storage.get(PREFIX + f"session:{sid}") is None
    stale = await http.post(
        a + "/mcp", headers=session_headers(sid), json={"jsonrpc": "2.0", "id": 16, "method": "ping"}
    )
    assert stale.status_code == 404


async def test_streamable_session_survives_gateway_process_crash_without_reinitialize(cluster):
    http, storage = cluster["http"], cluster["storage"]
    a, b = cluster["nodes"]["a"], cluster["nodes"]["b"]
    sid = await initialize(http, a.url)
    assert (await increment(http, a.url, sid, 5, 20))["count"] == 5
    before = json.loads(await storage.get(PREFIX + f"backend:{sid}:demo"))
    await a.crash()
    async with asyncio.timeout(4):
        while await storage.get(PREFIX + "instance:node-a") is not None:  # noqa: ASYNC110 - external Redis TTL
            await asyncio.sleep(0.03)
    assert (
        json.loads(await storage.get(PREFIX + f"backend:{sid}:demo"))["backend_session_id"]
        == before["backend_session_id"]
    )
    continued = await increment(http, b.url, sid, 7, 21)
    assert continued["count"] == 12, "A crash must not silently create a new upstream session"
    assert continued["routing_hash"] == before["routing_hash"]
    after = json.loads(await storage.get(PREFIX + f"backend:{sid}:demo"))
    assert after["backend_session_id"] == before["backend_session_id"]
    assert (await http.get(b.url + "/healthz")).status_code == 200
