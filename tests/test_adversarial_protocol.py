"""Independent MCP boundary/security acceptance tests (no upstream dependencies).

Protocol references:
https://modelcontextprotocol.io/specification/2025-11-25/basic/transports
https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
These deliberately exercise raw HTTP, independently of the server implementation.
"""

from contextlib import asynccontextmanager

import httpx
import pytest

from mcp_gateway.app import create_app
from mcp_gateway.config import GatewayConfig, MCPBackendConfig, PrincipalConfig

pytestmark = pytest.mark.functional

VERSIONS = ["2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"]
ALICE = "acceptance-alice-token"
BOB = "acceptance-bob-token"


def headers(token=ALICE, sid=None, version="2025-11-25"):
    result = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/event-stream",
    }
    if sid:
        result["MCP-Session-Id"] = sid
        result["MCP-Protocol-Version"] = version
    return result


def rpc(method, request_id=1, params=None):
    result = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        result["params"] = params
    return result


def initialize_request(version="2025-11-25"):
    return rpc(
        "initialize",
        params={
            "protocolVersion": version,
            "capabilities": {},
            "clientInfo": {"name": "independent-acceptance", "version": "1.0"},
        },
    )


@asynccontextmanager
async def gateway_client():
    config = GatewayConfig(
        instance_id="adversarial-boundary",
        principals=[
            PrincipalConfig(subject="alice", token=ALICE, allow=["svc__public_echo"]),
            PrincipalConfig(subject="bob", token=BOB, allow=["*"]),
        ],
        mcp_backends=[
            MCPBackendConfig(
                name="svc",
                url="http://127.0.0.1:1/mcp",
                transport="streamable-http",
                tools=[
                    {
                        "name": "public_echo",
                        "description": "Public echo tool",
                        "inputSchema": {"type": "object"},
                    },
                    {
                        "name": "private_destroy",
                        "description": "Delete private infrastructure",
                        "inputSchema": {"type": "object"},
                    },
                ],
            )
        ],
        request_timeout=1,
    )
    app = create_app(config)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://testserver",
            headers=headers(),
        ) as client:
            yield client


async def initialize(client, version="2025-11-25", token=ALICE):
    response = await client.post("/mcp", json=initialize_request(version), headers=headers(token))
    assert response.status_code == 200, response.text
    assert response.json()["result"]["protocolVersion"] in VERSIONS
    sid = response.headers["MCP-Session-Id"]
    assert sid and all(0x21 <= ord(char) <= 0x7E for char in sid)
    accepted = await client.post(
        "/mcp",
        headers=headers(token, sid, response.json()["result"]["protocolVersion"]),
        json={"jsonrpc": "2.0", "method": "notifications/initialized"},
    )
    assert accepted.status_code == 202 and accepted.content == b"", accepted.text
    return sid


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "incorrect-token"])
async def test_authentication_required_before_initialization(token):
    async with gateway_client() as client:
        supplied = {"Accept": "application/json, text/event-stream"}
        if token:
            supplied["Authorization"] = f"Bearer {token}"
        client.headers.pop("Authorization", None)
        result = await client.post("/mcp", headers=supplied, json=initialize_request())
        assert result.status_code == 401
        assert ALICE not in result.text and BOB not in result.text


@pytest.mark.asyncio
@pytest.mark.parametrize("version", VERSIONS)
async def test_supported_initialize_version_is_preserved(version):
    async with gateway_client() as client:
        response = await client.post("/mcp", json=initialize_request(version))
        assert response.status_code == 200, response.text
        assert response.json()["result"]["protocolVersion"] == version
        assert response.json()["id"] == 1
        assert "tools" in response.json()["result"]["capabilities"]


@pytest.mark.asyncio
async def test_unknown_initialize_version_negotiates_supported_fallback():
    async with gateway_client() as client:
        response = await client.post("/mcp", json=initialize_request("2099-01-01"))
        assert response.status_code == 200, response.text
        assert response.json()["result"]["protocolVersion"] in VERSIONS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        [],
        [rpc("ping")],
        None,
        7,
        "ping",
        {},
        {"jsonrpc": "1.0", "id": 3, "method": "ping"},
        {"jsonrpc": "2.0", "id": True, "method": "ping"},
        {"jsonrpc": "2.0", "id": None, "method": "ping"},
        {"jsonrpc": "2.0", "id": [], "method": "ping"},
        {"jsonrpc": "2.0", "id": 3, "method": 8},
        {"jsonrpc": "2.0", "id": 3, "method": "ping", "params": []},
    ],
)
async def test_malformed_jsonrpc_never_reaches_dispatch(message):
    async with gateway_client() as client:
        sid = await initialize(client)
        # Explicit JSON serialization retains JSON null, unlike httpx json=None.
        import json

        result = await client.post(
            "/mcp",
            content=json.dumps(message),
            headers={**headers(sid=sid), "Content-Type": "application/json"},
        )
        assert result.status_code == 400, result.text
        assert result.json()["error"]["code"] == -32600


@pytest.mark.asyncio
async def test_bad_json_returns_parse_error_without_server_exception():
    async with gateway_client() as client:
        result = await client.post(
            "/mcp", content=b'{"jsonrpc":', headers={"Content-Type": "application/json"}
        )
        assert result.status_code == 400
        assert result.json()["error"]["code"] == -32700


@pytest.mark.asyncio
async def test_session_is_required_and_unknown_session_is_not_recreated():
    async with gateway_client() as client:
        absent = await client.post("/mcp", json=rpc("tools/list", 2))
        unknown = await client.post("/mcp", json=rpc("tools/list", 2), headers=headers(sid="unknown"))
        assert absent.status_code == 400
        assert unknown.status_code == 404
        assert "MCP-Session-Id" not in unknown.headers


@pytest.mark.asyncio
async def test_session_isolation_covers_requests_and_deletion():
    async with gateway_client() as client:
        sid = await initialize(client)
        stolen = await client.post("/mcp", json=rpc("tools/list", 2), headers=headers(BOB, sid))
        deletion = await client.delete("/mcp", headers=headers(BOB, sid))
        assert stolen.status_code in (403, 404)
        assert deletion.status_code in (403, 404)
        legitimate = await client.post("/mcp", json=rpc("tools/list", 3), headers=headers(sid=sid))
        assert legitimate.status_code == 200 and "result" in legitimate.json()


@pytest.mark.asyncio
async def test_deleted_session_cannot_be_reused():
    async with gateway_client() as client:
        sid = await initialize(client)
        deleted = await client.delete("/mcp", headers=headers(sid=sid))
        assert deleted.status_code in (200, 202, 204)
        stale = await client.post("/mcp", json=rpc("tools/list", 2), headers=headers(sid=sid))
        assert stale.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["2099-01-01", "garbage", ""])
async def test_unsupported_protocol_header_is_rejected(version):
    async with gateway_client() as client:
        sid = await initialize(client)
        result = await client.post(
            "/mcp", json=rpc("tools/list", 2), headers=headers(sid=sid, version=version)
        )
        assert result.status_code == 400


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path", [("POST", "/mcp"), ("GET", "/sse"), ("POST", "/messages?session_id=unknown")]
)
async def test_hostile_origin_is_rejected_on_every_ingress(method, path):
    async with gateway_client() as client:
        result = await client.request(
            method,
            path,
            headers={"Origin": "https://attacker.invalid"},
            json=initialize_request() if method == "POST" else None,
        )
        assert result.status_code == 403


@pytest.mark.asyncio
async def test_denied_tool_cannot_be_listed_searched_or_called():
    async with gateway_client() as client:
        sid = await initialize(client)
        h = headers(sid=sid)
        listed = await client.post("/mcp", json=rpc("tools/list", 2), headers=h)
        assert listed.status_code == 200
        names = {tool["name"] for tool in listed.json()["result"]["tools"]}
        assert "svc__public_echo" in names and "gateway.search" in names
        assert "svc__private_destroy" not in names
        searched = await client.post(
            "/mcp",
            json=rpc(
                "tools/call",
                3,
                {
                    "name": "gateway.search",
                    "arguments": {"query": "private_destroy delete infrastructure", "top_k": 10},
                },
            ),
            headers=h,
        )
        assert searched.status_code == 200, searched.text
        assert "svc__private_destroy" not in searched.text
        denied = await client.post(
            "/mcp",
            json=rpc(
                "tools/call",
                4,
                {
                    "name": "svc__private_destroy",
                    "arguments": {},
                },
            ),
            headers=h,
        )
        assert denied.status_code in (200, 403)
        body = denied.json()
        assert "error" in body or body.get("result", {}).get("isError") is True
        # The inaccessible backend (port 1) must never be contacted for authorization failure.
        assert "connect" not in denied.text.lower()


@pytest.mark.asyncio
async def test_unknown_method_and_invalid_call_parameters_have_rpc_errors():
    async with gateway_client() as client:
        sid = await initialize(client)
        unknown = await client.post("/mcp", json=rpc("not/a/method", 2), headers=headers(sid=sid))
        invalid = await client.post(
            "/mcp", json=rpc("tools/call", 3, {"arguments": {}}), headers=headers(sid=sid)
        )
        assert unknown.json()["error"]["code"] == -32601
        assert invalid.json()["error"]["code"] == -32602


@pytest.mark.asyncio
async def test_unknown_cancellation_is_safe_fire_and_forget():
    async with gateway_client() as client:
        sid = await initialize(client)
        result = await client.post(
            "/mcp",
            headers=headers(sid=sid),
            json={
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": {"requestId": "never-issued", "reason": "late cancellation"},
            },
        )
        assert result.status_code == 202 and result.content == b""
        alive = await client.post("/mcp", json=rpc("ping", 2), headers=headers(sid=sid))
        assert alive.status_code == 200 and "result" in alive.json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"protocolVersion": 3, "capabilities": {}, "clientInfo": {"name": "x", "version": "1"}},
        {"protocolVersion": "2025-11-25", "capabilities": [], "clientInfo": {"name": "x", "version": "1"}},
        {"protocolVersion": "2025-11-25", "capabilities": {}},
    ],
)
async def test_initialize_required_fields_are_validated(params):
    async with gateway_client() as client:
        response = await client.post("/mcp", json=rpc("initialize", params=params))
        assert response.status_code in (200, 400), response.text
        assert response.json()["error"]["code"] == -32602
        assert "MCP-Session-Id" not in response.headers


@pytest.mark.asyncio
async def test_tool_call_before_initialized_notification_is_rejected():
    async with gateway_client() as client:
        initialized = await client.post("/mcp", json=initialize_request())
        sid = initialized.headers["MCP-Session-Id"]
        result = await client.post("/mcp", headers=headers(sid=sid), json=rpc("tools/list", 2))
        assert result.status_code in (200, 400), result.text
        assert "error" in result.json()


@pytest.mark.asyncio
async def test_oversized_body_is_rejected_before_json_parsing():
    async with gateway_client() as client:
        result = await client.post(
            "/mcp", content=b" " * (1_048_576 + 1), headers={"Content-Type": "application/json"}
        )
        assert result.status_code == 413


@pytest.mark.asyncio
@pytest.mark.parametrize("params", [[], None, 0, "invalid"])
async def test_malformed_notification_parameters_do_not_crash(params):
    async with gateway_client() as client:
        sid = await initialize(client)
        response = await client.post(
            "/mcp",
            headers=headers(sid=sid),
            json={
                "jsonrpc": "2.0",
                "method": "notifications/cancelled",
                "params": params,
            },
        )
        assert response.status_code == 400
        assert response.json()["error"]["code"] == -32600


@pytest.mark.asyncio
async def test_supported_header_cannot_change_negotiated_session_version():
    async with gateway_client() as client:
        sid = await initialize(client)
        response = await client.post(
            "/mcp", headers=headers(sid=sid, version="2024-11-05"), json=rpc("tools/list", 2)
        )
        assert response.status_code == 400


@pytest.mark.asyncio
async def test_deeply_nested_json_does_not_raise_internal_server_error():
    async with gateway_client() as client:
        response = await client.post(
            "/mcp", content=(b"[" * 1100 + b"0" + b"]" * 1100), headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 400, response.text
