"""Backend lifecycle, authentication, lock and ownership tests.

SDK contexts are instrumented here to assert task ownership; real SDK transports
and independent Redis/gateway processes are covered by the system suite.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
from mcp.types import CallToolResult, TextContent

import mcp_gateway.backend as module
from mcp_gateway.backend import BackendManager, BackendUnavailable, secure_http_client_factory
from mcp_gateway.broker import Broker, OwnerUnavailable
from mcp_gateway.config import GatewayConfig, MCPBackendConfig, PrincipalConfig
from mcp_gateway.sessions import SessionForbidden, SessionManager, SessionMissing
from mcp_gateway.store import MemoryStore

TOOL = {
    "name": "record",
    "description": "Record a message",
    "inputSchema": {
        "type": "object",
        "properties": {"message": {"type": "string"}},
        "required": ["message"],
        "additionalProperties": False,
    },
}


def config(instance="gateway-a", transport="streamable-http", **kwargs):
    return GatewayConfig(
        instance_id=instance,
        session_ttl=30,
        lease_ttl=5,
        request_timeout=kwargs.pop("request_timeout", 0.3),
        connect_timeout=kwargs.pop("connect_timeout", 0.3),
        principals=[
            PrincipalConfig(subject="alice", token="alice-secret", allow=["backend__record"]),
            PrincipalConfig(subject="bob", token="bob-secret", allow=[]),
        ],
        mcp_backends=[
            MCPBackendConfig(
                name="backend",
                url="http://localhost.invalid/mcp",
                transport=transport,
                tools=[TOOL],
                headers_env={"Authorization": "BACKEND_CREDENTIAL"},
            )
        ],
        **kwargs,
    )


@pytest.fixture
def sdk(monkeypatch):
    state = SimpleNamespace(
        initializations=0,
        closed=0,
        calls=[],
        transports=[],
        headers=[],
        owners=[],
        call_delay=0.0,
        init_delay=0.0,
        init_error=None,
        call_error=None,
        cancelled=0,
        entered_call=asyncio.Event(),
        entered_init=asyncio.Event(),
        deleted=0,
        direct_requests=[],
    )
    monkeypatch.setenv("BACKEND_CREDENTIAL", "Bearer backend-only-secret")

    @asynccontextmanager
    async def transport(url, **kwargs):
        owner = asyncio.current_task()
        state.owners.append(owner)
        state.transports.append(url)
        if "http_client" in kwargs:
            state.headers.append(dict(kwargs["http_client"].headers))
        else:
            state.headers.append(kwargs["headers"])
            kwargs["on_session_created"]("actual-sse-id")
        try:
            if "http_client" in kwargs:
                yield None, None, lambda: "actual-http-id"
            else:
                yield None, None
        finally:
            assert asyncio.current_task() is owner, "SDK transport exited from a different task"
            state.closed += 1

    class FakeClientSession:
        def __init__(self, read, write, **kwargs):
            self.owner = None

        async def __aenter__(self):
            self.owner = asyncio.current_task()
            return self

        async def __aexit__(self, *args):
            assert asyncio.current_task() is self.owner, "SDK session exited from a different task"

        async def initialize(self):
            state.initializations += 1
            state.entered_init.set()
            await asyncio.sleep(state.init_delay)
            if state.init_error:
                raise state.init_error
            return SimpleNamespace(protocolVersion="2025-11-25")

        async def call_tool(self, name, arguments):
            state.calls.append((name, dict(arguments)))
            state.entered_call.set()
            try:
                await asyncio.sleep(state.call_delay)
            except asyncio.CancelledError:
                state.cancelled += 1
                raise
            if state.call_error:
                raise state.call_error
            return CallToolResult(content=[TextContent(type="text", text=arguments["message"])])

    async def handler(request):
        state.direct_requests.append(request)
        if request.method == "DELETE":
            state.deleted += 1
            return httpx.Response(200)
        body = json.loads(request.content)
        if body.get("method") == "notifications/cancelled":
            return httpx.Response(202)
        result = await FakeClientSession(None, None).call_tool(
            body["params"]["name"], body["params"]["arguments"]
        )
        return httpx.Response(
            200, json={"jsonrpc": "2.0", "id": body["id"], "result": result.model_dump(mode="json")}
        )

    real_factory = module.secure_http_client_factory

    def factory(**kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_factory(**kwargs)

    monkeypatch.setattr(module, "secure_http_client_factory", factory)
    monkeypatch.setattr(module, "streamable_http_client", transport)
    monkeypatch.setattr(module, "sse_client", transport)
    monkeypatch.setattr(module, "ClientSession", FakeClientSession)
    return state


@asynccontextmanager
async def manager_context(configuration=None, store=None):
    configuration = configuration or config()
    shared = store or MemoryStore()
    await shared.start()
    sessions = SessionManager(shared, configuration.session_ttl, 2)
    broker = Broker(shared, configuration.instance_id, configuration.lease_ttl, configuration.request_timeout)
    await broker.start()
    manager = BackendManager(configuration, shared, sessions, broker)

    async def handler(method, args):
        assert method == "backend_call"
        return await manager.call_local(args)

    broker.handler = handler
    try:
        yield manager, sessions, broker, shared
    finally:
        await manager.close()
        await broker.close()
        if store is None:
            await shared.close()


@pytest.mark.parametrize("transport", ["sse", "streamable-http"])
async def test_lazy_reuse_headers_actual_session_and_same_task_cleanup(sdk, transport):
    async with manager_context(config(transport=transport)) as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        assert manager.connection_count == 0
        first = await manager.call(frontend, manager.backends["backend"], "record", {"message": "one"})
        second = await manager.call(frontend, manager.backends["backend"], "record", {"message": "two"})
        assert first["content"][0]["text"] == "one"
        assert second["content"][0]["text"] == "two"
        assert sdk.initializations == 1 and manager.connection_count == 1
        headers = {name.lower(): value for name, value in sdk.headers[0].items()}
        assert headers["authorization"] == "Bearer backend-only-secret"
        assert headers["routing-hash"] == frontend["routing_hash"]
        assert "alice-secret" not in str(headers) and frontend["id"] not in str(headers)
        metadata = await store.get(f"backend:{frontend['id']}:backend")
        assert metadata["backend_session_id"] == f"actual-{'sse' if transport == 'sse' else 'http'}-id"
        await manager.close_session(frontend["id"])
        assert manager.connection_count == 0 and sdk.closed == 1
        assert await store.get(f"backend:{frontend['id']}:backend") is None
        with pytest.raises(BackendUnavailable, match="state was lost"):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "never replay"})
        assert len(sdk.calls) == 2


async def test_simultaneous_first_calls_create_only_one_connection(sdk):
    sdk.init_delay = 0.04
    async with manager_context() as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        results = await asyncio.gather(
            *(
                manager.call(frontend, manager.backends["backend"], "record", {"message": str(i)})
                for i in range(12)
            )
        )
        assert len(results) == 12 and sdk.initializations == 1 and len(sdk.calls) == 12


async def test_cross_gateway_owner_forwarding_reuses_connection(sdk):
    shared = MemoryStore()
    try:
        async with manager_context(config("a", transport="sse"), shared) as (a, sessions, _, __):
            async with manager_context(config("b", transport="sse"), shared) as (b, _, broker_b, __):
                frontend = await sessions.create("alice", "streamable-http")
                await a.call(frontend, a.backends["backend"], "record", {"message": "local"})
                result = await b.call(frontend, b.backends["backend"], "record", {"message": "forwarded"})
                assert result["content"][0]["text"] == "forwarded"
                assert sdk.initializations == 1 and a.connection_count == 1 and b.connection_count == 0
                assert broker_b.forwarded_requests == 1
    finally:
        await shared.close()


async def test_caller_timeout_cancels_without_replay_and_session_remains_usable(sdk):
    async with manager_context(config(request_timeout=0.03)) as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        sdk.call_delay = 1
        with pytest.raises(TimeoutError, match="outcome may be unknown"):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "side effect"})
        await asyncio.sleep(0.01)
        assert len(sdk.calls) == 1 and sdk.cancelled == 1
        sdk.call_delay = 0
        assert (await manager.call(frontend, manager.backends["backend"], "record", {"message": "later"}))[
            "isError"
        ] is False
        assert sdk.initializations == 1


async def test_failed_initialization_leaves_durable_no_reconnect_marker(sdk):
    sdk.init_error = RuntimeError("credential=SECRET must never escape")
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        with pytest.raises(BackendUnavailable) as error:
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "first"})
        assert "SECRET" not in str(error.value)
        assert manager.connection_count == 0 and sdk.closed == 1
        assert (await sessions.get(frontend["id"], "alice"))["backends"] == ["backend"]
        assert await store.get(f"backend:{frontend['id']}:backend") is None
        sdk.init_error = None
        with pytest.raises(BackendUnavailable, match="state was lost"):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "retry"})
        assert sdk.initializations == 1


async def test_initialize_timeout_cleans_contexts_and_never_runs_tool(sdk):
    sdk.init_delay = 1
    async with manager_context(config(connect_timeout=0.02)) as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        with pytest.raises(BackendUnavailable):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "never"})
        assert sdk.closed == 1 and not sdk.calls and manager.connection_count == 0


async def test_unknown_expired_foreign_and_denied_sessions_fail_closed(sdk):
    async with manager_context() as (manager, sessions, _, __):
        backend = manager.backends["backend"]
        frontend = await sessions.create("alice", "streamable-http")
        with pytest.raises(SessionForbidden):
            await manager.call(frontend | {"subject": "bob"}, backend, "record", {"message": "denied"})
        with pytest.raises(SessionMissing):
            await manager.call(frontend | {"id": "unknown"}, backend, "record", {"message": "missing"})
        await sessions.delete(frontend["id"], "alice")
        with pytest.raises(SessionMissing):
            await manager.call(frontend, backend, "record", {"message": "expired"})
        assert not sdk.initializations


async def test_invalid_tool_and_arguments_rejected_before_connect(sdk):
    async with manager_context() as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        backend = manager.backends["backend"]
        with pytest.raises(SessionForbidden):
            await manager.call(frontend, backend, "admin", {})
        with pytest.raises(ValueError, match="arguments"):
            await manager.call(frontend, backend, "record", {"message": 9})
        assert not sdk.initializations


async def test_rpc_cannot_bypass_acl_or_ownership(sdk):
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "first"})
        args = {
            "session_id": frontend["id"],
            "subject": "bob",
            "backend": "backend",
            "tool": "record",
            "arguments": {"message": "attack"},
        }
        with pytest.raises(SessionForbidden):
            await manager.call_local(args)
        args["subject"] = "alice"
        metadata = await store.get(f"backend:{frontend['id']}:backend")
        metadata["token"] = "replacement-connection"
        await store.put(f"backend:{frontend['id']}:backend", metadata, 10)
        with pytest.raises(BackendUnavailable):
            await manager.call_local(args)
        assert len(sdk.calls) == 1


async def test_lost_lease_fences_existing_connections_and_reap_cleans_expired(sdk):
    async with manager_context(config(transport="sse")) as (manager, sessions, broker, store):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "first"})
        await store.put(broker.lease_key, {"token": "other-owner-generation"}, 10)
        with pytest.raises(OwnerUnavailable):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "not replayed"})
        await sessions.delete(frontend["id"], "alice")
        await manager.reap()
        assert manager.connection_count == 0 and sdk.closed == 1 and len(sdk.calls) == 1


async def test_missing_mapping_with_live_connection_never_reinitializes(sdk):
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "first"})
        await store.delete(f"backend:{frontend['id']}:backend")
        with pytest.raises(BackendUnavailable):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "never"})
        assert sdk.initializations == 1 and len(sdk.calls) == 1


async def test_close_cancels_active_requests_and_blocks_future_calls(sdk):
    sdk.call_delay = 10
    async with manager_context(config(transport="sse")) as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        pending = asyncio.create_task(
            manager.call(frontend, manager.backends["backend"], "record", {"message": "x"})
        )
        await asyncio.wait_for(sdk.entered_call.wait(), 1)
        await manager.close()
        with pytest.raises(BackendUnavailable):
            await pending
        with pytest.raises(BackendUnavailable, match="shutting down"):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "never"})
        assert manager.connection_count == 0 and sdk.closed == 1


async def test_caller_cancelled_during_initialize_does_not_orphan_connection(sdk):
    sdk.init_delay = 10
    async with manager_context() as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        pending = asyncio.create_task(
            manager.call(frontend, manager.backends["backend"], "record", {"message": "x"})
        )
        await asyncio.wait_for(sdk.entered_init.wait(), 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert manager.connection_count == 0 and sdk.closed == 1 and not sdk.calls


async def test_http_factory_ignores_environment_proxies_and_redirect_override(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "https://proxy.invalid:9999")
    async with secure_http_client_factory(follow_redirects=True, trust_env=True) as client:
        assert client.follow_redirects is False
        assert not client._trust_env


async def test_streamable_backend_survives_initializer_gateway_shutdown(sdk):
    shared = MemoryStore()
    try:
        async with manager_context(config("a"), shared) as (a, sessions, broker_a, _):
            async with manager_context(config("b"), shared) as (b, _, broker_b, __):
                frontend = await sessions.create("alice", "streamable-http")
                await a.call(frontend, a.backends["backend"], "record", {"message": "before"})
                await a.close()
                await broker_a.close()
                metadata = await shared.get(f"backend:{frontend['id']}:backend")
                assert metadata is not None and metadata["protocol_version"] == "2025-11-25"
                result = await b.call(frontend, b.backends["backend"], "record", {"message": "after"})
                assert result["content"][0]["text"] == "after"
                assert sdk.initializations == 1 and sdk.deleted == 0 and broker_b.forwarded_requests == 0
                headers = sdk.direct_requests[-1].headers
                assert headers["MCP-Session-Id"] == "actual-http-id"
                assert headers["MCP-Protocol-Version"] == "2025-11-25"
                assert headers["Routing-Hash"] == frontend["routing_hash"]
                await b.close_session(frontend["id"])
                assert sdk.deleted == 1
                assert await shared.get(f"backend:{frontend['id']}:backend") is None
    finally:
        await shared.close()


@pytest.mark.parametrize("response_kind", ["json", "sse_lf", "sse_crlf", "sse_cr", "sse_split_crlf"])
async def test_direct_response_parsing_preserves_structured_and_error_results(sdk, response_kind):
    async with manager_context() as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "init"})
        await manager._http.aclose()

        async def handler(request):
            request_id = json.loads(request.content)["id"]
            result = {
                "content": [{"type": "text", "text": "tool-domain-error"}],
                "isError": True,
                "structuredContent": {"recovered": False},
            }
            payload = {"jsonrpc": "2.0", "id": request_id, "result": result}
            if response_kind == "json":
                return httpx.Response(200, json=payload)
            newline = "\r\n" if "crlf" in response_kind else "\r" if response_kind == "sse_cr" else "\n"
            other = json.dumps({"jsonrpc": "2.0", "method": "notifications/progress", "params": {}})
            data = (
                f": keepalive{newline}{newline}data: {other}{newline}{newline}"
                f"event: message{newline}data: {json.dumps(payload)}{newline}{newline}"
            ).encode()
            if response_kind == "sse_split_crlf":

                class SplitStream(httpx.AsyncByteStream):
                    async def __aiter__(self):
                        for value in data:
                            yield bytes([value])

                return httpx.Response(
                    200, stream=SplitStream(), headers={"content-type": "text/event-stream"}
                )
            return httpx.Response(200, content=data, headers={"content-type": "text/event-stream"})

        manager._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
        result = await manager.call(frontend, manager.backends["backend"], "record", {"message": "request"})
        assert result["isError"] is True and result["structuredContent"] == {"recovered": False}


@pytest.mark.parametrize(
    "kind",
    [
        "expired",
        "wrong_id",
        "rpc_error",
        "invalid_result",
        "wrong_content_type",
        "over_limit_json",
        "over_limit_sse",
        "incomplete_sse",
    ],
)
async def test_direct_response_failures_are_bounded_sanitized_and_never_retried(sdk, kind):
    async with manager_context() as (manager, sessions, _, __):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "init"})
        await manager._http.aclose()
        requests = []

        async def handler(request):
            requests.append(request)
            request_id = json.loads(request.content)["id"]
            if kind == "expired":
                return httpx.Response(404)
            if kind == "wrong_id":
                return httpx.Response(200, json={"jsonrpc": "2.0", "id": "wrong", "result": {"content": []}})
            if kind == "rpc_error":
                return httpx.Response(
                    200,
                    json={
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "error": {"code": -1, "message": "SECRET TOKEN"},
                    },
                )
            if kind == "invalid_result":
                return httpx.Response(
                    200, json={"jsonrpc": "2.0", "id": request_id, "result": {"content": 9}}
                )
            if kind == "wrong_content_type":
                return httpx.Response(200, text="SECRET TOKEN")
            if kind.startswith("over_limit"):
                content_type = "application/json" if kind.endswith("json") else "text/event-stream"
                return httpx.Response(
                    200, content=b"x" * (8 * 1024 * 1024 + 1), headers={"content-type": content_type}
                )
            return httpx.Response(
                200, content='data: {"jsonrpc":"2.0"}', headers={"content-type": "text/event-stream"}
            )

        manager._http = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
        with pytest.raises(BackendUnavailable) as error:
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "request"})
        assert "SECRET" not in str(error.value)
        assert len(requests) == 1 and sdk.initializations == 1


async def test_frontend_deleted_during_initialization_cannot_publish_orphan_mapping(sdk):
    sdk.init_delay = 0.04
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        pending = asyncio.create_task(
            manager.call(frontend, manager.backends["backend"], "record", {"message": "x"})
        )
        await asyncio.wait_for(sdk.entered_init.wait(), 1)
        await sessions.delete(frontend["id"], "alice")
        with pytest.raises(BackendUnavailable):
            await pending
        assert await store.get(f"backend:{frontend['id']}:backend") is None
        assert not sdk.calls and manager.connection_count == 0


async def test_shutdown_cancels_direct_streamable_call_without_deleting_backend_state(sdk):
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "initialize"})
        sdk.entered_call.clear()
        sdk.call_delay = 10
        pending = asyncio.create_task(
            manager.call(frontend, manager.backends["backend"], "record", {"message": "x"})
        )
        await asyncio.wait_for(sdk.entered_call.wait(), 1)
        await manager.close()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert sdk.deleted == 0 and manager.connection_count == 0
        assert await store.get(f"backend:{frontend['id']}:backend") is not None
        bodies = [json.loads(request.content) for request in sdk.direct_requests if request.content]
        tool_requests = [body for body in bodies if body["method"] == "tools/call"]
        cancellations = [body for body in bodies if body["method"] == "notifications/cancelled"]
        assert len(tool_requests) == 2 and len(cancellations) == 1
        assert cancellations[0]["params"]["requestId"] == tool_requests[-1]["id"]


async def test_missing_streamable_protocol_metadata_fails_closed(sdk):
    async with manager_context() as (manager, sessions, _, store):
        frontend = await sessions.create("alice", "streamable-http")
        await manager.call(frontend, manager.backends["backend"], "record", {"message": "initialize"})
        key = f"backend:{frontend['id']}:backend"
        metadata = await store.get(key)
        del metadata["protocol_version"]
        await store.put(key, metadata, 10)
        with pytest.raises(BackendUnavailable, match="protocol metadata"):
            await manager.call(frontend, manager.backends["backend"], "record", {"message": "never"})
        assert len(sdk.calls) == 1 and sdk.initializations == 1
