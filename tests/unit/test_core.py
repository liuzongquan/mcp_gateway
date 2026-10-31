"""Core identity, configuration, metadata and distributed request bridge invariants."""

import asyncio
import json

import pytest
from pydantic import ValidationError

from mcp_gateway.app import valid_message
from mcp_gateway.auth import Authenticator, Identity
from mcp_gateway.broker import Broker, OwnerUnavailable
from mcp_gateway.config import GatewayConfig, MCPBackendConfig, PrincipalConfig, load_config
from mcp_gateway.sessions import SessionForbidden, SessionManager, SessionMissing
from mcp_gateway.store import MemoryStore


def principal(**kwargs):
    return PrincipalConfig(subject="alice", token="test-token-alice", allow=["svc__read*"], **kwargs)


def test_authentication_and_acl_deny_default():
    auth = Authenticator([principal()])
    identity = auth.authenticate("Bearer test-token-alice")
    assert identity == Identity("alice", ("svc__read*",))
    assert identity.permits("svc__read_item")
    assert identity.permits("gateway.search")
    assert not identity.permits("svc__delete")
    for invalid in (None, "", "Basic test-token-alice", "Bearer wrong", "Bearer " + "x" * 5000):
        assert auth.authenticate(invalid) is None


def test_duplicate_credentials_and_missing_secret_fail_startup(monkeypatch):
    with pytest.raises(ValueError, match="unique"):
        Authenticator([principal(), PrincipalConfig(subject="bob", token="test-token-alice")])
    with pytest.raises(ValueError, match="credential"):
        Authenticator([PrincipalConfig(subject="bob", token_env="MISSING_GATEWAY_SECRET")])
    monkeypatch.setenv("TEST_GATEWAY_SECRET", "test-environment-secret")
    auth = Authenticator([PrincipalConfig(subject="alice", token_env="TEST_GATEWAY_SECRET")])
    assert auth.authenticate("bearer test-environment-secret").subject == "alice"


@pytest.mark.parametrize("arguments", [{}, {"token": "12345678", "token_env": "ENV"}])
def test_credential_source_is_explicit(arguments):
    with pytest.raises(ValidationError):
        PrincipalConfig(subject="alice", **arguments)


def test_config_strict_unique_and_url_rules():
    with pytest.raises(ValidationError):
        GatewayConfig(principals=[principal()], typo=True)
    with pytest.raises(ValidationError, match="subjects"):
        GatewayConfig(principals=[principal(), principal()])
    for url in (
        "file:///etc/passwd",
        "http://user:secret@localhost/mcp",
        "/relative",
        "https://host/mcp#fragment",
    ):
        with pytest.raises(ValidationError):
            MCPBackendConfig(name="svc", url=url, tools=[{"name": "x"}])


def test_egress_credentials_are_separate_and_identity_scoped(monkeypatch):
    monkeypatch.setenv("SERVICE_TOKEN", "Bearer service-private")
    monkeypatch.setenv("ALICE_SERVICE_TOKEN", "Bearer service-alice-private")
    backend = MCPBackendConfig(
        name="svc",
        url="https://backend.example/mcp",
        tools=[{"name": "x"}],
        headers_env={"Authorization": "SERVICE_TOKEN"},
        identity_headers_env={"alice": {"Authorization": "ALICE_SERVICE_TOKEN"}},
    )
    assert backend.credentials("alice") == {"Authorization": "Bearer service-alice-private"}
    assert backend.credentials("bob") == {"Authorization": "Bearer service-private"}
    backend.identity_headers_env["alice"] = {"authorization": "ALICE_SERVICE_TOKEN"}
    assert backend.credentials("alice") == {"authorization": "Bearer service-alice-private"}
    monkeypatch.setenv("SERVICE_TOKEN", "value\r\nInjected: bad")
    with pytest.raises(ValueError):
        backend.credentials("bob")


@pytest.mark.parametrize(
    "header", ["Host", "Content-Length", "Mcp-Session-Id", "Routing-Hash", "x\nInjected"]
)
def test_reserved_backend_headers_cannot_be_configured(header, monkeypatch):
    monkeypatch.setenv("TEST_CREDENTIAL", "secret")
    backend = MCPBackendConfig(
        name="svc", url="http://localhost/mcp", tools=[{"name": "x"}], headers_env={header: "TEST_CREDENTIAL"}
    )
    with pytest.raises(ValueError):
        backend.credentials("alice")


def test_config_document_paths_relative_to_config_file(tmp_path):
    (tmp_path / "spec.json").write_text('{"openapi":"3.1.0"}')
    path = tmp_path / "gateway.json"
    path.write_text(
        json.dumps(
            {
                "principals": [{"subject": "alice", "token": "test-token-alice"}],
                "openapi_services": [{"name": "svc", "document": "spec.json"}],
            }
        )
    )
    assert load_config(path).openapi_services[0].document == {"openapi": "3.1.0"}


async def test_session_identity_initialization_touch_and_expiry():
    clock = [0.0]
    store = MemoryStore(clock=lambda: clock[0])
    manager = SessionManager(store, ttl=10, lock_timeout=0.2)
    session = await manager.create("alice", "streamable-http")
    assert session["routing_hash"] != session["id"]
    with pytest.raises(SessionForbidden):
        await manager.get(session["id"], "bob")
    with pytest.raises(SessionMissing):
        await manager.get("x" * 129, "alice")
    results = await asyncio.gather(
        *(manager.initialize(session["id"], "alice", "2025-11-25") for _ in range(8))
    )
    assert results.count(True) == 1
    await manager.attach_backend(session["id"], "alice", "svc")
    await manager.attach_backend(session["id"], "alice", "svc")
    assert (await manager.get(session["id"], "alice"))["backends"] == ["svc"]
    clock[0] = 9
    await manager.get(session["id"], "alice")
    clock[0] = 18
    assert await manager.get(session["id"], "alice", touch=False)
    clock[0] = 20
    with pytest.raises(SessionMissing):
        await manager.get(session["id"], "alice")


async def test_session_lock_timeout_and_owner_safe_release():
    store = MemoryStore()
    manager = SessionManager(store, ttl=10, lock_timeout=0.03)
    async with manager.lock("key"):
        with pytest.raises(TimeoutError):
            async with manager.lock("key"):
                pytest.fail("must not enter an already held lock")
    assert await store.get("lock:key") is None


async def test_session_delete_does_not_resurrect():
    manager = SessionManager(MemoryStore(), ttl=10, lock_timeout=0.1)
    session = await manager.create("alice", "sse", "owner")
    await manager.delete(session["id"], "alice")
    with pytest.raises(SessionMissing):
        await manager.update(session["id"], "alice", initialized=True)


async def test_broker_remote_result_unavailable_owner_and_duplicate_instance():
    store = MemoryStore()
    a, b = Broker(store, "a", 3, 0.2), Broker(store, "b", 3, 0.2)
    await a.start()
    await b.start()
    try:

        async def handler(method, args):
            return {"method": method, "value": args["value"]}

        b.handler = handler
        assert await a.call("b", "echo", {"value": "private-result"}) == {
            "method": "echo",
            "value": "private-result",
        }
        assert a.forwarded_requests == 1 and b.forwarded_responses == 1
        with pytest.raises(OwnerUnavailable):
            await a.call("missing", "echo", {})
        with pytest.raises(ValueError, match="already active"):
            await Broker(store, "a", 3, 0.2).start()
    finally:
        await a.close()
        await b.close()


async def test_broker_timeout_cancels_remote_without_replay():
    store = MemoryStore()
    a, b = Broker(store, "a", 3, 0.04), Broker(store, "b", 3, 0.04)
    called = []
    cancelled = asyncio.Event()

    async def handler(method, args):
        called.append(method)
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    b.handler = handler
    await a.start()
    await b.start()
    try:
        with pytest.raises(TimeoutError):
            await a.call("b", "mutate", {})
        await asyncio.wait_for(cancelled.wait(), 0.5)
        assert called == ["mutate"]
    finally:
        await a.close()
        await b.close()


async def test_broker_capacity_fails_fast_without_second_side_effect():
    store = MemoryStore()
    a, b = Broker(store, "a", 3, 0.5), Broker(store, "b", 3, 0.5, max_inflight=1)
    entered, release = asyncio.Event(), asyncio.Event()

    async def handler(method, args):
        entered.set()
        await release.wait()
        return {"ok": True}

    b.handler = handler
    await a.start()
    await b.start()
    try:
        first = asyncio.create_task(a.call("b", "first", {}))
        await entered.wait()
        with pytest.raises(OwnerUnavailable):
            await a.call("b", "second", {})
        release.set()
        assert await first == {"ok": True}
    finally:
        await a.close()
        await b.close()


def test_jsonrpc_requests_and_notifications_shape():
    assert valid_message({"jsonrpc": "2.0", "id": 0, "method": "ping"})
    assert valid_message({"jsonrpc": "2.0", "method": "notifications/initialized"})
    assert not valid_message({"jsonrpc": "2.0", "method": "initialize"})
    assert not valid_message({"jsonrpc": "2.0", "method": "ping", "id": None})
