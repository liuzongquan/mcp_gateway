"""Contract tests run against memory and, when configured, an actual Redis server.

Set REDIS_SERVER to a redis-server executable (or have it on PATH), or REDIS_URL
to a disposable test server. No FLUSHDB, wildcard deletion, or global mutation is
used: each RedisStore gets an isolated random prefix and short-lived keys.
"""

import asyncio
import math
import os
import shutil
import socket
import subprocess
import time
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from mcp_gateway.store import (
    QUEUE_CAPACITY,
    MemoryStore,
    RedisStore,
    Store,
    StoreUnavailable,
)


@pytest.fixture(scope="module")
def redis_url(tmp_path_factory):
    configured = os.environ.get("REDIS_URL")
    if configured:
        yield configured
        return
    executable = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")
    if not executable:
        pytest.skip("Real Redis tests require REDIS_SERVER or REDIS_URL")
    directory = tmp_path_factory.mktemp("store-redis")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    with (directory / "redis.log").open("wb") as log:
        process = subprocess.Popen(
            [executable, "--bind", "127.0.0.1", "--port", str(port), "--save", "", "--appendonly", "no"],
            cwd=directory,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 8
            while True:
                if process.poll() is not None:
                    pytest.fail("Test Redis exited before readiness")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        pytest.fail("Test Redis did not become ready")
                    time.sleep(0.02)
            yield f"redis://127.0.0.1:{port}/0"
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


@pytest.fixture(params=["memory", "redis"])
async def store(request):
    if request.param == "memory":
        instance = MemoryStore()
    else:
        instance = RedisStore(request.getfixturevalue("redis_url"), prefix=f"test-store:{uuid4()}:")
    await instance.start()
    yield instance
    await instance.close()


async def test_json_round_trip_and_copy_isolation(store):
    assert isinstance(store, Store)
    original = {"nested": {"items": [1, None, True]}, "unicode": "会话"}
    assert await store.put("one", original, 10)
    original["nested"]["items"].append(2)
    first = await store.get("one")
    assert first == {"nested": {"items": [1, None, True]}, "unicode": "会话"}
    first["nested"]["items"].clear()
    assert (await store.get("one"))["nested"]["items"] == [1, None, True]
    assert await store.get("missing") is None
    await store.delete("one")
    await store.delete("one")
    assert await store.get("one") is None


async def test_put_nx_and_overwrite(store):
    assert await store.put("key", {"x": 1}, 10, nx=True)
    assert not await store.put("key", {"x": 2}, 10, nx=True)
    assert await store.get("key") == {"x": 1}
    assert await store.put("key", {"x": 3}, 10)
    assert await store.get("key") == {"x": 3}


async def test_concurrent_nx_has_one_winner(store):
    results = await asyncio.gather(*(store.put("lock", {"token": str(i)}, 10, nx=True) for i in range(30)))
    assert sum(results) == 1


async def test_expiry_and_nx_reacquisition(store):
    assert await store.put("short", {}, 0.025)
    await asyncio.sleep(0.05)
    assert await store.get("short") is None
    assert await store.put("short", {"fresh": True}, 10, nx=True)
    assert not await store.expire("absent", 10)
    assert await store.expire("short", 0.025)
    await asyncio.sleep(0.05)
    assert await store.get("short") is None


async def test_atomic_lock_ownership_and_expiry(store):
    assert not await store.compare_delete("lease", "a")
    assert not await store.compare_expire("lease", "a", 10)
    assert await store.put("lease", {"token": "a\"\\会话"}, 10, nx=True)
    assert not await store.compare_delete("lease", "b")
    assert not await store.compare_expire("lease", "b", 0.01)
    assert await store.compare_expire("lease", "a\"\\会话", 0.025)
    await asyncio.sleep(0.05)
    assert await store.get("lease") is None
    assert await store.put("lease", {"token": "b"}, 10, nx=True)
    # A previous owner cannot delete a lease acquired after its own expiry.
    assert not await store.compare_delete("lease", "a\"\\会话")
    assert await store.compare_delete("lease", "b")
    assert await store.get("lease") is None


@pytest.mark.parametrize("ttl", [0, -1, math.inf, -math.inf, math.nan, True, "1"])
async def test_invalid_ttl_fails_without_mutation(store, ttl):
    with pytest.raises(ValueError, match="TTL"):
        await store.put("bad", {}, ttl)
    assert await store.get("bad") is None
    with pytest.raises(ValueError, match="TTL"):
        await store.expire("bad", ttl)
    with pytest.raises(ValueError, match="TTL"):
        await store.compare_expire("bad", "owner", ttl)


async def test_reject_non_json_or_non_object_values(store):
    for value in [[], None, "data", {"bad": object()}, {"bad": math.nan}]:
        with pytest.raises((TypeError, ValueError)):
            await store.put("bad", value, 10)
    assert await store.get("bad") is None


async def test_subscribe_is_ready_before_enter_returns(store):
    assert await store.publish("channel", {"gone": True}) == 0
    async with store.subscribe("channel") as first:
        async with store.subscribe("channel") as second:
            assert first.maxsize == QUEUE_CAPACITY
            assert await store.publish("channel", {"hello": [1]}) == 2
            one, two = await asyncio.wait_for(asyncio.gather(first.get(), second.get()), timeout=2)
            one["hello"].append(2)
            assert two == {"hello": [1]}
        assert await store.publish("channel", {"second": True}) == 1
        assert await asyncio.wait_for(first.get(), 2) == {"second": True}
    assert await store.publish("channel", {}) == 0


async def test_channels_are_isolated(store):
    async with store.subscribe("left") as left, store.subscribe("right") as right:
        assert await store.publish("left", {"id": 1}) == 1
        assert await asyncio.wait_for(left.get(), 2) == {"id": 1}
        assert right.empty()


async def test_close_notifies_subscribers_and_rejects_operations(store):
    async with store.subscribe("channel") as queue:
        await store.publish("channel", {"must_not_deliver_after_close": True})
        await store.close()
        assert await asyncio.wait_for(queue.get(), 2) == {"_store_error": True}
    await store.close()  # Idempotent cleanup.
    with pytest.raises(StoreUnavailable, match="closed"):
        await store.get("key")
    with pytest.raises(StoreUnavailable, match="closed"):
        await store.publish("channel", {})
    with pytest.raises(StoreUnavailable, match="closed"):
        async with store.subscribe("channel"):
            pass


async def test_slow_subscriber_is_failed_instead_of_silent_loss(store):
    async with store.subscribe("channel") as queue:
        for i in range(QUEUE_CAPACITY + 1):
            await store.publish("channel", {"index": i})
        # The Redis reader exits only after observing overflow. MemoryStore
        # delivers synchronously and has already poisoned its queue.
        if isinstance(store, RedisStore):
            task = next(iter(store._listeners))
            await asyncio.wait_for(asyncio.shield(task), 3)
        assert await queue.get() == {"_store_error": True}


async def test_memory_clock_expiry_and_garbage_collection():
    now = [100.0]
    store = MemoryStore(clock=lambda: now[0])
    await store.put("unused", {}, 0.01)
    now[0] += 1
    assert await store.put("new", {}, 10)
    assert "unused" not in store._values
    assert await store.compare_expire("new", "missing-token", 10) is False
    await store.close()


async def test_memory_subscription_context_cleans_up_after_error():
    store = MemoryStore()
    with pytest.raises(RuntimeError, match="consumer"):
        async with store.subscribe("channel"):
            raise RuntimeError("consumer")
    assert await store.publish("channel", {}) == 0
    await store.close()


async def test_redis_instances_share_data_and_pubsub(redis_url):
    prefix = f"test-shared:{uuid4()}:"
    first, second = RedisStore(redis_url, prefix), RedisStore(redis_url, prefix)
    isolated = RedisStore(redis_url, prefix + "isolated:")
    try:
        await asyncio.gather(first.start(), second.start(), isolated.start())
        assert await first.put("session", {"backend": "replica-1"}, 10)
        assert await second.get("session") == {"backend": "replica-1"}
        assert await isolated.get("session") is None
        async with first.subscribe("return") as queue:
            assert await second.publish("return", {"id": 7}) == 1
            assert await asyncio.wait_for(queue.get(), 2) == {"id": 7}
            assert await isolated.publish("return", {}) == 0
    finally:
        await asyncio.gather(first.close(), second.close(), isolated.close())


async def test_redis_corrupt_data_fails_closed(redis_url):
    store = RedisStore(redis_url, f"test-corrupt:{uuid4()}:")
    try:
        await store.start()
        for raw in ["broken", "[]", '"str"']:
            await store._client.set(store._prefix + "bad", raw, ex=10)
            with pytest.raises(StoreUnavailable, match="Invalid data"):
                await store.get("bad")
            assert not await store.compare_delete("bad", "owner")
        async with store.subscribe("channel") as queue:
            await store._client.publish(store._prefix + "channel", "not-json")
            assert await asyncio.wait_for(queue.get(), 2) == {"_store_error": True}
    finally:
        await store.close()


async def test_redis_command_errors_are_sanitized(monkeypatch):
    from redis.exceptions import ConnectionError

    store = RedisStore("redis://127.0.0.1:1/0", timeout=0.1)
    monkeypatch.setattr(store._client, "get", AsyncMock(side_effect=ConnectionError("secret:password@host")))
    with pytest.raises(StoreUnavailable) as error:
        await store.get("key")
    assert "secret" not in str(error.value)
    assert "password" not in str(error.value)
    await store.close()


async def test_redis_subscription_failure_is_bounded_and_sanitized():
    store = RedisStore("redis://127.0.0.1:1/0", timeout=0.1)
    try:
        with pytest.raises(StoreUnavailable, match="subscription unavailable"):
            async with store.subscribe("channel"):
                pytest.fail("Cannot yield before Redis acknowledged subscription")
    finally:
        await store.close()


async def test_redis_background_subscription_failure_poison(monkeypatch, redis_url):
    from redis.exceptions import ConnectionError

    store = RedisStore(redis_url, f"test-failure:{uuid4()}:")
    try:
        await store.start()
        async with store.subscribe("channel") as queue:
            task, (_queue, pubsub) = next(iter(store._listeners.items()))
            monkeypatch.setattr(pubsub, "get_message", AsyncMock(side_effect=ConnectionError("secret")))
            assert await asyncio.wait_for(queue.get(), 3) == {"_store_error": True}
            await task
    finally:
        await store.close()


@pytest.mark.parametrize("url", ["invalid://secret@host", "not-a-url"])
def test_invalid_redis_url_is_sanitized(url):
    with pytest.raises(StoreUnavailable, match="Invalid session store configuration"):
        RedisStore(url)


@pytest.mark.parametrize("timeout", [0, -1, math.inf, math.nan])
def test_invalid_redis_timeout(timeout):
    with pytest.raises(ValueError, match="timeout"):
        RedisStore("redis://127.0.0.1:6379/0", timeout=timeout)


async def test_redis_idle_subscription_stays_healthy(redis_url):
    store = RedisStore(redis_url, f"test-idle:{uuid4()}:")
    try:
        await store.start()
        async with store.subscribe("channel") as queue:
            await asyncio.sleep(1.05)  # Cross the reader's one-second polling interval.
            assert queue.empty()
            assert await store.publish("channel", {"still": "alive"}) == 1
            assert await asyncio.wait_for(queue.get(), 2) == {"still": "alive"}
    finally:
        await store.close()


async def test_real_redis_connection_loss_fails_subscription(redis_url):
    store = RedisStore(redis_url, f"test-disconnect:{uuid4()}:")
    try:
        await store.start()
        async with store.subscribe("channel") as queue:
            _task, (_queue, pubsub) = next(iter(store._listeners.items()))
            # Abort only this store's real TCP transport, never another client's
            # connection or the shared Redis server. No fake Redis is involved.
            pubsub.connection._writer.transport.abort()
            assert await asyncio.wait_for(queue.get(), 3) == {"_store_error": True}
    finally:
        await store.close()
