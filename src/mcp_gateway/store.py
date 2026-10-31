"""Shared session metadata, leases and bounded, fail-closed Pub/Sub.

Redis Pub/Sub is deliberately at-most-once, not a durable message queue. A lost
subscription or overflowing local queue produces ``{"_store_error": True}``;
callers must fail pending work rather than silently claiming response delivery.
MemoryStore is only suitable for a single process sharing the same store object.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from typing import Any, Protocol, runtime_checkable

QUEUE_CAPACITY = 256


class StoreUnavailable(RuntimeError):
    """Sanitized infrastructure failure; never include credentials or Redis URLs."""


@runtime_checkable
class Store(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def get(self, key: str) -> dict[str, Any] | None: ...
    async def put(self, key: str, value: dict[str, Any], ttl: float, nx: bool = False) -> bool: ...
    async def delete(self, key: str) -> None: ...
    async def expire(self, key: str, ttl: float) -> bool: ...
    async def compare_delete(self, key: str, token: str) -> bool: ...
    async def compare_expire(self, key: str, token: str, ttl: float) -> bool: ...
    def subscribe(self, channel: str) -> AbstractAsyncContextManager[asyncio.Queue[dict[str, Any]]]: ...
    async def publish(self, channel: str, message: dict[str, Any]) -> int: ...


def _milliseconds(ttl: float) -> int:
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or not math.isfinite(ttl) or ttl <= 0:
        raise ValueError("TTL must be a positive finite number of seconds")
    return max(1, math.ceil(ttl * 1000))


def _encode(value: dict[str, Any]) -> str:
    if not isinstance(value, dict):
        raise TypeError("Store values must be JSON objects")
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _decode(value: str | bytes) -> dict[str, Any]:
    try:
        result = json.loads(value)
        if not isinstance(result, dict):
            raise ValueError("not an object")
        return result
    except (ValueError, TypeError, UnicodeError):
        raise StoreUnavailable("Invalid data in session store") from None


def _poison(queue: asyncio.Queue[dict[str, Any]]) -> None:
    # Discard partial delivery: the consumer must see failure before more work.
    while not queue.empty():
        queue.get_nowait()
    queue.put_nowait({"_store_error": True})


class MemoryStore:
    """Single-event-loop store; share this object for in-process gateway tests.

    JSON round trips intentionally isolate mutations just as Redis does. All
    operations are atomic within the event loop because they never yield.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._values: dict[str, tuple[str, float]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self._closed = False

    async def start(self) -> None:
        self._check_open()

    def _check_open(self) -> None:
        if self._closed:
            raise StoreUnavailable("Session store is closed")

    def _lookup(self, key: str) -> str | None:
        item = self._values.get(key)
        if item is None:
            return None
        if item[1] <= self._clock():
            self._values.pop(key, None)
            return None
        return item[0]

    async def close(self) -> None:
        self._closed = True
        for subscribers in self._subscribers.values():
            for queue in subscribers:
                _poison(queue)
        self._subscribers.clear()
        self._values.clear()

    async def get(self, key: str) -> dict[str, Any] | None:
        self._check_open()
        value = self._lookup(key)
        return None if value is None else _decode(value)

    async def put(self, key: str, value: dict[str, Any], ttl: float, nx: bool = False) -> bool:
        self._check_open()
        milliseconds = _milliseconds(ttl)
        payload = _encode(value)
        now = self._clock()
        # Expired, never-read sessions must not accumulate indefinitely.
        for expired in [name for name, item in self._values.items() if item[1] <= now]:
            self._values.pop(expired, None)
        if nx and self._lookup(key) is not None:
            return False
        self._values[key] = (payload, now + milliseconds / 1000)
        return True

    async def delete(self, key: str) -> None:
        self._check_open()
        self._values.pop(key, None)

    async def expire(self, key: str, ttl: float) -> bool:
        self._check_open()
        milliseconds = _milliseconds(ttl)
        value = self._lookup(key)
        if value is None:
            return False
        self._values[key] = (value, self._clock() + milliseconds / 1000)
        return True

    async def compare_delete(self, key: str, token: str) -> bool:
        self._check_open()
        value = self._lookup(key)
        if value is None or _decode(value).get("token") != token:
            return False
        self._values.pop(key, None)
        return True

    async def compare_expire(self, key: str, token: str, ttl: float) -> bool:
        self._check_open()
        milliseconds = _milliseconds(ttl)
        value = self._lookup(key)
        if value is None or _decode(value).get("token") != token:
            return False
        self._values[key] = (value, self._clock() + milliseconds / 1000)
        return True

    @asynccontextmanager
    async def subscribe(self, channel: str) -> AsyncIterator[asyncio.Queue[dict[str, Any]]]:
        self._check_open()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=QUEUE_CAPACITY)
        subscribers = self._subscribers.setdefault(channel, set())
        subscribers.add(queue)
        try:
            yield queue
        finally:
            subscribers.discard(queue)
            if not subscribers and self._subscribers.get(channel) is subscribers:
                self._subscribers.pop(channel, None)

    async def publish(self, channel: str, message: dict[str, Any]) -> int:
        self._check_open()
        payload = _encode(message)
        subscribers = self._subscribers.get(channel, set())
        count = len(subscribers)
        for queue in list(subscribers):
            try:
                queue.put_nowait(_decode(payload))
            except asyncio.QueueFull:
                _poison(queue)
                subscribers.discard(queue)
        return count


_COMPARE_DELETE = """
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local ok, decoded = pcall(cjson.decode, value)
if ok and type(decoded) == 'table' and decoded.token == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
_COMPARE_EXPIRE = """
local value = redis.call('GET', KEYS[1])
if not value then return 0 end
local ok, decoded = pcall(cjson.decode, value)
if ok and type(decoded) == 'table' and decoded.token == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""


class RedisStore:
    """Real Redis session store with atomic leases and acknowledged subscriptions.

    Connection/command timeouts are bounded. Transparent retries are disabled:
    reconnecting a lost Pub/Sub connection cannot recover missed responses.
    Prefixes isolate independent gateways; Redis ACLs/network isolation remain
    deployment responsibilities. Positive TTL seconds are rounded up to ms.
    """

    def __init__(self, url: str, prefix: str = "mcpgw:", *, timeout: float = 2.0) -> None:
        import redis.asyncio as redis
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff
        from redis.exceptions import RedisError

        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Store timeout must be positive and finite")
        self._errors = (RedisError, OSError, TimeoutError, UnicodeError)
        self._prefix = prefix
        self._timeout = timeout
        try:
            self._client = redis.from_url(
                url,
                decode_responses=True,
                socket_connect_timeout=timeout,
                socket_timeout=timeout,
                retry=Retry(NoBackoff(), 0),
                retry_on_timeout=False,
            )
        except (ValueError, TypeError):
            raise StoreUnavailable("Invalid session store configuration") from None
        self._closed = False
        self._listeners: dict[asyncio.Task[None], tuple[asyncio.Queue[dict[str, Any]], Any]] = {}

    def _check_open(self) -> None:
        if self._closed:
            raise StoreUnavailable("Session store is closed")

    async def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        self._check_open()
        try:
            async with asyncio.timeout(self._timeout):
                return await getattr(self._client, method)(*args, **kwargs)
        except self._errors:
            raise StoreUnavailable("Session store unavailable") from None

    async def start(self) -> None:
        await self._call("ping")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        listeners = list(self._listeners.items())
        for task, (queue, _pubsub) in listeners:
            _poison(queue)
            task.cancel()
        for task, (_queue, pubsub) in listeners:
            with suppress(asyncio.CancelledError):
                await task
            with suppress(*self._errors):
                await pubsub.aclose()
        self._listeners.clear()
        with suppress(*self._errors):
            await self._client.aclose()

    async def get(self, key: str) -> dict[str, Any] | None:
        value = await self._call("get", self._prefix + key)
        return None if value is None else _decode(value)

    async def put(self, key: str, value: dict[str, Any], ttl: float, nx: bool = False) -> bool:
        return bool(await self._call("set", self._prefix + key, _encode(value), px=_milliseconds(ttl), nx=nx))

    async def delete(self, key: str) -> None:
        await self._call("delete", self._prefix + key)

    async def expire(self, key: str, ttl: float) -> bool:
        return bool(await self._call("pexpire", self._prefix + key, _milliseconds(ttl)))

    async def compare_delete(self, key: str, token: str) -> bool:
        return bool(await self._call("eval", _COMPARE_DELETE, 1, self._prefix + key, token))

    async def compare_expire(self, key: str, token: str, ttl: float) -> bool:
        return bool(await self._call("eval", _COMPARE_EXPIRE, 1, self._prefix + key, token, _milliseconds(ttl)))

    async def _listen(self, pubsub: Any, queue: asyncio.Queue[dict[str, Any]]) -> None:
        try:
            while not self._closed:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None or message.get("type") != "message":
                    continue
                queue.put_nowait(_decode(message["data"]))
        except asyncio.CancelledError:
            raise
        except (*self._errors, StoreUnavailable, asyncio.QueueFull, KeyError):
            _poison(queue)
        finally:
            with suppress(*self._errors):
                await pubsub.aclose()

    @asynccontextmanager
    async def subscribe(self, channel: str) -> AsyncIterator[asyncio.Queue[dict[str, Any]]]:
        self._check_open()
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=QUEUE_CAPACITY)
        pubsub = self._client.pubsub()
        task: asyncio.Task[None] | None = None
        try:
            try:
                async with asyncio.timeout(self._timeout):
                    await pubsub.subscribe(self._prefix + channel)
                    # subscribe() sends a command; only its Redis ACK proves readiness.
                    while True:
                        message = await pubsub.get_message(ignore_subscribe_messages=False, timeout=self._timeout)
                        if message and message.get("type") == "subscribe":
                            if message.get("channel") == self._prefix + channel:
                                break
                self._check_open()
            except self._errors:
                _poison(queue)
                raise StoreUnavailable("Session store subscription unavailable") from None
            task = asyncio.create_task(self._listen(pubsub, queue), name="mcp-store-subscription")
            self._listeners[task] = (queue, pubsub)
            yield queue
        finally:
            if task is not None:
                self._listeners.pop(task, None)
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            with suppress(*self._errors):
                await pubsub.aclose()

    async def publish(self, channel: str, message: dict[str, Any]) -> int:
        return int(await self._call("publish", self._prefix + channel, _encode(message)))
