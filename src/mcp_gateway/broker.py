"""Bounded, request/reply Pub/Sub bridge. Never replay a timed-out backend call."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack

from .store import Store, StoreUnavailable


class OwnerUnavailable(RuntimeError):
    pass


class Broker:
    def __init__(
        self, store: Store, instance_id: str, lease_ttl: float, timeout: float, max_inflight: int = 256
    ):
        self.store = store
        self.instance_id = instance_id
        self.lease_ttl = lease_ttl
        self.timeout = timeout
        self.max_inflight = max_inflight
        self.token = uuid.uuid4().hex
        self._stack = AsyncExitStack()
        self._tasks: set[asyncio.Task] = set()
        self._active: dict[str, asyncio.Task] = {}
        self.available = False
        self.handler: Callable[[str, dict], Awaitable[dict | None]] | None = None
        self.forwarded_requests = 0
        self.forwarded_responses = 0

    @property
    def lease_key(self) -> str:
        return f"instance:{self.instance_id}"

    async def start(self):
        if not await self.store.put(self.lease_key, {"token": self.token}, self.lease_ttl, nx=True):
            raise ValueError("instance_id already active; each gateway process needs a unique ID")
        try:
            self._queue = await self._stack.enter_async_context(
                self.store.subscribe(f"rpc:{self.instance_id}")
            )
            self.available = True
            self._spawn(self._listen())
            self._spawn(self._heartbeat())
        except BaseException:
            await self.store.compare_delete(self.lease_key, self.token)
            raise

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _heartbeat(self):
        try:
            while True:
                await asyncio.sleep(self.lease_ttl / 3)
                if not await self.store.compare_expire(self.lease_key, self.token, self.lease_ttl):
                    raise OwnerUnavailable("instance lease was lost")
        except (StoreUnavailable, OwnerUnavailable):
            self.available = False
            for task in tuple(self._active.values()):
                task.cancel()

    async def _listen(self):
        while True:
            message = await self._queue.get()
            if message.get("_store_error"):
                self.available = False
                for task in tuple(self._active.values()):
                    task.cancel()
                return
            if message.get("kind") == "cancel":
                if task := self._active.get(message.get("id")):
                    task.cancel()
                continue
            if not self.available or not isinstance(message.get("id"), str):
                continue
            if message["id"] in self._active:
                continue
            if len(self._active) >= self.max_inflight:
                try:
                    await self.store.publish(message["reply"], {"error": "owner request capacity reached"})
                except StoreUnavailable:
                    self.available = False
                    return
                continue
            self._active[message["id"]] = self._spawn(self._dispatch(message))

    async def _dispatch(self, message):
        try:
            if not self.handler:
                raise OwnerUnavailable("instance is not ready")
            result = await self.handler(message["method"], message["args"])
            reply = {"result": result}
        except asyncio.CancelledError:
            reply = {"error": "request cancelled"}
        except Exception:
            # Do not send credentials, transport errors or tracebacks over public responses.
            reply = {"error": "owner could not process request"}
        finally:
            self._active.pop(message["id"], None)
        try:
            await self.store.publish(message["reply"], reply)
            self.forwarded_responses += 1
        except StoreUnavailable:
            self.available = False

    async def call(self, owner: str, method: str, args: dict) -> dict | None:
        if not self.available:
            raise OwnerUnavailable("gateway coordination unavailable")
        if owner == self.instance_id:
            if not self.handler:
                raise OwnerUnavailable("instance is not ready")
            return await self.handler(method, args)
        if not await self.store.get(f"instance:{owner}"):
            raise OwnerUnavailable("session owner unavailable; establish a new session")
        correlation = uuid.uuid4().hex
        channel = f"reply:{self.instance_id}:{correlation}"
        try:
            async with self.store.subscribe(channel) as queue:
                subscribers = await self.store.publish(
                    f"rpc:{owner}",
                    {
                        "id": correlation,
                        "method": method,
                        "args": args,
                        "reply": channel,
                    },
                )
                if not subscribers:
                    raise OwnerUnavailable("session owner unavailable; establish a new session")
                self.forwarded_requests += 1
                async with asyncio.timeout(self.timeout):
                    reply = await queue.get()
                if reply.get("_store_error") or "error" in reply:
                    raise OwnerUnavailable("session owner could not complete request")
                return reply.get("result")
        except (TimeoutError, asyncio.CancelledError):
            with contextlib.suppress(StoreUnavailable):
                await self.store.publish(f"rpc:{owner}", {"kind": "cancel", "id": correlation})
            raise

    async def close(self):
        self.available = False
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        await self._stack.aclose()
        with contextlib.suppress(StoreUnavailable):
            await self.store.compare_delete(self.lease_key, self.token)
