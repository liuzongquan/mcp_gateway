"""Lazy, session-affine official MCP SDK connections with cross-node forwarding.

Each connection is owned by a dedicated task. That same task enters AND leaves
all SDK/AnyIO context managers; request tasks only use the initialized session.
Timeouts are never retried because a tool may already have produced side effects.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import timedelta

import httpx
import jsonschema
from mcp import ClientSession
from mcp.client.sse import sse_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult

from .auth import Identity
from .broker import Broker, OwnerUnavailable
from .config import GatewayConfig, MCPBackendConfig
from .sessions import SessionForbidden, SessionManager, SessionMissing
from .store import Store


class BackendUnavailable(RuntimeError):
    """Backend state was lost or transport failed; create a new frontend session."""


def secure_http_client_factory(**kwargs) -> httpx.AsyncClient:
    """SDK factory: never forward credentials through redirects or ambient proxies."""
    kwargs.pop("follow_redirects", None)
    kwargs.pop("trust_env", None)
    return httpx.AsyncClient(**kwargs, follow_redirects=False, trust_env=False)


@dataclass
class _Command:
    tool: str
    arguments: dict
    future: asyncio.Future
    task: asyncio.Task | None = None


class _Connection:
    def __init__(self, manager: BackendManager, session_record: dict, backend: MCPBackendConfig):
        self.manager, self.record, self.backend = manager, session_record, backend
        self.key = f"backend:{session_record['id']}:{backend.name}"
        self.token = uuid.uuid4().hex
        self.queue: asyncio.Queue[_Command | None] = asyncio.Queue(manager.config.max_concurrent_requests)
        self.ready = asyncio.get_running_loop().create_future()
        # Read exception even if the initiating caller disappears during startup.
        self.ready.add_done_callback(lambda future: None if future.cancelled() else future.exception())
        self.active: set[asyncio.Task] = set()
        self.session: ClientSession | None = None
        self.closing = False
        self.task = asyncio.create_task(
            self.run(), name=f"mcp-backend:{backend.name}:{session_record['id'][:8]}"
        )

    async def run(self):
        failure = BackendUnavailable("backend connection unavailable; establish a new session")
        try:
            async with AsyncExitStack() as stack:
                async with asyncio.timeout(self.manager.config.connect_timeout):
                    headers = self.backend.credentials(self.record["subject"])
                    headers["Routing-Hash"] = self.record["routing_hash"]
                    backend_session_id = None

                    def on_session_created(value):
                        nonlocal backend_session_id
                        backend_session_id = value

                    if self.backend.transport == "sse":
                        streams = await stack.enter_async_context(
                            sse_client(
                                self.backend.url,
                                headers=headers,
                                timeout=self.manager.config.request_timeout,
                                sse_read_timeout=max(
                                    self.manager.config.session_ttl, self.manager.config.request_timeout
                                )
                                + 5,
                                httpx_client_factory=secure_http_client_factory,
                                on_session_created=on_session_created,
                            )
                        )
                        read, write = streams

                        def get_session_id():
                            return backend_session_id
                    else:
                        client = await stack.enter_async_context(
                            secure_http_client_factory(
                                headers=headers,
                                timeout=httpx.Timeout(
                                    self.manager.config.request_timeout,
                                    read=max(
                                        self.manager.config.session_ttl, self.manager.config.request_timeout
                                    )
                                    + 5,
                                ),
                            )
                        )
                        read, write, get_session_id = await stack.enter_async_context(
                            streamable_http_client(
                                self.backend.url,
                                http_client=client,
                                terminate_on_close=False,
                            )
                        )
                    self.session = await stack.enter_async_context(
                        ClientSession(
                            read,
                            write,
                            read_timeout_seconds=timedelta(seconds=self.manager.config.request_timeout),
                        )
                    )
                    initialization = await self.session.initialize()
                    metadata = {
                        "owner": self.manager.config.instance_id,
                        "owner_token": self.manager.broker.token,
                        "token": self.token,
                        "backend_session_id": get_session_id(),
                        "protocol_version": initialization.protocolVersion,
                        "subject": self.record["subject"],
                        "routing_hash": self.record["routing_hash"],
                    }
                    # Serialize publication with frontend deletion so a late init
                    # cannot recreate an orphan mapping after close_session scanned.
                    async with self.manager.sessions.lock(f"session:{self.record['id']}"):
                        await self.manager.sessions.get(
                            self.record["id"], self.record["subject"], touch=False
                        )
                        await self.manager._assert_live_owner()
                        if not await self.manager.store.put(
                            self.key, metadata, self.manager.metadata_ttl, nx=True
                        ):
                            raise BackendUnavailable("backend session mapping already exists")
                self.ready.set_result(None)
                try:
                    while True:
                        command = await self.queue.get()
                        if command is None:
                            break
                        if command.future.cancelled():
                            continue
                        command.task = asyncio.create_task(self._execute(command))
                        self.active.add(command.task)
                        command.task.add_done_callback(self.active.discard)
                finally:
                    for task in tuple(self.active):
                        task.cancel()
                    await asyncio.gather(*tuple(self.active), return_exceptions=True)
        except asyncio.CancelledError:
            # Shutdown is graceful at this layer: pending callers get a stable error.
            pass
        except Exception:
            # SDK exceptions may include backend URLs/headers; never expose them.
            failure = BackendUnavailable(
                "backend initialization or transport failed; establish a new session"
            )
        finally:
            self.closing = True
            if not self.ready.done():
                self.ready.set_exception(failure)
            while not self.queue.empty():
                command = self.queue.get_nowait()
                if command is not None and not command.future.done():
                    command.future.set_exception(failure)
            if self.backend.transport == "sse":
                with contextlib.suppress(Exception):
                    await self.manager.store.compare_delete(self.key, self.token)
            if self.manager._connections.get(self.key) is self:
                self.manager._connections.pop(self.key, None)

    async def _execute(self, command: _Command):
        try:
            async with asyncio.timeout(self.manager.config.request_timeout):
                async with self.manager._capacity:
                    await self.manager.sessions.get(self.record["id"], self.record["subject"])
                    await self.manager._assert_live_owner()
                    assert self.session is not None
                    result = await self.session.call_tool(command.tool, command.arguments)
                    payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
            if not command.future.done():
                command.future.set_result(payload)
        except TimeoutError:
            if not command.future.done():
                command.future.set_exception(
                    TimeoutError("backend tool request timed out; outcome may be unknown")
                )
        except asyncio.CancelledError:
            if not command.future.done():
                command.future.set_exception(
                    BackendUnavailable("backend request cancelled; outcome may be unknown")
                )
        except (SessionMissing, SessionForbidden, OwnerUnavailable) as exc:
            if not command.future.done():
                command.future.set_exception(exc)
        except Exception:
            if not command.future.done():
                command.future.set_exception(
                    BackendUnavailable("backend tool request failed; outcome may be unknown")
                )

    async def request(self, tool: str, arguments: dict) -> dict:
        await asyncio.shield(self.ready)
        if self.closing or self.task.done():
            raise BackendUnavailable("backend connection unavailable; establish a new session")
        future = asyncio.get_running_loop().create_future()
        command = _Command(tool, arguments, future)
        try:
            async with asyncio.timeout(self.manager.config.request_timeout):
                await self.queue.put(command)
                return await future
        except TimeoutError as exc:
            raise TimeoutError("backend tool request timed out; outcome may be unknown") from exc
        finally:
            if not future.done():
                future.cancel()
            if future.cancelled() and command.task is not None:
                command.task.cancel()

    async def close(self):
        if not self.task.done():
            self.closing = True
            self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)


class BackendManager:
    def __init__(self, config: GatewayConfig, store: Store, sessions: SessionManager, broker: Broker):
        self.config, self.store, self.sessions, self.broker = config, store, sessions, broker
        self.backends = {backend.name: backend for backend in config.mcp_backends}
        self.identities = {
            principal.subject: Identity(principal.subject, tuple(principal.allow))
            for principal in config.principals
        }
        self._connections: dict[str, _Connection] = {}
        self._capacity = asyncio.Semaphore(config.max_concurrent_requests)
        self._closed = False
        self._http = secure_http_client_factory(timeout=config.request_timeout)
        self._direct_tasks: set[asyncio.Task] = set()
        self.metadata_ttl = config.session_ttl + config.connect_timeout + config.request_timeout + 5

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    async def _assert_live_owner(self):
        if not self.broker.available:
            raise OwnerUnavailable("gateway coordination unavailable")
        lease = await self.store.get(self.broker.lease_key)
        if not lease or lease.get("token") != self.broker.token:
            raise OwnerUnavailable("gateway ownership lease was lost")

    def _authorize(
        self, subject: str, backend_name: str, tool_name: str, arguments: dict
    ) -> MCPBackendConfig:
        backend = self.backends.get(backend_name)
        identity = self.identities.get(subject)
        if backend is None or identity is None:
            raise SessionForbidden("backend access denied")
        if not identity.permits(f"{backend.name}__{tool_name}"):
            raise SessionForbidden("tool access denied")
        definition = next((tool for tool in backend.tools if tool.get("name") == tool_name), None)
        if definition is None:
            raise ValueError("unknown backend tool")
        if not isinstance(arguments, dict):
            raise ValueError("backend tool arguments must be an object")
        try:
            jsonschema.validate(arguments, definition.get("inputSchema", {"type": "object"}))
        except (jsonschema.ValidationError, jsonschema.SchemaError) as exc:
            raise ValueError("invalid backend tool arguments") from exc
        return backend

    async def call(
        self, session_record: dict, backend: MCPBackendConfig, tool_name: str, arguments: dict
    ) -> dict:
        if self._closed:
            raise BackendUnavailable("gateway is shutting down")
        sid, subject = session_record["id"], session_record["subject"]
        backend = self._authorize(subject, backend.name, tool_name, arguments)
        fresh = await self.sessions.get(sid, subject)
        key = f"backend:{sid}:{backend.name}"
        metadata = await self.store.get(key)
        if metadata is None:
            async with self.sessions.lock(key):
                fresh = await self.sessions.get(sid, subject)
                metadata = await self.store.get(key)
                if metadata is None:
                    if backend.name in fresh["backends"]:
                        raise BackendUnavailable("backend session state was lost; establish a new session")
                    await self._assert_live_owner()
                    # Durable tombstone prevents reconnect/replay after an uncertain init.
                    await self.sessions.attach_backend(sid, subject, backend.name)
                    connection = _Connection(self, fresh, backend)
                    self._connections[key] = connection
                    try:
                        await asyncio.shield(connection.ready)
                    except BaseException:
                        await connection.close()
                        raise
                    metadata = await self.store.get(key)
        return await self._route(fresh, backend, tool_name, arguments, metadata)

    async def _route(self, session_record, backend, tool_name, arguments, metadata):
        if not metadata or metadata.get("routing_hash") != session_record["routing_hash"]:
            raise BackendUnavailable("backend session mapping unavailable")
        if backend.transport == "streamable-http":
            return await self._direct_call(session_record, backend, tool_name, arguments, metadata)
        owner = metadata.get("owner")
        if not isinstance(owner, str):
            raise BackendUnavailable("backend owner unavailable")
        lease = await self.store.get(f"instance:{owner}")
        if not lease or lease.get("token") != metadata.get("owner_token"):
            raise OwnerUnavailable("backend owner unavailable; establish a new session")
        if owner != self.config.instance_id:
            result = await self.broker.call(
                owner,
                "backend_call",
                {
                    "session_id": session_record["id"],
                    "subject": session_record["subject"],
                    "backend": backend.name,
                    "tool": tool_name,
                    "arguments": arguments,
                },
            )
            if not isinstance(result, dict):
                raise BackendUnavailable("invalid backend owner response")
            return result
        return await self.call_local(
            {
                "session_id": session_record["id"],
                "subject": session_record["subject"],
                "backend": backend.name,
                "tool": tool_name,
                "arguments": arguments,
            }
        )

    async def call_local(self, args: dict) -> dict:
        if self._closed:
            raise BackendUnavailable("gateway is shutting down")
        sid, subject = args["session_id"], args["subject"]
        backend = self._authorize(subject, args["backend"], args["tool"], args["arguments"])
        record = await self.sessions.get(sid, subject)
        await self._assert_live_owner()
        key = f"backend:{sid}:{backend.name}"
        metadata = await self.store.get(key)
        connection = self._connections.get(key)
        if (
            not metadata
            or metadata.get("owner") != self.config.instance_id
            or metadata.get("owner_token") != self.broker.token
            or metadata.get("routing_hash") != record["routing_hash"]
            or connection is None
            or metadata.get("token") != connection.token
        ):
            raise BackendUnavailable("backend session state unavailable; establish a new session")
        return await connection.request(args["tool"], args["arguments"])

    def _direct_headers(self, backend: MCPBackendConfig, subject: str, metadata: dict) -> dict:
        if not isinstance(metadata.get("protocol_version"), str) or not metadata["protocol_version"]:
            raise BackendUnavailable("backend protocol metadata unavailable")
        headers = backend.credentials(subject)
        headers.update(
            {
                "Routing-Hash": metadata["routing_hash"],
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": metadata["protocol_version"],
            }
        )
        if metadata.get("backend_session_id"):
            headers["MCP-Session-Id"] = metadata["backend_session_id"]
        return headers

    async def _direct_call(self, session_record, backend, tool_name, arguments, metadata):
        request_id = uuid.uuid4().hex
        headers = self._direct_headers(backend, session_record["subject"], metadata)
        body = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        task = asyncio.current_task()
        assert task is not None
        self._direct_tasks.add(task)
        try:
            async with asyncio.timeout(self.config.request_timeout):
                async with self._capacity:
                    # Request-scoped transport can be used by any gateway, even after
                    # the initializing gateway has stopped. Redis is the source of truth.
                    await self.sessions.get(session_record["id"], session_record["subject"])
                    async with self._http.stream(
                        "POST", backend.url, headers=headers, json=body, timeout=self.config.request_timeout
                    ) as response:
                        if response.status_code == 404:
                            raise BackendUnavailable("backend session expired; establish a new session")
                        response.raise_for_status()
                        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip()
                        if content_type == "application/json":
                            raw = bytearray()
                            async for chunk in response.aiter_bytes():
                                raw.extend(chunk)
                                if len(raw) > 8 * 1024 * 1024:
                                    raise BackendUnavailable("backend response exceeds byte limit")
                            return self._rpc_result(json.loads(raw), request_id)
                        if content_type == "text/event-stream":
                            async for payload in self._sse_payloads(response):
                                message = json.loads(payload)
                                if isinstance(message, dict) and message.get("id") == request_id:
                                    return self._rpc_result(message, request_id)
                            raise BackendUnavailable("backend stream ended before matching response")
                        raise BackendUnavailable("unsupported backend response content type")
        except TimeoutError as exc:
            await self._cancel_direct(backend, headers, request_id)
            raise TimeoutError("backend tool request timed out; outcome may be unknown") from exc
        except asyncio.CancelledError:
            await self._cancel_direct(backend, headers, request_id)
            raise
        except (BackendUnavailable, SessionMissing, SessionForbidden):
            raise
        except Exception:
            raise BackendUnavailable("backend tool request failed; outcome may be unknown") from None
        finally:
            self._direct_tasks.discard(task)

    @staticmethod
    def _rpc_result(message, request_id):
        if (
            not isinstance(message, dict)
            or message.get("jsonrpc") != "2.0"
            or message.get("id") != request_id
        ):
            raise BackendUnavailable("invalid backend JSON-RPC response")
        if "error" in message:
            raise BackendUnavailable("backend returned a JSON-RPC error")
        if "result" not in message:
            raise BackendUnavailable("backend JSON-RPC response has no result")
        try:
            return CallToolResult.model_validate(message["result"]).model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        except Exception:
            raise BackendUnavailable("invalid backend tool result") from None

    @staticmethod
    async def _sse_payloads(response):
        """Incremental SSE data parsing with decoded-byte and event bounds.

        Supports LF, CRLF, CR-only, comments, and multi-line data. Unfinished
        events are never accepted as completed JSON-RPC responses.
        """
        buffer = b""
        data = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > 8 * 1024 * 1024:
                raise BackendUnavailable("backend response exceeds byte limit")
            buffer += chunk
            while match := re.search(rb"\r\n|\r|\n", buffer):
                if match.group() == b"\r" and match.end() == len(buffer):
                    break  # May be the first half of a CRLF across network chunks.
                line = buffer[: match.start()].decode("utf-8")
                buffer = buffer[match.end() :]
                if not line:
                    if data:
                        yield "\n".join(data)
                        data = []
                elif not line.startswith(":"):
                    field, _, value = line.partition(":")
                    if field == "data":
                        data.append(value[1:] if value.startswith(" ") else value)
        # A final CR is a legal delimiter, not an unfinished UTF-8 data line.
        if buffer == b"\r" and data:
            yield "\n".join(data)

    async def _cancel_direct(self, backend, headers, request_id):
        with contextlib.suppress(Exception):
            async with asyncio.timeout(min(1.0, self.config.request_timeout)):
                async with self._http.stream(
                    "POST",
                    backend.url,
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "method": "notifications/cancelled",
                        "params": {"requestId": request_id, "reason": "gateway request cancelled"},
                    },
                    timeout=min(1.0, self.config.request_timeout),
                ):
                    pass  # Do not buffer an untrusted notification response body.

    async def close_session(self, sid: str):
        await asyncio.gather(
            *(
                connection.close()
                for connection in tuple(self._connections.values())
                if connection.record["id"] == sid
            ),
            return_exceptions=True,
        )
        for backend in self.backends.values():
            if backend.transport != "streamable-http":
                continue
            key = f"backend:{sid}:{backend.name}"
            metadata = await self.store.get(key)
            if metadata is None:
                continue
            # Explicit frontend session deletion terminates the backend session;
            # ordinary gateway shutdown deliberately does not.
            with contextlib.suppress(Exception):
                headers = self._direct_headers(backend, metadata["subject"], metadata)
                async with asyncio.timeout(min(2.0, self.config.request_timeout)):
                    async with self._http.stream(
                        "DELETE", backend.url, headers=headers, timeout=min(2.0, self.config.request_timeout)
                    ):
                        pass
            await self.store.compare_delete(key, metadata["token"])

    async def reap(self):
        for connection in tuple(self._connections.values()):
            if not await self.store.get(f"session:{connection.record['id']}"):
                await self.close_session(connection.record["id"])

    async def close(self):
        self._closed = True
        direct_tasks = tuple(task for task in self._direct_tasks if task is not asyncio.current_task())
        for task in direct_tasks:
            task.cancel()
        await asyncio.gather(*direct_tasks, return_exceptions=True)
        await asyncio.gather(
            *(connection.close() for connection in tuple(self._connections.values())), return_exceptions=True
        )
        await self._http.aclose()
