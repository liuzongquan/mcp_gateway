"""Gateway lifecycle and MCP dispatch, independent of the HTTP transport."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
from pathlib import Path

import httpx
from jsonschema import Draft202012Validator
from mcp.types import Tool

from .auth import Authenticator, Identity
from .backend import BackendManager, BackendUnavailable
from .broker import Broker, OwnerUnavailable
from .config import GatewayConfig
from .openapi import compile_openapi
from .retrieval import HTTPEmbeddingProvider, HybridRetriever
from .sessions import SessionManager, SessionMissing
from .store import MemoryStore, RedisStore, Store, StoreUnavailable

SUPPORTED_VERSIONS = ("2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25")
SEARCH_TOOL = {
    "name": "gateway.search",
    "description": "Find permitted tools for an intent. Call returned tool names using tools/call.",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1, "maxLength": 8192},
            "top_k": {"type": "integer", "minimum": 1, "maximum": 120, "default": 15},
        },
        "required": ["query"],
        "additionalProperties": False,
    },
}


def rpc_error(request_id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def text_result(value, *, is_error: bool = False) -> dict:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, allow_nan=False)
    result = {"content": [{"type": "text", "text": text}], "isError": is_error}
    if isinstance(value, dict):
        result["structuredContent"] = value
    return result


class InvalidParams(ValueError):
    pass


class Gateway:
    def __init__(self, config: GatewayConfig, store: Store | None = None):
        self.config = config
        self.auth = Authenticator(config.principals)
        self.store = store or (
            RedisStore(config.redis_url, config.redis_prefix) if config.redis_url else MemoryStore()
        )
        self.sessions = SessionManager(
            self.store, config.session_ttl, config.connect_timeout + config.request_timeout
        )
        self.broker = Broker(
            self.store,
            config.instance_id,
            config.lease_ttl,
            config.request_timeout + 2,
            config.max_concurrent_requests,
        )
        self.broker.handler = self._remote
        self.backends = BackendManager(config, self.store, self.sessions, self.broker)
        self.tools: dict[str, dict] = {}
        self.routes: dict[str, tuple] = {}
        self.validators: dict[str, Draft202012Validator] = {}
        self.frontend_queues: dict[str, asyncio.Queue] = {}
        self._tasks: set[asyncio.Task] = set()
        self._requests: dict[str, asyncio.Task] = {}
        self._stopping = False
        self.http: httpx.AsyncClient | None = None
        self.retriever: HybridRetriever | None = None

    def _catalog(self):
        for backend in self.config.mcp_backends:
            for definition in backend.tools:
                tool = Tool.model_validate(definition).model_dump(mode="json", exclude_none=True)
                original = tool["name"]
                tool["name"] = f"{backend.name}__{original}"
                self._mount(tool, ("mcp", backend, original))
        for service in self.config.openapi_services:
            document = service.document
            if isinstance(document, str):
                document = json.loads(Path(document).read_text())
            for operation in compile_openapi(document, service.name, service.base_url):
                self._mount(operation.tool_definition(), ("openapi", service, operation))
        self.validators["gateway.search"] = Draft202012Validator(SEARCH_TOOL["inputSchema"])
        for backend in self.config.mcp_backends + self.config.openapi_services:
            for subject in self.auth.identities:
                backend.credentials(subject)  # All configured secret references fail fast.

    def _mount(self, tool, route):
        name = tool["name"]
        if name in self.tools or name == "gateway.search":
            raise ValueError(f"duplicate tool name: {name}")
        if len(name) > 128:
            raise ValueError("namespaced tool names must fit MCP's 128-character limit")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise ValueError("tool name contains unsupported characters")
        pending = [tool["inputSchema"]]
        while pending:
            node = pending.pop()
            if isinstance(node, dict):
                if "$ref" in node and (not isinstance(node["$ref"], str) or not node["$ref"].startswith("#")):
                    raise ValueError("external tool schema references are not supported")
                pending.extend(node.values())
            elif isinstance(node, list):
                pending.extend(node)
        Draft202012Validator.check_schema(tool["inputSchema"])
        if tool["inputSchema"].get("type") != "object":
            raise ValueError("tool inputSchema must have type object")
        self.tools[name] = tool
        self.routes[name] = route
        self.validators[name] = Draft202012Validator(tool["inputSchema"])

    async def start(self):
        self._catalog()
        provider = None
        if embedding := self.config.embedding:
            key = os.environ.get(embedding.api_key_env) if embedding.api_key_env else None
            if embedding.api_key_env and not key:
                raise ValueError("embedding credential environment variable unavailable")
            provider = HTTPEmbeddingProvider(embedding.endpoint, embedding.model, api_key=key)
        self.retriever = HybridRetriever(list(self.tools.values()), provider)
        await self.retriever.build()
        await self.store.start()
        try:
            self.http = httpx.AsyncClient(
                timeout=self.config.request_timeout, follow_redirects=False, trust_env=False
            )
            await self.broker.start()
            self._spawn(self._reaper())
        except BaseException:
            if self.http:
                await self.http.aclose()
            await self.store.close()
            raise

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _reaper(self):
        while True:
            await asyncio.sleep(min(self.config.session_ttl / 3, 5))
            try:
                await self.backends.reap()
                for sid, queue in list(self.frontend_queues.items()):
                    if not await self.store.get(f"session:{sid}"):
                        with contextlib.suppress(asyncio.QueueFull):
                            queue.put_nowait(None)
            except StoreUnavailable:
                self.broker.available = False
                return

    async def close(self):
        self._stopping = True
        for queue in self.frontend_queues.values():
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*tuple(self._tasks), return_exceptions=True)
        await self.backends.close()
        await self.broker.close()
        if self.http:
            await self.http.aclose()
        await self.store.close()

    async def _remote(self, method: str, args: dict):
        # Redis is a trusted coordination network, not an externally exposed API.
        identity = self.auth.identities.get(args.get("subject"))
        if not identity:
            raise OwnerUnavailable("unknown identity")
        session = await self.sessions.get(args["session_id"], identity.subject)
        if method == "frontend":
            if session["owner"] != self.config.instance_id or session["transport"] != "sse":
                raise OwnerUnavailable("frontend owner changed")
            await self._schedule_frontend(session, identity, args["message"])
            return {"accepted": True}
        if method == "backend_call":
            name = f"{args['backend']}__{args['tool']}"
            if not identity.permits(name) or name not in self.tools:
                raise OwnerUnavailable("tool not permitted")
            return await self.backends.call_local(args)
        if method == "cancel":
            task = self._requests.get(args["request_key"])
            if task:
                task.cancel()
            return {"cancelled": bool(task)}
        if method == "close_backends":
            await self.backends.close_session(session["id"])
            return {"closed": True}
        raise OwnerUnavailable("unsupported bridge operation")

    async def deliver_frontend(self, session: dict, identity: Identity, message: dict):
        await self.broker.call(
            session["owner"],
            "frontend",
            {
                "session_id": session["id"],
                "subject": identity.subject,
                "message": message,
            },
        )

    async def _schedule_frontend(self, session, identity, message):
        if session["id"] not in self.frontend_queues:
            raise OwnerUnavailable("frontend stream unavailable")
        if "id" not in message:
            await self.process(session["id"], identity, message)
        else:
            if len(self._tasks) >= self.config.max_concurrent_requests + 1:
                raise OwnerUnavailable("gateway is busy")
            self._spawn(self._emit(session["id"], identity, message))

    async def _emit(self, sid, identity, message):
        response = await self.process(sid, identity, message)
        if response is not None and (queue := self.frontend_queues.get(sid)) is not None:
            try:
                queue.put_nowait(response)
            except asyncio.QueueFull:
                # Backpressure is an error, never an unbounded in-memory response buffer.
                await self.end_session(sid, identity)

    @staticmethod
    def request_key(sid: str, request_id) -> str:
        encoded = json.dumps([type(request_id).__name__, request_id], separators=(",", ":"))
        return f"request:{sid}:{hashlib.sha256(encoded.encode()).hexdigest()}"

    async def process(self, sid: str, identity: Identity, message: dict) -> dict | None:
        request_id = message.get("id")
        if "id" not in message:
            await self._notification(sid, identity, message)
            return None
        if not self.broker.available:
            return rpc_error(request_id, -32000, "Gateway coordination unavailable")
        if len(self._requests) >= self.config.max_concurrent_requests:
            return rpc_error(request_id, -32000, "Gateway is busy")
        key = self.request_key(sid, request_id)
        token = os.urandom(16).hex()
        if not await self.store.put(
            key,
            {"token": token, "owner": self.config.instance_id},
            self.config.request_timeout + self.config.connect_timeout + 5,
            nx=True,
        ):
            return rpc_error(request_id, -32600, "Duplicate in-flight request ID")
        self._requests[key] = asyncio.current_task()
        try:
            async with asyncio.timeout(self.config.request_timeout + self.config.connect_timeout):
                session = await self.sessions.get(sid, identity.subject)
                result = await self._dispatch(session, identity, message)
                if isinstance(result, dict) and "__rpc_error" in result:
                    code, error = result["__rpc_error"]
                    return rpc_error(request_id, code, error)
                return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except InvalidParams:
            return rpc_error(request_id, -32602, "Invalid parameters")
        except (TimeoutError, OwnerUnavailable, BackendUnavailable):
            return rpc_error(
                request_id, -32000, "Operation timed out or session owner unavailable; call was not retried"
            )
        except SessionMissing:
            return rpc_error(request_id, -32000, "Session no longer available")
        except StoreUnavailable:
            return rpc_error(request_id, -32000, "Gateway coordination unavailable")
        except asyncio.CancelledError:
            return None
        except Exception:
            return rpc_error(request_id, -32603, "Internal gateway error")
        finally:
            self._requests.pop(key, None)
            with contextlib.suppress(StoreUnavailable):
                await self.store.compare_delete(key, token)

    async def _notification(self, sid, identity, message):
        session = await self.sessions.get(sid, identity.subject)
        method = message["method"]
        if method == "notifications/initialized" and session["protocol_version"]:
            await self.sessions.update(sid, identity.subject, initialized=True)
        elif method == "notifications/cancelled":
            params = message.get("params", {})
            request_id = params.get("requestId")
            if isinstance(request_id, bool) or not isinstance(request_id, (str, int)):
                return
            key = self.request_key(sid, request_id)
            request = await self.store.get(key)
            if request:
                await self.broker.call(
                    request["owner"],
                    "cancel",
                    {
                        "session_id": sid,
                        "subject": identity.subject,
                        "request_key": key,
                    },
                )

    async def _dispatch(self, session, identity, message):
        method = message["method"]
        params = message.get("params", {})
        if method == "initialize":
            if session["protocol_version"]:
                return {"__rpc_error": (-32600, "Session already initialized")}
            if (
                not isinstance(params.get("protocolVersion"), str)
                or not isinstance(params.get("capabilities"), dict)
                or not isinstance(params.get("clientInfo"), dict)
                or not isinstance(params["clientInfo"].get("name"), str)
                or not isinstance(params["clientInfo"].get("version"), str)
            ):
                raise InvalidParams
            version = params["protocolVersion"]
            if version not in SUPPORTED_VERSIONS:
                version = SUPPORTED_VERSIONS[-1]
            if not await self.sessions.initialize(session["id"], identity.subject, version):
                return {"__rpc_error": (-32600, "Session already initialized")}
            return {
                "protocolVersion": version,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "paper-mcp-gateway", "version": "0.1.0"},
                "instructions": "Use gateway.search to discover relevant permitted tools.",
            }
        if method == "ping":
            return {}
        if not session["initialized"]:
            return {"__rpc_error": (-32000, "Complete initialization before using tools")}
        if method == "tools/list":
            if any(k not in {"cursor", "_meta"} for k in params) or params.get("cursor"):
                raise InvalidParams
            tools = [SEARCH_TOOL]
            if self.config.list_mode == "all":
                tools += [tool for name, tool in self.tools.items() if identity.permits(name)]
            return {"tools": tools}
        if method == "tools/call":
            name, arguments = params.get("name"), params.get("arguments", {})
            if not isinstance(name, str) or not isinstance(arguments, dict):
                raise InvalidParams
            if not identity.permits(name) or (name != "gateway.search" and name not in self.tools):
                return {"__rpc_error": (-32602, "Tool not found or not permitted")}
            if not self.validators[name].is_valid(arguments):
                raise InvalidParams
            if name == "gateway.search":
                results = await self.retriever.search(
                    arguments["query"],
                    {n for n in self.tools if identity.permits(n)},
                    arguments.get("top_k", 15),
                )
                return text_result({"tools": [r["tool"] for r in results]})
            kind, backend, target = self.routes[name]
            if kind == "mcp":
                return await self.backends.call(session, backend, target, arguments)
            return await self._openapi_call(session, backend, target, arguments)
        return {"__rpc_error": (-32601, "Method not found")}

    async def _openapi_call(self, session, service, operation, arguments):
        try:
            method, url, kwargs = operation.build_request(arguments)
        except ValueError as exc:
            raise InvalidParams from exc
        headers = kwargs.pop("headers", {})
        # Gateway-managed egress credentials have precedence over public tool arguments.
        credentials = service.credentials(session["subject"])
        private_names = {name.lower() for name in credentials}
        headers = {name: value for name, value in headers.items() if name.lower() not in private_names}
        headers.update(credentials)
        headers["Routing-Hash"] = session["routing_hash"]
        try:
            async with self.http.stream(method, url, headers=headers, **kwargs) as response:
                if response.is_error or response.is_redirect:
                    return text_result(f"Backend returned HTTP {response.status_code}", is_error=True)
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > 8 * 1024 * 1024:
                        return text_result("Backend response exceeds 8 MiB limit", is_error=True)
                try:
                    value = json.loads(data) if data else None
                except (ValueError, UnicodeDecodeError):
                    value = data.decode("utf-8", errors="replace")
                return text_result(value)
        except httpx.HTTPError:
            return text_result("Backend request failed; call was not retried", is_error=True)

    async def end_session(self, sid: str, identity: Identity):
        record = await self.sessions.get(sid, identity.subject, touch=False)
        # Cancellation is best effort: an already dispatched mutation may have
        # committed upstream. Never retry it to infer whether cancellation won.
        for key, task in tuple(self._requests.items()):
            if key.startswith(f"request:{sid}:") and task is not asyncio.current_task():
                task.cancel()
        owners = set()
        for name in record["backends"]:
            if mapping := await self.store.get(f"backend:{sid}:{name}"):
                backend = self.backends.backends.get(name)
                if backend and backend.transport == "sse":
                    owners.add(mapping["owner"])
        # Streamable HTTP sessions are not tied to the initializing gateway.
        # The receiving node must terminate them even when that owner has died.
        await self.backends.close_session(sid)
        # Close while the identity-bound session still exists so remote validation works.
        for owner in owners:
            with contextlib.suppress(OwnerUnavailable, TimeoutError):
                await self.broker.call(
                    owner, "close_backends", {"session_id": sid, "subject": identity.subject}
                )
        with contextlib.suppress(SessionMissing):
            record = await self.sessions.delete(sid, identity.subject)
        for name in record["backends"]:
            await self.store.delete(f"backend:{sid}:{name}")
        if queue := self.frontend_queues.get(sid):
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(None)
