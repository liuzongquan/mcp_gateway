"""MCP Streamable HTTP and legacy HTTP+SSE ASGI transports."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .broker import OwnerUnavailable
from .config import GatewayConfig
from .gateway import SUPPORTED_VERSIONS, Gateway, rpc_error
from .sessions import SessionForbidden, SessionMissing
from .store import Store, StoreUnavailable


def _reject_constant(value):
    raise ValueError("non-finite JSON number")


def _finite_float(value):
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


def valid_message(value) -> bool:
    if not isinstance(value, dict) or value.get("jsonrpc") != "2.0":
        return False
    if not isinstance(value.get("method"), str) or not value["method"]:
        return False
    if "id" in value and (isinstance(value["id"], bool) or not isinstance(value["id"], (str, int))):
        return False
    if "params" in value and not isinstance(value["params"], dict):
        return False
    if value["method"] == "initialize" and "id" not in value:
        return False
    return True


def create_app(config: GatewayConfig, store: Store | None = None) -> Starlette:
    gateway = Gateway(config, store)
    sse_reservations = 0

    @asynccontextmanager
    async def lifespan(app):
        await gateway.start()
        try:
            yield
        finally:
            await gateway.close()

    def authorize(request: Request):
        origin = request.headers.get("origin")
        if origin is not None and origin not in config.allowed_origins:
            return None, JSONResponse({"error": "Origin is not allowed"}, 403)
        identity = gateway.auth.authenticate(request.headers.get("authorization"))
        if not identity:
            return None, JSONResponse(
                {"error": "Authentication required"},
                401,
                headers={"WWW-Authenticate": 'Bearer realm="mcp-gateway"'},
            )
        version = request.headers.get("mcp-protocol-version")
        if version is not None and version not in SUPPORTED_VERSIONS:
            return None, JSONResponse({"error": "Unsupported MCP protocol version"}, 400)
        return identity, None

    async def read_message(request: Request):
        content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
        if content_type != "application/json":
            return None, JSONResponse({"error": "Content-Type must be application/json"}, 415)
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > config.max_request_bytes:
                return None, JSONResponse({"error": "Request body too large"}, 413)
        try:
            message = json.loads(data, parse_constant=_reject_constant, parse_float=_finite_float)
        except (ValueError, UnicodeDecodeError, RecursionError):
            return None, JSONResponse(rpc_error(None, -32700, "Parse error"), 400)
        if not valid_message(message):
            return None, JSONResponse(rpc_error(None, -32600, "Invalid Request"), 400)
        return message, None

    async def get_session(request, identity, sid, transport):
        if not sid:
            return None, JSONResponse({"error": "MCP session ID required"}, 400)
        try:
            session = await gateway.sessions.get(sid, identity.subject)
        except SessionMissing:
            return None, JSONResponse({"error": "Unknown or expired MCP session"}, 404)
        except SessionForbidden:
            return None, JSONResponse({"error": "MCP session access denied"}, 403)
        if session["transport"] != transport:
            return None, JSONResponse({"error": "Session transport mismatch"}, 400)
        version = request.headers.get("mcp-protocol-version")
        if version is not None and session["protocol_version"] and version != session["protocol_version"]:
            return None, JSONResponse({"error": "MCP protocol version does not match session"}, 400)
        return session, None

    async def mcp(request: Request):
        identity, error = authorize(request)
        if error:
            return error
        if request.method == "GET":
            return Response(status_code=405, headers={"Allow": "POST, DELETE"})
        try:
            sid = request.headers.get("mcp-session-id")
            if request.method == "DELETE":
                session, error = await get_session(request, identity, sid, "streamable-http")
                if error:
                    return error
                await gateway.end_session(session["id"], identity)
                return Response(status_code=204)
            accept = request.headers.get("accept", "")
            if "application/json" not in accept or "text/event-stream" not in accept:
                return JSONResponse(
                    {"error": "Accept must include application/json and text/event-stream"}, 406
                )
            message, error = await read_message(request)
            if error:
                return error
            created = False
            if not sid and message["method"] == "initialize":
                session = await gateway.sessions.create(identity.subject, "streamable-http")
                sid = session["id"]
                created = True
            else:
                session, error = await get_session(request, identity, sid, "streamable-http")
                if error:
                    return error
            response = await gateway.process(sid, identity, message)
            if response is None:
                return Response(status_code=202)
            if created and "error" in response:
                await gateway.sessions.delete(sid, identity.subject)
                return JSONResponse(response)
            headers = {"MCP-Session-Id": sid} if created else {}
            return JSONResponse(response, headers=headers)
        except (StoreUnavailable, OwnerUnavailable, TimeoutError):
            return JSONResponse({"error": "Session coordination unavailable; request was not retried"}, 503)
        except SessionMissing:
            return JSONResponse({"error": "Unknown or expired MCP session"}, 404)

    async def sse(request: Request):
        nonlocal sse_reservations
        identity, error = authorize(request)
        if error:
            return error
        if "last-event-id" in request.headers:
            return JSONResponse({"error": "SSE resumption is not supported; initialize a new session"}, 400)
        if not gateway.broker.available:
            return JSONResponse({"error": "Session coordination unavailable"}, 503)
        if sse_reservations >= config.max_sse_connections:
            return JSONResponse({"error": "SSE connection capacity reached"}, 503)
        # Reserve before awaiting Redis, otherwise concurrent connects could over-admit.
        sse_reservations += 1
        try:
            session = await gateway.sessions.create(identity.subject, "sse", config.instance_id)
        except StoreUnavailable:
            sse_reservations -= 1
            return JSONResponse({"error": "Session coordination unavailable"}, 503)
        except BaseException:
            sse_reservations -= 1
            raise
        sid = session["id"]
        queue = gateway.frontend_queues[sid] = asyncio.Queue(maxsize=256)

        async def events():
            nonlocal sse_reservations
            try:
                yield f"event: endpoint\ndata: /messages?session_id={sid}\n\n"
                while not gateway._stopping:
                    try:
                        async with asyncio.timeout(min(15, config.session_ttl / 3)):
                            message = await queue.get()
                    except TimeoutError:
                        await gateway.sessions.get(sid, identity.subject)
                        if not gateway.broker.available:
                            break
                        yield ": keepalive\n\n"
                        continue
                    if message is None:
                        break
                    yield (
                        "event: message\ndata: "
                        + json.dumps(message, ensure_ascii=False, allow_nan=False)
                        + "\n\n"
                    )
            except (SessionMissing, StoreUnavailable):
                return
            finally:
                sse_reservations -= 1
                gateway.frontend_queues.pop(sid, None)
                with contextlib.suppress(SessionMissing, StoreUnavailable, OwnerUnavailable, TimeoutError):
                    await gateway.end_session(sid, identity)

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def messages(request: Request):
        identity, error = authorize(request)
        if error:
            return error
        try:
            session, error = await get_session(
                request, identity, request.query_params.get("session_id"), "sse"
            )
            if error:
                return error
            message, error = await read_message(request)
            if error:
                return error
            await gateway.deliver_frontend(session, identity, message)
            return Response(status_code=202)
        except (StoreUnavailable, OwnerUnavailable, TimeoutError):
            return JSONResponse({"error": "Session owner unavailable; request was not retried"}, 503)

    async def healthz(request):
        try:
            ready = gateway.broker.available and bool(await gateway.store.get(gateway.broker.lease_key))
        except StoreUnavailable:
            ready = False
        return JSONResponse(
            {"status": "ready" if ready else "unavailable"}, status_code=200 if ready else 503
        )

    app = Starlette(
        routes=[
            Route("/mcp", mcp, methods=["GET", "POST", "DELETE"]),
            Route("/sse", sse, methods=["GET"]),
            Route("/messages", messages, methods=["POST"]),
            Route("/healthz", healthz, methods=["GET"]),
        ],
        lifespan=lifespan,
    )
    app.state.gateway = gateway
    return app
