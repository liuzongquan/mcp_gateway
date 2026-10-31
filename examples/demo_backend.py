"""Official MCP SDK demonstration backend. Never expose this unauthenticated example publicly."""

from collections import defaultdict

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse

server = FastMCP(
    "gateway-demo-backend",
    json_response=True,
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=["backend:*", "127.0.0.1:*", "localhost:*", "[::1]:*"],
        allowed_origins=["http://localhost:*", "http://127.0.0.1:*", "http://[::1]:*"],
    ),
)
_counts: dict[int, int] = defaultdict(int)


@server.tool()
def echo(message: str) -> str:
    """Echo a message back unchanged."""
    return message


@server.tool()
def increment(ctx: Context, amount: int = 1) -> dict:
    """Increment a session-local counter, demonstrating backend affinity."""
    sid = id(ctx.request_context.session)
    _counts[sid] += amount
    request = ctx.request_context.request
    return {"count": _counts[sid], "routing_hash": request.headers.get("routing-hash", "") if request else ""}


@server.custom_route("/healthz", methods=["GET"])
async def healthz(request: Request):
    return JSONResponse({"status": "ready"})


app = server.streamable_http_app()
