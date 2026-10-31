"""Run an explicitly configured gateway; no implicit unauthenticated mode."""

import argparse
import os

import uvicorn

from .app import create_app
from .config import load_config


def main():
    parser = argparse.ArgumentParser(description="Run the paper-inspired MCP Gateway")
    parser.add_argument("--config", default=os.getenv("MCP_GATEWAY_CONFIG"), help="JSON configuration path")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--log-level", choices=["error", "warning", "info", "debug"], default="info")
    args = parser.parse_args()
    if not args.config:
        parser.error("--config or MCP_GATEWAY_CONFIG is required")
    app = create_app(load_config(args.config))
    # Access logs include legacy session query strings, so keep them disabled.
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level, access_log=False)


if __name__ == "__main__":
    main()
