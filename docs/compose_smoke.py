"""Smoke-check two running gateways; credentials come only from the environment.

Run on the host with --a http://127.0.0.1:8080 --b http://127.0.0.1:8082,
or pipe this script into `docker compose exec -T gateway-a python -`.
"""
import argparse
import json
import os

import httpx


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", default="http://gateway-a:8080")
    parser.add_argument("--b", default="http://gateway-b:8080")
    args = parser.parse_args()
    headers = {
        "Authorization": "Bearer " + os.environ["GATEWAY_TOKEN"],
        "Accept": "application/json, text/event-stream",
    }
    with httpx.Client(headers=headers, timeout=20, trust_env=False) as client:
        for base in (args.a, args.b):
            client.get(base + "/healthz").raise_for_status()
        response = client.post(args.a + "/mcp", json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "compose-smoke", "version": "1.0"},
            },
        })
        response.raise_for_status()
        client.headers["MCP-Session-Id"] = response.headers["MCP-Session-Id"]
        client.headers["MCP-Protocol-Version"] = response.json()["result"]["protocolVersion"]
        try:
            initialized = client.post(args.b + "/mcp", json={
                "jsonrpc": "2.0", "method": "notifications/initialized",
            })
            assert initialized.status_code == 202, initialized.text
            routing_hash = None
            for request_id, base in enumerate((args.a, args.b, args.a, args.b), start=2):
                reply = client.post(base + "/mcp", json={
                    "jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {
                        "name": "demo__increment", "arguments": {"amount": 1},
                    },
                })
                reply.raise_for_status()
                payload = reply.json()
                assert payload.get("id") == request_id and "error" not in payload, payload
                result = payload["result"]
                assert not result.get("isError", False), result
                counter = result.get("structuredContent")
                if counter is None:
                    counter = json.loads(result["content"][0]["text"])
                assert counter["count"] == request_id - 1, result
                value = counter["routing_hash"]
                assert isinstance(value, str) and len(value) == 64, counter
                if routing_hash is None:
                    routing_hash = value
                assert value == routing_hash, "Routing-Hash changed across gateways"
            print("PASS: two gateways, one shared session, counter 1 → 2 → 3 → 4")
        finally:
            deleted = client.delete(args.b + "/mcp")
            deleted.raise_for_status()


if __name__ == "__main__":
    main()
