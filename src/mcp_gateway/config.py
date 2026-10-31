"""Validated, fail-closed startup configuration. Secrets are resolved only at startup."""

from __future__ import annotations

import json
import os
import re
import uuid
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PrincipalConfig(StrictModel):
    subject: str = Field(min_length=1, max_length=128)
    token: str | None = Field(default=None, repr=False)
    token_env: str | None = None
    allow: list[str] = Field(default_factory=list)

    def resolved_token(self) -> str:
        value = os.environ.get(self.token_env, "") if self.token_env else self.token
        if not value or len(value) < 8:
            raise ValueError(f"principal {self.subject}: credential must be at least eight characters")
        return value

    @model_validator(mode="after")
    def one_token_source(self):
        if bool(self.token) == bool(self.token_env):
            raise ValueError("set exactly one of token and token_env")
        return self


class EgressConfig(StrictModel):
    headers_env: dict[str, str] = Field(default_factory=dict)
    identity_headers_env: dict[str, dict[str, str]] = Field(default_factory=dict)

    def credentials(self, subject: str) -> dict[str, str]:
        # HTTP header identity is case-insensitive. Preserve the winning spelling
        # but never send two Authorization headers for an identity override.
        sources = {}
        for mapping in (self.headers_env, self.identity_headers_env.get(subject, {})):
            for header, env_name in mapping.items():
                sources[header.lower()] = (header, env_name)
        result = {}
        for header, env_name in sources.values():
            if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", header):
                raise ValueError("invalid backend header name")
            if header.lower() in {
                "host",
                "content-length",
                "connection",
                "mcp-session-id",
                "mcp-protocol-version",
                "routing-hash",
                "accept",
                "content-type",
                "transfer-encoding",
            }:
                raise ValueError(f"reserved backend header {header}")
            value = os.environ.get(env_name)
            if not value or "\r" in value or "\n" in value:
                raise ValueError(f"backend credential environment variable unavailable: {env_name}")
            result[header] = value
        return result


class MCPBackendConfig(EgressConfig):
    name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    url: str
    transport: Literal["sse", "streamable-http"] = "streamable-http"
    tools: list[dict] = Field(min_length=1)

    @model_validator(mode="after")
    def valid_url(self):
        parsed = urlparse(self.url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.fragment
        ):
            raise ValueError("backend URL must be an absolute HTTP(S) URL without credentials or fragment")
        return self


class OpenAPIServiceConfig(EgressConfig):
    name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    document: dict | str
    base_url: str | None = None


class EmbeddingConfig(StrictModel):
    endpoint: str
    model: str
    api_key_env: str | None = None


class GatewayConfig(StrictModel):
    instance_id: str = Field(default_factory=lambda: uuid.uuid4().hex, pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    redis_url: str | None = Field(default=None, repr=False)
    redis_prefix: str = Field(default="mcpgw:", min_length=1, max_length=64)
    principals: list[PrincipalConfig] = Field(min_length=1)
    mcp_backends: list[MCPBackendConfig] = Field(default_factory=list)
    openapi_services: list[OpenAPIServiceConfig] = Field(default_factory=list)
    embedding: EmbeddingConfig | None = None
    session_ttl: float = Field(default=1800, ge=2)
    request_timeout: float = Field(default=30, gt=0, le=600)
    connect_timeout: float = Field(default=15, gt=0, le=300)
    lease_ttl: float = Field(default=10, ge=1)
    max_request_bytes: int = Field(default=1_048_576, ge=1024)
    max_concurrent_requests: int = Field(default=256, ge=1)
    max_sse_connections: int = Field(default=1024, ge=1)
    list_mode: Literal["all", "search_only"] = "all"
    allowed_origins: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_names(self):
        names = [x.name for x in self.mcp_backends + self.openapi_services]
        if len(names) != len(set(names)):
            raise ValueError("backend service names must be unique")
        subjects = [x.subject for x in self.principals]
        if len(subjects) != len(set(subjects)):
            raise ValueError("principal subjects must be unique")
        return self


def load_config(path: str | Path) -> GatewayConfig:
    path = Path(path)
    raw = json.loads(path.read_text())
    for service in raw.get("openapi_services", []):
        if isinstance(service.get("document"), str):
            document = Path(service["document"])
            if not document.is_absolute():
                document = path.parent / document
            service["document"] = json.loads(document.read_text())
    return GatewayConfig.model_validate(raw)
