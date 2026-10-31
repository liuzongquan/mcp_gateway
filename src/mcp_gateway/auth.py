"""Ingress identity and deny-by-default tool authorization."""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from fnmatch import fnmatchcase

from .config import PrincipalConfig


@dataclass(frozen=True)
class Identity:
    subject: str
    allow: tuple[str, ...]

    def permits(self, tool_name: str) -> bool:
        return tool_name == "gateway.search" or any(fnmatchcase(tool_name, p) for p in self.allow)


class Authenticator:
    def __init__(self, principals: list[PrincipalConfig]):
        self._credentials = [(p.resolved_token(), Identity(p.subject, tuple(p.allow))) for p in principals]
        if len({token for token, _ in self._credentials}) != len(self._credentials):
            raise ValueError("principal credentials must be unique")
        self.identities = {identity.subject: identity for _, identity in self._credentials}

    def authenticate(self, authorization: str | None) -> Identity | None:
        if not authorization:
            return None
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token or len(token) > 4096:
            return None
        found = None
        for expected, identity in self._credentials:
            if hmac.compare_digest(token.encode(), expected.encode()):
                found = identity
        return found
