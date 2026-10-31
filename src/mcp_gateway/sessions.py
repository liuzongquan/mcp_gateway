"""Session metadata and locks. Unknown/expired state is never silently reconstructed."""

from __future__ import annotations

import asyncio
import hashlib
import secrets
from contextlib import asynccontextmanager

from .store import Store


class SessionMissing(RuntimeError):
    pass


class SessionForbidden(RuntimeError):
    pass


class SessionManager:
    def __init__(self, store: Store, ttl: float, lock_timeout: float):
        self.store, self.ttl, self.lock_timeout = store, ttl, lock_timeout

    @asynccontextmanager
    async def lock(self, key: str):
        token = secrets.token_hex(16)
        async with asyncio.timeout(self.lock_timeout):
            while not await self.store.put(f"lock:{key}", {"token": token}, self.lock_timeout + 5, nx=True):  # noqa: ASYNC110 -- distributed lock polling
                await asyncio.sleep(0.02)
        try:
            yield
        finally:
            await self.store.compare_delete(f"lock:{key}", token)

    async def create(self, subject: str, transport: str, owner: str | None = None) -> dict:
        session_id = secrets.token_urlsafe(32)
        record = {
            "id": session_id,
            "subject": subject,
            "transport": transport,
            "owner": owner,
            "routing_hash": hashlib.sha256(session_id.encode()).hexdigest(),
            "initialized": False,
            "protocol_version": None,
            "backends": [],
        }
        if not await self.store.put(f"session:{session_id}", record, self.ttl, nx=True):
            raise RuntimeError("session identifier collision")
        return record

    async def get(self, session_id: str, subject: str, touch: bool = True) -> dict:
        if len(session_id) > 128 or not session_id:
            raise SessionMissing("unknown session")
        record = await self.store.get(f"session:{session_id}")
        if not record:
            raise SessionMissing("unknown or expired session")
        if record["subject"] != subject:
            raise SessionForbidden("session belongs to another identity")
        if touch:
            if not await self.store.expire(f"session:{session_id}", self.ttl):
                raise SessionMissing("session expired during request")
            # Keep every established backend mapping alive, including temporarily idle tools.
            for name in record["backends"]:
                await self.store.expire(f"backend:{session_id}:{name}", self.ttl + self.lock_timeout + 5)
        return record

    async def update(self, session_id: str, subject: str, **fields) -> dict:
        async with self.lock(f"session:{session_id}"):
            record = await self.get(session_id, subject, touch=False)
            record.update(fields)
            await self.store.put(f"session:{session_id}", record, self.ttl)
            return record

    async def attach_backend(self, session_id: str, subject: str, name: str):
        async with self.lock(f"session:{session_id}"):
            record = await self.get(session_id, subject, touch=False)
            if name not in record["backends"]:
                record["backends"].append(name)
                await self.store.put(f"session:{session_id}", record, self.ttl)

    async def initialize(self, session_id: str, subject: str, version: str) -> bool:
        async with self.lock(f"session:{session_id}"):
            record = await self.get(session_id, subject, touch=False)
            if record["protocol_version"]:
                return False
            record["protocol_version"] = version
            await self.store.put(f"session:{session_id}", record, self.ttl)
            return True

    async def delete(self, session_id: str, subject: str) -> dict:
        async with self.lock(f"session:{session_id}"):
            record = await self.get(session_id, subject, touch=False)
            await self.store.delete(f"session:{session_id}")
        return record
