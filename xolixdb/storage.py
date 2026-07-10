"""
xolixdb.storage — TTL-native, verify-on-write storage engine.

Every record is cryptographically verified before it touches memory
(zero-trust ingestion). Expiry is a first-class property: NANDA agents
are ephemeral, so records age out unless the owner re-signs a heartbeat.
A bounded per-agent audit chain (hash-linked versions) provides the
tamper-evident history NANDA's governance pain point calls for.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .record import FactRecord

AUDIT_DEPTH = 32          # versions of chain metadata retained per agent
EXPIRY_GRACE = 60.0       # seconds past TTL before the sweeper deletes


@dataclass
class PutResult:
    ok: bool
    code: str              # stored | stale | duplicate | equivocation | invalid:<why>
    http: int


@dataclass
class AuditEntry:
    version: int
    record_hash: str
    prev_hash: str
    issued_at: float
    revoked: bool


class Storage:
    def __init__(self) -> None:
        self._latest: dict[str, FactRecord] = {}
        self._audit: dict[str, deque[AuditEntry]] = {}
        self.equivocations: list[dict] = []
        self.stats = {"puts_ok": 0, "puts_rejected": 0, "expired_swept": 0}

    # ------------------------------------------------------------ write --
    def put(self, rec: FactRecord) -> PutResult:
        ok, why = rec.verify()
        if not ok:
            self.stats["puts_rejected"] += 1
            return PutResult(False, f"invalid:{why}", 403)

        cur = self._latest.get(rec.agent_id)
        if cur is not None:
            if rec.version < cur.version:
                self.stats["puts_rejected"] += 1
                return PutResult(False, "stale", 409)
            if rec.version == cur.version:
                if rec.record_hash() == cur.record_hash():
                    return PutResult(True, "duplicate", 200)
                # Same signed version, different content: the OWNER key
                # produced two conflicting histories. Flag, keep current.
                self.equivocations.append({
                    "agent_id": rec.agent_id, "version": rec.version,
                    "kept": cur.record_hash(), "rejected": rec.record_hash(),
                    "at": time.time(),
                })
                self.stats["puts_rejected"] += 1
                return PutResult(False, "equivocation", 409)

        self._latest[rec.agent_id] = rec
        chain = self._audit.setdefault(rec.agent_id, deque(maxlen=AUDIT_DEPTH))
        chain.append(AuditEntry(rec.version, rec.record_hash(),
                                rec.prev_hash, rec.issued_at, rec.revoked))
        self.stats["puts_ok"] += 1
        return PutResult(True, "stored", 200)

    # ------------------------------------------------------------- read --
    def get(self, agent_id: str, include_expired: bool = False) -> Optional[FactRecord]:
        rec = self._latest.get(agent_id)
        if rec is None:
            return None
        if rec.is_expired() and not include_expired:
            return None
        return rec

    def audit_chain(self, agent_id: str) -> list[AuditEntry]:
        return list(self._audit.get(agent_id, []))

    def live_records(self, limit: int = 500) -> list[FactRecord]:
        """Non-expired, non-revoked records — the discoverable population."""
        out = []
        for rec in self._latest.values():
            if not rec.is_expired() and not rec.revoked:
                out.append(rec)
                if len(out) >= limit:
                    break
        return out

    def digest(self) -> dict[str, int]:
        """agent_id -> version map, used by gossip anti-entropy."""
        return {aid: r.version for aid, r in self._latest.items()}

    # ------------------------------------------------------------ sweep --
    def sweep_expired(self) -> int:
        now = time.time()
        gone = [a for a, r in self._latest.items()
                if r.is_expired(now, grace=EXPIRY_GRACE) and not r.revoked]
        for a in gone:
            del self._latest[a]
        self.stats["expired_swept"] += len(gone)
        return len(gone)

    def __len__(self) -> int:
        return len(self._latest)
