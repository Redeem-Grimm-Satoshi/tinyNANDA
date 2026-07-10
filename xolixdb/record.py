"""
xolixdb.record — FactRecord: signed, versioned, hash-chained AgentFacts.

Design insight ("Owner-Serialized Consistency"): AgentFacts have exactly
one legitimate writer — the holder of the agent's private key. So instead
of vector clocks or multi-master conflict resolution, XolixDB orders every
record by a *signed monotonic version number*. Highest verified version
wins, everywhere, deterministically. This is both simpler and more secure
than Dynamo-style LWW for this workload:

  * A replica cannot be tricked into regressing (stale replays rejected).
  * Two different records signed with the SAME version = provable owner
    equivocation, which nodes flag rather than silently resolve.
  * Each record embeds prev_hash, forming a per-agent tamper-evident
    hash chain — NANDA's "transparent, append-only log" for governance
    and liability, without a blockchain.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from . import crypto


def canonical_bytes(obj: dict) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


@dataclass
class FactRecord:
    agent_id: str
    agent_name: str
    facts: dict[str, Any]          # NANDA-style AgentFacts document
    version: int                   # signed, monotonically increasing
    ttl: int                       # seconds this record may be cached/served
    issued_at: float               # unix epoch, set by signer
    prev_hash: str                 # hash of previous version ("" for v1)
    public_key: str                # b64 Ed25519 pubkey of the owner
    signature: str = ""            # b64 sig over signable_bytes()
    revoked: bool = False          # tombstone: sub-second revocation

    # ---------------------------------------------------------- signing --
    def signable_dict(self) -> dict:
        d = asdict(self)
        d.pop("signature")
        return d

    def signable_bytes(self) -> bytes:
        return canonical_bytes(self.signable_dict())

    def record_hash(self) -> str:
        return crypto.sha256_hex(self.signable_bytes() + self.signature.encode())

    # ------------------------------------------------------- validation --
    def verify(self) -> tuple[bool, str]:
        """Full zero-trust check: identity binding + signature + sanity."""
        if not self.agent_id.startswith(crypto.DID_PREFIX):
            return False, "bad_agent_id_prefix"
        try:
            vk = crypto.pubkey_from_b64(self.public_key)
        except Exception:
            return False, "bad_public_key"
        if crypto.agent_id_from_pubkey(vk) != self.agent_id:
            return False, "agent_id_pubkey_mismatch"   # forged identity
        if self.version < 1:
            return False, "bad_version"
        if not (0 < self.ttl <= 7 * 24 * 3600):
            return False, "bad_ttl"
        if not crypto.verify_signature(self.public_key, self.signable_bytes(), self.signature):
            return False, "bad_signature"              # tampered payload
        return True, "ok"

    def is_expired(self, now: Optional[float] = None, grace: float = 0.0) -> bool:
        now = now or time.time()
        return now > self.issued_at + self.ttl + grace

    def ttl_remaining(self, now: Optional[float] = None) -> float:
        now = now or time.time()
        return max(0.0, self.issued_at + self.ttl - now)

    # -------------------------------------------------------------- io --
    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "FactRecord":
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


def create_signed_record(
    signing_key,
    agent_name: str,
    facts: dict,
    version: int,
    ttl: int,
    prev_hash: str = "",
    revoked: bool = False,
) -> FactRecord:
    vk = signing_key.verify_key
    rec = FactRecord(
        agent_id=crypto.agent_id_from_pubkey(vk),
        agent_name=agent_name,
        facts=facts,
        version=version,
        ttl=ttl,
        issued_at=time.time(),
        prev_hash=prev_hash,
        public_key=crypto.pubkey_b64(vk),
        revoked=revoked,
    )
    rec.signature = crypto.sign(signing_key, rec.signable_bytes())
    return rec
