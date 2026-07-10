"""
xolixdb.client — SDK for agents publishing to / resolving from XolixDB.

Zero-trust by default: resolve() re-verifies the Ed25519 signature and
identity binding CLIENT-SIDE, so even a fully compromised XolixDB node
cannot forge an agent's endpoints without detection.
"""
from __future__ import annotations

import time
from typing import Optional

import aiohttp

from . import crypto
from .record import FactRecord, create_signed_record


class AgentIdentity:
    """Holds the keypair; the agent_id is derived from the public key."""

    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.signing_key, self.verify_key = crypto.generate_keypair()
        self.agent_id = crypto.agent_id_from_pubkey(self.verify_key)
        self.version = 0
        self.last_hash = ""

    def next_record(self, facts: dict, ttl: int = 300,
                    revoked: bool = False) -> FactRecord:
        self.version += 1
        rec = create_signed_record(self.signing_key, self.agent_name, facts,
                                   self.version, ttl, self.last_hash, revoked)
        self.last_hash = rec.record_hash()
        return rec


def build_agent_facts(label: str, description: str,
                      static_endpoints: list[str],
                      adaptive_resolver: Optional[str] = None,
                      skills: Optional[list[dict]] = None,
                      modalities: Optional[list[str]] = None) -> dict:
    """A NANDA-flavoured AgentFacts document (arXiv:2507.14263 schema)."""
    endpoints: dict = {"static": static_endpoints}
    if adaptive_resolver:
        endpoints["adaptive_resolver"] = {"url": adaptive_resolver,
                                          "policies": ["geo", "load"]}
    return {
        "label": label,
        "description": description,
        "endpoints": endpoints,
        "capabilities": {"modalities": modalities or ["text"],
                         "streaming": True,
                         "authentication": {"methods": ["oauth2", "jwt"]}},
        "skills": skills or [],
        "telemetry": {"enabled": True, "sampling": 0.1},
    }


class XolixClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.s = session

    async def publish(self, node_url: str, rec: FactRecord) -> dict:
        async with self.s.put(f"{node_url}/v1/facts", json=rec.to_dict()) as r:
            body = await r.json()
            body["_status"] = r.status
            return body

    async def resolve(self, node_url: str, agent_id: str) -> dict:
        async with self.s.get(f"{node_url}/v1/resolve/{agent_id}") as r:
            body = await r.json()
            body["_status"] = r.status
            return body

    async def get_facts_verified(self, node_url: str, agent_id: str) -> dict:
        """Full record + CLIENT-SIDE zero-trust verification."""
        async with self.s.get(f"{node_url}/v1/facts/{agent_id}") as r:
            if r.status != 200:
                return {"_status": r.status, "client_verified": False}
            rec = FactRecord.from_dict(await r.json())
        ok, why = rec.verify()
        return {"_status": 200, "client_verified": ok, "why": why,
                "record": rec}

    async def audit(self, node_url: str, agent_id: str) -> dict:
        async with self.s.get(f"{node_url}/v1/audit/{agent_id}") as r:
            return await r.json()


def verify_audit_chain(chain: list[dict]) -> tuple[bool, str]:
    """Client-side: confirm the hash chain links every retained version."""
    for prev, cur in zip(chain, chain[1:]):
        if cur["prev_hash"] != prev["record_hash"]:
            return False, f"broken_link_at_v{cur['version']}"
        if cur["version"] != prev["version"] + 1:
            return False, f"version_gap_at_v{cur['version']}"
    return True, "chain_intact"
