"""
xolixdb.node — A single XolixDB node.

Every node is a full coordinator (any-node reads/writes, like Dynamo):
  * Writes:  verify -> route to RF replicas on the ring -> W-quorum ack.
             Sloppy tail keeps writes available while replicas are down.
  * Reads:   R-quorum fan-out -> highest signed version wins ->
             async read-repair pushes the winner to lagging replicas.
  * Gossip:  ~1s anti-entropy digests heal partitions and repopulate
             restarted nodes, bounding staleness even without quorums.
"""
from __future__ import annotations

import asyncio
import random
import time
from typing import Optional

import aiohttp
from aiohttp import web

from .record import FactRecord
from .ring import HashRing
from .storage import Storage

INTERNAL_TIMEOUT = aiohttp.ClientTimeout(total=0.8)
GOSSIP_INTERVAL = 0.7
SWEEP_INTERVAL = 5.0
SLOP = 2  # extra ring candidates for sloppy quorum


class XolixNode:
    def __init__(self, node_id: str, host: str, port: int,
                 membership: dict[str, str],  # node_id -> base_url (incl. self)
                 rf: int = 3, write_quorum: int = 2, read_quorum: int = 2):
        self.node_id, self.host, self.port = node_id, host, port
        self.membership = dict(membership)
        self.rf, self.W, self.R = rf, write_quorum, read_quorum
        self.storage = Storage()
        self.ring = HashRing()
        for nid in self.membership:
            self.ring.add_node(nid)
        self._runner: Optional[web.AppRunner] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: list[asyncio.Task] = []
        self.metrics = {"coord_writes": 0, "coord_reads": 0,
                        "read_repairs": 0, "gossip_pulls": 0}

    # ------------------------------------------------------------ lifecycle
    async def start(self) -> None:
        app = web.Application(client_max_size=1024**2)
        app.add_routes([
            web.put("/v1/facts", self.h_put_facts),
            web.get("/v1/facts/{agent_id}", self.h_get_facts),
            web.get("/v1/resolve/{agent_id}", self.h_resolve),
            web.get("/v1/discover", self.h_discover),
            web.get("/v1/audit/{agent_id}", self.h_audit),
            web.get("/v1/stats", self.h_stats),
            web.get("/health", self.h_health),
            web.post("/internal/store", self.h_internal_store),
            web.get("/internal/fetch/{agent_id}", self.h_internal_fetch),
            web.get("/internal/digest", self.h_internal_digest),
        ])
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        self._session = aiohttp.ClientSession(timeout=INTERNAL_TIMEOUT)
        self._tasks = [asyncio.create_task(self._gossip_loop()),
                       asyncio.create_task(self._sweep_loop())]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()
        if self._session:
            await self._session.close(); self._session = None
        if self._runner:
            await self._runner.cleanup(); self._runner = None

    # ------------------------------------------------------------ helpers
    def _candidates(self, agent_id: str) -> list[str]:
        return self.ring.preference_list(agent_id, self.rf + SLOP)

    async def _peer_post_store(self, nid: str, payload: dict) -> str | None:
        """Returns the replica's code ('stored', 'stale', ...) or None if down."""
        if nid == self.node_id:
            return self.storage.put(FactRecord.from_dict(payload)).code
        try:
            async with self._session.post(
                f"{self.membership[nid]}/internal/store", json=payload) as r:
                return (await r.json()).get("code", "error")
        except Exception:
            return None

    async def _peer_fetch(self, nid: str, agent_id: str):
        """Returns (responded: bool, record_dict | None)."""
        if nid == self.node_id:
            rec = self.storage.get(agent_id, include_expired=True)
            return True, (rec.to_dict() if rec else None)
        try:
            async with self._session.get(
                f"{self.membership[nid]}/internal/fetch/{agent_id}") as r:
                if r.status == 200:
                    return True, await r.json()
                if r.status == 404:
                    return True, None
        except Exception:
            pass
        return False, None

    # ------------------------------------------------------ coordinator: W
    async def coordinate_write(self, payload: dict) -> web.Response:
        self.metrics["coord_writes"] += 1
        try:
            rec = FactRecord.from_dict(payload)
        except Exception:
            return web.json_response({"error": "malformed_record"}, status=400)
        ok, why = rec.verify()
        if not ok:
            return web.json_response({"error": why}, status=403)

        cands = self._candidates(rec.agent_id)
        primary, slop = cands[:self.rf], cands[self.rf:]
        ACK, VETO = ("stored", "duplicate"), ("stale", "equivocation")
        results = await asyncio.gather(*[self._peer_post_store(n, payload)
                                         for n in primary])
        codes = {n: c for n, c in zip(primary, results)}
        # A replica rejecting as stale/equivocation is AUTHORITATIVE: it
        # proves a higher (or conflicting) signed version already exists.
        # Never route around it via the sloppy tail.
        veto = [c for c in codes.values() if c in VETO]
        if veto:
            return web.json_response({"stored": False, "error": veto[0],
                                      "detail": codes}, status=409)
        acked = [n for n, c in codes.items() if c in ACK]
        for n in slop:      # sloppy tail: only for UNAVAILABLE replicas
            if len(acked) >= self.W:
                break
            c = await self._peer_post_store(n, payload)
            if c in VETO:
                return web.json_response({"stored": False, "error": c},
                                         status=409)
            if c in ACK:
                acked.append(n)
        if len(acked) >= self.W:
            return web.json_response({"stored": True, "version": rec.version,
                                      "acks": len(acked), "replicas": acked})
        return web.json_response({"stored": False, "acks": len(acked),
                                  "error": "write_quorum_failed"}, status=503)

    # ------------------------------------------------------ coordinator: R
    async def coordinate_read(self, agent_id: str):
        self.metrics["coord_reads"] += 1
        cands = self._candidates(agent_id)
        results = await asyncio.gather(*[self._peer_fetch(n, agent_id)
                                         for n in cands])
        responded = [(n, rec) for n, (ok, rec) in zip(cands, results) if ok]
        if len(responded) < min(self.R, len(cands)):
            return None, 503
        best: Optional[FactRecord] = None
        for _, rd in responded:
            if rd is None:
                continue
            try:
                cand = FactRecord.from_dict(rd)
            except Exception:
                continue
            if cand.verify()[0] and (best is None or cand.version > best.version):
                best = cand
        if best is None:
            return None, 404
        # async read-repair: push winner to lagging responders
        lag = [n for n, rd in responded
               if rd is None or rd.get("version", 0) < best.version]
        if lag:
            self.metrics["read_repairs"] += len(lag)
            payload = best.to_dict()
            for n in lag:
                asyncio.create_task(self._peer_post_store(n, payload))
        return best, 200

    # ------------------------------------------------------------ handlers
    async def h_put_facts(self, req: web.Request) -> web.Response:
        return await self.coordinate_write(await req.json())

    async def h_get_facts(self, req: web.Request) -> web.Response:
        rec, status = await self.coordinate_read(req.match_info["agent_id"])
        if rec is None:
            return web.json_response({"error": "not_found" if status == 404
                                      else "read_quorum_failed"}, status=status)
        return web.json_response(rec.to_dict())

    async def h_resolve(self, req: web.Request) -> web.Response:
        """Lean resolution: just what a caller needs to reach the agent."""
        rec, status = await self.coordinate_read(req.match_info["agent_id"])
        if rec is None:
            return web.json_response({"error": "not_found"}, status=status)
        if rec.revoked:
            return web.json_response({"agent_id": rec.agent_id, "revoked": True,
                                      "version": rec.version}, status=410)
        if rec.is_expired():
            return web.json_response({"error": "expired_lease"}, status=404)
        return web.json_response({
            "agent_id": rec.agent_id, "agent_name": rec.agent_name,
            "version": rec.version, "endpoints": rec.facts.get("endpoints", {}),
            "capabilities": rec.facts.get("capabilities", {}),
            "ttl_remaining_s": round(rec.ttl_remaining(), 1),
            "verified": True, "served_by": self.node_id,
        })

    async def h_discover(self, req: web.Request) -> web.Response:
        """Directory view: every live agent this node knows about."""
        cards = [{
            "agent_id": r.agent_id, "agent_name": r.agent_name,
            "label": r.facts.get("label", ""),
            "endpoints": r.facts.get("endpoints", {}),
            "skills": [s.get("id") for s in r.facts.get("skills", [])],
            "version": r.version,
            "ttl_remaining_s": round(r.ttl_remaining(), 1),
        } for r in self.storage.live_records()]
        return web.json_response({"count": len(cards), "agents": cards,
                                  "served_by": self.node_id})

    async def h_audit(self, req: web.Request) -> web.Response:
        chain = self.storage.audit_chain(req.match_info["agent_id"])
        return web.json_response({"chain": [e.__dict__ for e in chain],
                                  "equivocations": [
                                      e for e in self.storage.equivocations
                                      if e["agent_id"] == req.match_info["agent_id"]]})

    async def h_stats(self, req: web.Request) -> web.Response:
        return web.json_response({"node_id": self.node_id,
                                  "records": len(self.storage),
                                  **self.storage.stats, **self.metrics})

    async def h_health(self, req: web.Request) -> web.Response:
        return web.json_response({"ok": True, "node_id": self.node_id})

    async def h_internal_store(self, req: web.Request) -> web.Response:
        try:
            rec = FactRecord.from_dict(await req.json())
        except Exception:
            return web.json_response({"error": "malformed"}, status=400)
        res = self.storage.put(rec)
        return web.json_response({"code": res.code}, status=res.http)

    async def h_internal_fetch(self, req: web.Request) -> web.Response:
        rec = self.storage.get(req.match_info["agent_id"], include_expired=True)
        if rec is None:
            return web.json_response({"error": "not_found"}, status=404)
        return web.json_response(rec.to_dict())

    async def h_internal_digest(self, req: web.Request) -> web.Response:
        return web.json_response(self.storage.digest())

    # ------------------------------------------------------------- gossip
    async def _gossip_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(GOSSIP_INTERVAL * random.uniform(0.7, 1.3))
                peers = [n for n in self.membership if n != self.node_id]
                if not peers or self._session is None:
                    continue
                peer = random.choice(peers)
                try:
                    async with self._session.get(
                        f"{self.membership[peer]}/internal/digest") as r:
                        if r.status != 200:
                            continue
                        peer_digest = await r.json()
                except Exception:
                    continue
                mine = self.storage.digest()
                pulled = 0
                for aid, ver in peer_digest.items():
                    if pulled >= 300:
                        break
                    if mine.get(aid, 0) >= ver:
                        continue
                    if self.node_id not in self._candidates(aid):
                        continue          # not my key — don't hoard
                    ok, rd = await self._peer_fetch(peer, aid)
                    if ok and rd:
                        if self.storage.put(FactRecord.from_dict(rd)).ok:
                            pulled += 1
                self.metrics["gossip_pulls"] += pulled
            except asyncio.CancelledError:
                return
            except Exception:
                continue

    async def _sweep_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(SWEEP_INTERVAL)
                self.storage.sweep_expired()
            except asyncio.CancelledError:
                return
            except Exception:
                continue
