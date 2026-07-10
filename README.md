# XolixDB

**A distributed AgentFacts database for the agentic web.**
*Xolix.AI Research Labs — v0.1 research prototype*

---

## The gap XolixDB fills

Project NANDA's architecture for the Internet of AI Agents deliberately keeps its global index *lean* — records of ≤120 bytes that hold little more than an agent ID and pointers (`facts_url`, `private_facts_url`). Everything dynamic — endpoints that rotate every few minutes, capability claims, credentials, telemetry — lives in signed **AgentFacts** documents hosted *somewhere else*.

That "somewhere else" is the load-bearing wall of the whole design, and NANDA's own paper (arXiv:2507.14263, §XI.A) lists it as an **unresolved open question**: self-hosting leaks access patterns, third-party hosting imports someone else's security posture, and generic infrastructure (IPFS, CDNs, S3 buckets) was never built for records that must be cryptographically verified, versioned, revocable in under a second, and expiring by design.

XolixDB is a database built *specifically* to be that layer: **the metadata distribution tier the lean index points into.** Not a general-purpose KV store with signatures bolted on — a store whose data model *is* the signed, versioned, expiring AgentFacts record.

```
AgentName → NANDA Index → AgentAddr → facts_url / private_facts_url
                                            │
                                            ▼
                                   ┌─────────────────┐
                                   │     XolixDB     │   ← this project
                                   └─────────────────┘
```

## The core idea: Owner-Serialized Consistency

General distributed databases assume many writers per key, so they carry heavy machinery — vector clocks, last-writer-wins timestamps, consensus rounds — to resolve conflicts. AgentFacts break that assumption in a useful way: **every record has exactly one legitimate writer, the holder of the agent's private key.**

XolixDB exploits this with three interlocking primitives:

1. **Self-certifying names.** An `agent_id` is *derived from* the agent's Ed25519 public key (`did:xolix:<b32(sha256(pubkey))>`). Only the keyholder can ever produce a valid record for that ID — no registrar, no CA in the update path. This answers NANDA's open question §XI.F ("can agents update their own metadata autonomously?") with a cryptographic yes.

2. **Signed monotonic versions instead of clocks.** Every record carries a version number covered by the owner's signature. Replicas resolve conflicts by one deterministic rule: *highest verified version wins.* A replica cannot be regressed by replaying an old-but-validly-signed record (rejected `409 stale`), and two different records signed with the *same* version constitute provable **owner equivocation** — flagged for audit rather than silently merged. Replica rejections are authoritative: a `stale` veto short-circuits the write path and can never be routed around.

3. **A per-agent hash chain.** Each record embeds the hash of its predecessor, giving every agent a tamper-evident, append-only history — NANDA's "transparent, append-only log" for governance and liability, without a blockchain, verifiable by any client from the chain alone.

Everything else is deliberately boring, proven engineering: a consistent-hash ring with virtual nodes (Chord-style — NANDA's own citation [6]), replication factor 3, tunable W/R quorums with a sloppy tail for availability, async read-repair, and ~1 s gossip anti-entropy that heals partitions and repopulates restarted nodes with zero operator action.

## What the store enforces that generic hosting can't

**Verify-on-write, verify-on-read.** A record is checked (identity binding + signature + sanity) before it touches memory; forged identities and tampered payloads die at the door with `403`. The client SDK re-verifies on read, so even a fully compromised XolixDB node cannot forge an agent's endpoints without detection — true zero-trust resolution, matching NANDA's `PrivateFactsURL` threat model.

**TTL as a first-class citizen.** Agents on the agentic web are ephemeral. Every record carries a signed TTL; resolution returns `ttl_remaining`; a sweeper retires expired leases. Liveness is something an agent *proves* by re-signing a heartbeat, not something the database assumes.

**Sub-second revocation.** Publishing a signed tombstone (`revoked: true`) propagates through the write quorum in milliseconds; every node answers `410 Gone`, and any replay of the pre-revocation record is rejected as stale — NANDA's guarantee #3, measured below.

## Measured against NANDA's design goals (v0.1 prototype, 5-node localhost cluster)

| NANDA design goal | XolixDB mechanism | Demo result |
|---|---|---|
| C — Endpoint agility, sub-second reachability | W-quorum writes, any-node reads | Rotation visible cross-node in **9.1 ms** |
| Sub-second revocation (guarantee #3) | Signed tombstones + stale rejection | `410 Gone` cluster-wide in **7.8 ms** |
| D — Decentralized updates, 10k/s/shard target | Owner-signed records, no central writer | **421 writes/s through a single client**, p50 2.1 ms |
| G — From self-advertising to audited metadata | Verify-on-write + equivocation detection | Forged ID, tampered payload, stale replay, equivocation: **all rejected** |
| Governance & liability (append-only log) | Per-agent hash chain | Client-verified `chain_intact` |
| E — Privacy-preserving resolution | Neutral hosting; reads never touch agent infra | Fits the `private_facts_url` slot natively |
| Resilience | Sloppy quorum + gossip anti-entropy | 1/5 nodes down: 30/30 writes, 50/50 reads OK; restarted node self-healed 225 records in ~1 s |

*(Localhost numbers characterize the protocol path, not WAN latency; the sub-second targets hold with two orders of magnitude of headroom for real network RTTs.)*

## Quickstart

```bash
pip install aiohttp pynacl
python3 demo.py        # boots a 5-node cluster and runs all 8 scenarios
```

Publishing and resolving in ~10 lines:

```python
from xolixdb.client import AgentIdentity, XolixClient, build_agent_facts

me = AgentIdentity("urn:agent:xolix:translator")     # keys -> did:xolix:...
facts = build_agent_facts("Translator", "Real-time translation",
                          ["https://api.example.com/v1"])
rec = me.next_record(facts, ttl=600)                 # signed v1
await client.publish("http://node:8701", rec)        # W-quorum commit
out = await client.get_facts_verified("http://any-node:8703", me.agent_id)
assert out["client_verified"]                        # zero-trust read
```

## HTTP API

| Route | Purpose |
|---|---|
| `PUT /v1/facts` | Publish a signed record (coordinator: verify → RF replicas → W acks) |
| `GET /v1/resolve/{agent_id}` | Lean resolution: endpoints, capabilities, `ttl_remaining` |
| `GET /v1/facts/{agent_id}` | Full record for client-side verification |
| `GET /v1/audit/{agent_id}` | Hash chain + any equivocation flags |
| `GET /v1/stats`, `/health` | Node metrics |

## TinyNANDA: the trillion-agent internet at 2-Pi scale

The whole point of starting small: **the protocol doesn't know it's small.** Two Raspberry Pis on a switch run the same crypto, the same quorums, the same gossip as a planetary deployment — scale is just more entries in the membership map.

```bash
# Pi A (192.168.1.10)          # Pi B (192.168.1.11)
python3 run_node.py --node-id pi-a --port 8701 \
  --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701
# (same command on Pi B with --node-id pi-b)

# Agents, from any machine on the LAN:
python3 tiny_agent.py --name translator --skill translation --node http://192.168.1.10:8701
python3 tiny_agent.py --name scheduler  --skill calendar    --node http://192.168.1.11:8701
```

Agents announce a signed card, heartbeat their lease, and greet every new agent they discover via `GET /v1/discover` — across nodes, within a second. Pull one Pi's ethernet cable mid-demo: the survivor keeps serving; plug it back and gossip rebuilds it. Ctrl-C an agent: its card expires within one TTL. Liveness is proven, never assumed.

## Repository layout

```
xolixdb/crypto.py    self-certifying identity (Ed25519, did:xolix)
xolixdb/record.py    FactRecord: signed versions + hash chain
xolixdb/ring.py      consistent hashing, virtual nodes
xolixdb/storage.py   verify-on-write engine, TTL sweep, equivocation log
xolixdb/node.py      coordinator, quorums, read-repair, gossip, /v1/discover
xolixdb/client.py    SDK + zero-trust verification helpers
demo.py              the 8-scenario proof
run_node.py          boot one node on real hardware (TinyNANDA)
tiny_agent.py        self-announcing agent: publish, heartbeat, discover
```

## Honest limitations & roadmap

This is a research prototype: in-memory storage (persistence layer next), static membership (dynamic join/leave with ring rebalancing planned), and Ed25519 self-signatures rather than full W3C Verifiable Credential chains (the record format leaves room for issuer credentials, which is the path to NANDA's federated trust zones). Also on the roadmap: FSST-style compression of AgentFacts payloads — at trillions of small, highly-templated JSON documents, string compression *is* the storage bill — and a Merkle-tree digest exchange to replace the flat gossip digest at scale.

## Citation

NANDA architecture: Raskar et al., *Beyond DNS: Unlocking the Internet of AI Agents via the NANDA Index and Verified AgentFacts*, arXiv:2507.14263.

---
*XolixDB — Xolix.AI Research Labs. They index the agents; we remember what the agents are.*
