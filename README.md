# tinyNANDA

**Project NANDA, demonstrated at 2-Raspberry-Pi scale.**
*Xolix.AI Research Labs — v0.1 research prototype*

---

## What this is

[Project NANDA](https://arxiv.org/abs/2507.14263) (MIT) proposes an architecture for the Internet of AI Agents: a lean global index of ≤120-byte records that point to signed **AgentFacts** documents — the dynamic metadata (endpoints, capabilities, credentials, liveness) that actually makes an agent reachable and trustworthy.

tinyNANDA demonstrates that architecture end-to-end on **two Raspberry Pis on a switch**. The point of starting small: **the protocol doesn't know it's small.** Two Pis run the same cryptography, the same write/read quorums, and the same gossip anti-entropy as a planetary deployment — scale is just more entries in the membership map.

What you get on the LAN:

- Each Pi runs one node of **XolixDB**, a purpose-built distributed store for signed, versioned, expiring AgentFacts records (see [Under the hood](#under-the-hood-xolixdb) below).
- Any machine on the network can launch **agents** that generate their own cryptographic identity, publish a signed contact card, heartbeat their lease, and discover each other across nodes within a second.
- You can pull one Pi's ethernet cable mid-demo and the survivor keeps serving; plug it back in and gossip heals it in about a second. Ctrl-C an agent and its card expires cluster-wide within one TTL. **Liveness is proven, never assumed.**

## Hardware

- 2× Raspberry Pi (anything that runs Python 3.10+; a laptop pair works just as well)
- 1× ethernet switch (or any shared LAN / Wi-Fi network)

## Running the demo

Install the two dependencies on each Pi:

```bash
pip install aiohttp pynacl
```

Boot one XolixDB node per Pi (full membership list on both sides):

```bash
# Pi A (192.168.1.10)
python3 run_node.py --node-id pi-a --port 8701 \
  --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701

# Pi B (192.168.1.11) — same command, just --node-id pi-b
python3 run_node.py --node-id pi-b --port 8701 \
  --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701
```

Quorums auto-adapt to cluster size: with 2 nodes it runs RF=2, W=1, R=1.

Launch agents from any machine on the LAN, pointed at either Pi:

```bash
python3 tiny_agent.py --name translator --skill translation --node http://192.168.1.10:8701
python3 tiny_agent.py --name scheduler  --skill calendar    --node http://192.168.1.11:8701
```

Each agent announces a signed card, re-signs it every `ttl/2` as a heartbeat, polls `GET /v1/discover`, and greets every new agent it finds — across nodes, within a second.

Things to try mid-demo:

- **Partition:** pull Pi A's cable. Agents pointed at Pi B keep publishing and discovering. Plug it back in — gossip repopulates Pi A with zero operator action.
- **Death:** Ctrl-C an agent. Within one TTL its card expires on both Pis and it vanishes from `/v1/discover`.
- **Inspection:** `curl http://<pi>:8701/v1/discover` from your laptop to watch the agent roster live.

### No hardware handy?

The same protocol runs as a 5-node localhost cluster with 8 scripted scenarios (endpoint rotation, revocation, forged records, equivocation, node failure, self-healing):

```bash
python3 demo.py
```

## Under the hood: XolixDB

NANDA's paper leaves AgentFacts hosting as an open question (§XI.A): self-hosting leaks access patterns, third-party hosting imports someone else's security posture, and generic infrastructure (IPFS, CDNs, S3) was never built for records that must be cryptographically verified, versioned, revocable in under a second, and expiring by design. XolixDB is the storage tier built specifically for that slot — its data model *is* the signed AgentFacts record.

The core idea is **Owner-Serialized Consistency**: every AgentFacts record has exactly one legitimate writer — the holder of the agent's private key — so the heavy conflict machinery of general distributed databases (vector clocks, consensus rounds) is unnecessary. Three primitives replace it:

1. **Self-certifying names.** An `agent_id` is *derived from* the agent's Ed25519 public key (`did:xolix:<b32(sha256(pubkey))>`). Only the keyholder can produce a valid record for that ID — no registrar, no CA in the update path.

2. **Signed monotonic versions instead of clocks.** Every record carries a version number covered by the owner's signature; replicas resolve conflicts by one deterministic rule: *highest verified version wins.* Replaying an old-but-validly-signed record is rejected (`409 stale`), and two different records signed with the same version constitute provable **owner equivocation** — flagged for audit, never silently merged.

3. **A per-agent hash chain.** Each record embeds the hash of its predecessor, giving every agent a tamper-evident, append-only history — verifiable by any client, no blockchain required.

Everything else is deliberately boring, proven engineering: a consistent-hash ring with virtual nodes, replication, tunable W/R quorums with a sloppy tail for availability, async read-repair, and ~1 s gossip anti-entropy.

Records are verified on write (forged identities and tampered payloads die at the door with `403`) and re-verified by the client SDK on read — so even a fully compromised node cannot forge an agent's endpoints without detection. Every record carries a signed TTL, and revocation is a signed tombstone that propagates through the write quorum in milliseconds.

## HTTP API

| Route | Purpose |
|---|---|
| `PUT /v1/facts` | Publish a signed record (coordinator: verify → replicate → W acks) |
| `GET /v1/resolve/{agent_id}` | Lean resolution: endpoints, capabilities, `ttl_remaining` |
| `GET /v1/facts/{agent_id}` | Full record for client-side verification |
| `GET /v1/discover` | Live roster of unexpired agents |
| `GET /v1/audit/{agent_id}` | Hash chain + any equivocation flags |
| `GET /v1/stats`, `/health` | Node metrics |

## Measured against NANDA's design goals (5-node localhost cluster)

| NANDA design goal | Mechanism | Demo result |
|---|---|---|
| Endpoint agility, sub-second reachability | W-quorum writes, any-node reads | Rotation visible cross-node in **9.1 ms** |
| Sub-second revocation | Signed tombstones + stale rejection | `410 Gone` cluster-wide in **7.8 ms** |
| Decentralized updates | Owner-signed records, no central writer | **421 writes/s through a single client**, p50 2.1 ms |
| Audited metadata | Verify-on-write + equivocation detection | Forged ID, tampered payload, stale replay, equivocation: **all rejected** |
| Append-only governance log | Per-agent hash chain | Client-verified `chain_intact` |
| Resilience | Sloppy quorum + gossip anti-entropy | 1/5 nodes down: 30/30 writes, 50/50 reads OK; restarted node self-healed 225 records in ~1 s |

*(Localhost numbers characterize the protocol path, not WAN latency; the sub-second targets hold with two orders of magnitude of headroom for real network RTTs.)*

## Repository layout

```
xolixdb/crypto.py    self-certifying identity (Ed25519, did:xolix)
xolixdb/record.py    FactRecord: signed versions + hash chain
xolixdb/ring.py      consistent hashing, virtual nodes
xolixdb/storage.py   verify-on-write engine, TTL sweep, equivocation log
xolixdb/node.py      coordinator, quorums, read-repair, gossip, /v1/discover
xolixdb/client.py    SDK + zero-trust verification helpers
demo.py              8-scenario localhost proof (no hardware needed)
run_node.py          boot one node on real hardware (one per Pi)
tiny_agent.py        self-announcing agent: publish, heartbeat, discover
```

## Honest limitations & roadmap

This is a research prototype: in-memory storage (persistence layer next), static membership (dynamic join/leave with ring rebalancing planned), and Ed25519 self-signatures rather than full W3C Verifiable Credential chains (the record format leaves room for issuer credentials — the path to NANDA's federated trust zones). Also on the roadmap: FSST-style compression of AgentFacts payloads and a Merkle-tree digest exchange to replace the flat gossip digest at scale.

## Citation

NANDA architecture: Raskar et al., *Beyond DNS: Unlocking the Internet of AI Agents via the NANDA Index and Verified AgentFacts*, arXiv:2507.14263.

---
*tinyNANDA / XolixDB — Xolix.AI Research Labs. They index the agents; we remember what the agents are.*
