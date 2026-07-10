# tinyNANDA

**Project NANDA demonstrated at two-Raspberry-Pi scale.**
*Xolix.AI Research Labs. v0.1 research prototype.*

---

## Overview

[Project NANDA](https://arxiv.org/abs/2507.14263) (MIT) proposes an architecture for the Internet of AI Agents: a lean global index of records no larger than 120 bytes, each pointing to a signed **AgentFacts** document that carries the dynamic metadata required to reach and trust an agent (endpoints, capabilities, credentials, liveness).

tinyNANDA implements that architecture end to end on two Raspberry Pis connected by a switch. The deployment is deliberately minimal; the protocol stack is not. Both nodes run the same cryptographic verification, the same write and read quorums, and the same gossip anti-entropy that a large deployment would run. Cluster size is a configuration detail, not an architectural one.

The demonstration consists of three parts:

- Each Pi runs one node of **XolixDB**, a distributed store designed specifically for signed, versioned, expiring AgentFacts records (see [Design](#design-xolixdb) below).
- Any machine on the network can run agents that generate their own cryptographic identity, publish a signed contact card, renew their lease with periodic heartbeats, and discover one another across nodes within approximately one second.
- The cluster tolerates the failures you would exercise in any availability test. Disconnecting one Pi leaves the surviving node fully operational, and reconnecting it triggers gossip-based repair within about a second. Terminating an agent causes its record to expire cluster-wide within one TTL. Liveness is established by re-signing a lease, never assumed.

## Hardware

- 2x Raspberry Pi (any hardware running Python 3.10+ works; a pair of laptops is equivalent)
- 1x ethernet switch, or any shared LAN or Wi-Fi network

## Running the demonstration

Install the two dependencies on each Pi:

```bash
pip install aiohttp pynacl
```

Start one XolixDB node per Pi. Both nodes receive the full membership list:

```bash
# Pi A (192.168.1.10)
python3 run_node.py --node-id pi-a --port 8701 \
  --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701

# Pi B (192.168.1.11): same command with --node-id pi-b
python3 run_node.py --node-id pi-b --port 8701 \
  --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701
```

Quorum parameters adapt to cluster size. With two nodes the cluster runs RF=2, W=1, R=1.

Start agents from any machine on the LAN, pointed at either node:

```bash
python3 tiny_agent.py --name translator --skill translation --node http://192.168.1.10:8701
python3 tiny_agent.py --name scheduler  --skill calendar    --node http://192.168.1.11:8701
```

Each agent announces a signed card, re-signs it every `ttl/2` seconds as a heartbeat, polls `GET /v1/discover`, and reports every new agent it observes, across nodes, within approximately one second.

Scenarios worth exercising:

- **Partition.** Disconnect Pi A. Agents pointed at Pi B continue publishing and discovering. Reconnect Pi A and gossip repopulates it with no operator action.
- **Agent failure.** Terminate an agent with Ctrl-C. Within one TTL its card expires on both nodes and disappears from `/v1/discover`.
- **Inspection.** Run `curl http://<pi>:8701/v1/discover` from a third machine to observe the live agent roster.

### Running without hardware

The same protocol runs as a five-node localhost cluster with eight scripted scenarios: endpoint rotation, revocation, forged records, equivocation, node failure, and self-healing.

```bash
python3 demo.py
```

## Design: XolixDB

The NANDA paper leaves AgentFacts hosting as an open question (section XI.A). Self-hosting leaks access patterns, third-party hosting imports an external security posture, and generic infrastructure (IPFS, CDNs, object storage) was not built for records that must be cryptographically verified, versioned, revocable in under a second, and expiring by design. XolixDB is a storage tier built for that slot. Its data model is the signed AgentFacts record itself.

The central design decision is **owner-serialized consistency**. Every AgentFacts record has exactly one legitimate writer, the holder of the agent's private key, so the conflict-resolution machinery of general-purpose distributed databases (vector clocks, last-writer-wins timestamps, consensus rounds) is unnecessary. Three mechanisms replace it:

1. **Self-certifying names.** An `agent_id` is derived from the agent's Ed25519 public key (`did:xolix:<b32(sha256(pubkey))>`). Only the keyholder can produce a valid record for that ID. No registrar or certificate authority participates in the update path.

2. **Signed monotonic versions.** Every record carries a version number covered by the owner's signature. Replicas resolve conflicts with a single deterministic rule: highest verified version wins. Replaying an older, validly signed record is rejected with `409 stale`. Two distinct records signed at the same version constitute provable owner equivocation; they are flagged for audit rather than silently merged.

3. **Per-agent hash chains.** Each record embeds the hash of its predecessor, giving every agent a tamper-evident, append-only history that any client can verify independently. No blockchain is required.

The remaining machinery is conventional: a consistent-hash ring with virtual nodes, configurable replication, tunable W/R quorums with a sloppy tail for availability, asynchronous read repair, and gossip anti-entropy on an approximately one-second interval.

Records are verified on write. Forged identities and tampered payloads are rejected with `403` before reaching storage. The client SDK re-verifies signatures on read, so a compromised node cannot forge an agent's endpoints without detection. Every record carries a signed TTL, and revocation is a signed tombstone that propagates through the write quorum in milliseconds.

## HTTP API

| Route | Purpose |
|---|---|
| `PUT /v1/facts` | Publish a signed record (coordinator: verify, replicate, await W acks) |
| `GET /v1/resolve/{agent_id}` | Lean resolution: endpoints, capabilities, `ttl_remaining` |
| `GET /v1/facts/{agent_id}` | Full record for client-side verification |
| `GET /v1/discover` | Live roster of unexpired agents |
| `GET /v1/audit/{agent_id}` | Hash chain and any equivocation flags |
| `GET /v1/stats`, `/health` | Node metrics |

## Measured results (five-node localhost cluster)

| NANDA design goal | Mechanism | Result |
|---|---|---|
| Endpoint agility, sub-second reachability | W-quorum writes, any-node reads | Rotation visible cross-node in 9.1 ms |
| Sub-second revocation | Signed tombstones, stale rejection | `410 Gone` cluster-wide in 7.8 ms |
| Decentralized updates | Owner-signed records, no central writer | 421 writes/s through a single client, p50 2.1 ms |
| Audited metadata | Verify-on-write, equivocation detection | Forged ID, tampered payload, stale replay, equivocation: all rejected |
| Append-only governance log | Per-agent hash chain | Client-verified `chain_intact` |
| Resilience | Sloppy quorum, gossip anti-entropy | 1 of 5 nodes down: 30/30 writes, 50/50 reads succeeded; restarted node self-healed 225 records in about 1 s |

Localhost numbers characterize the protocol path, not WAN latency. The sub-second targets hold with roughly two orders of magnitude of headroom for real network round trips.

## Repository layout

```
xolixdb/crypto.py    self-certifying identity (Ed25519, did:xolix)
xolixdb/record.py    FactRecord: signed versions and hash chain
xolixdb/ring.py      consistent hashing, virtual nodes
xolixdb/storage.py   verify-on-write engine, TTL sweep, equivocation log
xolixdb/node.py      coordinator, quorums, read repair, gossip, /v1/discover
xolixdb/client.py    SDK and client-side verification helpers
demo.py              eight-scenario localhost demonstration (no hardware required)
run_node.py          boot one node on real hardware (one per Pi)
tiny_agent.py        self-announcing agent: publish, heartbeat, discover
```

## Limitations and roadmap

This is a research prototype. Storage is in-memory; a persistence layer is the next milestone. Membership is static; dynamic join and leave with ring rebalancing is planned. Records are self-signed with Ed25519 rather than carrying full W3C Verifiable Credential chains, although the record format reserves room for issuer credentials, which is the path to NANDA's federated trust zones. Also on the roadmap: FSST-style compression of AgentFacts payloads, and a Merkle-tree digest exchange to replace the flat gossip digest at scale.

## Citation

NANDA architecture: Raskar et al., *Beyond DNS: Unlocking the Internet of AI Agents via the NANDA Index and Verified AgentFacts*, arXiv:2507.14263.

---
*tinyNANDA / XolixDB, Xolix.AI Research Labs.*
