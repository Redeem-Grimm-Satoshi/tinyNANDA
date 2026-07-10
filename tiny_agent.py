"""
tiny_agent.py — A minimal AI agent for the TinyNANDA demo.

What it does, forever:
  1. Generates its own cryptographic identity (keys -> did:xolix:...)
  2. Publishes a signed "contact card" (AgentFacts) to any XolixDB node
  3. Heartbeats: re-signs the card every ttl/2 to prove it's alive
  4. Polls /v1/discover and greets every NEW agent it finds

Run several of these against either Pi and watch them find each other:

  python3 tiny_agent.py --name translator --skill translation \
      --node http://192.168.1.10:8701
  python3 tiny_agent.py --name scheduler --skill calendar \
      --node http://192.168.1.11:8701

Ctrl-C an agent and, within one TTL, its card expires cluster-wide —
liveness is proven, never assumed.
"""
import argparse
import asyncio
import time

import aiohttp

from xolixdb.client import AgentIdentity, XolixClient, build_agent_facts


async def main() -> None:
    ap = argparse.ArgumentParser(description="Run a TinyNANDA agent")
    ap.add_argument("--name", required=True)
    ap.add_argument("--node", required=True, help="any XolixDB node URL")
    ap.add_argument("--skill", default="chat")
    ap.add_argument("--ttl", type=int, default=30,
                    help="lease seconds; heartbeat fires at ttl/2")
    ap.add_argument("--discover-every", type=float, default=3.0)
    a = ap.parse_args()

    me = AgentIdentity(f"urn:agent:tinynanda:{a.name}")
    facts = build_agent_facts(
        a.name, f"TinyNANDA {a.skill} agent",
        [f"http://tinynanda.local/{a.name}"],
        skills=[{"id": a.skill, "latencyBudgetMs": 500}])

    known: set[str] = set()
    async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=3)) as sess:
        cli = XolixClient(sess)

        async def heartbeat() -> None:
            while True:
                rec = me.next_record(facts, ttl=a.ttl)
                try:
                    res = await cli.publish(a.node, rec)
                    tag = "announced" if rec.version == 1 else "heartbeat"
                    print(f"[{a.name}] {tag} v{rec.version} "
                          f"(lease {a.ttl}s, {res.get('acks', '?')} replicas)")
                except Exception as e:
                    print(f"[{a.name}] node unreachable ({e!r}) — retrying")
                await asyncio.sleep(a.ttl / 2)

        async def discover() -> None:
            while True:
                try:
                    async with sess.get(f"{a.node}/v1/discover") as r:
                        body = await r.json()
                    for card in body.get("agents", []):
                        if card["agent_id"] == me.agent_id:
                            continue
                        if card["agent_id"] not in known:
                            known.add(card["agent_id"])
                            print(f"[{a.name}] >> discovered '{card['label']}' "
                                  f"(skills={card['skills']}, "
                                  f"id={card['agent_id'][:22]}..., "
                                  f"lease {card['ttl_remaining_s']}s)")
                except Exception:
                    pass
                await asyncio.sleep(a.discover_every)

        print(f"[{a.name}] identity: {me.agent_id}")
        await asyncio.gather(heartbeat(), discover())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[agent] stopped — my card will expire on its own. Goodbye.")
