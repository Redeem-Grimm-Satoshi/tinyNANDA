"""
run_node.py — Boot one XolixDB node on real hardware (TinyNANDA).

Two Raspberry Pis on a switch:

  Pi A (192.168.1.10):
    python3 run_node.py --node-id pi-a --port 8701 \
      --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701

  Pi B (192.168.1.11):
    python3 run_node.py --node-id pi-b --port 8701 \
      --peers pi-a=http://192.168.1.10:8701,pi-b=http://192.168.1.11:8701

Quorums auto-adapt to cluster size: with 2 nodes it runs RF=2, W=1, R=1 —
so you can pull one Pi's ethernet cable mid-demo and the survivor keeps
serving; plug it back in and gossip heals it in about a second.
"""
import argparse
import asyncio

from xolixdb.node import XolixNode


def parse_peers(s: str) -> dict[str, str]:
    out = {}
    for part in s.split(","):
        nid, url = part.split("=", 1)
        out[nid.strip()] = url.strip().rstrip("/")
    return out


async def main() -> None:
    ap = argparse.ArgumentParser(description="Run one XolixDB node")
    ap.add_argument("--node-id", required=True)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--peers", required=True,
                    help="full membership: id=url,id=url (include this node)")
    ap.add_argument("--rf", type=int, default=0, help="0 = auto")
    ap.add_argument("--write-quorum", type=int, default=0)
    ap.add_argument("--read-quorum", type=int, default=0)
    a = ap.parse_args()

    membership = parse_peers(a.peers)
    if a.node_id not in membership:
        raise SystemExit(f"--peers must include this node's id ({a.node_id})")
    n = len(membership)
    rf = a.rf or min(3, n)
    w = a.write_quorum or (1 if n <= 2 else 2)
    r = a.read_quorum or (1 if n <= 2 else 2)

    node = XolixNode(a.node_id, a.host, a.port, membership,
                     rf=rf, write_quorum=w, read_quorum=r)
    await node.start()
    print(f"[xolixdb] {a.node_id} up on {a.host}:{a.port} | "
          f"cluster={n} RF={rf} W={w} R={r}")
    print(f"[xolixdb] peers: {membership}")
    print(f"[xolixdb] try:  curl http://<this-ip>:{a.port}/v1/discover")
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[xolixdb] node stopped")
