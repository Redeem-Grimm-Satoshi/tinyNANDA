"""
xolixdb.crypto — Self-certifying agent identity.

The core primitive of XolixDB: an agent_id is *derived from* the agent's
Ed25519 public key (did:xolix:<base32(sha256(pubkey)[:20])>). This makes
names self-certifying: only the holder of the private key can publish or
update AgentFacts under that ID. No central CA is required for updates,
which answers NANDA's open question XI.F ("Can agents update their own
metadata autonomously?") with a cryptographic yes.
"""
from __future__ import annotations

import base64
import hashlib

from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

DID_PREFIX = "did:xolix:"


def generate_keypair() -> tuple[SigningKey, VerifyKey]:
    sk = SigningKey.generate()
    return sk, sk.verify_key


def pubkey_b64(vk: VerifyKey) -> str:
    return base64.b64encode(bytes(vk)).decode()


def pubkey_from_b64(b64: str) -> VerifyKey:
    return VerifyKey(base64.b64decode(b64))


def agent_id_from_pubkey(vk: VerifyKey) -> str:
    """did:xolix:<base32(sha256(pubkey)[:20])> — 32 chars, lowercase."""
    digest = hashlib.sha256(bytes(vk)).digest()[:20]
    b32 = base64.b32encode(digest).decode().lower().rstrip("=")
    return DID_PREFIX + b32


def sign(sk: SigningKey, data: bytes) -> str:
    return base64.b64encode(sk.sign(data).signature).decode()


def verify_signature(pubkey_b64_str: str, data: bytes, sig_b64: str) -> bool:
    try:
        vk = pubkey_from_b64(pubkey_b64_str)
        vk.verify(data, base64.b64decode(sig_b64))
        return True
    except (BadSignatureError, ValueError, TypeError):
        return False


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
