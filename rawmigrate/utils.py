import hashlib


def hash_str(s: str) -> int:
    """
    Hashes a string deterministically.
    """
    return int(hashlib.sha256(s.encode()).hexdigest()[:15], 16)
