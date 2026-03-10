"""
dedup.py - Duplicate message detection for telegram-monitor.

Uses exact MD5 hashing + SimHash fingerprinting with Hamming distance
for near-duplicate detection.
"""

import hashlib
import asyncio
import time


def _simhash(text, hashbits=64):
    """Compute a SimHash fingerprint for the given text using 3-char shingles."""
    cleaned = " ".join(text.lower().split())
    if len(cleaned) < 3:
        return int(hashlib.md5(cleaned.encode()).hexdigest(), 16) % (2 ** hashbits)
    shingles = [cleaned[i:i+3] for i in range(len(cleaned) - 2)]
    v = [0] * hashbits
    for shingle in shingles:
        h = int(hashlib.md5(shingle.encode()).hexdigest(), 16)
        for i in range(hashbits):
            v[i] += 1 if h & (1 << i) else -1
    fingerprint = 0
    for i in range(hashbits):
        if v[i] > 0:
            fingerprint |= (1 << i)
    return fingerprint


def _hamming_distance(a, b):
    """Compute Hamming distance between two integers.
    Uses int.bit_count() on Python 3.10+, falls back to Kernighan's method.
    """
    x = a ^ b
    if hasattr(x, 'bit_count'):
        return x.bit_count()
    count = 0
    while x:
        count += 1
        x &= x - 1
    return count


class DuplicateDetector:
    """Scalable dedup: exact MD5 + simhash with Hamming distance.

    Uses periodic cleanup (every 60s) instead of per-call cleanup.
    """
    __slots__ = (
        'max_history', 'ttl_seconds', 'hamming_threshold',
        'seen_hashes', 'seen_simhashes', 'stats',
        '_lock', '_last_clean_time',
    )

    def __init__(self, threshold=0.85, max_history=500, ttl_hours=6):
        self.max_history = max_history
        self.ttl_seconds = ttl_hours * 3600
        self.hamming_threshold = int((1 - threshold) * 64)
        self.seen_hashes = {}
        self.seen_simhashes = []
        self.stats = {"total": 0, "unique": 0, "duplicate": 0, "skipped": 0}
        self._lock = asyncio.Lock()
        self._last_clean_time = time.time()

    def _clean_old(self):
        """Only clean if 60s have passed since last clean."""
        now = time.time()
        if now - self._last_clean_time < 60:
            return
        self._last_clean_time = now
        old_count = len(self.seen_hashes)
        self.seen_hashes = {
            h: t for h, t in self.seen_hashes.items()
            if now - t < self.ttl_seconds
        }
        self.seen_simhashes = [
            (sh, t) for sh, t in self.seen_simhashes
            if now - t < self.ttl_seconds
        ]
        if len(self.seen_simhashes) > self.max_history:
            self.seen_simhashes = self.seen_simhashes[-self.max_history:]
        cleaned = old_count - len(self.seen_hashes)
        if cleaned:
            print(f"\U0001f9f9 Cleaned {cleaned} old entries")

    async def is_duplicate(self, text):
        """Check if text is a duplicate. Returns True if duplicate or empty."""
        async with self._lock:
            self.stats["total"] += 1
            if not text or not text.strip():
                self.stats["skipped"] += 1
                return True
            self._clean_old()
            now = time.time()

            text_hash = hashlib.md5(
                " ".join(text.lower().split()).encode()
            ).hexdigest()

            if text_hash in self.seen_hashes:
                self.stats["duplicate"] += 1
                return True

            if len(text.strip()) > 15:
                text_simhash = _simhash(text)
                for old_simhash, _ in self.seen_simhashes:
                    if _hamming_distance(text_simhash, old_simhash) <= self.hamming_threshold:
                        self.stats["duplicate"] += 1
                        return True
                self.seen_simhashes.append((text_simhash, now))

            self.seen_hashes[text_hash] = now
            self.stats["unique"] += 1
            return False

    def get_stats(self):
        """Return current dedup statistics."""
        return self.stats.copy()
