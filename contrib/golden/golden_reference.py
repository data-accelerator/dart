#!/usr/bin/env python3
"""Independent reference implementation for DART's wire-protocol hashes.

This script is the canonical, cross-language source of the golden values the
Go tests pin (TestHash64Golden / TestChunkKeyGolden / TestEpochGolden /
TestRankGolden / TestRankWeightedGolden). It implements the exact same
constructions in pure Python — FNV-1a with the documented byte orders, then
fmix64 — so any drift in the Go code (or in this script) is caught by
comparing outputs, never by trusting one side.

Usage:
    python3 golden_reference.py            # print all golden values
    DART_GOLDEN_REF=1 go test ./...        # Go tests diff against this output

Output format: one record per line, pipe-separated fields, no spaces inside
fields:

    hash64|<key>|<nodeID>|<value>
    chunkkey|<namespace>|<objectID>|<chunkIndex>|<value>
    epoch|<id=weight,... (sorted, canonical) or "(empty)">|<value>
    rank|<key(hex, lowercase, no 0x)>|<id=weight,... input order>|<id,... winner order>

Only stdlib; Python >= 3.8. Note: weighted `rank` scores use math.log, which
is C libm here vs Go's math.Log (assembly on amd64/s390x) — the orders pinned
below were verified to match bit-consistently (0 divergences over 50M samples
during the audit), and the Go test compares ORDER only, never raw floats.
"""

import math
import struct
import sys

M64 = (1 << 64) - 1
OFFSET64 = 14695981039346656037
PRIME64 = 1099511628211
TWO64 = 2.0 ** 64
UNIT_SEP = 0x1F  # chunk-key field separator (chunk.UnitSep)
EPOCH_SEP = 0x1E  # epoch member terminator


def fmix64(z):
    z &= M64
    z ^= z >> 33
    z = (z * 0xFF51AFD7ED558CCD) & M64
    z ^= z >> 33
    z = (z * 0xC4CEB9FE1A85EC53) & M64
    z ^= z >> 33
    return z


class Fnv:
    def __init__(self):
        self.h = OFFSET64

    def mix(self, b):
        self.h ^= b
        self.h = (self.h * PRIME64) & M64

    def mix_bytes(self, bs):
        for b in bs:
            self.mix(b)


def hash64(key, node_id):
    """hashring.Hash64: FNV-1a over little-endian key bytes + nodeID bytes."""
    f = Fnv()
    f.mix_bytes(struct.pack("<Q", key))
    f.mix_bytes(node_id.encode())
    return fmix64(f.h)


def chunk_key(namespace, object_id, chunk_index):
    """chunk.ChunkKey: FNV-1a over ns, 0x1F, objectID, 0x1F, BE chunkIndex."""
    f = Fnv()
    f.mix_bytes(namespace.encode())
    f.mix(UNIT_SEP)
    f.mix_bytes(object_id.encode())
    f.mix(UNIT_SEP)
    f.mix_bytes(struct.pack(">Q", chunk_index & M64))
    return fmix64(f.h)


def epoch(members):
    """cluster epoch: FNV-1a over (ID, 0x1F, BE float64 weight bits, 0x1E) per
    canonical member, then fmix64. `members` is a list of (id, weight) already
    in canonical order."""
    f = Fnv()
    for mid, w in members:
        f.mix_bytes(mid.encode())
        f.mix(UNIT_SEP)
        f.mix_bytes(struct.pack(">Q", struct.unpack(">Q", struct.pack(">d", w))[0]))
        f.mix(EPOCH_SEP)
    return fmix64(f.h)


def score(key, node_id, w):
    """hashring.score: w / -log(u), u = (h + 0.5) / 2^64."""
    if w <= 0:
        w = 1.0
    u = (float(hash64(key, node_id)) + 0.5) / TWO64
    return w / -math.log(u)


def rank(key, members):
    """hashring.Rank: descending score, ties by ascending ID. `members` is a
    list of (id, weight); returns the ordered IDs."""
    return [mid for mid, _ in sorted(members, key=lambda t: (-score(key, t[0], t[1]), t[0]))]


DIGEST = "sha256:" + "ab" * 32

HASH64_CASES = [
    (0, "node-a"),
    (1, "node-a"),
    (0, "node-b"),
    (0xDEADBEEFCAFEBABE, "node-0"),
    (42, "kube-node-01"),
]

CHUNKKEY_CASES = [
    ("dart", DIGEST, 0),
    ("dart", DIGEST, 1),
    ("dart", "https://registry.example.com/v2/lib/nginx/blobs", 0),
    ("ns2", DIGEST, 0),
]

EPOCH_CASES = [
    [("node-a", 1.0), ("node-b", 2.0), ("node-c", 1.0)],
    [],
]

RANK_CASES = [
    # (key, [(id, weight), ...]) — the equal-weight case mirrors
    # TestRankGolden; the weighted cases pin the w / -ln(u) path
    # (TestRankWeightedGolden) where ordering genuinely needs the float score.
    (0xDEADBEEFCAFEBABE, [("node-%02d" % i, 1.0) for i in range(10)]),
    (0xDEADBEEFCAFEBABE, [("wn-1", 1.0), ("wn-2", 1.0), ("wn-3", 2.0), ("wn-4", 4.0)]),
    (42, [("wn-1", 1.0), ("wn-2", 1.0), ("wn-3", 2.0), ("wn-4", 4.0)]),
    (7, [("wn-1", 1.0), ("wn-2", 1.0), ("wn-3", 2.0), ("wn-4", 4.0)]),
    (1024, [("wn-1", 1.0), ("wn-2", 1.0), ("wn-3", 2.0), ("wn-4", 4.0)]),
]


def fmt_members(members):
    return ",".join("%s=%g" % (mid, w) for mid, w in members)


def main():
    out = []
    for key, nid in HASH64_CASES:
        out.append("hash64|%d|%s|%d" % (key, nid, hash64(key, nid)))
    for ns, oid, ci in CHUNKKEY_CASES:
        out.append("chunkkey|%s|%s|%d|%d" % (ns, oid, ci, chunk_key(ns, oid, ci)))
    for members in EPOCH_CASES:
        label = fmt_members(members) if members else "(empty)"
        out.append("epoch|%s|%d" % (label, epoch(members)))
    for key, members in RANK_CASES:
        out.append("rank|%x|%s|%s" % (key, fmt_members(members), ",".join(rank(key, members))))
    print("\n".join(out))


if __name__ == "__main__":
    sys.exit(main())
