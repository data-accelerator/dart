# Design Assumptions

DART trades generality for simplicity and speed (target: 100 Gbps/node). That
trade rests on a small set of **deliberate assumptions** collected here. They
are load-bearing: every package was designed under them, and behavior outside
them is out of scope — by design, not by omission.

Each entry states the assumption, what it buys, and what breaks when it is
violated. When a violation must be tolerated, the answer is an explicit
configuration/contract extension — never silent degradation.

## A1. The origin is trusted, read-only, and the single source of truth

Content-addressed objects (e.g. `sha256:` blobs) are immutable at the origin.
DART does not detect, verify, or repair origin-side content changes or
corruption; a cached block is never re-validated against the origin.

- **Buys**: write-once caches with zero revalidation traffic; digest-keyed
  cluster-wide dedup; no coherence protocol.
- **If violated** (a compromised or misbehaving origin serves wrong bytes for
  a digest): the wrong bytes are cached under that digest and re-served until
  evicted/wiped. Content-addressed *clients* (containerd) still verify the
  digest themselves, so the failure is a pull error, not silent corruption —
  but the cache entry stays poisoned until eviction. Protecting against a
  malicious origin is out of scope; protecting the *path* to the origin is
  the operator's responsibility (A2).
- Relied on by: docs/registry.md §2/§4, docs/engine.md §4, docs/store.md.

## A2. The deployment network is a trusted domain

The peer, client, and admin planes are plaintext and unauthenticated by
design; so is the path to an internal upstream unless the operator secures it
(TLS, network policy). See SECURITY.md's trust model — it is part of this
contract.

- **Buys**: no TLS termination on the 100 Gbps data path; no credential
  distribution problem inside the cluster.
- **If violated**: anyone who can reach a plane can read anything cached,
  join the peer plane, or call admin endpoints. A MITM on a plain-http
  upstream hop can additionally steer the registry token exchange (the
  `realm` is trusted as delivered by the upstream — docs/registry.md §5).
- DART therefore does **not** ship allowlist-style network configuration;
  enforcement belongs to the deployment environment.

## A3. Cluster configuration is uniform

All nodes run the same `BlockSize`, `ChunkSize`, namespace, fanout, and
feature set. Rolling changes that mix geometries are allowed to happen, but
byte-level guarantees across a mixed-geometry window are out of scope.

- **Buys**: placement and block math need no negotiation or version
  handshake; every node derives the same answer from the same inputs.
- **If violated**: a peer may legitimately serve its own geometry's blocks;
  the engine's geometry validation (docs/engine.md §3.3) prevents caching
  them, and reads fall back to origin — safe, but slower until the rollout
  converges.
- Relied on by: docs/chunk.md, docs/engine.md, docs/peer.md.

## A4. Weights are finite; non-positive ones are normalized to 1

Placement math assumes a **finite** weight; the score formula `w / -ln(u)`
divides by it. Non-positive weights are *not* an error: every boundary
normalizes `weight <= 0` to 1 (hashring's `Node.Weight` semantics, cluster's
self/member normalization in docs/cluster.md), so placement always sees a
positive finite number. The JSON wire additionally cannot carry NaN/±Inf at
all, so non-finite weights cannot arrive through any current path.

- **Buys**: the HRW score formula needs no validity plumbing, and a config
  typo (`weight: 0`) degrades to "average member" instead of a placement
  anomaly.
- **If violated** (a future API accepting arbitrary floats, bypassing
  normalization): a NaN weight would void the input-order-independence
  invariant. Any such API must reject non-finite values at its boundary — the
  invariant is the API's job, not the ring's. Note cluster's epoch preserves
  the *pre-normalization* weight verbatim (a documented wart: two byte-wise
  different views that normalize identically produce different epochs;
  docs/cluster.md).
- Relied on by: docs/hashring.md §4/§6.

## A5. Operator-supplied configuration values are sane

Namespace, self ID, advertised addresses, seed lists, and registry endpoint
come from trusted deployment configuration. DART validates only where a
mistake would produce *silent corruption* rather than a clean startup or
request failure (e.g. a wildcard advertise address is rejected because it
would silently make peers dial themselves).

- **Buys**: no defensive validation layer on the configuration path.
- **If violated**: expect loud startup/request failures in most cases; the
  exceptions have their own validation and docs (docs/node.md, docs/cluster.md).

## A6. The block cache is write-once; eviction is the only invalidation

A cached block is never refreshed, re-validated, or overwritten. This is what
makes wrong bytes at ingest permanent — which is exactly why ingest-side
validation (block-length geometry, §3.3 of docs/engine.md) is load-bearing.

- **Buys**: lock-free-ish read path, trivially correct concurrent readers,
  cheap accounting.
- **If violated**: nothing in DART violates it; the assumption constrains
  *future* features (no cache refresh, no TTL on entries).
- Relied on by: docs/engine.md §4, docs/store.md §3.

## Changing an assumption

Dropping or weakening one of these is a design change: say so explicitly in
the proposal, update this file, and audit the "relied on by" documents.
