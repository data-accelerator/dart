# Security Policy

## Reporting a vulnerability

**Please do not report security issues in public GitHub issues, pull requests, or
discussions.**

Use GitHub's private vulnerability reporting instead: go to the
[Security tab](https://github.com/data-accelerator/dart/security) of this
repository and choose **Report a vulnerability**. That opens a private channel
visible only to the maintainers.

Please include, as far as you can determine it:

- affected version or commit;
- the configuration involved (node flags, deployment shape);
- what an attacker can achieve, and the position they need to be in to do it;
- reproduction steps or a proof of concept.

We will acknowledge your report and keep you informed while we investigate. Once
a fix is available we will publish an advisory and credit you, unless you prefer
otherwise.

When writing a report, note that an upstream URL's **query string can itself be a
credential** (a presigned object-storage link). Please redact it.

## Supported versions

DART is pre-1.0 and not yet production-verified. Security fixes are applied to
the latest release and to `main`; older tags are not maintained.

## Trust model — please read before deploying

Several of DART's design decisions assume a trusted network. They are deliberate
trade-offs, not oversights, but they determine how you must deploy it:

- **The peer plane is plaintext HTTP/1.1 and unauthenticated.** Any host that can
  reach a node's peer listener can request blocks from it, and traffic between
  nodes is not encrypted. Encryption and isolation are delegated to the cluster
  network layer (CNI / service mesh / network policy). Do not expose the peer
  listener outside the cluster.
- **The client plane is unauthenticated.** A node serves whatever its clients ask
  for. Bind it to the local node or an internal network, not to the public
  internet.
- **The admin and metrics listener is not authenticated** and exposes cluster
  membership and cache diagnostics. Keep it on an internal address; it is a
  separate listener (`-admin`) precisely so it can be firewalled independently.
- **A credential authorizes the first fetch, not later reads.** DART is a cache,
  so an upstream credential (a presigned URL, a registry token) is verified by the
  origin only when a block is actually fetched. Once cached, that block is served
  to any client that asks for the same object — including one presenting an expired
  signature, or none at all. Concretely: a client who can reach a DART node can
  read anything the cluster has already pulled, without holding a valid credential
  for it. Deploy accordingly, and treat a node's cache as readable by everything
  that can reach its client plane.
- **A relay fetches on behalf of a requester using the requester's own upstream
  URL and credential.** A node in the cluster can therefore ask a peer to fetch a
  URL that peer would not otherwise fetch. This is what makes fetch-on-behalf
  work; it is another reason the peer plane must stay inside a trust boundary.

## What is not a vulnerability

- **Cache misses, evictions, or a cold cache after restart.** DART is a read-only
  cache: the origin is the source of truth, so these cost a re-fetch and cannot
  lose or corrupt data.
- **A node disagreeing with its peers about membership.** By design this can only
  produce a suboptimal route (an extra hop, or a fetch from origin), never a
  wrong byte.
- **Two DART instances pointed at the same `-cache-dir`.** This is a
  misconfiguration and is rejected at startup by an exclusive lock on the cache
  directory.
