# escrow_ratelimit — the rule as a Python library

**An exact rate limit across regions, decided locally almost always.** "5,000 requests per hour per key", enforced in
several regions at once: never one over, and most requests never wait for another region.

The rule and why it works: [the README one level up](../README.md). This is that rule as a small Python library on Redis.

## Use

```bash
pip install redis
# copy python/escrow_ratelimit.py into your project
```

```python
import redis
from escrow_ratelimit import EscrowLimiter

r = redis.Redis(host="your-shared-redis")
limiter = EscrowLimiter(r, region="eu-west", limit=5000, regions=3).start()   # one per region

def handle(request):
    if not limiter.allow(request.api_key):
        return 429
    ...
```

Run the same code in every region with its own `region` name, all pointing at one Redis.
`regions` is how many regions enforce the limit. On shutdown, `limiter.stop()` hands back unused share.

## What it does

- Every second (in the background) each region reports what it used and takes a share of what is left.
- Between syncs it admits from its share **without asking anyone**.
- When its share runs out it takes a new share at once, in one atomic step. Near the end of the window, when less than
  one share per region is left, it asks for the single request: admitted + **every share the regions still hold** < limit?

A share stays counted until the region reports, so admits still in flight are never shared out twice, and the question
counts the others' unused share. These are the two mistakes a periodic counter sync usually makes; the tests include the
second one on purpose, to show it breaks the limit.

## Check it yourself

```bash
pip install redis pytest
cd python && pytest -q -s          # starts a throwaway redis-server; needs redis-server on PATH
```

On a laptop (3 regions, limit 2,000, traffic 80/10/10, sync every 20 ms, requests far faster than in real life):
admitted exactly 2,000, none after the limit, ~99% decided locally. With the question that ignores the others' shares,
the same kind of test goes over: 303–313 admitted for a limit of 300.

With your own numbers (starts its own throwaway Redis):

```bash
cd python && python demo.py --limit 5000 --traffic 80,10,10 --rate 300 --sync 1 --seconds 30
```

It prints every second how many were sent, admitted, decided locally, asked and refused. On a laptop, the line above
gave 5,000 of 5,000, 99.4% decided locally.

`limiter.stats` counts `local`, `asked` and `refused` — log them: admitted should equal the limit and most should be local.

## Limits

- Fixed windows (default one hour); no sliding window.
- One Redis that all regions reach. Asking and syncing go there; local admits do not. If Redis is unreachable a region
  keeps its current share and refuses once it is used — the limit is never exceeded, some requests are refused.
- How many requests reach Redis depends on traffic: a busy region takes new shares more often, and the last few
  requests of the window ask one by one.
- Equal shares for every region. Shares weighted by traffic are also safe (as long as they add up to no more than what
  is left) but are not in this version.

## What is not new

Sharing out what is left is **escrow / demarcation** (O'Neil 1986; Barbará & Garcia-Molina 1994). How many regions must
be heard, and that fewer is impossible: [where-coordination-goes](https://github.com/vasilisnasopoulos/where-coordination-goes).

Questions, or want it for your system: open an issue or DM me.

---
© 2026 Vasilis Nasopoulos. All rights reserved. No permission is granted to use, copy, modify, distribute, sublicense, or sell this material, including source code or binaries, without prior written permission. See the repository [LICENSE](../LICENSE).
