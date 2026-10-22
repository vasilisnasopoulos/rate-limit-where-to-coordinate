# Rate limits across regions: where coordination is needed

**A limit like "5,000 requests per hour per key", enforced in several regions at once, can be exact — zero overshoot — while
almost every request is decided locally. Only the last few requests of the window need the regions to talk, and no algorithm
can do with fewer.** Measured with the [Silence Theorem](https://github.com/vasilisnasopoulos/where-coordination-goes).

## The problem

Your API runs in Europe, the US and Asia. One API key may call all three at the same time. The limit is 5,000 per hour **in total**.

- **Each region counts alone** → each lets 5,000 through: up to 15,000.
- **Each region gets a fixed third (1,666)** → a customer who works mostly from Europe is refused while the other regions sit on unused budget.
- **Ask a central counter on every request** → exact, but every request waits for a round trip (~100–200 ms across continents), and the counter is a single point of failure.

## The rule

1. **Every second (in the background):** the regions exchange how many they admitted — **including admits still in flight** — and
   share out what is left: each region gets `remaining / N`.
2. **Between syncs:** a region admits **locally, without asking anyone**, while it has share left. Safe: the shares together never exceed what is left.
3. **When its share runs out:** if the limit is already reached → refuse locally (429). Otherwise **ask** the others, atomically:
   admitted so far + the others' unused shares < limit? Admit only if it fits.

While much of the window is left, shares are large and nobody asks. Questions happen only at the tail, when shares shrink to a few.

**Why no algorithm can do better:** here **b** is how many requests one region may admit on its own before it next hears the
others — in the rule above, its share between two syncs. If fewer than `N × b` requests are left (N regions, each able to admit up to `b` before it
hears the others), there is always a way for the regions, acting at once, to exceed the limit — so someone must ask. Above that, nobody needs to.

## Example (one hour, 3 regions, limit 5,000)

15,035 requests arrive — three times the limit — 80% in Europe, 10% each in the US and Asia. Sync every 1 s.
The timeline is approximate (from the arrival rate, ~4.2 requests/s in total); the second table is the measured result.

| minute | left | each region's share | what happens |
|---|---|---|---|
| 0 | 5,000 | 1,666 | Europe admits ~3/s from its share. Nobody asks. |
| 10 | ~2,500 | ~833 | Same. Nobody asks. |
| 19 | ~250 | ~83 | Same. Nobody asks. |
| 20 | 6 | 2 | Europe gets 3 in one second: 2 from its share, the 3rd **asks** — the others have unused share, it fits. |
| after | 0 | 0 | ~10,000 requests refused **locally** (429), no questions. |

| approach | admitted | over the limit | wrongly refused | requests that waited |
|---|---|---|---|---|
| each region alone | 8,060 | **+3,060** | 0 | 0 |
| fixed thirds | 4,726 | 0 | **10,309** | 0 |
| central counter | 5,000 | 0 | 0 | **15,035** (all) |
| **the rule** | **5,000** | **0** | **0** | **8** |

Sync period vs requests that waited (same traffic): 0.1 s → 0.01% · 1 s → 0.05% · 10 s → 0.4% · 60 s → 3.3%. Overshoot 0 in all.

## The bug to check in your own limiter

**Does your background sync count admits that are still in flight?** If it shares out the remaining budget from a counter that has
not seen them yet, the limit is exceeded — rarely, so tests will not show it. This was found by model checking with TLA+, with
reports that arrive late.

## Use it — Python + Redis

A working version of the rule is in [`python/`](python/): one file to copy, tests, and a demo you can run with your own
numbers. On a laptop, 3 regions, limit 5,000, traffic 80/10/10: **5,000 of 5,000 admitted, 99.4% decided locally**.

```bash
pip install redis
cd python && python demo.py --limit 5000 --traffic 80,10,10 --rate 300 --sync 1 --seconds 30
```

```python
limiter = EscrowLimiter(redis_client, region="eu-west", limit=5000, regions=3).start()
if not limiter.allow(api_key):
    return 429
```

## In code (pseudocode)

```python
LIMIT = 5000        # your limit
N_REGIONS = 3       # how many regions enforce it

share = 0           # my share since the last sync
used = 0            # how much of it I used
limit_reached = False

def sync():                                    # every second, in the background
    global share, used, limit_reached
    total = all_regions_admitted_count()       # INCLUDING admits still in flight
    remaining = max(LIMIT - total, 0)
    share = remaining // N_REGIONS
    used = 0
    limit_reached = (remaining == 0)

def on_request():
    global used
    if used < share:                           # local, no waiting
        used += 1
        return 200
    if limit_reached:                          # local refusal
        return 429
    if others_atomically_fit():                # rare: only at the tail of the window
        return 200
    return 429
```

You fill in `all_regions_admitted_count()` and `others_atomically_fit()` with what you already run (Redis `INCR`, etcd
compare-and-set, an `UPDATE ... WHERE count < LIMIT`). The question must be **atomic**: two regions asking at once must not both
get the last slot. Call `on_request()` where you check the limit today (API gateway middleware); run `sync()` on a 1 s timer.

The one line people get wrong is the question. "Admitted so far < LIMIT" is **not enough**: the other regions may
already hold unused share they are allowed to spend without asking. Count it:

```python
def others_atomically_fit():                   # one atomic step (Redis Lua script, etcd txn, SQL transaction)
    admitted = all_regions_admitted_count()    # including in flight
    held = sum(r.share - r.used for r in other_regions() if r.share > r.used)
    if admitted + held < LIMIT:
        record_admit()                         # inside the same atomic step
        return True
    return False
```

Without `held`, two regions can both spend their shares and the question still says yes — overshoot.
This is the second bug the model checker found.

**How to see it works:** log how many requests took the local path and how many asked. Admitted should equal the limit exactly,
and almost all should be local.

## What is not new

The mechanism — sharing out what is left — is **escrow / demarcation** (O'Neil 1986; Barbará & Garcia-Molina 1994). Lease-based
limiters with bounded overshoot are industry practice. What is added here is the lower bound: **which** requests must wait, and
**how many** regions must be heard, with proof that fewer is impossible.

## Limits

- The numbers come from a simulation (one seed, instant sync). The rule's safety and the lower bound were checked with TLA+ and Z3;
  delayed reports were model-checked for small sizes only.
- "GitHub-style" is an example: GitHub does not say how it implements its limit. This is not a measurement of GitHub.
- Lost messages were not modelled.
- Where the limit is plain protection and "about 5,000" is fine, what you have is enough. The rule matters where the limit is a
  promise or a cost: budgets, subscriptions, per-call billing, LLM tokens.

Questions or want it run on your numbers: open an issue or DM me.

---
© 2026 Vasilis Nasopoulos — text [CC BY 4.0](LICENSE) · code in `python/` [MIT](python/LICENSE)
