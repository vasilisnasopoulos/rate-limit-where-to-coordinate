"""Exact rate limit across regions, decided locally almost always.

Each region holds a share of what is left in the window and admits from it without asking anyone.
Every `sync_interval` it reports what it used and takes a new share of what is left. When its share is
gone it does the same at once, atomically; when less than one share per region is left, it asks for the single
request: admitted + every share still held by the regions < limit?

All counts live in one Redis hash per key and window:
    committed   admits already reported (and admits granted by asking)
    reserved    sum of the shares the regions currently hold, used or not
Invariant: real admits <= committed + reserved <= limit. A share stays in `reserved` until the region
reports, so admits "in flight" are never shared out twice, and the question counts the others' unused share.
"""
import threading
import time

__all__ = ["EscrowLimiter"]
__version__ = "0.1.0"

# KEYS[1] window hash. ARGV: region, used since last sync, limit, regions, ttl seconds, release (1 = take no new share).
_SYNC = """
local h = KEYS[1]
local field = 'share:' .. ARGV[1]
local used = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local old = tonumber(redis.call('HGET', h, field) or '0')
local committed = tonumber(redis.call('HINCRBY', h, 'committed', used))
local reserved = tonumber(redis.call('HINCRBY', h, 'reserved', -old))
local free = limit - committed - reserved
local new = 0
if free > 0 and ARGV[6] ~= '1' then new = math.floor(free / n) end
redis.call('HSET', h, field, new)
redis.call('HINCRBY', h, 'reserved', new)
redis.call('EXPIRE', h, tonumber(ARGV[5]))
return {new, free - new}
"""

# KEYS[1] window hash. ARGV: limit, ttl seconds. Returns 1 if admitted.
_ASK = """
local h = KEYS[1]
local committed = tonumber(redis.call('HGET', h, 'committed') or '0')
local reserved = tonumber(redis.call('HGET', h, 'reserved') or '0')
if committed + reserved < tonumber(ARGV[1]) then
  redis.call('HINCRBY', h, 'committed', 1)
  redis.call('EXPIRE', h, tonumber(ARGV[2]))
  return 1
end
return 0
"""


class _State:
    __slots__ = ("window", "share", "used", "full")

    def __init__(self, window):
        self.window = window
        self.share = 0      # my share since the last sync
        self.used = 0       # how much of it I used
        self.full = False   # an ask was refused: refuse locally until the next sync


class EscrowLimiter:
    """One instance per region. `allow(key)` returns True (admit) or False (refuse, e.g. HTTP 429).

    limit           requests per window per key, across all regions
    regions         how many regions enforce the limit
    region          this region's name (unique per region)
    window_seconds  fixed window length (default one hour)
    sync_interval   seconds between background syncs (default 1)
    """

    def __init__(self, redis_client, region, limit, regions, window_seconds=3600, sync_interval=1.0,
                 prefix="escrow", clock=time.time):
        if limit < 0 or regions < 1 or window_seconds <= 0:
            raise ValueError("limit >= 0, regions >= 1, window_seconds > 0")
        self.r = redis_client
        self.region = str(region)
        self.limit = int(limit)
        self.regions = int(regions)
        self.window_seconds = window_seconds
        self.sync_interval = sync_interval
        self.prefix = prefix
        self.clock = clock
        self._sync = redis_client.register_script(_SYNC)
        self._ask = redis_client.register_script(_ASK)
        self._lock = threading.Lock()
        self._keys = {}
        self._thread = None
        self._stop = threading.Event()
        self.stats = {"local": 0, "asked": 0, "refused": 0}

    # ---- public -------------------------------------------------------------------------------------
    def allow(self, key="default"):
        window = self._window()
        with self._lock:
            st = self._state(key, window)
            if st.used < st.share:                  # local, no waiting
                st.used += 1
                self.stats["local"] += 1
                return True
            if st.full:                             # local refusal until the next sync
                self.stats["refused"] += 1
                return False
            h = self._hash(key, window)
            try:
                # share used up: report it and take a new share of what is left, in one atomic step
                new, _ = self._sync(keys=[h], args=[self.region, st.used, self.limit, self.regions, self._ttl(), 0])
                st.share, st.used = int(new), 0
                if st.share > 0:
                    st.used = 1
                    self.stats["asked"] += 1
                    return True
                # less than one share per region is left: ask for this one request
                ok = self._ask(keys=[h], args=[self.limit, self._ttl()]) == 1
            except Exception:                       # Redis unreachable: refuse, never exceed
                ok = False
            if ok:
                self.stats["asked"] += 1
            else:
                st.full = True
                self.stats["refused"] += 1
            return ok

    def sync(self, release=False):
        """Report what each key used and take a new share (none with release=True). Run by the background thread."""
        window = self._window()
        with self._lock:
            for key in list(self._keys):
                st = self._keys[key]
                if st.window != window:             # window ended: start fresh
                    st = self._keys[key] = _State(window)
                new, _ = self._sync(keys=[self._hash(key, window)],
                                    args=[self.region, st.used, self.limit, self.regions, self._ttl(),
                                          1 if release else 0])
                st.share, st.used, st.full = int(new), 0, False

    def start(self):
        if self._thread is None:
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, name="escrow-sync", daemon=True)
            self._thread.start()
        return self

    def stop(self):
        if self._thread is not None:
            self._stop.set()
            self._thread.join()
            self._thread = None
        self.sync(release=True)                     # hand back unused share

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()

    # ---- internals ----------------------------------------------------------------------------------
    def _loop(self):
        while not self._stop.wait(self.sync_interval):
            try:
                self.sync()
            except Exception:                       # Redis unreachable: keep the current share, retry
                pass

    def _state(self, key, window):
        st = self._keys.get(key)
        if st is None or st.window != window:
            st = self._keys[key] = _State(window)   # no share yet: the first requests ask
        return st

    def _window(self):
        return int(self.clock() // self.window_seconds)

    def _hash(self, key, window):
        return "%s:{%s}:%d" % (self.prefix, key, window)

    def _ttl(self):
        return int(self.window_seconds * 2) + 60
