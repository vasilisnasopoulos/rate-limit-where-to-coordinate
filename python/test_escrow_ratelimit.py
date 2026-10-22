"""Run: pytest -q   (starts a throwaway redis-server on a free port; needs redis-server on PATH)."""
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import pytest
import redis

import escrow_ratelimit
from escrow_ratelimit import EscrowLimiter


@pytest.fixture(scope="module")
def r():
    if not shutil.which("redis-server"):
        pytest.skip("redis-server not installed")
    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    d = tempfile.mkdtemp()
    p = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--dir", d],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    c = redis.Redis(port=port)
    for _ in range(100):
        try:
            c.ping(); break
        except redis.ConnectionError:
            time.sleep(0.05)
    yield c
    p.terminate(); p.wait()


def drive(limiters, weights, key, stop_when, deadline_s=20.0, threads_per_region=4):
    """Send requests from every region (traffic split by weights) until stop_when() or the deadline.
    Returns admits per region, and the largest committed + reserved seen in Redis."""
    admitted = [0] * len(limiters)
    peak = [0]
    lock = threading.Lock()
    end = time.time() + deadline_s
    done = threading.Event()

    def worker(i):
        rnd = random.Random(i * 7919 + threading.get_ident())
        while not done.is_set() and time.time() < end:
            if limiters[i].allow(key):
                with lock:
                    admitted[i] += 1
            time.sleep(rnd.random() * 0.002 / weights[i])

    def watcher():
        while not done.is_set() and time.time() < end:
            for h in limiters[0].r.scan_iter("escrow:{%s}:*" % key):
                v = limiters[0].r.hmget(h, "committed", "reserved")
                tot = sum(int(x or 0) for x in v)
                peak[0] = max(peak[0], tot)
            if stop_when(admitted):
                done.set()
            time.sleep(0.005)

    ts = [threading.Thread(target=worker, args=(i,)) for i, _ in enumerate(limiters) for _ in range(threads_per_region)]
    ts.append(threading.Thread(target=watcher))
    for t in ts: t.start()
    for t in ts: t.join()
    return admitted, peak[0]


def regions(r, n, limit, sync=0.02, **kw):
    return [EscrowLimiter(r, "region%d" % i, limit, n, sync_interval=sync, **kw).start() for i in range(n)]


def test_exact_limit_mostly_local(r):
    L = 2000
    lims = regions(r, 3, L)
    try:
        admitted, peak = drive(lims, [0.8, 0.1, 0.1], "k1", lambda a: sum(a) >= L)
        # keep pushing after the limit: nothing more gets in
        extra, _ = drive(lims, [0.8, 0.1, 0.1], "k1", lambda a: False, deadline_s=1.0)
    finally:
        for x in lims: x.stop()
    local = sum(x.stats["local"] for x in lims)
    asked = sum(x.stats["asked"] for x in lims)
    assert sum(admitted) == L, admitted
    assert sum(extra) == 0, extra
    assert peak <= L
    assert local + asked == L
    assert local / L > 0.8, (local, asked)   # load-dependent: fast traffic vs a 20 ms sync
    print("\nexact: admitted %s, local %d, asked %d, peak committed+reserved %d"
          % (admitted, local, asked, peak))


def test_race_at_the_tail(r):
    L = 60
    lims = regions(r, 3, L, sync=0.005)
    try:
        admitted, peak = drive(lims, [1, 1, 1], "k2", lambda a: False, deadline_s=1.5, threads_per_region=10)
    finally:
        for x in lims: x.stop()
    assert sum(admitted) == L, admitted
    assert peak <= L


def test_window_rolls_over(r):
    now = [1000.0]
    lims = [EscrowLimiter(r, "region%d" % i, 10, 2, window_seconds=60, clock=lambda: now[0]) for i in range(2)]
    a = sum(lims[i % 2].allow("k3") for i in range(40))
    for x in lims: x.sync()
    a += sum(lims[i % 2].allow("k3") for i in range(40))
    assert a == 10
    now[0] += 60
    for x in lims: x.sync()
    b = sum(lims[i % 2].allow("k3") for i in range(40))
    assert b == 10


def test_without_held_the_limit_breaks(r, monkeypatch):
    """The bug the README warns about: the question checks only `committed < limit` and ignores
    shares the other regions still hold. This test shows the check above would catch it."""
    bad = escrow_ratelimit._ASK.replace("committed + reserved <", "committed <")
    monkeypatch.setattr(escrow_ratelimit, "_ASK", bad)
    L = 300
    lims = regions(r, 3, L)
    try:
        admitted, _ = drive(lims, [0.8, 0.1, 0.1], "k4", lambda a: False, deadline_s=3.0)
    finally:
        for x in lims: x.stop()
    assert sum(admitted) > L, admitted
    print("\nwithout held: admitted %d over a limit of %d" % (sum(admitted), L))
