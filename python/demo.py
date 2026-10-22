"""Try it with your own numbers. Starts a throwaway redis-server, runs N regions, prints every second.

    python demo.py --limit 5000 --traffic 80,10,10 --rate 300 --sync 1 --seconds 30
"""
import argparse
import random
import shutil
import socket
import subprocess
import tempfile
import threading
import time

import redis

from escrow_ratelimit import EscrowLimiter

ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--limit", type=int, default=5000, help="requests per window, all regions together (5000)")
ap.add_argument("--traffic", default="80,10,10", help="share of traffic per region, also sets how many regions (80,10,10)")
ap.add_argument("--rate", type=float, default=300, help="requests per second, all regions together (300)")
ap.add_argument("--sync", type=float, default=1.0, help="seconds between syncs (1)")
ap.add_argument("--seconds", type=int, default=30, help="how long to run (30)")
ap.add_argument("--window", type=int, default=3600, help="window length in seconds (3600)")
a = ap.parse_args()

weights = [float(x) for x in a.traffic.split(",")]
weights = [w / sum(weights) for w in weights]
n = len(weights)

if not shutil.which("redis-server"):
    raise SystemExit("needs redis-server on PATH (macOS: brew install redis)")
s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
srv = subprocess.Popen(["redis-server", "--port", str(port), "--save", "", "--dir", tempfile.mkdtemp()],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
r = redis.Redis(port=port)
for _ in range(100):
    try:
        r.ping(); break
    except redis.ConnectionError:
        time.sleep(0.05)

lims = [EscrowLimiter(r, "region%d" % i, a.limit, n, window_seconds=a.window, sync_interval=a.sync).start()
        for i in range(n)]
admitted = [0] * n
sent = [0] * n
lock = threading.Lock()
stop = threading.Event()


def region(i):
    rnd = random.Random(i)
    rate = a.rate * weights[i]
    while not stop.is_set() and rate > 0:
        time.sleep(rnd.expovariate(rate))      # Poisson arrivals
        ok = lims[i].allow("demo")
        with lock:
            sent[i] += 1
            admitted[i] += ok


ts = [threading.Thread(target=region, args=(i,), daemon=True) for i in range(n)]
for t in ts: t.start()
print("limit %d, %d regions, traffic %s, %.0f req/s, sync every %gs\n"
      % (a.limit, n, a.traffic, a.rate, a.sync))
print("  sec    sent  admitted   local   asked  refused   per region")
try:
    for sec in range(1, a.seconds + 1):
        time.sleep(1)
        with lock:
            st = [x.stats.copy() for x in lims]
            print("%5d %7d %9d %7d %7d %8d   %s" % (
                sec, sum(sent), sum(admitted), sum(x["local"] for x in st), sum(x["asked"] for x in st),
                sum(x["refused"] for x in st), admitted))
finally:
    stop.set()
    for x in lims: x.stop()
    srv.terminate()

tot = sum(admitted); local = sum(x.stats["local"] for x in lims); asked = sum(x.stats["asked"] for x in lims)
print("\nadmitted %d of limit %d (%s)" % (tot, a.limit, "OVER" if tot > a.limit else "never over"))
if tot:
    print("decided locally %.1f%%, asked %.1f%%" % (100.0 * local / tot, 100.0 * asked / tot))
