#!/usr/bin/env python3
import os, sys, subprocess
from datetime import datetime, timedelta

def parse(d):
    return datetime.strptime(d, "%Y-%m-%d").date()

def daterange(start, end):
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one

def run_day(dstr):
    env = os.environ.copy()
    env["START_DATE"] = dstr
    env["END_DATE"] = dstr
    print(f"=== Running for {dstr} ===", flush=True)
    # Call the existing script as-is
    subprocess.run([sys.executable, "sl_daily_reporting.py"], env=env, check=True)

def main():
    if len(sys.argv) != 3:
        print("Usage: backfill_range.py START_DATE END_DATE  (YYYY-MM-DD)", file=sys.stderr)
        sys.exit(2)
    sd = parse(sys.argv[1])
    ed = parse(sys.argv[2])
    if ed < sd:
        print("END_DATE must be >= START_DATE", file=sys.stderr)
        sys.exit(2)
    for d in daterange(sd, ed):
        run_day(d.isoformat())

if __name__ == "__main__":
    main()
