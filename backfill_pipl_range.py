#!/usr/bin/env python3
import os, sys, subprocess
from datetime import datetime, timedelta

def parse(d):
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except ValueError:
        print("Dates must be YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

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
    # pipl_daily_reports.py reads CLI args, not env, so pass args
    print(f"=== Running PV backfill for {dstr} ===", flush=True)
    subprocess.run([sys.executable, "pipl_daily_reports.py", "--start", dstr, "--end", dstr], env=env, check=True)

def main():
    if len(sys.argv) != 3:
        print("Usage: backfill_pipl_range.py START_DATE END_DATE", file=sys.stderr)
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
