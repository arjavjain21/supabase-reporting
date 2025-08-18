#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
import pytz

# -------------------------------
# Logging setup
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# -------------------------------
# Config from environment
# -------------------------------
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL")
PLUSVIBE_API_KEY = os.environ.get("PLUSVIBE_API_KEY")
REPORT_TZ = os.environ.get("REPORT_TZ", "America/New_York")  # default: EDT
WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "8"))

if not SUPABASE_DB_URL or not PLUSVIBE_API_KEY:
    logging.error("Missing required env vars: SUPABASE_DB_URL or PLUSVIBE_API_KEY")
    sys.exit(1)

# -------------------------------
# Date utilities
# -------------------------------
def resolve_date_range(start_arg: str, end_arg: str):
    tz = pytz.timezone(REPORT_TZ)
    today = datetime.now(tz).date()
    if start_arg and end_arg:
        return start_arg, end_arg
    # default: yesterday
    yday = today - timedelta(days=1)
    return yday.isoformat(), yday.isoformat()

# -------------------------------
# Database setup
# -------------------------------
def init_pool():
    return ThreadedConnectionPool(1, max(WORKER_THREADS, 8), SUPABASE_DB_URL)

def upsert_rows(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        sql = """
        INSERT INTO campaign_reporting (
            campaign_date_key, campaign_id, parent_campaign_id, campaign_name,
            client_name, status, start_date, end_date,
            total_email_sent, new_leads_reached, replies_count,
            positive_reply, bounce_count, sequencer_platform
        )
        VALUES %s
        ON CONFLICT (campaign_date_key) DO UPDATE SET
            parent_campaign_id = excluded.parent_campaign_id,
            campaign_name = excluded.campaign_name,
            client_name = excluded.client_name,
            status = excluded.status,
            total_email_sent = excluded.total_email_sent,
            new_leads_reached = excluded.new_leads_reached,
            replies_count = excluded.replies_count,
            positive_reply = excluded.positive_reply,
            bounce_count = excluded.bounce_count,
            sequencer_platform = excluded.sequencer_platform
        """
        execute_values(cur, sql, rows, page_size=500)
    conn.commit()

# -------------------------------
# API calls
# -------------------------------
def pv_get(path, params=None):
    base_url = "https://api.plusvibe.ai/api/v1"
    headers = {"api-key": PLUSVIBE_API_KEY}
    url = f"{base_url}{path}"
    resp = requests.get(url, headers=headers, params=params, timeout=60, verify=False)
    if resp.status_code != 200:
        raise RuntimeError(f"PV API error {resp.status_code}: {resp.text[:200]}")
    return resp.json()

def fetch_campaigns():
    data = pv_get("/campaigns")
    if isinstance(data, dict) and "campaigns" in data:
        return data["campaigns"]
    if isinstance(data, list):
        return data
    return []

def fetch_campaign_metrics(cid: str, start_date: str, end_date: str):
    data = pv_get(f"/campaigns/{cid}/metrics", {"start_date": start_date, "end_date": end_date})
    return {
        "total_email_sent": int(data.get("sent", 0)),
        "new_leads_reached": int(data.get("new_leads", 0)),
        "replies_count": int(data.get("replies", 0)),
        "positive_reply": int(data.get("positive", 0)),
        "bounce_count": int(data.get("bounces", 0)),
    }

# -------------------------------
# Main processing
# -------------------------------
def main(start_date: str, end_date: str):
    logging.info(f"Fetching Pipl campaigns for {start_date} → {end_date}")
    campaigns = fetch_campaigns()
    logging.info(f"/campaigns returned {len(campaigns)} campaigns")

    pool = init_pool()
    rows = []
    processed = 0
    skipped = 0

    for c in campaigns:
        try:
            cid = str(c.get("id") or c.get("campaign_id") or "").strip()
            if not cid:
                continue
            metrics = fetch_campaign_metrics(cid, start_date, end_date)

            # skip zero-activity
            if (
                metrics["total_email_sent"] <= 0
                and metrics["replies_count"] <= 0
                and metrics["new_leads_reached"] <= 0
            ):
                skipped += 1
                logging.debug(f"Skipping campaign {cid} (no activity)")
                continue

            row = (
                f"{start_date}_{cid}_{end_date}",  # campaign_date_key
                cid,
                c.get("parent_campaign_id") or None,
                c.get("name") or "",
                c.get("ws_name") or "Unknown",  # direct client name
                c.get("status") or "",
                start_date,
                end_date,
                metrics["total_email_sent"],
                metrics["new_leads_reached"],
                metrics["replies_count"],
                metrics["positive_reply"],
                metrics["bounce_count"],
                "PV",  # sequencer_platform
            )
            rows.append(row)
            processed += 1

            if processed % 100 == 0:
                logging.info(f"Processed {processed} campaigns (skipped {skipped})")

        except Exception as e:
            logging.error(f"Campaign {c.get('id')} failed: {e}")

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        upsert_rows(conn, rows)

    logging.info(f"Finished. Inserted/updated {processed} campaigns, skipped {skipped}")

# -------------------------------
# CLI entrypoint
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    start_date, end_date = resolve_date_range(args.start, args.end)
    try:
        main(start_date, end_date)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
