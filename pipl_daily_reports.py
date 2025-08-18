#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values
import pytz

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(threadName)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# -------------------------------
# Env vars
# -------------------------------
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL")
PLUSVIBE_API_KEY = os.environ.get("PLUSVIBE_API_KEY")
REPORT_TZ = os.environ.get("REPORT_TZ", "America/New_York")

if not SUPABASE_DB_URL or not PLUSVIBE_API_KEY:
    logging.error("Missing required env vars: SUPABASE_DB_URL or PLUSVIBE_API_KEY")
    sys.exit(1)

HEADERS = {"api-key": PLUSVIBE_API_KEY}
BASE_URL = "https://piplapi.plusvibe.ai/api/v1/report"

# -------------------------------
# Date range
# -------------------------------
def resolve_date_range(start_arg, end_arg):
    tz = pytz.timezone(REPORT_TZ)
    today = datetime.now(tz).date()
    if start_arg and end_arg:
        return start_arg, end_arg
    yday = today - timedelta(days=1)
    return yday.isoformat(), yday.isoformat()

# -------------------------------
# DB helpers
# -------------------------------
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
# API
# -------------------------------
def fetch_campaigns():
    url = f"{BASE_URL}/campaigns"
    resp = requests.get(url, headers=HEADERS, timeout=60, verify=False)  # disable SSL check if cert issue
    if resp.status_code != 200:
        raise RuntimeError(f"PV API error {resp.status_code}: {resp.text[:200]}")
    data = resp.json()
    return data.get("campaigns", [])

def fetch_campaign_metrics(cid, start_date, end_date):
    url = f"{BASE_URL}/campaigns/{cid}/stats"
    params = {"start_date": start_date, "end_date": end_date}
    resp = requests.get(url, headers=HEADERS, params=params, timeout=60, verify=False)
    if resp.status_code != 200:
        raise RuntimeError(f"PV metrics error {resp.status_code} for {cid}: {resp.text[:200]}")
    data = resp.json()
    return {
        "total_email_sent": int(data.get("sent", 0)),
        "new_leads_reached": int(data.get("new_leads", 0)),
        "replies_count": int(data.get("replies", 0)),
        "positive_reply": int(data.get("positive", 0)),
        "bounce_count": int(data.get("bounces", 0)),
    }

# -------------------------------
# Main
# -------------------------------
def main(start_date, end_date):
    logging.info(f"Fetching Pipl campaigns for {start_date} → {end_date}")
    campaigns = fetch_campaigns()
    logging.info(f"Fetched {len(campaigns)} campaigns")

    rows, processed, skipped = [], 0, 0
    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        for c in campaigns:
            try:
                cid = str(c.get("id") or "").strip()
                if not cid:
                    continue
                metrics = fetch_campaign_metrics(cid, start_date, end_date)

                if (
                    metrics["total_email_sent"] <= 0
                    and metrics["replies_count"] <= 0
                    and metrics["new_leads_reached"] <= 0
                ):
                    skipped += 1
                    continue

                row = (
                    f"{start_date}_{cid}_{end_date}",
                    cid,
                    c.get("parent_campaign_id"),
                    c.get("name") or "",
                    c.get("ws_name") or "Unknown",
                    c.get("status") or "",
                    start_date,
                    end_date,
                    metrics["total_email_sent"],
                    metrics["new_leads_reached"],
                    metrics["replies_count"],
                    metrics["positive_reply"],
                    metrics["bounce_count"],
                    "PV",
                )
                rows.append(row)
                processed += 1

                if processed % 100 == 0:
                    logging.info(f"Processed {processed} campaigns (skipped {skipped})")

            except Exception as e:
                logging.error(f"Campaign {c.get('id')} failed: {e}")

        if rows:
            upsert_rows(conn, rows)

    logging.info(f"Finished. Inserted/updated {processed}, skipped {skipped}")

# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    start_date, end_date = resolve_date_range(args.start, args.end)
    try:
        main(start_date, end_date)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
