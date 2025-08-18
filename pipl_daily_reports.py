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
# Env config
# -------------------------------
SUPABASE_DB_URL   = os.getenv("SUPABASE_DB_URL")
PLUSVIBE_API_KEY  = os.getenv("PLUSVIBE_API_KEY")
REPORT_TZ         = os.getenv("REPORT_TZ", "America/New_York")
PV_VERIFY_SSL     = os.getenv("PV_VERIFY_SSL", "true").lower() not in ("0", "false", "no")  # toggle if their cert chain is broken

if not SUPABASE_DB_URL or not PLUSVIBE_API_KEY:
    logging.error("Missing required env vars: SUPABASE_DB_URL or PLUSVIBE_API_KEY")
    sys.exit(1)

HEADERS  = {"x-api-key": PLUSVIBE_API_KEY}
BASE_URL = "https://api.plusvibe.ai/api/v1"  # correct base

# -------------------------------
# Dates
# -------------------------------
def resolve_date_range(start_arg: str | None, end_arg: str | None) -> tuple[str, str]:
    tz = pytz.timezone(REPORT_TZ)
    today = datetime.now(tz).date()
    if start_arg and end_arg:
        return start_arg, end_arg
    yday = today - timedelta(days=1)
    return yday.isoformat(), yday.isoformat()

# -------------------------------
# DB
# -------------------------------
def upsert_rows(conn, rows: list[tuple]):
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
            parent_campaign_id = EXCLUDED.parent_campaign_id,
            campaign_name      = EXCLUDED.campaign_name,
            client_name        = EXCLUDED.client_name,
            status             = EXCLUDED.status,
            total_email_sent   = EXCLUDED.total_email_sent,
            new_leads_reached  = EXCLUDED.new_leads_reached,
            replies_count      = EXCLUDED.replies_count,
            positive_reply     = EXCLUDED.positive_reply,
            bounce_count       = EXCLUDED.bounce_count,
            sequencer_platform = EXCLUDED.sequencer_platform
        """
        execute_values(cur, sql, rows, page_size=500)
    conn.commit()

# -------------------------------
# PV API
# -------------------------------
def pv_get(path: str, params: dict | None = None) -> dict | list:
    url = f"{BASE_URL}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=60, verify=PV_VERIFY_SSL)
    if r.status_code != 200:
        raise RuntimeError(f"PV API error {r.status_code}: {r.text[:200]}")
    return r.json()

def get_workspaces() -> list[dict]:
    # returns: {"workspaces": [ {id, name, ...}, ... ]}
    data = pv_get("/authenticate")
    if isinstance(data, dict):
        return data.get("workspaces", [])
    return []

def list_campaigns(ws_id: str, skip: int = 0, limit: int = 100) -> list[dict]:
    data = pv_get("/campaign/list-all", {"workspace_id": ws_id, "skip": skip, "limit": limit})
    # API returns list of campaigns
    return data if isinstance(data, list) else []

def fetch_campaign_metrics(ws_id: str, cid: str, start_date: str, end_date: str | None) -> dict:
    params = {"workspace_id": ws_id, "campaign_id": cid, "start_date": start_date}
    if end_date:
        params["end_date"] = end_date
    data = pv_get("/analytics/campaign/stats", params)
    # API can return dict of counts or a dict keyed by metric
    # Normalize with .get defaults and cast to int
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
def main(start_date: str, end_date: str):
    logging.info(f"Fetching Pipl campaigns for {start_date} to {end_date}")

    workspaces = get_workspaces()
    if not workspaces:
        raise RuntimeError("No workspaces returned by PV. Check PLUSVIBE_API_KEY")

    rows = []
    processed = 0
    skipped = 0

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        for ws in workspaces:
            ws_id = str(ws.get("id") or ws.get("workspace_id") or "").strip()
            ws_name = ws.get("name") or "Unknown"
            if not ws_id:
                continue

            # paginate campaigns
            skip = 0
            batch = 100
            while True:
                camps = list_campaigns(ws_id, skip=skip, limit=batch)
                if not camps:
                    break

                for camp in camps:
                    try:
                        cid = str(camp.get("id") or camp.get("campaign_id") or "").strip()
                        if not cid:
                            continue

                        metrics = fetch_campaign_metrics(ws_id, cid, start_date, end_date)

                        # skip pure zero activity
                        if (
                            metrics["total_email_sent"] == 0
                            and metrics["replies_count"] == 0
                            and metrics["new_leads_reached"] == 0
                        ):
                            skipped += 1
                            continue

                        row = (
                            f"{start_date}_{cid}_{end_date}",  # campaign_date_key aligned with SL
                            cid,
                            camp.get("parent_campaign_id"),  # often None on PV
                            camp.get("name") or "",
                            ws_name,                          # direct mapping by workspace name
                            camp.get("status") or "",
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
                            logging.info(f"Processed {processed} PV campaigns so far (skipped {skipped})")

                    except Exception as e:
                        logging.error(f"PV campaign {camp.get('id')} failed: {e}")

                if len(camps) < batch:
                    break
                skip += batch

        if rows:
            upsert_rows(conn, rows)

    logging.info(f"Finished PV. Inserted or updated {processed}, skipped {skipped}")

# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="YYYY-MM-DD")
    parser.add_argument("--end", help="YYYY-MM-DD")
    args = parser.parse_args()

    start_date, end_date = resolve_date_range(args.start, args.end)
    try:
        main(start_date, end_date)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
