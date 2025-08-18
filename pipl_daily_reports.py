#!/usr/bin/env python3
import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
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
PLUSVIBE_API_KEY  = os.getenv("PLUSVIBE_API_KEY", "").strip()
REPORT_TZ         = os.getenv("REPORT_TZ", "America/New_York")
PV_VERIFY_SSL     = os.getenv("PV_VERIFY_SSL", "true").strip().lower() not in ("0", "false", "no")
PV_BASE_URL       = os.getenv("PV_BASE_URL", "https://api.plusvibe.ai/api/v1").rstrip("/")

if not SUPABASE_DB_URL or not PLUSVIBE_API_KEY:
    logging.error("Missing required env vars: SUPABASE_DB_URL or PLUSVIBE_API_KEY")
    sys.exit(1)

# try both header conventions
HEADERS_CANDIDATES = [
    {"x-api-key": PLUSVIBE_API_KEY},
    {"api-key": PLUSVIBE_API_KEY},
]

# candidate endpoints by tenant
WORKSPACES_PATHS = ["/authenticate", "/workspaces"]
CAMPAIGNS_PATH   = "/campaign/list-all"  # observed working
STATS_PATH       = "/analytics/campaign/stats"  # observed working

# -------------------------------
# Dates
# -------------------------------
def resolve_date_range(start_arg: Optional[str], end_arg: Optional[str]) -> Tuple[str, str]:
    tz = pytz.timezone(REPORT_TZ)
    today = datetime.now(tz).date()
    if start_arg and end_arg:
        return start_arg, end_arg
    yday = today - timedelta(days=1)
    return yday.isoformat(), yday.isoformat()

# -------------------------------
# DB
# -------------------------------
def upsert_rows(conn, rows: List[tuple]):
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
# HTTP helpers
# -------------------------------
def _get_json(path: str, params: Dict = None) -> Dict:
    url = f"{PV_BASE_URL}{path}"
    last_err = None
    for hdr in HEADERS_CANDIDATES:
        try:
            r = requests.get(url, headers=hdr, params=params or {}, timeout=60, verify=PV_VERIFY_SSL)
            if r.status_code != 200:
                last_err = RuntimeError(f"{r.status_code} {r.text[:200]}")
                continue
            logging.debug(f"GET {path} ok with headers {list(hdr.keys())[0]}")
            return r.json()
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"GET {path} failed. last_err={last_err}")

# -------------------------------
# PV API with fallbacks
# -------------------------------
def get_workspaces() -> List[Dict]:
    for p in WORKSPACES_PATHS:
        try:
            data = _get_json(p)
            # shapes seen: {"workspaces":[{id,name,...},...]} or list of workspaces
            if isinstance(data, dict) and "workspaces" in data and isinstance(data["workspaces"], list):
                logging.info(f"Workspaces from {p}: {len(data['workspaces'])}")
                return data["workspaces"]
            if isinstance(data, list):
                logging.info(f"Workspaces from {p}: {len(data)}")
                return data
        except Exception as e:
            logging.warning(f"Workspace fetch via {p} failed: {e}")
    return []

def list_campaigns(ws_id: str, skip: int = 0, limit: int = 100) -> List[Dict]:
    params = {"workspace_id": ws_id, "skip": skip, "limit": limit}
    data = _get_json(CAMPAIGNS_PATH, params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # tolerate shapes like {"campaigns":[...]} if tenant returns wrapped lists
        if isinstance(data.get("campaigns"), list):
            return data["campaigns"]
    return []

def fetch_campaign_metrics(ws_id: str, cid: str, start_date: str, end_date: Optional[str]) -> Dict[str, int]:
    params = {"workspace_id": ws_id, "campaign_id": cid, "start_date": start_date}
    if end_date:
        params["end_date"] = end_date
    data = _get_json(STATS_PATH, params)
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
    if not PV_VERIFY_SSL:
        logging.warning("PV_VERIFY_SSL=false. TLS verification is disabled")

    workspaces = get_workspaces()
    logging.info(f"Total workspaces discovered: {len(workspaces)}")
    if not workspaces:
        raise RuntimeError("PV returned zero workspaces. Check API key, base URL, or header name")

    rows: List[tuple] = []
    processed = 0
    skipped = 0
    total_campaigns_seen = 0

    with psycopg2.connect(SUPABASE_DB_URL) as conn:
        for ws in workspaces:
            ws_id = str(ws.get("id") or ws.get("workspace_id") or "").strip()
            ws_name = ws.get("name") or ws.get("workspace_name") or "Unknown"
            if not ws_id:
                logging.warning(f"Skipping workspace without id: {ws}")
                continue

            skip = 0
            batch = 100
            while True:
                try:
                    camps = list_campaigns(ws_id, skip=skip, limit=batch)
                except Exception as e:
                    logging.error(f"Failed listing campaigns for workspace {ws_id}: {e}")
                    break

                count = len(camps)
                total_campaigns_seen += count
                logging.info(f"Workspace {ws_name} ({ws_id}) campaigns batch size={count} skip={skip}")

                if count == 0:
                    break

                for camp in camps:
                    try:
                        cid = str(camp.get("id") or camp.get("campaign_id") or "").strip()
                        if not cid:
                            continue

                        metrics = fetch_campaign_metrics(ws_id, cid, start_date, end_date)

                        if (
                            metrics["total_email_sent"] == 0
                            and metrics["replies_count"] == 0
                            and metrics["new_leads_reached"] == 0
                        ):
                            skipped += 1
                            continue

                        row = (
                            f"{start_date}_{cid}_{end_date}",
                            cid,
                            camp.get("parent_campaign_id"),
                            camp.get("name") or "",
                            ws_name,
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

                if count < batch:
                    break
                skip += batch

        if rows:
            upsert_rows(conn, rows)

    logging.info(f"Finished PV. Workspaces={len(workspaces)}, campaigns_seen={total_campaigns_seen}, inserted_or_updated={processed}, skipped={skipped}")

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
