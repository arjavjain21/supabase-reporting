#!/usr/bin/env python3
import sys, os, logging
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import SSLError
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor
import certifi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipl_supabase_reporting.log", encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

if os.name == "nt":
    os.environ["PYTHONIOENCODING"] = "utf-8"

def _pick_ca_bundle_path() -> str:
    # Priority: explicit override, common envs, repo-pinned bundle, certifi
    candidates = [
        os.environ.get("PV_CA_BUNDLE"),
        os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("CURL_CA_BUNDLE"),
        os.path.join(os.path.dirname(__file__), "certs", "custom-ca.pem"),
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return certifi.where()

def _build_session() -> requests.Session:
    s = requests.Session()
    insecure = os.getenv("PV_INSECURE", "").strip() == "1"
    if insecure:
        logger.warning("PV_INSECURE=1 set, TLS certificate verification is DISABLED for this run")
        s.verify = False
    else:
        s.verify = _pick_ca_bundle_path()
        logger.info(f"Using CA bundle: {s.verify}")
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = _build_session()

# Secrets via env in CI, fallback to local defaults for dev
PIPL_API_KEY = os.getenv("PIPL_API_KEY", "04c54bbe-679d49da-80156ac0-3f7212f8")
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "postgresql://postgres.auzoezucrrhrtmaucbbg:SB0dailyreporting@aws-1-us-east-2.pooler.supabase.com:6543/postgres")

TABLE_NAME = "public.campaign_reporting"
UNIQUE_FIELD = "campaign_date_key"

ACTUAL_TABLE_COLUMNS = [
    "campaign_date_key","campaign_id","parent_campaign_id","campaign_name","client_name","status",
    "start_date","end_date","total_sent","new_leads_reached","replies_count","positive_reply",
    "bounce_count","reply_rate","positive_reply_rate","sequencer_platform","inserted_at","updated_at"
]

def get_workspaces():
    logger.info("Fetching workspaces from Pipl API...")
    r = SESSION.get("https://api.plusvibe.ai/api/v1/authenticate", headers={"x-api-key": PIPL_API_KEY}, timeout=30)
    r.raise_for_status()
    ws = r.json().get("workspaces", [])
    logger.info(f"Successfully fetched {len(ws)} workspaces")
    return ws

def list_campaigns(ws_id, skip=0, limit=100):
    r = SESSION.get(
        "https://api.plusvibe.ai/api/v1/campaign/list-all",
        headers={"x-api-key": PIPL_API_KEY},
        params={"workspace_id": ws_id, "skip": skip, "limit": limit},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()

def get_campaign_stats(ws_id, cid, start_date, end_date=None):
    params = {"workspace_id": ws_id, "campaign_id": cid, "start_date": start_date}
    if end_date:
        params["end_date"] = end_date
    r = SESSION.get(
        "https://api.plusvibe.ai/api/v1/analytics/campaign/stats",
        headers={"x-api-key": PIPL_API_KEY},
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    return list(data.values()) if isinstance(data, dict) else data

def connect_to_supabase():
    logger.info("Connecting to Supabase database...")
    conn = psycopg2.connect(SUPABASE_DB_URL, cursor_factory=RealDictCursor)
    logger.info("Successfully connected to Supabase database")
    return conn

def discover_table_columns(conn):
    logger.info("Discovering table schema...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'campaign_reporting'
            ORDER BY ordinal_position
        """)
        cols = cur.fetchall()
        logger.info("Available columns in campaign_reporting table:")
        for c in cols:
            logger.info(f"  - {c['column_name']}: {c['data_type']}")
        return [c["column_name"] for c in cols]

def map_pipl_to_supabase(campaign_data, workspace_name, report_date):
    camp = campaign_data["campaign"]; stats = campaign_data["stats"]
    camp_id = camp.get("id", ""); camp_name = camp.get("camp_name") or camp.get("name", ""); parent_camp_id = camp.get("parent_camp_id", ""); status = camp.get("status", "")
    sent = stats.get("sent_count", 0); replied = stats.get("replied_count", 0); new_leads = stats.get("new_lead_contacted_count", 0); positive = stats.get("positive_reply_count", 0); bounced = stats.get("bounced_count", 0)
    start_date_str = stats.get("start_date", "").split("T")[0] if stats.get("start_date") else None
    end_date_str = stats.get("end_date", "").split("T")[0] if stats.get("end_date") else None
    reply_rate = (replied / sent * 100) if sent > 0 else 0
    positive_rate = (positive / replied * 100) if replied > 0 else 0
    now = datetime.utcnow().isoformat()
    return {
        "campaign_date_key": f"{camp_id}_{report_date}",
        "campaign_id": camp_id,
        "parent_campaign_id": parent_camp_id,
        "campaign_name": camp_name,
        "client_name": workspace_name,
        "status": status,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "total_sent": sent,
        "new_leads_reached": new_leads,
        "replies_count": replied,
        "positive_reply": positive,
        "bounce_count": bounced,
        "reply_rate": round(reply_rate, 2),
        "positive_reply_rate": round(positive_rate, 2),
        "sequencer_platform": "PV",
        "inserted_at": now,
        "updated_at": now,
    }

def upsert_campaign_data(conn, mapped):
    cols = list(mapped.keys())
    placeholders = ", ".join(f"%({c})s" for c in cols)
    col_list = ", ".join(cols)
    update = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != UNIQUE_FIELD)
    sql = f"""
        INSERT INTO {TABLE_NAME} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({UNIQUE_FIELD}) DO UPDATE
        SET {update};
    """
    with conn.cursor() as cur:
        cur.execute(sql, mapped)
    conn.commit()
    logger.debug(f"Upserted campaign: {mapped[UNIQUE_FIELD]}")

def run_for_date(report_date: str):
    logger.info("=" * 60)
    logger.info(f"Starting Pipl to Supabase pipeline for {report_date}")
    logger.info("=" * 60)
    start = datetime.now()
    total_processed = 0; total_upserted = 0
    conn = connect_to_supabase()
    try:
        _ = discover_table_columns(conn)
        workspaces = get_workspaces()
        logger.info(f"Processing {len(workspaces)} workspaces")
        for ws in workspaces:
            ws_id = ws["_id"]; ws_name = ws.get("name", f"Workspace_{ws_id}")
            logger.info(f"\nProcessing workspace: {ws_name} ({ws_id})")
            skip = 0; ws_upserted = 0
            while True:
                campaigns = list_campaigns(ws_id, skip=skip)
                if not campaigns: break
                logger.info(f"  Processing batch of {len(campaigns)} campaigns (skip={skip})")
                for camp in campaigns:
                    cid = camp["id"]; cname = camp.get("camp_name") or camp.get("name", "Unknown")
                    try:
                        stats_list = get_campaign_stats(ws_id, cid, report_date, report_date)
                        if not stats_list: continue
                        for st in stats_list:
                            sent = st.get("sent_count", 0); replied = st.get("replied_count", 0)
                            if sent <= 0 and replied <= 0: continue
                            mapped = map_pipl_to_supabase({"campaign": camp, "stats": st}, ws_name, report_date)
                            upsert_campaign_data(conn, mapped)
                            total_upserted += 1; ws_upserted += 1
                            logger.debug(f"    SUCCESS: Processed {cname} ({cid})")
                    except Exception as e:
                        logger.error(f"    ERROR: Processing campaign {cname} ({cid}): {e}")
                        continue
                total_processed += len(campaigns)
                skip += len(campaigns)
                if len(campaigns) < 100: break
            logger.info(f"  Workspace summary: {ws_upserted} campaigns upserted")
        dur = datetime.now() - start
        logger.info("\n" + "=" * 60)
        logger.info(f"Pipeline completed successfully for {report_date}")
        logger.info(f"Total campaigns processed: {total_processed}")
        logger.info(f"Total campaigns upserted: {total_upserted}")
        logger.info(f"Execution time: {dur}")
        logger.info("=" * 60)
    finally:
        if conn: conn.close(); logger.info("Database connection closed")

if __name__ == "__main__":
    logger.info("Pipl to Supabase Daily Reporting Script Started")
    try:
        IST = pytz.timezone("Asia/Kolkata")
        # yesterday = datetime.now(IST) - timedelta(days=1)
        # run_for_date(yesterday.strftime("%Y-%m-%d"))
        from_date = datetime(2025, 9, 19); to_date = datetime(2025, 10, 10); current = from_date
        while current <= to_date:
            try:
                run_for_date(current.strftime("%Y-%m-%d"))
            except Exception as e:
                logger.error(f"Failed to process date {current.strftime('%Y-%m-%d')}: {e}")
            current += timedelta(days=1)
        logger.info("All dates processed successfully!")
    except KeyboardInterrupt:
        logger.info("Script interrupted by user"); sys.exit(1)
    except Exception as e:
        logger.error(f"Script failed with error: {e}"); sys.exit(1)
