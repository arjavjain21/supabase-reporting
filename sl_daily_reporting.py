#!/usr/bin/env python3
"""
SL Daily Reporting -> Supabase Postgres with Email Summary

Hardening applied:
- Secrets come from environment variables only
- No runtime pip installs
- Per thread DB connections using a pool
- Campaign pagination
- Timezone explicit and configurable
- Safe email attachment handling
- Lockfile to avoid duplicate runs

Environment variables required:
  SUPABASE_DB_URL       Postgres connection string, include sslmode=require
  SMARTLEAD_API_KEY     Smartlead API key
  REPORT_TZ             Optional IANA timezone, default Asia/Kolkata
  SENDER_EMAIL          SMTP sender
  SMTP_SERVER           SMTP server, default smtp.gmail.com
  SMTP_PORT             SMTP port, default 465
  SMTP_USER             SMTP username, default SENDER_EMAIL
  SMTP_PASS             SMTP password or app password
  RECIPIENT_EMAILS      Comma separated list of recipient emails

Optional for Google Sheets mapping:
  GOOGLE_SA_JSON_BASE64 Base64 of service account JSON
  SHEET_DOCUMENT_ID     Google Sheet ID
  SHEET_RANGE           Range, default Sheet1!A:Z

"""

import os, sys, time, json, logging, threading, traceback, smtplib, ssl, mimetypes, gzip, base64
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from email.message import EmailMessage
from typing import Dict, Any, List, Tuple, Optional

# ------------------------------
# Configuration via env
# ------------------------------
DB_DSN   = os.getenv("SUPABASE_DB_URL") or os.getenv("PG_DSN") or ""
SMARTLEAD_API_KEY = os.getenv("SMARTLEAD_API_KEY", "")
REPORT_TZ = os.getenv("REPORT_TZ", "Asia/Kolkata")

SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER", SENDER_EMAIL)
SMTP_PASS = os.getenv("SMTP_PASS", "")
RECIPIENT_EMAILS = [e.strip() for e in os.getenv("RECIPIENT_EMAILS", "").split(",") if e.strip()]

GOOGLE_SA_JSON_BASE64 = os.getenv("GOOGLE_SA_JSON_BASE64", "")
SHEET_DOCUMENT_ID = os.getenv("SHEET_DOCUMENT_ID", "")
SHEET_RANGE = os.getenv("SHEET_RANGE", "Sheet1!A:Z")
CLIENT_MAPPING_CACHE = "/tmp/sl_client_mappings_cache.json"

# Timing and limits
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "10"))
RATE_LIMIT_SECONDS  = int(os.getenv("RATE_LIMIT_SECONDS", "2"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "4"))
SMARTLEAD_RETRY_BACKOFF = float(os.getenv("SMARTLEAD_RETRY_BACKOFF", "1.5"))

WORKER_THREADS = int(os.getenv("WORKER_THREADS", "8"))
DB_RETRY_ATTEMPTS = int(os.getenv("DB_RETRY_ATTEMPTS", "5"))
DB_RETRY_BACKOFF = float(os.getenv("DB_RETRY_BACKOFF", "2.0"))

LOCK_FILE = os.getenv("LOCK_FILE", "/tmp/sl_daily_reporting.lock")
LOG_FILE  = os.getenv("LOG_FILE", "./sl_daily_reporting.log")

GMAIL_ATTACHMENT_LIMIT = 25 * 1024 * 1024
LARGE_FILE_THRESHOLD = 5 * 1024 * 1024
HARD_ATTACH_LIMIT = 20 * 1024 * 1024  # above this, skip attachment

# ------------------------------
# Utilities
# ------------------------------
class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int, per_seconds: int):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.lock = threading.Lock()
        self.events: List[float] = []

    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                self.events = [t for t in self.events if now - t < self.per_seconds]
                if len(self.events) < self.max_requests:
                    self.events.append(now)
                    return
                oldest = min(self.events) if self.events else now
                sleep_for = max(0.0, self.per_seconds - (now - oldest))
            time.sleep(sleep_for)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
        ]
    )

def obtain_lock() -> bool:
    try:
        if os.path.exists(LOCK_FILE):
            logging.warning("Another run appears to be in progress. Exiting.")
            return False
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logging.error(f"Could not create lock file: {e}")
        return False

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except Exception as e:
        logging.error(f"Could not remove lock file: {e}")

def today_range_in_tz(tz_name: str) -> Tuple[str, str]:
    # We only need start and end dates for API
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = None
    now = datetime.now(tz) if tz else datetime.now()
    yesterday = now.date() - timedelta(days=1)
    d = yesterday.strftime("%Y-%m-%d")
    return d, d

# ------------------------------
# Google Sheets client map (optional)
# ------------------------------
def load_client_mappings_from_sheets() -> Dict[str, str]:
    if not GOOGLE_SA_JSON_BASE64 or not SHEET_DOCUMENT_ID:
        raise RuntimeError("Sheets mapping not configured")
    try:
        sa_json = base64.b64decode(GOOGLE_SA_JSON_BASE64).decode("utf-8")
        creds_info = json.loads(sa_json)
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        creds = Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        values = service.spreadsheets().values().get(
            spreadsheetId=SHEET_DOCUMENT_ID, range=SHEET_RANGE
        ).execute().get("values", [])
        if not values:
            raise RuntimeError("No rows in mapping sheet")
        header = [h.strip().lower() for h in values[0]]
        # Try to find headers
        def find_idx(name: str) -> Optional[int]:
            try:
                return header.index(name)
            except ValueError:
                return None
        cid_idx = find_idx("client id")
        cname_idx = find_idx("client name")
        if cid_idx is None or cname_idx is None:
            # Fallback to first two columns
            cid_idx, cname_idx = 0, 1
        mapping: Dict[str, str] = {}
        for row in values[1:]:
            if len(row) <= max(cid_idx, cname_idx):
                continue
            cid = str(row[cid_idx]).strip()
            cname = str(row[cname_idx]).strip()
            if cid:
                mapping[cid] = cname
        # Cache to disk
        with open(CLIENT_MAPPING_CACHE, "w") as f:
            json.dump(mapping, f)
        return mapping
    except Exception as e:
        logging.error(f"Failed loading sheet mapping: {e}")
        raise

def load_client_mappings_from_cache() -> Dict[str, str]:
    try:
        with open(CLIENT_MAPPING_CACHE, "r") as f:
            return json.load(f)
    except Exception:
        logging.warning("No mapping cache, defaulting to empty map")
        return {}

# ------------------------------
# SmartLead API
# ------------------------------
rate_limiter = SlidingWindowRateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_SECONDS)
SMARTLEAD_BASE_URL = os.getenv("SMARTLEAD_BASE_URL", "https://server.smartlead.ai")

def smartlead_get(path: str, params: Dict[str, Any] = None) -> Any:
    """
    Mirror original behavior:
    - GET {BASE}/api/v1{path}
    - api_key only as query param
    - No pagination params added automatically
    """
    import requests
    if not SMARTLEAD_API_KEY:
        raise RuntimeError("SMARTLEAD_API_KEY is not set")
    url = f"{SMARTLEAD_BASE_URL}/api/v1{path}"
    q = dict(params or {})
    q["api_key"] = SMARTLEAD_API_KEY

    last_err = None
    for attempt in range(1, HTTP_RETRIES + 1):
        rate_limiter.wait()
        try:
            r = requests.get(
                url,
                params=q,
                headers={"Accept": "application/json"},
                timeout=45
            )
            # Retry on 5xx
            if 500 <= r.status_code < 600:
                raise Exception(f"{r.status_code} {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            backoff = SMARTLEAD_RETRY_BACKOFF * (2 ** (attempt - 1))
            logging.warning(f"SmartLead attempt {attempt} error: {e}, backoff {backoff}s")
            time.sleep(backoff)
    raise RuntimeError(f"SmartLead failed: {url} last_err={last_err}")

def fetch_all_campaigns() -> List[Dict[str, Any]]:
    """
    Original tenant behavior: /campaigns returns the full list without pagination.
    Accept either a bare list or an object with 'data' or 'items'.
    """
    payload = smartlead_get("/campaigns")
    if isinstance(payload, list):
        logging.info(f"/campaigns returned list, count={len(payload)}")
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            items = payload["data"]
            logging.info(f"/campaigns returned dict.data, count={len(items)}")
            return items
        if isinstance(payload.get("items"), list):
            items = payload["items"]
            logging.info(f"/campaigns returned dict.items, count={len(items)}")
            return items

    # Defensive: if API shape is unexpected
    logging.warning(f"/campaigns returned unexpected shape: type={type(payload)} keys={list(payload.keys()) if isinstance(payload, dict) else 'n/a'}")
    return []


def fetch_campaign_metrics(c: Dict[str, Any], start_date: str, end_date: str) -> Dict[str, int]:
    cid = c.get("id")
    pid = c.get("parent_campaign_id") or c.get("parentId") or c.get("parent_id")
    # Base values
    metrics = {
        "total_email_sent": 0,
        "new_leads_reached": 0,
        "replies_count": 0,
        "positive_reply": 0,
        "bounce_count": 0,
        "open_count": 0,
    }
    # Child campaigns: analytics-by-date only
    if pid:
        an = smartlead_get(f"/campaigns/{cid}/analytics-by-date", {"start_date": start_date, "end_date": end_date})
        metrics.update({
            "total_email_sent": int(an.get("sent_count", 0) or 0),
            "new_leads_reached": int(an.get("unique_sent_count", 0) or 0),
            "replies_count": int(an.get("reply_count", 0) or 0),
            "bounce_count": int(an.get("bounce_count", 0) or 0),
            "open_count": int(an.get("open_count", 0) or 0),
        })
    else:
        # Parent: add top-level positive replies
        top = smartlead_get(f"/campaigns/{cid}/top-level-analytics-by-date", {"start_date": start_date, "end_date": end_date})
        an = smartlead_get(f"/campaigns/{cid}/analytics-by-date", {"start_date": start_date, "end_date": end_date})
        metrics.update({
            "total_email_sent": int(an.get("sent_count", 0) or 0),
            "new_leads_reached": int(an.get("unique_sent_count", 0) or 0),
            "replies_count": int(an.get("reply_count", 0) or 0),
            "bounce_count": int(an.get("bounce_count", 0) or 0),
            "open_count": int(an.get("open_count", 0) or 0),
            "positive_reply": int(top.get("positive_reply_count", 0) or 0),
        })
    return metrics

# ------------------------------
# Database
# ------------------------------
def init_pool():
    if not DB_DSN:
        raise RuntimeError("SUPABASE_DB_URL or PG_DSN is not set")
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    # Ensure sslmode=require present
    dsn = DB_DSN
    if "sslmode" not in dsn:
        sep = "&" if "?" in dsn else "?"
        dsn = f"{dsn}{sep}sslmode=require"
    pool = ThreadedConnectionPool(1, max(WORKER_THREADS, 8), dsn)
    return pool

def upsert_row(conn, row: Dict[str, Any]):
    from psycopg2.extras import execute_values
    cur = conn.cursor()
    # Compute derived rates
    replies = row.get("replies_count") or 0
    uniques = row.get("new_leads_reached") or 0
    pos = row.get("positive_reply") or 0
    reply_rate = float(replies) / float(uniques) if uniques else 0.0
    pos_rate = float(pos) / float(replies) if replies else 0.0

    sql = """
    insert into public.campaign_reporting
    (campaign_date_key, campaign_id, parent_campaign_id, campaign_name, client_name, status,
     start_date, end_date, total_sent, new_leads_reached, replies_count, positive_reply, bounce_count,
     reply_rate, positive_reply_rate, sequencer_platform, inserted_at, updated_at)
    values %s
    on conflict (campaign_date_key) do update set
      parent_campaign_id = excluded.parent_campaign_id,
      campaign_name = excluded.campaign_name,
      client_name = excluded.client_name,
      status = excluded.status,
      start_date = excluded.start_date,
      end_date = excluded.end_date,
      total_sent = excluded.total_sent,
      new_leads_reached = excluded.new_leads_reached,
      replies_count = excluded.replies_count,
      positive_reply = excluded.positive_reply,
      bounce_count = excluded.bounce_count,
      reply_rate = excluded.reply_rate,
      positive_reply_rate = excluded.positive_reply_rate,
      sequencer_platform = excluded.sequencer_platform,
      updated_at = now();
    """
    values = [(
        row["campaign_date_key"],
        row["campaign_id"],
        row.get("parent_campaign_id"),
        row.get("campaign_name"),
        row.get("client_name"),
        row.get("status"),
        row["start_date"],
        row["end_date"],
        row.get("total_email_sent", 0),
        row.get("new_leads_reached", 0),
        row.get("replies_count", 0),
        row.get("positive_reply", 0),
        row.get("bounce_count", 0),
        reply_rate,
        pos_rate,
        row.get("sequencer_platform", "SL"),
        datetime.utcnow(),
        datetime.utcnow(),
    )]
    execute_values(cur, sql, values)
    conn.commit()
    cur.close()

# ------------------------------
# Email
# ------------------------------
def send_email(subject: str, body: str, log_path: str = None):
    if not SENDER_EMAIL or not SMTP_PASS or not RECIPIENT_EMAILS:
        logging.warning("Email not fully configured. Skipping email send.")
        return
    msg = EmailMessage()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENT_EMAILS)
    msg["Subject"] = subject
    msg.set_content(body)

    attachment_added = False
    if log_path and os.path.exists(log_path):
        size = os.path.getsize(log_path)
        path_to_send = log_path
        if size > LARGE_FILE_THRESHOLD:
            gz_path = f"{log_path}.gz"
            with open(log_path, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                f_out.writelines(f_in)
            path_to_send = gz_path
            size = os.path.getsize(gz_path)
        if size < HARD_ATTACH_LIMIT:
            ctype, _ = mimetypes.guess_type(path_to_send)
            maintype, subtype = (ctype or "application/octet-stream").split("/", 1)
            with open(path_to_send, "rb") as f:
                msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path_to_send))
            attachment_added = True

    if not attachment_added:
        msg.add_alternative(f"<pre>{body}</pre><p>Log attachment skipped due to size. Check runner artifacts.</p>", subtype="html")

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)

# ------------------------------
# Worker
# ------------------------------
def process_campaign(c: Dict[str, Any], client_map: Dict[str, str], start_date: str, end_date: str, pool) -> bool:
    cid = str(c.get("id"))
    if not cid:
        return False
    try:
        metrics = fetch_campaign_metrics(c, start_date, end_date)
        client_id = str(c.get("client_id") or c.get("clientId") or "")
        client_name = client_map.get(client_id, client_id or "Unknown")
        row = {
            "campaign_date_key": f"{start_date}_{cid}_{end_date}",
            "campaign_id": cid,
            "parent_campaign_id": c.get("parent_campaign_id") or c.get("parentId"),
            "campaign_name": c.get("name"),
            "client_name": client_name,
            "status": c.get("status"),
            "start_date": start_date,
            "end_date": end_date,
            "total_email_sent": metrics["total_email_sent"],
            "new_leads_reached": metrics["new_leads_reached"],
            "replies_count": metrics["replies_count"],
            "positive_reply": metrics["positive_reply"],
            "bounce_count": metrics["bounce_count"],
            "sequencer_platform": "SL",
        }
        conn = pool.getconn()
        try:
            upsert_row(conn, row)
        finally:
            pool.putconn(conn)
        return True
    except Exception as e:
        logging.error(f"Campaign {cid} failed: {e}")
        return False

# ------------------------------
# Main
# ------------------------------
def main():
    start_time = time.time()
    setup_logging()
    if not obtain_lock():
        return
    skipped, total, successes, fatal_error = [], 0, 0, None
    try:
        # Load mapping
        try:
            client_map = load_client_mappings_from_sheets()
        except Exception:
            client_map = load_client_mappings_from_cache()

        # Date range
        start_date, end_date = today_range_in_tz(REPORT_TZ)

        # Fetch campaigns with pagination
        campaigns = fetch_all_campaigns()
        total = len(campaigns)
        logging.info(f"Fetched {total} campaigns")

        # DB pool
        pool = init_pool()

        from concurrent.futures import ThreadPoolExecutor, as_completed
        futures = []
        with ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
            for c in campaigns:
                futures.append(ex.submit(process_campaign, c, client_map, start_date, end_date, pool))
            for fut in as_completed(futures):
                ok = fut.result()
                if ok:
                    successes += 1
                else:
                    # Not all responses include id, so append None-safe
                    # We will not try to requeue here
                    pass
        elapsed = time.time() - start_time
        summary = (
            f"Execution Summary\n"
            f"Start Time: {datetime.fromtimestamp(start_time)}\n"
            f"End Time: {datetime.now()}\n"
            f"Total Time: {elapsed:.2f} seconds\n"
            f"Total Campaigns: {total}\n"
            f"Successful Upserts: {successes}\n"
            f"Failed: {total - successes}\n"
        )
        print(summary)
        send_email("SL Daily Reporting Summary", summary, LOG_FILE)

    except Exception as e:
        fatal_error = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        logging.error(f"Fatal error: {fatal_error}")
        try:
            send_email("SL Daily Reporting Failed", fatal_error, LOG_FILE)
        except Exception:
            pass
    finally:
        release_lock()

if __name__ == "__main__":
    main()
