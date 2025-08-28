#!/usr/bin/env python3
"""
Pipl → Supabase Campaign Reporting Script

Fetches Pipl campaign stats for a single date (default: yesterday in IST)
or for a hard‑coded date range, and upserts them into the Supabase
`public.campaign_reporting` table via INSERT … ON CONFLICT.

Usage:
  • By default, runs for yesterday in Asia/Kolkata time.
  • To run for a custom date range, uncomment the "BULK RANGE" block in `__main__`.
"""

import sys
import requests
import logging
from datetime import datetime, timedelta
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Configure logging with UTF-8 encoding to handle Unicode characters
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipl_supabase_reporting.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set environment variable for UTF-8 output on Windows
if os.name == 'nt':  # Windows
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ─── Configuration ─────────────────────────────────────────────────────────────
PIPL_API_KEY = "04c54bbe-679d49da-80156ac0-3f7212f8"

# Supabase Database Configuration (from AMwise script)
SUPABASE_DB_URL = "postgresql://postgres.auzoezucrrhrtmaucbbg:SB0dailyreporting@aws-1-us-east-2.pooler.supabase.com:6543/postgres"

TABLE_NAME = "public.campaign_reporting"
UNIQUE_FIELD = "campaign_date_key"

# Actual Supabase table columns (based on discovered schema)
ACTUAL_TABLE_COLUMNS = [
    "campaign_date_key", "campaign_id", "parent_campaign_id", "campaign_name", 
    "client_name", "status", "start_date", "end_date", "total_sent", 
    "new_leads_reached", "replies_count", "positive_reply", "bounce_count", 
    "reply_rate", "positive_reply_rate", "sequencer_platform", 
    "inserted_at", "updated_at"
]

# ─── Pipl API Helpers ─────────────────────────────────────────────────────────
def get_workspaces():
    """Fetch available workspaces from Pipl API"""
    logger.info("Fetching workspaces from Pipl API...")
    
    try:
        response = requests.get(
            "https://api.plusvibe.ai/api/v1/authenticate",
            headers={"x-api-key": PIPL_API_KEY},
            timeout=30
        )
        response.raise_for_status()
        workspaces = response.json().get("workspaces", [])
        logger.info(f"Successfully fetched {len(workspaces)} workspaces")
        return workspaces
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching workspaces: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching workspaces: {e}")
        raise

def list_campaigns(ws_id, skip=0, limit=100):
    """Fetch campaigns for a workspace with pagination"""
    logger.debug(f"Fetching campaigns for workspace {ws_id} (skip={skip}, limit={limit})")
    
    try:
        response = requests.get(
            "https://api.plusvibe.ai/api/v1/campaign/list-all",
            headers={"x-api-key": PIPL_API_KEY},
            params={"workspace_id": ws_id, "skip": skip, "limit": limit},
            timeout=30
        )
        response.raise_for_status()
        campaigns = response.json()
        logger.debug(f"Fetched {len(campaigns)} campaigns for workspace {ws_id}")
        return campaigns
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching campaigns for workspace {ws_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching campaigns for workspace {ws_id}: {e}")
        raise

def get_campaign_stats(ws_id, cid, start_date, end_date=None):
    """Fetch campaign statistics for a specific date range"""
    logger.debug(f"Fetching stats for campaign {cid} in workspace {ws_id} for date {start_date}")
    
    try:
        params = {"workspace_id": ws_id, "campaign_id": cid, "start_date": start_date}
        if end_date:
            params["end_date"] = end_date
            
        response = requests.get(
            "https://api.plusvibe.ai/api/v1/analytics/campaign/stats",
            headers={"x-api-key": PIPL_API_KEY},
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        stats = list(data.values()) if isinstance(data, dict) else data
        logger.debug(f"Fetched {len(stats) if stats else 0} stats for campaign {cid}")
        return stats
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching stats for campaign {cid}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching stats for campaign {cid}: {e}")
        raise

# ─── Database Helpers ─────────────────────────────────────────────────────────
def connect_to_supabase():
    """Establish connection to Supabase database"""
    logger.info("Connecting to Supabase database...")
    
    try:
        conn = psycopg2.connect(
            SUPABASE_DB_URL,
            cursor_factory=RealDictCursor
        )
        logger.info("Successfully connected to Supabase database")
        return conn
        
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to Supabase database: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to database: {e}")
        raise

def discover_table_columns(conn):
    """Discover available columns in the campaign_reporting table"""
    logger.info("Discovering table schema...")
    
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'campaign_reporting'
            ORDER BY ordinal_position
            """
            cursor.execute(query)
            columns = cursor.fetchall()
            
            logger.info("Available columns in campaign_reporting table:")
            for col in columns:
                logger.info(f"  - {col['column_name']}: {col['data_type']}")
                
            return [col['column_name'] for col in columns]
            
    except Exception as e:
        logger.error(f"Error discovering table columns: {e}")
        raise

def map_pipl_to_supabase(campaign_data, workspace_name, report_date):
    """Map Pipl API data to actual Supabase table schema"""
    camp_id = campaign_data['campaign'].get('id', '')
    camp_name = campaign_data['campaign'].get('camp_name') or campaign_data['campaign'].get('name', '')
    parent_camp_id = campaign_data['campaign'].get('parent_camp_id', '')
    status = campaign_data['campaign'].get('status', '')
    
    stats = campaign_data['stats']
    sent_count = stats.get('sent_count', 0)
    replied_count = stats.get('replied_count', 0)
    new_lead_contacted = stats.get('new_lead_contacted_count', 0)
    positive_replies = stats.get('positive_reply_count', 0)
    bounced_count = stats.get('bounced_count', 0)
    
    # Extract and format dates
    start_date_str = stats.get('start_date', '').split('T')[0] if stats.get('start_date') else None
    end_date_str = stats.get('end_date', '').split('T')[0] if stats.get('end_date') else None
    
    # Create unique key
    date_key = f"{camp_id}_{report_date}"
    
    # Calculate rates (handle division by zero)
    reply_rate = (replied_count / sent_count * 100) if sent_count > 0 else 0
    positive_reply_rate = (positive_replies / replied_count * 100) if replied_count > 0 else 0
    
    # Current timestamp for inserted_at and updated_at
    current_time = datetime.utcnow().isoformat()
    
    # Map to actual Supabase table schema (only existing columns)
    mapped_data = {
        "campaign_date_key": date_key,
        "campaign_id": camp_id,
        "parent_campaign_id": parent_camp_id,
        "campaign_name": camp_name,
        "client_name": workspace_name,
        "status": status,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "total_sent": sent_count,
        "new_leads_reached": new_lead_contacted,
        "replies_count": replied_count,
        "positive_reply": positive_replies,
        "bounce_count": bounced_count,
        "reply_rate": round(reply_rate, 2),
        "positive_reply_rate": round(positive_reply_rate, 2),
        "sequencer_platform": "PV",
        "inserted_at": current_time,
        "updated_at": current_time
    }
    
    return mapped_data

def upsert_campaign_data(conn, mapped_data):
    """Upsert campaign data into Supabase using ON CONFLICT"""
    try:
        with conn.cursor() as cursor:
            # Get available columns from mapped_data that exist in the table
            columns = list(mapped_data.keys())
            values_placeholder = ", ".join(f"%({col})s" for col in columns)
            columns_list = ", ".join(columns)
            
            # Build UPDATE clause excluding the unique field but including updated_at
            update_clauses = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in columns 
                if col != UNIQUE_FIELD
            )
            
            sql = f"""
            INSERT INTO {TABLE_NAME} ({columns_list})
            VALUES ({values_placeholder})
            ON CONFLICT ({UNIQUE_FIELD}) DO UPDATE
            SET {update_clauses};
            """
            
            cursor.execute(sql, mapped_data)
            conn.commit()
            
            logger.debug(f"Upserted campaign: {mapped_data[UNIQUE_FIELD]}")
            
    except Exception as e:
        logger.error(f"Error upserting campaign {mapped_data.get(UNIQUE_FIELD, 'unknown')}: {e}")
        conn.rollback()
        raise

# ─── Core Pipeline Runner ─────────────────────────────────────────────────────
def run_for_date(report_date: str):
    """Run the complete pipeline for a specific date"""
    logger.info("=" * 60)
    logger.info(f"Starting Pipl to Supabase pipeline for {report_date}")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    total_campaigns_processed = 0
    total_campaigns_upserted = 0
    
    # Connect to database
    conn = connect_to_supabase()
    
    try:
        # Discover table schema for debugging
        available_columns = discover_table_columns(conn)
        
        # Fetch workspaces from Pipl
        workspaces = get_workspaces()
        logger.info(f"Processing {len(workspaces)} workspaces")
        
        # Process each workspace
        for workspace in workspaces:
            ws_id = workspace["_id"]
            ws_name = workspace.get("name", f"Workspace_{ws_id}")
            
            logger.info(f"\nProcessing workspace: {ws_name} ({ws_id})")
            
            # Fetch campaigns for this workspace with pagination
            skip = 0
            workspace_campaigns_processed = 0
            
            while True:
                campaigns = list_campaigns(ws_id, skip=skip)
                if not campaigns:
                    break
                    
                logger.info(f"  Processing batch of {len(campaigns)} campaigns (skip={skip})")
                
                # Process each campaign
                for campaign in campaigns:
                    campaign_id = campaign["id"]
                    campaign_name = campaign.get("camp_name") or campaign.get("name", "Unknown")
                    
                    try:
                        # Fetch campaign stats for the report date
                        stats_list = get_campaign_stats(ws_id, campaign_id, report_date, report_date)
                        
                        if not stats_list:
                            logger.debug(f"    No stats found for campaign {campaign_name} ({campaign_id})")
                            continue
                            
                        # Process each stat entry
                        for stats in stats_list:
                            sent_count = stats.get("sent_count", 0)
                            replied_count = stats.get("replied_count", 0)
                            
                            # Skip campaigns with no activity
                            if sent_count <= 0 and replied_count <= 0:
                                logger.debug(f"    Skipping inactive campaign {campaign_name}")
                                continue
                            
                            # Combine campaign and stats data
                            combined_data = {
                                'campaign': campaign,
                                'stats': stats
                            }
                            
                            # Map to Supabase schema
                            mapped_data = map_pipl_to_supabase(combined_data, ws_name, report_date)
                            
                            # Upsert to database
                            upsert_campaign_data(conn, mapped_data)
                            
                            total_campaigns_upserted += 1
                            workspace_campaigns_processed += 1
                            
                            logger.debug(f"    SUCCESS: Processed {campaign_name} ({campaign_id})")
                            
                    except Exception as e:
                        logger.error(f"    ERROR: Processing campaign {campaign_name} ({campaign_id}): {e}")
                        continue
                
                total_campaigns_processed += len(campaigns)
                skip += len(campaigns)
                
                # Break if we got fewer campaigns than the limit (end of pagination)
                if len(campaigns) < 100:
                    break
            
            logger.info(f"  Workspace summary: {workspace_campaigns_processed} campaigns upserted")
        
        # Pipeline completion summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n" + "=" * 60)
        logger.info(f"Pipeline completed successfully for {report_date}")
        logger.info(f"Total campaigns processed: {total_campaigns_processed}")
        logger.info(f"Total campaigns upserted: {total_campaigns_upserted}")
        logger.info(f"Execution time: {duration}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed for {report_date}: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")

# ─── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Pipl to Supabase Daily Reporting Script Started")
    
    try:
        # --- SINGLE DATE MODE (default: yesterday IST) ---
        IST = pytz.timezone("Asia/Kolkata")
        yesterday = datetime.now(IST) - timedelta(days=1)
        run_for_date(yesterday.strftime("%Y-%m-%d"))

        # --- BULK RANGE MODE (uncomment to activate) ---
        # from_date = datetime(2025, 7, 23)
        # to_date = datetime(2025, 7, 31)
        # current = from_date
        
        # while current <= to_date:
        #     try:
        #         run_for_date(current.strftime("%Y-%m-%d"))
        #     except Exception as e:
        #         logger.error(f"Failed to process date {current.strftime('%Y-%m-%d')}: {e}")
        #         # Continue with next date instead of failing completely
        #         continue
        #     current += timedelta(days=1)
            
        logger.info("All dates processed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        sys.exit(1)
