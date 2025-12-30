import hashlib
import time
import random
import os
import sys
from datetime import datetime, timedelta, timezone
from google_play_scraper import reviews, Sort
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# VERSION BANNER
# =========================================================
print("🧪 FINAL SCRIPT | Backfill July-2025 → D-1 | Append-only | >30 Chars", flush=True)

# ================= CONFIG =================
SHEET_ID = "1If1jJXtsyVZSNN3fASbmrma1pB74ikIQEq6qxJdwLRU"
SHEET_NAME = "Raw_Reviews"

IST = timezone(timedelta(hours=5, minutes=30))

# 📅 DATE CONFIGURATION
# -------------------------------------------
# GLOBAL BACKFILL: Set this to your specific start date
BACKFILL_START_UTC = datetime(2025, 7, 1, tzinfo=timezone.utc)

# ⚡ DAILY MODE (Uncomment below after your first successful backfill)
# If running daily, only look back 5 days to save time and API quota
# BACKFILL_START_UTC = datetime.now(timezone.utc) - timedelta(days=5)
# -------------------------------------------

MAX_SAFE_ROWS = 500_000 
APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"},
    {"name": "Kissht", "id": "com.fastbanking"}
]

REQUIRED_HEADERS = [
    "Review_Id", "App_Name", "Review_Date", 
    "Rating", "Inserted_On", "Review_Text"
]

# ================= SHEET =================
def get_sheet():
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/sa.json")
    
    if not os.path.exists(sa_path):
        print(f"❌ Error: Service Account file not found at {sa_path}")
        sys.exit(1)

    creds = Credentials.from_service_account_file(
        sa_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(creds)
    ss = client.open_by_key(SHEET_ID)

    try:
        ws = ss.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=SHEET_NAME, rows=1000, cols=len(REQUIRED_HEADERS))
        ws.append_row(REQUIRED_HEADERS)
        print("ℹ️ Created Raw_Reviews sheet", flush=True)
        return ws

    values = ws.get_all_values()
    if not values:
        ws.append_row(REQUIRED_HEADERS)
        return ws

    headers = ws.row_values(1)
    missing = [h for h in REQUIRED_HEADERS if h not in headers]
    if missing:
        print(f"⚠️ Adding missing headers: {missing}", flush=True)
        for h in missing:
            ws.update_cell(1, len(headers) + 1, h)
            headers.append(h)

    return ws

# ================= HELPERS =================
def used_rows(sheet):
    return len(sheet.get_all_values())

def load_existing_ids(sheet):
    vals = sheet.col_values(1)
    return set(vals[1:]) if len(vals) > 1 else set()

def generate_review_id(app_id, text, date_ist):
    clean_text = text.strip()
    raw = f"{app_id}|{clean_text}|{date_ist}"
    return hashlib.sha1(raw.encode()).hexdigest()

# ================= FETCH LOGIC =================
def fetch_all_reviews_since(existing_ids, app, start_utc, end_utc):
    token = None
    rows = []
    
    print(f"   🔍 Fetching {app['name']}...", end="", flush=True)

    while True:
        try:
            batch, token = reviews(
                app["id"],
                lang='en', 
                country="in",
                sort=Sort.NEWEST,
                count=200,
                continuation_token=token
            )
        except Exception as e:
            print(f"\n   ❌ Network Error for {app['name']}: {e}")
            break

        processed_count = 0
        
        for r in batch:
            rd_utc = r["at"].replace(tzinfo=timezone.utc)

            # 1. Skip if review is newer than our "Yesterday" cutoff
            if rd_utc > end_utc:
                continue

            # 2. STOP if we hit the backfill start date
            if rd_utc < start_utc:
                print(f" [Reached Limit: {rd_utc.date()}]", end="")
                return rows 

            text = (r.get("content") or "").strip()
            
            # ✅ FILTER: ONLY reviews with length > 30 allowed
            # If length is 30 or less, skip it.
            if len(text) <= 30: 
                continue

            date_ist = rd_utc.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")

            rid = generate_review_id(app["id"], text, date_ist)
            
            if rid in existing_ids:
                continue

            rows.append([
                rid,
                app["name"],
                date_ist,
                r["score"],
                datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                text
            ])
            existing_ids.add(rid)
            processed_count += 1

        print(".", end="", flush=True) 

        if not token:
            break
        
        time.sleep(random.uniform(1, 3))

    return rows

# ================= MAIN =================
def main():
    sheet = get_sheet()
    existing_ids = load_existing_ids(sheet)
    
    print(f"📊 Loaded {len(existing_ids)} existing review IDs.", flush=True)

    now_ist = datetime.now(IST)
    
    # End of Day Yesterday (D-1)
    yesterday_ist = (now_ist - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    end_utc = yesterday_ist.astimezone(timezone.utc)

    print(f"▶ Window: {BACKFILL_START_UTC.date()} → {end_utc.date()}", flush=True)

    total_added = 0

    for app in APPS:
        if used_rows(sheet) >= MAX_SAFE_ROWS:
            print("\n⚠️ Row limit reached — stopping script.", flush=True)
            break

        new_rows = fetch_all_reviews_since(
            existing_ids, app, BACKFILL_START_UTC, end_utc
        )

        if new_rows:
            sheet.append_rows(new_rows, value_input_option="RAW")
            total_added += len(new_rows)
            print(f"\n   ✅ Added {len(new_rows)} reviews for {app['name']}", flush=True)
        else:
            print(f"\n   ✔ No new reviews for {app['name']}", flush=True)

    print(f"\n🎉 JOB COMPLETE. Total New Rows: {total_added}", flush=True)

if __name__ == "__main__":
    main()
