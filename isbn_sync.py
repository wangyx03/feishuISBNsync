"""
isbn_sync.py — Auto-sync daemon
Continuously monitors the Feishu ISBN_match sheet.
When a new ISBN is found with no Title, it looks it up and writes back Title/Author/Condition.
Usage: python isbn_sync.py
"""
import os
import json
import time
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

# =============================================
# 配置（全部从 .env 读取）
# =============================================
FEISHU_APP_ID           = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET       = os.getenv("FEISHU_APP_SECRET", "")
SPREADSHEET_TOKEN       = os.getenv("FEISHU_SPREADSHEET_TOKEN", "")
SHEET_ID                = os.getenv("FEISHU_SHEET_ID", "")
INPUT_SPREADSHEET_TOKEN = os.getenv("FEISHU_INPUT_SPREADSHEET_TOKEN", "")
INPUT_SHEET_ID          = os.getenv("FEISHU_INPUT_SHEET_ID", "")
FEISHU_BASE             = "https://open.feishu.cn/open-apis"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))

_raw_keys = os.getenv("GOOGLE_API_KEYS", ",")
API_KEYS = [k.strip() for k in _raw_keys.split(",")]

# =============================================
# 日志
# =============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("isbn_sync.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# =============================================
# Feishu Token (refresh every 110 minutes)
# =============================================
_token_cache = {"token": None, "expires_at": 0}

def get_token() -> str:
    if time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]
    r = requests.post(
        f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal/",
        data={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Feishu token fetch failed: {data}")
    _token_cache["token"] = data["tenant_access_token"]
    _token_cache["expires_at"] = time.time() + 6600
    log.info("Feishu token refreshed")
    return _token_cache["token"]

def headers():
    return {"Authorization": f"Bearer {get_token()}", "Content-Type": "application/json"}

# =============================================
# ISBN Cleaning
# =============================================
def clean_isbn(raw) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    try:
        return str(int(float(s))) if s else ""
    except (ValueError, OverflowError):
        return "".join(c for c in s if c.isdigit())

# =============================================
# Feishu Read / Write
# =============================================
def read_input_sheet() -> list[dict]:
    r = requests.get(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{INPUT_SPREADSHEET_TOKEN}/values/{INPUT_SHEET_ID}!A1:D5000",
        headers=headers(),
        timeout=15,
    )
    payload = r.json()
    if payload.get("code", 0) != 0:
        raise RuntimeError(f"Failed to read sheet: {payload}")

    rows = payload.get("data", {}).get("valueRange", {}).get("values", [])
    if not rows or len(rows) < 2:
        return []

    header = [str(h).strip().lower() for h in rows[0]]
    records = []
    for i, row in enumerate(rows[1:], start=2):
        padded = row + [None] * max(0, len(header) - len(row))
        rec = {"_row": i}
        for j, col in enumerate(header):
            val = padded[j]
            rec[col] = clean_isbn(val) if col == "isbn" else ("" if val is None else str(val).strip())
        if rec.get("isbn"):
            records.append(rec)
    return records


def write_back(row_num: int, title: str, author: str, condition: str):
    payload = {"valueRange": {
        "range": f"{INPUT_SHEET_ID}!B{row_num}:D{row_num}",
        "values": [[title, author, condition]],
    }}
    r = requests.put(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{INPUT_SPREADSHEET_TOKEN}/values",
        headers=headers(),
        data=json.dumps(payload),
        timeout=10,
    )
    result = r.json()
    if result.get("code", 0) != 0:
        raise RuntimeError(f"Write failed at row {row_num}: {result}")


def save_to_db(isbn, title, author, condition):
    try:
        r = requests.get(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!A1:A5000",
            headers=headers(), timeout=10,
        )
        rows = r.json().get("data", {}).get("valueRange", {}).get("values", [])
        existing_isbns = [clean_isbn(row[0]) for row in rows[1:] if row]

        if isbn not in existing_isbns:
            requests.post(
                f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_append",
                headers=headers(),
                data=json.dumps({"valueRange": {
                    "range": f"{SHEET_ID}!A1:D1",
                    "values": [[isbn, title, author, condition]],
                }}),
                timeout=10,
            )
    except Exception as e:
        log.warning(f"DB cache write failed (non-critical): {e}")


def lookup_db_cache(isbn: str):
    try:
        r = requests.get(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!A1:D5000",
            headers=headers(), timeout=10,
        )
        rows = r.json().get("data", {}).get("valueRange", {}).get("values", [])
        for row in rows[1:]:
            if row and clean_isbn(row[0]) == isbn:
                title  = "" if len(row) < 2 or row[1] is None else str(row[1])
                author = "" if len(row) < 3 or row[2] is None else str(row[2])
                cond   = "" if len(row) < 4 or row[3] is None else str(row[3])
                if title:
                    return title, author, cond
    except Exception as e:
        log.warning(f"DB cache lookup failed: {e}")
    return None, None, None

# =============================================
# API Lookup
# =============================================
def lookup_google(isbn: str):
    for key in API_KEYS:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&country=US"
        if key:
            url += f"&key={key}"
        try:
            data = requests.get(url, timeout=8).json()
            if data.get("error", {}).get("code") in (429, 503):
                continue
            if data.get("totalItems", 0) > 0:
                info = data["items"][0]["volumeInfo"]
                return info.get("title", ""), ", ".join(info.get("authors", []))
        except Exception:
            continue
    return None, None


def lookup_openlibrary(isbn: str):
    try:
        data = requests.get(
            f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data",
            timeout=8,
        ).json()
        info = data.get(f"ISBN:{isbn}")
        if info:
            return info.get("title", ""), ", ".join(a["name"] for a in info.get("authors", []))
    except Exception:
        pass
    return None, None


def resolve(isbn: str) -> tuple[str, str, str]:
    t, a, c = lookup_db_cache(isbn)
    if t:
        return t, a, "DB"

    gb_title, gb_author = lookup_google(isbn)
    ol_title, ol_author = lookup_openlibrary(isbn)

    if gb_title and ol_title:
        match = gb_title.strip().lower() == ol_title.strip().lower()
        condition = "Google + OL" if match else "Mismatch"
        title, author = gb_title, gb_author
    elif gb_title:
        title, author, condition = gb_title, gb_author, "Google Books"
    elif ol_title:
        title, author, condition = ol_title, ol_author, "Open Library"
    else:
        return "Not Found", "", "Not Found"

    save_to_db(isbn, title, author, condition)
    return title, author, condition

# =============================================
# Main Loop
# =============================================
def sync_once():
    rows = read_input_sheet()
    pending = [r for r in rows if not r.get("title", "").strip()]

    if not pending:
        return 0

    log.info(f"Found {len(pending)} ISBN(s) to look up")
    count = 0
    for r in pending:
        isbn = r["isbn"]
        row_num = r["_row"]
        try:
            title, author, condition = resolve(isbn)
            write_back(row_num, title, author, condition)
            log.info(f"  ✅ Row {row_num} | {isbn} → {title} / {author} ({condition})")
            count += 1
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  ❌ Row {row_num} | {isbn} → Error: {e}")

    return count


def main():
    log.info("=" * 50)
    log.info("ISBN Sync daemon started")
    log.info(f"Poll interval: {POLL_INTERVAL}s")
    log.info("=" * 50)

    while True:
        try:
            count = sync_once()
            if count:
                log.info(f"Done — {count} row(s) written. Next check in {POLL_INTERVAL}s")
            else:
                log.debug(f"Nothing to do. Next check in {POLL_INTERVAL}s")
        except Exception as e:
            log.error(f"sync_once error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
