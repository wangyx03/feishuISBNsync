"""
isbn_sync.py — Auto-sync daemon
Continuously monitors multiple Feishu ISBN_match sheets (input sources).
When a new ISBN is found with no Title, it looks it up and writes back Title/Author/Binding/Condition.
Date/Language/Pages are stored in the shared DB cache only.
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
# Config (loaded from .env)
# =============================================
FEISHU_APP_ID           = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET       = os.getenv("FEISHU_APP_SECRET", "")
SPREADSHEET_TOKEN       = os.getenv("FEISHU_SPREADSHEET_TOKEN", "")
SHEET_ID                = os.getenv("FEISHU_SHEET_ID", "")
FEISHU_BASE             = "https://open.feishu.cn/open-apis"

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))

_raw_keys = os.getenv("GOOGLE_API_KEYS", ",")
API_KEYS = [k.strip() for k in _raw_keys.split(",")]

# =============================================
# Input sources — each has its own (name, spreadsheet_token, sheet_id)
# Add new sources here, or load from .env (see INPUT_SOURCES_JSON below).
# =============================================
DEFAULT_INPUT_SOURCES = [
    {
        "name": "main",
        "spreadsheet_token": os.getenv("FEISHU_INPUT_SPREADSHEET_TOKEN", "UAROsHnTMhnsYetPGu5cwg8on5g"),
        "sheet_id": os.getenv("FEISHU_INPUT_SHEET_ID", "caa5d1"),
    },
    {
        "name": "source2",
        "spreadsheet_token": "JtFXsgzyahRVyNtfwc3cehPqn9c",
        "sheet_id": "fefb70",
    },
]

# Optional: override/extend sources via .env as JSON, e.g.
# INPUT_SOURCES_JSON=[{"name":"main","spreadsheet_token":"...","sheet_id":"..."}, ...]
_input_sources_json = os.getenv("INPUT_SOURCES_JSON", "")
if _input_sources_json:
    try:
        INPUT_SOURCES = json.loads(_input_sources_json)
    except Exception:
        INPUT_SOURCES = DEFAULT_INPUT_SOURCES
else:
    INPUT_SOURCES = DEFAULT_INPUT_SOURCES

# =============================================
# Logging
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
# Feishu Read / Write (now parameterized by source)
# =============================================
def read_input_sheet(spreadsheet_token: str, sheet_id: str) -> list[dict]:
    r = requests.get(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!A1:E5000",
        headers=headers(),
        timeout=15,
    )
    payload = r.json()
    if payload.get("code", 0) != 0:
        raise RuntimeError(f"Failed to read sheet ({spreadsheet_token}/{sheet_id}): {payload}")

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


def write_back(spreadsheet_token: str, sheet_id: str, row_num: int, title: str, author: str, binding: str, condition: str):
    """Write back B:E — Title / Author / Binding / Condition (match sheet only)"""
    payload = {"valueRange": {
        "range": f"{sheet_id}!B{row_num}:E{row_num}",
        "values": [[title, author, binding, condition]],
    }}
    r = requests.put(
        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values",
        headers=headers(),
        data=json.dumps(payload),
        timeout=10,
    )
    result = r.json()
    if result.get("code", 0) != 0:
        raise RuntimeError(f"Write failed at row {row_num} ({spreadsheet_token}/{sheet_id}): {result}")


def save_to_db(isbn, title, author, binding, date, lang, pages, condition):
    """Save full record to shared DB cache sheet (A:H)"""
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
                    "range": f"{SHEET_ID}!A1:H1",
                    "values": [[isbn, title, author, binding, date, lang, pages, condition]],
                }}),
                timeout=10,
            )
    except Exception as e:
        log.warning(f"DB cache write failed (non-critical): {e}")


def lookup_db_cache(isbn: str):
    """Returns (title, author, binding, date, lang, pages, condition) or all None"""
    try:
        r = requests.get(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!A1:H5000",
            headers=headers(), timeout=10,
        )
        rows = r.json().get("data", {}).get("valueRange", {}).get("values", [])
        for row in rows[1:]:
            if row and clean_isbn(row[0]) == isbn:
                def _get(i): return "" if len(row) <= i or row[i] is None else str(row[i])
                title   = _get(1)
                author  = _get(2)
                binding = _get(3)
                date    = _get(4)
                lang    = _get(5)
                pages   = _get(6)
                cond    = _get(7)
                if title:
                    return title, author, binding, date, lang, pages, cond
    except Exception as e:
        log.warning(f"DB cache lookup failed: {e}")
    return None, None, None, None, None, None, None

# =============================================
# API Lookup
# =============================================
def _parse_google_binding(info: dict, item: dict) -> str:
    """
    Google Books has no dedicated binding field; infer from available signals:
    - item["saleInfo"]["isEbook"] -> True means digital edition
    - info["printType"]           -> "BOOK" | "MAGAZINE"
    - info["binding"]             -> present on some entries
    For standard print books Google cannot distinguish hardcover/paperback;
    return "" and let Open Library fill the gap.
    """
    sale_info = item.get("saleInfo", {})
    if sale_info.get("isEbook"):
        return "eBook"
    print_type = info.get("printType", "").upper()
    if print_type == "MAGAZINE":
        return "Magazine"
    binding = info.get("binding", "")
    if binding:
        return binding
    return ""


def lookup_google(isbn: str):
    """Returns (title, author, binding, date, lang, pages) or all None"""
    for key in API_KEYS:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}&country=US"
        if key:
            url += f"&key={key}"
        try:
            data = requests.get(url, timeout=8).json()
            if data.get("error", {}).get("code") in (429, 503):
                continue
            if data.get("totalItems", 0) > 0:
                info    = data["items"][0]["volumeInfo"]
                title   = info.get("title", "")
                author  = ", ".join(info.get("authors", []))
                binding = _parse_google_binding(info, data["items"][0])
                date    = info.get("publishedDate", "")
                lang    = info.get("language", "")
                pages   = str(info.get("pageCount", "")) if info.get("pageCount") else ""
                return title, author, binding, date, lang, pages
        except Exception:
            continue
    return None, None, None, None, None, None


def _parse_ol_language(data: dict) -> str:
    """Parse OL language field: [{"key": "/languages/eng"}] -> "eng" """
    langs = data.get("languages", [])
    if langs and isinstance(langs, list):
        key = langs[0].get("key", "")
        return key.split("/")[-1] if key else ""
    return ""


def _ol_isbn_endpoint(isbn: str) -> dict:
    """
    Query OL's /isbn/{isbn}.json for extended fields.
    Redirects (302) to /books/OLxxxxxxM.json which has more complete data.
    Returns a dict of extra fields, or empty dict on failure.
    """
    try:
        resp = requests.get(
            f"https://openlibrary.org/isbn/{isbn}.json",
            allow_redirects=True,
            timeout=15,
        )
        data = resp.json()
        return {
            "physical_format": data.get("physical_format", ""),
            "publish_date":    data.get("publish_date", ""),
            "languages":       _parse_ol_language(data),
            "number_of_pages": str(data.get("number_of_pages", "")) if data.get("number_of_pages") else "",
        }
    except Exception as e:
        log.warning(f"OL isbn endpoint failed ({isbn}): {e}")
        return {}


def lookup_openlibrary(isbn: str):
    """Returns (title, author, binding, date, lang, pages) or all None"""
    try:
        data = requests.get(
            f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data",
            timeout=8,
        ).json()
        info = data.get(f"ISBN:{isbn}")
        if info:
            title   = info.get("title", "")
            author  = ", ".join(a["name"] for a in info.get("authors", []))
            binding = info.get("physical_format", "")
            date    = info.get("publish_date", "")
            lang    = _parse_ol_language(info) if "languages" in info else ""
            pages   = str(info.get("number_of_pages", "")) if info.get("number_of_pages") else ""

            # fallback to /isbn/ endpoint for any missing fields
            if not binding or not date or not lang or not pages:
                extra   = _ol_isbn_endpoint(isbn)
                binding = binding or extra.get("physical_format", "")
                date    = date    or extra.get("publish_date", "")
                lang    = lang    or extra.get("languages", "")
                pages   = pages   or extra.get("number_of_pages", "")

            return title, author, binding, date, lang, pages
    except Exception:
        pass
    return None, None, None, None, None, None

# =============================================
# Resolve (merge both sources)
# =============================================
def resolve(isbn: str) -> tuple[str, str, str, str, str, str, str]:
    """Returns (title, author, binding, date, lang, pages, condition)"""
    t, a, b, d, l, p, c = lookup_db_cache(isbn)
    if t:
        return t, a, b, d, l, p, "DB"

    gb_title, gb_author, gb_binding, gb_date, gb_lang, gb_pages = lookup_google(isbn)
    ol_title, ol_author, ol_binding, ol_date, ol_lang, ol_pages = lookup_openlibrary(isbn)

    # --- title / author / condition ---
    if gb_title and ol_title:
        match = gb_title.strip().lower() == ol_title.strip().lower()
        condition = "Google + OL" if match else "Mismatch"
        title, author = gb_title, gb_author
    elif gb_title:
        title, author, condition = gb_title, gb_author, "Google Books"
    elif ol_title:
        title, author, condition = ol_title, ol_author, "Open Library"
    else:
        return "Not Found", "", "", "", "", "", "Not Found"

    # --- merge fields: prefer OL, fallback to Google ---
    binding = ol_binding or gb_binding or ""
    date    = ol_date    or gb_date    or ""
    lang    = ol_lang    or gb_lang    or ""
    pages   = ol_pages   or gb_pages   or ""

    save_to_db(isbn, title, author, binding, date, lang, pages, condition)
    return title, author, binding, date, lang, pages, condition

# =============================================
# Main Loop — now iterates over all input sources
# =============================================
def sync_source(source: dict) -> int:
    """Process one input source. Returns number of rows written."""
    name = source["name"]
    sp_token = source["spreadsheet_token"]
    sh_id = source["sheet_id"]

    rows = read_input_sheet(sp_token, sh_id)
    pending = [r for r in rows if not r.get("title", "").strip()]

    if not pending:
        return 0

    log.info(f"[{name}] Found {len(pending)} ISBN(s) to look up")
    count = 0
    for r in pending:
        isbn    = r["isbn"]
        row_num = r["_row"]
        try:
            title, author, binding, date, lang, pages, condition = resolve(isbn)
            write_back(sp_token, sh_id, row_num, title, author, binding, condition)
            log.info(f"  ✅ [{name}] Row {row_num} | {isbn} → {title} / {author} [{binding}] {date} ({condition})")
            count += 1
            time.sleep(0.5)
        except Exception as e:
            log.error(f"  ❌ [{name}] Row {row_num} | {isbn} → Error: {e}")

    return count


def sync_once() -> int:
    total = 0
    for source in INPUT_SOURCES:
        try:
            total += sync_source(source)
        except Exception as e:
            log.error(f"[{source.get('name')}] sync_source error: {e}")
    return total


def main():
    log.info("=" * 50)
    log.info("ISBN Sync daemon started")
    log.info(f"Poll interval: {POLL_INTERVAL}s")
    log.info(f"Input sources: {[s['name'] for s in INPUT_SOURCES]}")
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