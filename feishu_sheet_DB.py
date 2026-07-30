"""
isbn_sync.py — Auto-sync daemon
Continuously monitors multiple Feishu ISBN_match sheets (input sources).
When a new ISBN is found with no Title, it looks it up and writes back Title/Author/Binding/Condition.
Date/Language/Pages are stored in the shared DB cache only.

Each input source runs its own independent polling loop in its own thread,
so a slow source (many pending ISBNs) never blocks another source (few
pending ISBNs) from picking up new rows.

ISBN resolution is protected by a per-ISBN lock + in-process cache: if two
source threads try to resolve the SAME ISBN at nearly the same time, only
one of them actually calls the Google/OpenLibrary APIs and writes to the
DB cache — the other waits and reuses that result.

The in-process cache is intentionally NOT a rolling TTL per ISBN. Instead,
the whole cache is wiped clean at fixed local times each day (see
CACHE_RESET_TIMES below), so that manual edits to the shared Feishu DB
cache sheet are guaranteed to be picked up again within a bounded window,
while still avoiding duplicate API calls for the same ISBN within a short
span of time.

Feishu access tokens are refreshed proactively on a timer, AND reactively
whenever Feishu reports the token as invalid/expired (error code 99991663)
— this handles cases where Feishu invalidates a token sooner than our
local timer expects. All Feishu-calling functions retry once after a
forced token refresh before giving up.

Additionally, a low-frequency "stats" worker periodically reads the ISBN
column from EVERY input source independently (not just pending rows),
counts how many times each ISBN appears per source, and writes a summary
table (ISBN | Title | Author | <source1> | <source2> | ... | Total) to a
separate output sheet. This runs on its own, much slower interval so it
never competes with the real-time resolution workers for API/rate-limit
budget.

Usage: python isbn_sync.py
"""
import os
import json
import time
import logging
import threading
import datetime
import signal
from collections import Counter
from zoneinfo import ZoneInfo
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

# --- Frequency stats worker config ---
STATS_INTERVAL          = int(os.getenv("STATS_INTERVAL", "300"))  # seconds; slow on purpose
STATS_OUTPUT_TOKEN      = os.getenv("STATS_OUTPUT_SPREADSHEET_TOKEN", "")
STATS_OUTPUT_SHEET_ID   = os.getenv("STATS_OUTPUT_SHEET_ID", "")

# Feishu error code for "invalid/expired access token"
FEISHU_INVALID_TOKEN_CODE = 99991663

_raw_keys = os.getenv("GOOGLE_API_KEYS", ",")
API_KEYS = [k.strip() for k in _raw_keys.split(",")]

# =============================================
# In-process resolve() cache reset schedule
# The whole _memory_cache dict is cleared at these local times every day
# (24h clock), so manual edits to the DB cache sheet are re-read within
# a bounded window instead of being masked forever by a stale in-memory
# result. Timezone is explicit so this doesn't depend on the server's
# system timezone (which is UTC) — DST is handled automatically.
# =============================================
LOCAL_TZ = ZoneInfo("America/Detroit")
CACHE_RESET_TIMES = [(0, 0)]  # (hour, minute) pairs, 24h clock, local time

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
# Feishu Token (refresh every 110 minutes, or immediately on demand)
# Thread-safe.
# =============================================
_token_cache = {"token": None, "expires_at": 0}
_token_lock = threading.Lock()

def get_token(force_refresh: bool = False) -> str:
    """
    Returns a valid Feishu tenant_access_token.
    - Normally returns the cached token if it hasn't hit our local expiry timer.
    - If force_refresh=True, ALWAYS fetches a fresh token regardless of the
      timer — used when Feishu itself reports the cached token as invalid,
      since Feishu's real expiry can occur sooner than our local estimate.
    """
    if not force_refresh and time.time() < _token_cache["expires_at"]:
        return _token_cache["token"]
    with _token_lock:
        # re-check inside lock in case another thread already refreshed it
        if not force_refresh and time.time() < _token_cache["expires_at"]:
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
        log.info(f"Feishu token refreshed{' (forced — previous token was rejected)' if force_refresh else ''}")
        return _token_cache["token"]

def invalidate_token():
    """Force the next get_token() call to fetch a brand new token."""
    with _token_lock:
        _token_cache["expires_at"] = 0

def headers(force_refresh: bool = False):
    return {"Authorization": f"Bearer {get_token(force_refresh)}", "Content-Type": "application/json"}


def _is_invalid_token_response(payload: dict) -> bool:
    return isinstance(payload, dict) and payload.get("code") == FEISHU_INVALID_TOKEN_CODE

# =============================================
# ISBN Cleaning
# =============================================
def clean_isbn(raw) -> str:
    """
    Normalize a raw cell value into a plain digit-only ISBN string.

    IMPORTANT — bug history: this used to do
        str(int(float(s))) if s else ""
    for anything that wasn't caught by the except branch. That's fine for
    values like "9780593437476" (no decimal point — float() just round-trips
    it exactly, since Python's float has enough precision for a 13-digit
    integer). But if the upstream source (OCR / barcode recognition, manual
    entry, a spreadsheet auto-formatting a cell, etc.) ever produces a value
    that ACTUALLY contains a decimal point — e.g. "9.780593437476" instead of
    "9780593437476" — then float(s) parses it as the real number ~9.78, and
    int(...) truncates it down to just "9". Two completely different ISBNs
    mangled this way (e.g. "9.780593437476" and "9.781728297149") can both
    collapse to "9", which then makes resolve()'s per-ISBN lock/cache treat
    them as the SAME book — corrupting both rows with whichever one resolved
    first.

    Fix: if the string contains a literal decimal point and isn't scientific
    notation, treat it as mangled digits and just strip non-digit characters
    directly, instead of round-tripping through float/int (which silently
    truncates instead of erroring).
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""

    has_dot = "." in s
    is_scientific = "e" in s.lower()

    if has_dot and not is_scientific:
        # A decimal point here almost certainly means the ISBN got mangled
        # upstream (e.g. "9.780593437476"). Don't let float()/int() silently
        # truncate it to "9" — pull the digits out directly instead so all
        # 13 (or 10) digits survive.
        return "".join(c for c in s if c.isdigit())

    try:
        return str(int(float(s))) if s else ""
    except (ValueError, OverflowError):
        return "".join(c for c in s if c.isdigit())

# =============================================
# Feishu Read / Write (parameterized by source)
# Each function retries ONCE with a forcibly-refreshed token if Feishu
# reports the token as invalid/expired.
# =============================================
def read_input_sheet(spreadsheet_token: str, sheet_id: str) -> list[dict]:
    def _do(force_refresh: bool):
        r = requests.get(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{sheet_id}!A1:E5000",
            headers=headers(force_refresh),
            timeout=15,
        )
        return r.json()

    payload = _do(False)
    if _is_invalid_token_response(payload):
        log.warning(f"Access token rejected reading ({spreadsheet_token}/{sheet_id}); forcing refresh and retrying")
        invalidate_token()
        payload = _do(True)

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
            # Sanity check: a real ISBN-10/ISBN-13 is always 10 or 13 digits.
            # If clean_isbn() produced something shorter/longer, the source
            # value was malformed (truncated float, partial OCR read, etc.)
            # — skip it rather than silently resolving/writing garbage that
            # could collide in the cache with an unrelated ISBN.
            isbn_len = len(rec["isbn"])
            if isbn_len not in (10, 13):
                log.warning(
                    f"  ⚠️  Row {i}: suspicious ISBN '{rec['isbn']}' "
                    f"(len={isbn_len}, raw={padded[header.index('isbn')]!r}) — skipping"
                )
                continue
            records.append(rec)
    return records


def write_back(spreadsheet_token: str, sheet_id: str, row_num: int, title: str, author: str, binding: str, condition: str):
    """Write back B:E — Title / Author / Binding / Condition (match sheet only)"""
    payload_body = {"valueRange": {
        "range": f"{sheet_id}!B{row_num}:E{row_num}",
        "values": [[title, author, binding, condition]],
    }}

    def _do(force_refresh: bool):
        r = requests.put(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values",
            headers=headers(force_refresh),
            data=json.dumps(payload_body),
            timeout=10,
        )
        return r.json()

    result = _do(False)
    if _is_invalid_token_response(result):
        log.warning(f"Access token rejected writing row {row_num} ({spreadsheet_token}/{sheet_id}); forcing refresh and retrying")
        invalidate_token()
        result = _do(True)

    if result.get("code", 0) != 0:
        raise RuntimeError(f"Write failed at row {row_num} ({spreadsheet_token}/{sheet_id}): {result}")


# --- DB cache access is shared across all source threads. Guard with a lock
#     so that even outside of the per-ISBN lock (belt-and-suspenders), two
#     writes can't interleave inside the read-then-append sequence.
_db_lock = threading.Lock()


def save_to_db(isbn, title, author, binding, date, lang, pages, condition):
    """Save full record to shared DB cache sheet (A:H)"""
    try:
        with _db_lock:
            def _do_read(force_refresh: bool):
                r = requests.get(
                    f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!A1:A5000",
                    headers=headers(force_refresh), timeout=10,
                )
                return r.json()

            payload = _do_read(False)
            if _is_invalid_token_response(payload):
                invalidate_token()
                payload = _do_read(True)

            rows = payload.get("data", {}).get("valueRange", {}).get("values", [])
            existing_isbns = [clean_isbn(row[0]) for row in rows[1:] if row]

            if isbn not in existing_isbns:
                def _do_append(force_refresh: bool):
                    r = requests.post(
                        f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_append",
                        headers=headers(force_refresh),
                        data=json.dumps({"valueRange": {
                            "range": f"{SHEET_ID}!A1:H1",
                            "values": [[isbn, title, author, binding, date, lang, pages, condition]],
                        }}),
                        timeout=10,
                    )
                    return r.json()

                append_result = _do_append(False)
                if _is_invalid_token_response(append_result):
                    invalidate_token()
                    _do_append(True)
    except Exception as e:
        log.warning(f"DB cache write failed (non-critical): {e}")


def lookup_db_cache(isbn: str):
    """Returns (title, author, binding, date, lang, pages, condition) or all None"""
    try:
        def _do(force_refresh: bool):
            r = requests.get(
                f"{FEISHU_BASE}/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values/{SHEET_ID}!A1:H5000",
                headers=headers(force_refresh), timeout=10,
            )
            return r.json()

        payload = _do(False)
        if _is_invalid_token_response(payload):
            invalidate_token()
            payload = _do(True)

        rows = payload.get("data", {}).get("valueRange", {}).get("values", [])
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
# Resolve (merge both sources), thread-safe via per-ISBN lock + memory cache
#
# If two source threads try to resolve the SAME isbn at nearly the same
# time, without this only one of them would win the DB-cache dedup check
# by a hair — but timing with Feishu's read-after-write isn't guaranteed,
# so both could end up thinking "not in DB yet" and both write. This lock
# makes resolution of any single ISBN strictly one-at-a-time, in-process,
# regardless of Feishu's consistency behavior.
#
# The in-process cache (_memory_cache) is a plain dict with NO per-entry
# TTL. Instead, cache_reset_worker() wipes the whole dict clean at fixed
# times every day (see CACHE_RESET_TIMES), so:
#   - within a day, repeated lookups of the same ISBN never hit the API
#     or even the DB cache sheet again (fast, cheap)
#   - manual edits to the DB cache sheet are guaranteed to be picked up
#     again after the next scheduled reset, instead of being masked
#     forever by a stale in-memory result
#
# IMPORTANT: when a second thread reuses a cached result, the returned
# "condition" (e.g. "Google + OL") is whatever the FIRST thread computed.
# It is intentionally NOT rewritten to "DB" — "DB" is reserved for rows
# actually found in the persisted Feishu DB-cache sheet. Watch for the
# "⚡ reused..." log lines to know when a cache-hit happened vs a fresh
# lookup, since the printed condition alone can't tell you that.
#
# NOTE: this lock/cache is keyed by the ISBN string produced by
# clean_isbn(). It is only as correct as that key — if clean_isbn() ever
# collapses two different ISBNs to the same string (as it used to for
# malformed "9.780593437476"-style values, see clean_isbn()'s docstring),
# this whole mechanism will confidently and silently treat them as one
# book. read_input_sheet() now filters out any ISBN that isn't exactly
# 10 or 13 digits after cleaning, as a second line of defense.
# =============================================
_isbn_locks: dict[str, threading.Lock] = {}
_isbn_locks_guard = threading.Lock()

_memory_cache: dict[str, tuple] = {}
_memory_cache_lock = threading.Lock()


def _get_isbn_lock(isbn: str) -> threading.Lock:
    with _isbn_locks_guard:
        lock = _isbn_locks.get(isbn)
        if lock is None:
            lock = threading.Lock()
            _isbn_locks[isbn] = lock
        return lock


def resolve(isbn: str) -> tuple[str, str, str, str, str, str, str]:
    """
    Thread-safe wrapper: ensures only one thread ever actually performs
    the lookup + save_to_db for a given isbn between scheduled cache
    resets. Other threads asking for the same isbn will block briefly
    and then reuse the already-computed result.
    """
    with _memory_cache_lock:
        cached = _memory_cache.get(isbn)
    if cached:
        log.info(f"  ⚡ {isbn} → reused in-process cache (no duplicate API call)")
        return cached

    isbn_lock = _get_isbn_lock(isbn)
    with isbn_lock:
        # Another thread may have finished resolving this isbn while we
        # were waiting for the lock — check again before doing any work.
        with _memory_cache_lock:
            cached = _memory_cache.get(isbn)
        if cached:
            log.info(f"  ⚡ {isbn} → reused result from concurrent lookup (waited on lock)")
            return cached

        result = _resolve_uncached(isbn)

        with _memory_cache_lock:
            _memory_cache[isbn] = result
        return result


def _resolve_uncached(isbn: str) -> tuple[str, str, str, str, str, str, str]:
    """
    Actual resolve logic (DB cache -> Google/OpenLibrary -> merge -> save).
    Only ever called while holding that isbn's lock, so it's safe even
    though it isn't itself thread-safe.
    """
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
# Daily cache reset worker
# Wipes the entire in-process _memory_cache at fixed local times each day
# (America/Detroit, DST-aware), so resolve() re-reads the DB cache sheet
# (and re-hits the APIs if still not found there) after each reset,
# picking up any manual edits made to the DB sheet in the meantime.
# =============================================
def _next_reset_time(now: datetime.datetime) -> datetime.datetime:
    candidates = []
    for h, m in CACHE_RESET_TIMES:
        candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= now:
            candidate += datetime.timedelta(days=1)
        candidates.append(candidate)
    return min(candidates)


def _clear_memory_cache(reason: str):
    with _memory_cache_lock:
        count = len(_memory_cache)
        _memory_cache.clear()
    log.info(f"[cache] Reset ({reason}) — cleared {count} cached ISBN(s), will re-read from DB sheet as needed")


def _handle_manual_reset_signal(signum, frame):
    _clear_memory_cache("manual — SIGUSR1 received")


def cache_reset_worker():
    while True:
        now = datetime.datetime.now(LOCAL_TZ)
        next_reset = _next_reset_time(now)
        sleep_seconds = (next_reset - now).total_seconds()
        log.info(f"[cache] Next in-process cache reset at {next_reset.strftime('%Y-%m-%d %H:%M:%S %Z')} (in {sleep_seconds/3600:.1f}h)")
        time.sleep(sleep_seconds)
        _clear_memory_cache("scheduled")

# =============================================
# Per-source sync (one pass over one source's pending rows)
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

# =============================================
# Independent per-source worker loop
# =============================================
def source_worker(source: dict):
    """
    Runs forever in its own thread, polling ONLY this source.
    Never waits on any other source — a source with a long backlog
    just keeps looping on its own cadence without blocking others.
    """
    name = source["name"]
    log.info(f"[{name}] worker started")
    while True:
        try:
            count = sync_source(source)
            if count:
                log.info(f"[{name}] Done — {count} row(s) written. Next check in {POLL_INTERVAL}s")
            else:
                log.debug(f"[{name}] Nothing to do. Next check in {POLL_INTERVAL}s")
        except Exception as e:
            log.error(f"[{name}] sync_source error: {e}")
        time.sleep(POLL_INTERVAL)

# =============================================
# Frequency stats worker
#
# Independently of the resolve/write-back workers above, this worker
# periodically re-reads the ISBN column from EVERY input source (the
# full sheet, not just pending rows), counts how many times each ISBN
# appears per source, and writes a summary table to a separate output
# sheet:
#
#   ISBN | Title | Author | <source1 name> | <source2 name> | ... | Total
#
# It runs on its own STATS_INTERVAL (default 300s / 5min) — much slower
# than POLL_INTERVAL — since counting doesn't need to be near-real-time
# and re-reading full sheets on every tick would otherwise add avoidable
# load on top of the resolution workers.
#
# Title/Author for the summary are pulled from the shared DB cache
# sheet (lookup_db_cache), so no extra Google/OpenLibrary API calls are
# made just for stats — if an ISBN hasn't been resolved yet those
# columns are simply left blank until resolve() fills the DB cache in
# through the normal sync flow.
# =============================================
def _stats_sources() -> list[dict]:
    """
    Sources counted toward inventory stats. A source is excluded by
    setting "include_in_stats": false on it in INPUT_SOURCES_JSON — e.g.
    a testing/sandbox source that should still go through normal ISBN
    resolution (source_worker) but must not affect inventory counts.
    Defaults to included if the key is absent.
    """
    return [s for s in INPUT_SOURCES if s.get("include_in_stats", True)]


def compute_isbn_frequency() -> dict:
    """
    Reads the ISBN column of every stats-eligible input source independently
    and counts occurrences per source. Sources marked
    include_in_stats: false (e.g. a testing source) are skipped entirely.
    Returns: {isbn: {source_name: count, ...}, ...}
    """
    freq_by_source = {}
    for source in _stats_sources():
        rows = read_input_sheet(source["spreadsheet_token"], source["sheet_id"])
        isbns = [r["isbn"] for r in rows if r.get("isbn")]
        freq_by_source[source["name"]] = Counter(isbns)

    all_isbns = set()
    for counter in freq_by_source.values():
        all_isbns.update(counter.keys())

    merged = {}
    for isbn in all_isbns:
        merged[isbn] = {name: freq_by_source[name].get(isbn, 0) for name in freq_by_source}
    return merged


def _col_letter(n: int) -> str:
    """1 -> A, 2 -> B, ... 27 -> AA (simple A1-notation column helper)"""
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _current_stats_row_count() -> int:
    """
    Queries the stats output sheet for how many rows currently have
    content (by reading column A). Used instead of an in-process counter
    so that "how far back do we need to clear" survives process restarts
    — an in-memory counter would reset to 0 on every restart and forget
    about a much wider write from an earlier session, leaving those old
    rows stuck forever. Costs one extra read per stats cycle (every
    STATS_INTERVAL, so negligible).
    """
    if not STATS_OUTPUT_TOKEN or not STATS_OUTPUT_SHEET_ID:
        return 0

    def _do(force_refresh: bool):
        r = requests.get(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{STATS_OUTPUT_TOKEN}/values/{STATS_OUTPUT_SHEET_ID}!A1:A5000",
            headers=headers(force_refresh),
            timeout=15,
        )
        return r.json()

    payload = _do(False)
    if _is_invalid_token_response(payload):
        invalidate_token()
        payload = _do(True)

    if payload.get("code", 0) != 0:
        log.warning(f"[stats] Could not read existing stats sheet row count: {payload}")
        return 0

    rows = payload.get("data", {}).get("valueRange", {}).get("values", [])
    return len(rows)


def write_stats(merged: dict):
    """
    Full overwrite of the stats output sheet with the latest counts.
    Columns: ISBN | <source1> | <source2> | ... | Total
    Also blanks out any leftover rows from a previous, longer write —
    checked against the sheet's ACTUAL current row count (not an
    in-process counter) so stale ("zombie") rows get cleared even after
    a daemon restart.
    """
    if not STATS_OUTPUT_TOKEN or not STATS_OUTPUT_SHEET_ID:
        log.warning("[stats] STATS_OUTPUT_SPREADSHEET_TOKEN / STATS_OUTPUT_SHEET_ID not configured — skipping write")
        return

    source_names = [s["name"] for s in _stats_sources()]
    header = ["ISBN"] + source_names + ["Total"]
    rows = [header]

    for isbn, counts in sorted(merged.items(), key=lambda kv: -sum(kv[1].values())):
        per_source = [counts.get(name, 0) for name in source_names]
        rows.append([isbn] + per_source + [sum(per_source)])

    num_cols = len(header)
    last_col = _col_letter(num_cols)

    # Pad with fully-blank rows up to the sheet's ACTUAL current size,
    # so any rows this round doesn't use get explicitly cleared instead
    # of left with a previous, wider write's stale values.
    existing_row_count = _current_stats_row_count()
    total_rows_to_write = max(len(rows), existing_row_count)
    blank_row = [""] * num_cols
    while len(rows) < total_rows_to_write:
        rows.append(blank_row)

    payload_body = {"valueRange": {
        "range": f"{STATS_OUTPUT_SHEET_ID}!A1:{last_col}{len(rows)}",
        "values": rows,
    }}

    def _do(force_refresh: bool):
        r = requests.put(
            f"{FEISHU_BASE}/sheets/v2/spreadsheets/{STATS_OUTPUT_TOKEN}/values",
            headers=headers(force_refresh),
            data=json.dumps(payload_body),
            timeout=15,
        )
        return r.json()

    result = _do(False)
    if _is_invalid_token_response(result):
        log.warning("[stats] Access token rejected writing stats; forcing refresh and retrying")
        invalidate_token()
        result = _do(True)

    if result.get("code", 0) != 0:
        log.error(f"[stats] Write failed: {result}")


def stats_worker():
    log.info(f"[stats] worker started, interval={STATS_INTERVAL}s")
    while True:
        try:
            merged = compute_isbn_frequency()
            write_stats(merged)
            log.info(f"[stats] Updated frequency for {len(merged)} distinct ISBN(s) across {len(INPUT_SOURCES)} source(s)")
        except Exception as e:
            log.error(f"[stats] error: {e}")
        time.sleep(STATS_INTERVAL)

# =============================================
# Main — spins up one worker thread per source, plus the cache reset
# worker and the stats worker, then just watches them.
# =============================================
def main():
    log.info("=" * 50)
    log.info("ISBN Sync daemon started")
    log.info(f"Poll interval: {POLL_INTERVAL}s")
    log.info(f"Input sources: {[s['name'] for s in INPUT_SOURCES]} (independent per-source workers)")
    log.info(f"In-process cache reset times: {CACHE_RESET_TIMES} ({LOCAL_TZ})")
    log.info(f"Stats interval: {STATS_INTERVAL}s -> {STATS_OUTPUT_TOKEN or '(not configured)'}/{STATS_OUTPUT_SHEET_ID or '(not configured)'}")
    if hasattr(signal, "SIGUSR1"):
        signal.signal(signal.SIGUSR1, _handle_manual_reset_signal)
        log.info("Manual cache reset: kill -USR1 <pid>, or `systemctl kill -s SIGUSR1 isbn_sync`")
    elif hasattr(signal, "SIGBREAK"):
        # Windows has no SIGUSR1. SIGBREAK (triggered by Ctrl+Break in the
        # terminal — NOT Ctrl+C, which would kill the process) is used here
        # purely so the manual-reset feature can be tested locally. The
        # real deployment on the Linux VPS uses SIGUSR1 above.
        signal.signal(signal.SIGBREAK, _handle_manual_reset_signal)
        log.info("Manual cache reset (local Windows testing): press Ctrl+Break in this terminal")
    else:
        log.info("Manual cache reset signal is unavailable on this platform — skipped")
    log.info("=" * 50)

    threads = {}
    for source in INPUT_SOURCES:
        t = threading.Thread(target=source_worker, args=(source,), daemon=True, name=source["name"])
        t.start()
        threads[source["name"]] = (t, source)

    cache_thread = threading.Thread(target=cache_reset_worker, daemon=True, name="cache_reset")
    cache_thread.start()
    threads["cache_reset"] = (cache_thread, None)

    stats_thread = threading.Thread(target=stats_worker, daemon=True, name="stats")
    stats_thread.start()
    threads["stats"] = (stats_thread, None)

    # Main thread just supervises: if a worker thread dies unexpectedly
    # (shouldn't normally happen since sync_source/stats errors are caught
    # inside their own loops), restart it rather than silently losing that
    # source's polling forever.
    while True:
        time.sleep(30)
        for name, (t, source) in list(threads.items()):
            if not t.is_alive():
                log.error(f"[{name}] worker thread died — restarting")
                if name == "cache_reset":
                    new_t = threading.Thread(target=cache_reset_worker, daemon=True, name=name)
                elif name == "stats":
                    new_t = threading.Thread(target=stats_worker, daemon=True, name=name)
                else:
                    new_t = threading.Thread(target=source_worker, args=(source,), daemon=True, name=name)
                new_t.start()
                threads[name] = (new_t, source)


if __name__ == "__main__":
    main()