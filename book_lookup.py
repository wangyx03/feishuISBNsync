"""
book_lookup.py — Book metadata lookup providers (Google Books + Open Library)

Pure external-data-source lookups: given a SKU (ISBN-10/13), returns
(title, author, binding, date, lang, pages) or all-None if not found.
No Feishu, no threading, no caching — that orchestration stays in
sku_sync.py (see resolve()/_resolve_uncached() there), which imports
lookup_google() and lookup_openlibrary() from this module.

Kept separate so it can be reused by other tools (e.g. picocr's
match_isbn.py) and unit-tested without needing a Feishu connection or
spinning up any worker threads.
"""
import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

_raw_keys = os.getenv("GOOGLE_API_KEYS", ",")
API_KEYS = [k.strip() for k in _raw_keys.split(",")]

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


def lookup_google(sku: str):
    """Returns (title, author, binding, date, lang, pages) or all None"""
    for key in API_KEYS:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{sku}&country=US"
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


def _ol_sku_endpoint(sku: str) -> dict:
    """
    Query OL's /sku/{sku}.json for extended fields.
    Redirects (302) to /books/OLxxxxxxM.json which has more complete data.
    Returns a dict of extra fields, or empty dict on failure.
    """
    try:
        resp = requests.get(
            f"https://openlibrary.org/isbn/{sku}.json",
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
        log.warning(f"OL sku endpoint failed ({sku}): {e}")
        return {}


def lookup_openlibrary(sku: str):
    """Returns (title, author, binding, date, lang, pages) or all None"""
    try:
        data = requests.get(
            f"https://openlibrary.org/api/books?bibkeys=ISBN:{sku}&format=json&jscmd=data",
            timeout=8,
        ).json()
        info = data.get(f"ISBN:{sku}")
        if info:
            title   = info.get("title", "")
            author  = ", ".join(a["name"] for a in info.get("authors", []))
            binding = info.get("physical_format", "")
            date    = info.get("publish_date", "")
            lang    = _parse_ol_language(info) if "languages" in info else ""
            pages   = str(info.get("number_of_pages", "")) if info.get("number_of_pages") else ""

            # fallback to /sku/ endpoint for any missing fields
            if not binding or not date or not lang or not pages:
                extra   = _ol_sku_endpoint(sku)
                binding = binding or extra.get("physical_format", "")
                date    = date    or extra.get("publish_date", "")
                lang    = lang    or extra.get("languages", "")
                pages   = pages   or extra.get("number_of_pages", "")

            return title, author, binding, date, lang, pages
    except Exception:
        pass
    return None, None, None, None, None, None

# =============================================