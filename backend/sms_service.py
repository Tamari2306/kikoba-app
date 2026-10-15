"""
sms_service.py  -  Kikoba web app SMS (Beem Africa)

Drop this file next to app.py (kikoba-app/backend/).

Render environment variables (Dashboard -> your service -> Environment):
    BEEM_API_KEY      your Beem API key
    BEEM_SECRET_KEY   your Beem secret key
    BEEM_SENDER_ID    approved sender ID (default: LOOPAY)

Beem facts this module is built around (checked against Beem's public API
and SDKs):
    - Endpoint: POST https://apisms.beem.africa/v1/send, HTTP Basic auth
      (api key as username, secret as password).
    - One call can carry many recipients, but they all get the SAME text.
      Personalised texts therefore need one call each.
    - Beem does NOT support Unicode / emojis in SMS. Everything is forced
      to plain ASCII here (the Swahili alphabet is fine).
    - Phone numbers must look like 255712345678.
"""

import os
import re
import time
import unicodedata

import requests
from concurrent.futures import ThreadPoolExecutor

BEEM_URL   = "https://apisms.beem.africa/v1/send"
BATCH_SIZE = 100      # recipients per API call (same message only)
TIMEOUT    = 20       # seconds
WORKERS    = 5        # API calls in flight at once
MAX_CHARS  = 459      # 3 SMS parts of 153 characters


# ---------------------------------------------------------------- config
def is_configured():
    return bool(os.environ.get("BEEM_API_KEY") and os.environ.get("BEEM_SECRET_KEY"))


def sender_id():
    return os.environ.get("BEEM_SENDER_ID", "LOOPAY")


# ---------------------------------------------------------------- helpers
def normalize_phone(raw):
    """
    Return a Tanzanian mobile number as 255XXXXXXXXX, or None if it can't
    be one.  Accepts +255712345678, 255712345678, 0712345678, 712345678,
    and spaces/dashes inside the number.
    """
    if raw is None:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if digits.startswith("00255"):
        digits = digits[2:]
    if digits.startswith("255") and len(digits) == 12:
        pass
    elif digits.startswith("0") and len(digits) == 10:
        digits = "255" + digits[1:]
    elif len(digits) == 9:
        digits = "255" + digits
    else:
        return None
    return digits if re.fullmatch(r"255[67]\d{8}", digits) else None


_PUNCT = {
    "\u2014": "-", "\u2013": "-", "\u2018": "'", "\u2019": "'",
    "\u201c": '"', "\u201d": '"', "\u2026": "...", "\u00a0": " ",
}


def ascii_safe(text):
    """Make text safe for Beem: no emojis, no curly quotes, no accents."""
    text = str(text or "")
    for bad, good in _PUNCT.items():
        text = text.replace(bad, good)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def segments(text):
    """How many SMS parts a message will be billed as."""
    n = len(text or "")
    if n == 0:
        return 0
    return 1 if n <= 160 else -(-n // 153)


# ---------------------------------------------------------------- sending
def _post(phones, message):
    """One API call: same message to up to BATCH_SIZE phones."""
    payload = {
        "source_addr":   sender_id(),
        "schedule_time": "",
        "encoding":      0,
        "message":       message,
        "recipients":    [{"recipient_id": str(i + 1), "dest_addr": p}
                          for i, p in enumerate(phones)],
    }
    auth = (os.environ["BEEM_API_KEY"], os.environ["BEEM_SECRET_KEY"])

    # Only retry when the connection itself failed (nothing was sent).
    # A read timeout is ambiguous - the text may have gone out - so it is
    # NOT retried, to avoid double-texting people.
    last_error = "unknown error"
    for attempt in range(2):
        try:
            resp = requests.post(BEEM_URL, json=payload, auth=auth, timeout=TIMEOUT)
        except requests.ConnectionError as e:
            last_error = f"connection failed ({e.__class__.__name__})"
            time.sleep(1.5)
            continue
        except requests.Timeout:
            return {"ok": False, "request_id": "",
                    "error": "timed out - it may or may not have been delivered"}
        except requests.RequestException as e:
            return {"ok": False, "request_id": "", "error": f"request error ({e.__class__.__name__})"}

        try:
            data = resp.json()
        except ValueError:
            data = {}

        ok = resp.status_code == 200 and (
            data.get("successful") is True or str(data.get("code")) == "100"
        )
        if ok:
            return {"ok": True, "request_id": str(data.get("request_id") or ""), "error": None}
        if resp.status_code in (401, 403):
            return {"ok": False, "request_id": "", "error": "Beem rejected the API key/secret"}
        return {"ok": False, "request_id": str(data.get("request_id") or ""),
                "error": str(data.get("message") or f"HTTP {resp.status_code}")}

    return {"ok": False, "request_id": "", "error": last_error}


def send_many(items):
    """
    items: list of {"phone": <any format>, "message": <text>}
    Returns a list (same order) of
        {"phone": "255...", "ok": bool, "request_id": str, "error": str|None}

    People getting the identical message are grouped into as few API calls
    as possible (up to BATCH_SIZE each); personalised messages go one by one.
    """
    results = [None] * len(items)

    if not is_configured():
        err = "SMS not configured (set BEEM_API_KEY and BEEM_SECRET_KEY)"
        return [{"phone": None, "ok": False, "request_id": "", "error": err} for _ in items]

    groups = {}   # message -> list of (index, phone)
    for i, item in enumerate(items):
        phone = normalize_phone(item.get("phone"))
        message = ascii_safe(item.get("message"))
        if not phone:
            results[i] = {"phone": None, "ok": False, "request_id": "", "error": "invalid phone number"}
        elif not message or len(message) > MAX_CHARS:
            results[i] = {"phone": phone, "ok": False, "request_id": "", "error": "empty or too-long message"}
        else:
            groups.setdefault(message, []).append((i, phone))

    jobs = []
    for message, members in groups.items():
        for start in range(0, len(members), BATCH_SIZE):
            jobs.append((message, members[start:start + BATCH_SIZE]))

    def run(job):
        message, chunk = job
        return chunk, _post([p for _, p in chunk], message)

    if len(jobs) > 1:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            outcomes = list(pool.map(run, jobs))
    else:
        outcomes = [run(j) for j in jobs]

    for chunk, outcome in outcomes:
        for idx, phone in chunk:
            results[idx] = {"phone": phone, **outcome}

    return results


def send_one(phone, message):
    return send_many([{"phone": phone, "message": message}])[0]