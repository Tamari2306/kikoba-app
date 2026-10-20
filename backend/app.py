#kikoba-app/backend/app.py
from dotenv import load_dotenv
load_dotenv()

import zipfile
from flask import Flask, redirect, request, jsonify, render_template, send_file, send_from_directory, session, g
from flask_cors import CORS
import psycopg2
from db import get_db, init_app as init_db_app
from models import init_db, calculate_due_date, calculate_penalty 
from datetime import date, datetime, timedelta
from io import BytesIO, StringIO
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfgen import canvas 
from werkzeug.utils import secure_filename
import os
import csv
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2.extras
from config import Config
import calendar


app = Flask(__name__, static_folder="static")
app.config.from_object(Config)

# Load configuration
env = os.environ.get('FLASK_ENV', 'development')

# Initialize CORS
CORS(app, origins=app.config['CORS_ORIGINS'])

# ── Supabase config ───────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON = os.environ.get("SUPABASE_ANON_KEY", "")

# ── Email / Password Reset ────────────────────────────────────────────
app.config["MAIL_SERVER"]         = os.environ.get("MAIL_SERVER",   "smtp.gmail.com")
app.config["MAIL_PORT"]           = int(os.environ.get("MAIL_PORT", 587))
app.config["MAIL_USE_TLS"]        = True
app.config["MAIL_USERNAME"]       = os.environ.get("MAIL_USERNAME", "")
app.config["MAIL_PASSWORD"]       = os.environ.get("MAIL_PASSWORD", "")
app.config["MAIL_DEFAULT_SENDER"] = os.environ.get("MAIL_USERNAME", "noreply@kikoba.app")
# Mail initialized lazily — only when MAIL_USERNAME is configured
mail = None
ts   = None  # Supabase Auth handles password reset tokens

def get_mail():
    """Return mail instance, initializing only if credentials are set."""
    global mail
    if not MAIL_AVAILABLE:
        return None
    if app.config.get("MAIL_USERNAME"):
        if mail is None:
            mail = Mail(app)
        return mail
    return None

# Initialize database teardown
init_db_app(app)

# Create upload folder
UPLOAD_FOLDER = app.config['UPLOAD_FOLDER']
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# ==================== HELPER FUNCTION FOR CURSOR RESULTS ====================
def get_single_value(cursor, default=None):
    """Safely extract single value from cursor result (handles RealDictCursor)"""
    result = cursor.fetchone()
    if not result:
        return default
    values = list(result.values())
    return values[0] if values and values[0] is not None else default

def get_cursor(db):
    """Get cursor with RealDictCursor factory for dictionary results"""
    return db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def get_current_group_id():
    """Extract group_id from session, default to None"""
    return session.get("group_id")

# ==================== SUBSCRIPTION CONSTANTS & HELPERS ====================

RATE_PER_MEMBER = 1000  # TZS per member per month
GRACE_DAYS      = 7


def get_group_subscription_status(db, group_id):
    """
    Returns dict: status (active|free|grace|suspended), days_remaining, is_free
    """
    cursor = get_cursor(db)
    cursor.execute(
        """SELECT subscription_status, subscription_expires,
                  grace_until, is_free
           FROM groups WHERE id = %s""",
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()

    if not row:
        return {"status": "active", "is_free": False, "days_remaining": 999}

    is_free   = bool(row.get("is_free", 0))
    status    = row.get("subscription_status") or "active"
    expires   = row.get("subscription_expires")
    grace_end = row.get("grace_until")
    today     = date.today()

    if is_free or status == "free":
        return {"status": "free", "is_free": True, "days_remaining": 999,
                "grace_until": None}

    if expires is None:
        return {"status": "active", "is_free": False, "days_remaining": 999,
                "grace_until": None}

    if isinstance(expires, str):
        expires = datetime.strptime(expires[:10], "%Y-%m-%d").date()
    if isinstance(grace_end, str):
        grace_end = datetime.strptime(grace_end[:10], "%Y-%m-%d").date()

    days_remaining = (expires - today).days

    if days_remaining >= 0:
        return {"status": "active", "is_free": False,
                "days_remaining": days_remaining, "grace_until": grace_end}

    if grace_end and today <= grace_end:
        return {"status": "grace", "is_free": False,
                "days_remaining": (grace_end - today).days,
                "grace_until": grace_end}

    return {"status": "suspended", "is_free": False,
            "days_remaining": days_remaining, "grace_until": grace_end}


def generate_monthly_bill(db, group_id):
    """Generate a subscription record for the current month if one doesn't exist."""
    today        = date.today()
    period_start = today.replace(day=1)
    import calendar as _cal
    last_day   = _cal.monthrange(today.year, today.month)[1]
    period_end = today.replace(day=last_day)

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id FROM subscriptions WHERE group_id=%s AND period_start=%s",
        (group_id, period_start)
    )
    if cursor.fetchone():
        cursor.close()
        return

    cursor.execute(
        "SELECT COUNT(*) FROM members WHERE group_id=%s AND is_active=1",
        (group_id,)
    )
    member_count = get_single_value(cursor, 0)

    cursor.execute("SELECT COALESCE(is_free,0) FROM groups WHERE id=%s", (group_id,))
    row = cursor.fetchone()
    is_free = bool(list(row.values())[0]) if row else False

    amount_due = 0 if is_free else int(member_count) * RATE_PER_MEMBER
    status     = "free" if is_free else "unpaid"

    try:
        cursor.execute(
            """INSERT INTO subscriptions
               (group_id, period_start, period_end, member_count, amount_due, status)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (group_id, period_start) DO NOTHING""",
            (group_id, period_start, period_end, member_count, amount_due, status)
        )
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"generate_monthly_bill error: {e}")
    finally:
        cursor.close()


def check_subscription_access(group_id, role, db=None):
    """
    Returns (allowed: bool, read_only: bool, message: str|None)
    """
    if db is None:
        db = get_db()
    sub    = get_group_subscription_status(db, group_id)
    status = sub["status"]

    if status in ("active", "free"):
        return True, False, None

    if status == "grace":
        msg = (f"⚠️ Subscription expires in {sub['days_remaining']} day(s). "
               f"Please pay to continue uninterrupted access.")
        return True, False, msg

    if status == "suspended":
        if role == "admin":
            return True, True, "🔒 Subscription suspended. Read-only access. Contact support to renew."
        return False, False, "🔒 Group subscription has expired. Contact your admin."

    return True, False, None




# ==================== ROLE-BASED ACCESS CONTROL ====================

from functools import wraps
try:
    from auth import (
        get_current_user, get_all_memberships,
        require_auth,
        require_member as require_any_member_supabase,
        require_treasurer as require_treasurer_or_above_supabase,
        require_admin as require_admin_supabase,
        verify_supabase_jwt
    )
    SUPABASE_AUTH_ENABLED = True
except ImportError as e:
    print(f"⚠️  auth.py not found or error: {e} — Supabase Auth disabled")
    SUPABASE_AUTH_ENABLED = False
    def get_current_user(): return None
    def get_all_memberships(uid): return []
    def verify_supabase_jwt(t): return None
    def require_any_member_supabase(f): return f
    def require_treasurer_or_above_supabase(f): return f
    def require_admin_supabase(f): return f

try:
    from notifications import (
        send_password_reset_email, send_welcome_email,
        send_sms, send_loan_due_reminder, send_penalty_started_sms,
        send_subscription_reminder_sms
    )
    NOTIFICATIONS_ENABLED = True
except ImportError as e:
    print(f"⚠️  notifications.py not found or error: {e} — notifications disabled")
    NOTIFICATIONS_ENABLED = False
    def send_password_reset_email(*a, **k): return False
    def send_welcome_email(*a, **k): return False
    def send_sms(*a, **k): return False
    def send_loan_due_reminder(*a, **k): return False
    def send_penalty_started_sms(*a, **k): return False
    def send_subscription_reminder_sms(*a, **k): return False
try:
    from flask_mail import Mail, Message
    MAIL_AVAILABLE = True
except ImportError:
    MAIL_AVAILABLE = False
    Mail = None
    Message = None
    print("WARNING: flask_mail not installed. Run: pip install flask-mail")
try:
    from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
    TOKENS_AVAILABLE = True
except ImportError:
    TOKENS_AVAILABLE = False
    URLSafeTimedSerializer = None
    SignatureExpired = Exception
    BadSignature = Exception
    print("WARNING: itsdangerous not installed. Run: pip install itsdangerous")

def get_current_role():
    """Return the role of the currently logged-in user (admin or member)."""
    return session.get("role")

def get_current_member_id():
    """Return member_id for logged-in regular members."""
    return session.get("member_id")

def require_admin(f):
    """Route decorator: only admin role can access."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id") or session.get("role") != "admin":
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Admin access required"}), 403
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def require_treasurer_or_above(f):
    """Route decorator: admin or treasurer can access."""
    @wraps(f)
    def decorated(*args, **kwargs):
        role = session.get("role")
        if role not in ("admin", "treasurer"):
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Treasurer or admin access required"}), 403
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def require_any_member(f):
    """Route decorator: any logged-in user (admin, treasurer, member) can access."""
    @wraps(f)
    def decorated(*args, **kwargs):
        role = session.get("role")
        if not role:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

def is_admin_session():
    """True if the current session belongs to an admin."""
    return session.get("role") == "admin"

def is_own_member_data(member_id):
    """True if the logged-in member is accessing their own data."""
    return session.get("member_id") == member_id or is_admin_session()


def get_group_admin_member_id(db, group_id):
    """
    Returns the member_id of the group's admin (role='admin').
    is_system is reserved for the platform owner only and is never
    tied to a specific group's admin.
    """
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT id
        FROM members
        WHERE group_id = %s AND role = 'admin'
        ORDER BY id ASC
        LIMIT 1
        """,
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()

    if not row:
        raise Exception(f"No admin member found for group_id={group_id}")

    return row["id"]


# ==================== HELPER FUNCTIONS ====================
def create_new_group(db, group_name, admin_id):
    """Create a new group and associate it with an admin"""
    cursor = get_cursor(db)

    cursor.execute("""
        INSERT INTO groups (name, created_at)
        VALUES (%s, CURRENT_TIMESTAMP)
        RETURNING id
    """, (group_name,))

    group_id = cursor.fetchone()["id"]

    cursor.execute("""
        UPDATE members SET group_id = %s WHERE id = %s
    """, (group_id, admin_id))

    defaults = [
        ('group_name', group_name),
        ('interest_rate', '0.10'),
        ('daily_penalty_amount', '1000'),
        ('mercy_enabled', '1'),
        ('mercy_forgive_percent', '90'),
        ('mercy_half_percent', '75'),
        ('mercy_half_rate_percent', '50'),
        ('leadership_pay_amount', '0'),
        ('jamii_amount', '2000'),
        ('jamii_frequency', 'monthly'),
        ('cycle_start_date', ''),
        ('cycle_end_date', ''),
        ('hisa_unit_price', '5000'),
        ('loan_tier5_amount', '10000000'), 
        ('loan_tier5_months', '12')         
    ]

    for key, value in defaults:
        cursor.execute("""
            INSERT INTO settings (group_id, key, value)
            VALUES (%s, %s, %s)
        """, (group_id, key, value))

    db.commit()
    cursor.close()
    return group_id


def get_group_settings(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT key, value FROM settings WHERE group_id = %s", 
        (group_id,)
    )
    settings = cursor.fetchall()
    cursor.close()
    
    data = {s["key"]: s["value"] for s in settings}

    defaults = {
        'group_name': 'Kikoba App',
        'interest_rate': '0.10',
        'daily_penalty_amount': '1000',
        'mercy_enabled': '1',
        'mercy_forgive_percent': '90',
        'mercy_half_percent': '75',
        'mercy_half_rate_percent': '50',
        'leadership_pay_amount': '0',
        'jamii_amount': '2000',
        'jamii_frequency': 'monthly',
        'cycle_start_date': '',
        'cycle_end_date': '',
        'hisa_unit_price': '5000',
        'loan_tier1_amount': '500000',
        'loan_tier1_months': '1',
        'loan_tier2_amount': '1000000',
        'loan_tier2_months': '3',
        'loan_tier3_amount': '2000000',
        'loan_tier3_months': '6',
        'loan_tier4_amount': '5000000',
        'loan_tier4_months': '9',
        'loan_tier5_amount': '10000000',
        'loan_tier5_months': '12'         
    }
    
    for key, default_value in defaults.items():
        if key not in data:
            data[key] = default_value

    if "constitution_path" in data:
        data["constitution_view_url"] = "/api/constitution/view"
        data["constitution_download_url"] = "/api/constitution/download"

    return data


def calculate_cycle_weeks(start_date_str, end_date_str):
    if not start_date_str or not end_date_str:
        return 0
    try:
        start = datetime.strptime(start_date_str, "%Y-%m-%d")
        end = datetime.strptime(end_date_str, "%Y-%m-%d")
        delta = end - start
        weeks = delta.days / 7
        return max(0, weeks)
    except:
        return 0


def calculate_cycle_months(start_date_str, end_date_str):
    if not start_date_str or not end_date_str:
        return 0
    try:
        start = datetime.strptime(start_date_str, "%Y-%m-%d")
        end = datetime.strptime(end_date_str, "%Y-%m-%d")
        months = (end.year - start.year) * 12 + (end.month - start.month)
        return max(0, months)
    except:
        return 0


def get_member_hisa_units(db, member_id, group_id):
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE member_id = %s AND group_id = %s AND type IN ('hisa')
        """,
        (member_id, group_id)
    )
    total_hisa = get_single_value(cursor, 0)
    cursor.close()
    
    units = total_hisa / unit_price if unit_price > 0 else 0
    
    return {
        "total_contributed": total_hisa,
        "units": units,
        "unit_price": unit_price
    }


def get_total_hisa_units(db, group_id):
    settings = get_group_settings(db, group_id)

    unit_price = float(settings.get('hisa_unit_price', 5000))

    cursor = get_cursor(db)

    cursor.execute(
        """
        SELECT COALESCE(SUM(amount), 0)
        FROM contributions
        WHERE group_id = %s
          AND type = 'hisa'
        """,
        (group_id,)
    )

    total_hisa = get_single_value(cursor, 0) or 0

    cursor.close()

    units = float(total_hisa) / unit_price if unit_price > 0 else 0

    return units


def get_member_jamii_balance(db, member_id, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE member_id = %s AND group_id = %s AND type = 'jamii'
        """,
        (member_id, group_id)
    )
    total_paid = get_single_value(cursor, 0)
    cursor.close()
    
    return {
        "total_paid": total_paid,
        "expected_total": 0,
        "shortfall": 0,
        "periods": 0
    }


def get_total_principal_loaned(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(principal) FROM loans WHERE group_id = %s",
        (group_id,)
    )
    result = get_single_value(cursor)
    cursor.close()
    return result if result else 0


def get_current_group_profit(db, group_id):
    settings = get_group_settings(db, group_id)
    LEADERSHIP_PAY_AMOUNT = float(settings.get('leadership_pay_amount', 0))
    admin_id = get_group_admin_member_id(db, group_id)

    cursor = get_cursor(db)
    
    cursor.execute(
        "SELECT COUNT(id) FROM members WHERE group_id = %s",
        (group_id,)
    )
    total_members = get_single_value(cursor, 0)

    cursor.execute(
        "SELECT SUM(total - principal) FROM loans WHERE group_id = %s",
        (group_id,)
    )
    total_interest = get_single_value(cursor, 0)
    
    total_penalties_imposed = get_total_penalties_imposed(db, group_id)
    total_penalties_revenue = get_total_penalties_paid(db, group_id)

    cursor.execute(
        "SELECT SUM(amount) FROM contributions WHERE type='jamii' AND group_id = %s",
        (group_id,)
    )
    total_jamii_collected = get_single_value(cursor, 0)
    cursor.close()
    
    gross_distributable_pool = total_interest + total_penalties_imposed
    net_profit_pool = max(gross_distributable_pool - LEADERSHIP_PAY_AMOUNT, 0)
    
    return {
        "total_interest": total_interest,
        "total_penalties_imposed": total_penalties_imposed,
        "total_penalties_revenue": total_penalties_revenue,
        "total_jamii_collected": total_jamii_collected,
        "leadership_pay_amount": LEADERSHIP_PAY_AMOUNT, 
        "gross_distributable_pool": gross_distributable_pool,
        "net_profit_pool": net_profit_pool
    }


def get_total_savings(db, group_id):
    admin_id = get_group_admin_member_id(db, group_id)
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE group_id = %s AND type IN ('hisa', 'jamii') AND member_id != %s
        """,
        (group_id, admin_id)
    )
    result = get_single_value(cursor)
    cursor.close()
    return result if result else 0


def get_total_outstanding_loans(db, group_id):
    cursor = get_cursor(db)
    
    cursor.execute(
        "SELECT SUM(principal) FROM loans WHERE group_id = %s AND status != 'Cleared'",
        (group_id,)
    )
    total_liability = get_single_value(cursor, 0)

    cursor.execute(
        """
        SELECT SUM(r.amount) FROM rejesho r
        JOIN loans l ON r.loan_id = l.id
        WHERE l.group_id = %s AND l.status != 'Cleared'
        """,
        (group_id,)
    )
    total_repaid = get_single_value(cursor, 0)
    cursor.close()

    return max(total_liability - total_repaid, 0)


def update_loan_status(db, loan_id, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT * FROM loans WHERE id = %s AND group_id = %s", 
        (loan_id, group_id)
    )
    loan = cursor.fetchone()
    
    if not loan:
        cursor.close()
        return

    # Never override a Forgiven status through automatic recalculation
    if loan["status"] == "Forgiven":
        cursor.close()
        return
    
    cursor.execute(
        "SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s",
        (loan_id, group_id)
    )
    repaid = get_single_value(cursor, 0)
    
    remaining = loan["total"] - repaid
    
    if remaining <= 0:
        new_status = "Cleared"
    elif datetime.now().date() > datetime.strptime(loan["due_date"], "%Y-%m-%d").date():
        new_status = "Overdue"
    else:
        new_status = "Active"
    
    if loan["status"] != new_status:
        cursor.execute(
            "UPDATE loans SET status = %s WHERE id = %s AND group_id = %s",
            (new_status, loan_id, group_id)
        )
        db.commit()
    cursor.close()


def auto_insert_loan_penalties(db, group_id):
    """
    For every active/overdue loan, charge daily_penalty TZS/day for each
    monthly rejesho slot that is past due and not yet fully paid.

    Flat rate only — no mercy thresholds anywhere in this function.

    SELF-HEALING: every slot without real payment or forgiveness activity
    is deleted and recomputed from scratch on every single run,
    REGARDLESS of its previous is_frozen state. is_frozen is not trusted
    as "correct forever" — it only reflects the state of the LAST
    recompute. This exists because stale freezes have been found in
    production (rows frozen years ago under old rule versions, or before
    a later rejesho edit/delete changed the true payment history, that
    then never got revisited). Only a row with real amount_paid > 0 or
    forgiven_amount > 0 is left completely untouched — that reflects a
    genuine transaction and must never be silently erased.

    Algorithm, per month slot:
    1. Find the date cumulative rejesho first reached that slot's target
       (coverage_date).
    2. Covered by the due date -> no penalty, no row.
    3. Otherwise, the slot's daily penalty runs from its due_date until
       whichever comes first: it gets fully covered (coverage_date), or
       its cap_date arrives (the NEXT installment's due date — or, for
       the loan's last installment, the group's cycle_end_date setting).
       Reaching the cap freezes the penalty even if the installment is
       still unpaid — the debt itself doesn't disappear, only that
       month's daily counter stops. A new counter starts fresh for the
       next installment.
    3b. MERCY RULES (per-group settings, can be switched off): judged on
       the due date only. If >= mercy_forgive_percent (default 90) of the
       installment was already paid by then -> no penalty at all. Else if
       >= mercy_half_percent (default 75) -> charged at
       mercy_half_rate_percent (default 50) of the daily rate. Below
       that -> full rate. Set mercy_enabled = 0 for strict flat-rate.
    4. If the loan's last installment has no cycle_end_date to cap
       against (setting left blank), it falls back to accruing
       indefinitely, same as before this change — there's nothing to
       cap it to.

    Requires: penalties.month_num column (INTEGER).

    Returns: list of {loan_id, month_num, reason} for rows left untouched
    due to real payment/forgiveness activity, worth a manual look.
    """
    settings        = get_group_settings(db, group_id)
    daily_penalty   = float(settings.get("daily_penalty_amount", 1000))
    cycle_end_str   = (settings.get("cycle_end_date") or "").strip()
    cycle_end_date  = None
    if cycle_end_str:
        try:
            cycle_end_date = datetime.strptime(cycle_end_str, "%Y-%m-%d").date()
        except ValueError:
            cycle_end_date = None
    today         = datetime.now().date()
    TOLERANCE     = 1.0

    # ── Per-group mercy rules (all editable in Settings) ──
    def _setting_num(key, default):
        try:
            return float(settings.get(key, default))
        except (TypeError, ValueError):
            return float(default)

    mercy_enabled     = str(settings.get("mercy_enabled", "1")).strip() == "1"
    mercy_forgive_pct = _setting_num("mercy_forgive_percent", 90) / 100.0   # >= this: no penalty
    mercy_half_pct    = _setting_num("mercy_half_percent", 75) / 100.0      # >= this: reduced rate
    mercy_half_rate   = _setting_num("mercy_half_rate_percent", 50) / 100.0 # the reduced rate

    cursor = get_cursor(db)
    cursor.execute("""
        SELECT l.id, l.member_id, l.principal, l.months, l.due_date
        FROM loans l
        WHERE l.group_id = %s AND l.status IN ('Active', 'Overdue')
    """, (group_id,))
    active_loans = cursor.fetchall()

    skipped_for_review = []

    for loan in active_loans:
        loan_id         = loan['id']
        member_id       = loan['member_id']
        months          = int(loan['months'])
        monthly_rejesho = float(loan['principal']) / months

        final_due  = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        base_day   = final_due.day
        base_month = final_due.month - months
        base_year  = final_due.year
        if base_month <= 0:
            base_month += 12
            base_year  -= 1

        # Precompute every installment's due date up front, so each slot
        # can look up the NEXT slot's due date as its cap.
        due_dates = {}
        for mn in range(1, months + 1):
            dm = base_month + mn
            dy = base_year
            if dm > 12:
                dm -= 12
                dy += 1
            due_dates[mn] = datetime(
                dy, dm, min(base_day, calendar.monthrange(dy, dm)[1])
            ).date()

        cursor.execute("""
            SELECT amount, date FROM rejesho
            WHERE loan_id = %s AND group_id = %s
            ORDER BY date ASC
        """, (loan_id, group_id))
        payments = [(float(r['amount']), datetime.strptime(r['date'], "%Y-%m-%d").date())
                    for r in cursor.fetchall()]

        for month_num in range(1, months + 1):
            month_due_date = due_dates[month_num]

            if today <= month_due_date:
                continue  # not due yet

            # This slot's cap: the next slot's due date, or for the last
            # slot, the group's cycle end date (if one is set).
            if month_num < months:
                cap_date = due_dates[month_num + 1]
            else:
                cap_date = cycle_end_date  # may be None -> no cap

            # Real activity on the existing row? Then hands off.
            cursor.execute("""
                SELECT id, COALESCE(amount_paid, 0) AS amount_paid,
                       COALESCE(forgiven_amount, 0) AS forgiven_amount
                FROM penalties
                WHERE loan_id = %s AND group_id = %s
                  AND type = 'monthly_rejesho_late' AND month_num = %s
            """, (loan_id, group_id, month_num))
            existing = cursor.fetchone()

            if existing and (existing['amount_paid'] > 0 or existing['forgiven_amount'] > 0):
                skipped_for_review.append({
                    "loan_id": loan_id, "month_num": month_num,
                    "reason": f"has amount_paid={existing['amount_paid']}, "
                              f"forgiven_amount={existing['forgiven_amount']} — left untouched"
                })
                continue

            # Safe to recompute from scratch — clear whatever was there,
            # frozen or not, and rebuild it below.
            if existing:
                cursor.execute("DELETE FROM penalties WHERE id = %s", (existing['id'],))

            target      = monthly_rejesho * month_num
            paid_by_due = sum(a for a, d in payments if d <= month_due_date)
            if paid_by_due >= target - TOLERANCE:
                continue  # covered on time -> no penalty

            # ── MERCY (per-group setting). Judged ONCE, on the due date:
            # what share of THIS installment was already paid by then?
            rate_factor = 1.0
            mercy_note  = ""
            if mercy_enabled and monthly_rejesho > 0:
                prev_target = monthly_rejesho * (month_num - 1)
                slot_by_due = max(0.0, min(paid_by_due - prev_target, monthly_rejesho))
                pct_by_due  = slot_by_due / monthly_rejesho
                if pct_by_due + 1e-9 >= mercy_forgive_pct:
                    continue  # forgiven: enough was paid by the due date
                elif pct_by_due + 1e-9 >= mercy_half_pct:
                    rate_factor = mercy_half_rate
                    mercy_note  = f" (reduced rate: {pct_by_due * 100:.0f}% paid by due date)"

            coverage_date = None
            cumul = 0.0
            for amt, pdate in payments:
                cumul += amt
                if cumul >= target - TOLERANCE:
                    coverage_date = pdate
                    break

            if coverage_date is not None:
                # Paid, but late. Freeze at whichever came first: the
                # payment itself, or the cap date.
                freeze_at      = min(coverage_date, cap_date) if cap_date else coverage_date
                late_days      = max(0, (freeze_at - month_due_date).days)
                penalty_amount = round(late_days * daily_penalty * rate_factor)
                should_freeze  = True
            elif cap_date and today >= cap_date:
                # Still unpaid, but this month's window has closed —
                # freeze at the cap. The debt remains; the daily counter
                # for THIS slot stops.
                late_days      = (cap_date - month_due_date).days
                penalty_amount = round(late_days * daily_penalty * rate_factor)
                should_freeze  = True
            else:
                # Still unpaid, cap not reached yet (or no cap exists for
                # this slot) -> keep accruing daily.
                late_days      = (today - month_due_date).days
                penalty_amount = round(late_days * daily_penalty * rate_factor)
                should_freeze  = False

            if penalty_amount <= 0:
                continue

            desc = (f"Month {month_num} rejesho overdue by {late_days} days"
                    + (" — settled late" if coverage_date else
                       " — capped, still unpaid" if should_freeze else "")
                    + mercy_note)

            cursor.execute("""
                INSERT INTO penalties (
                    group_id, member_id, loan_id, type,
                    month_num, amount, description, date, is_frozen
                )
                VALUES (%s, %s, %s, 'monthly_rejesho_late', %s, %s, %s, %s, %s)
            """, (
                group_id, member_id, loan_id,
                month_num, penalty_amount, desc,
                today.strftime("%Y-%m-%d"),
                1 if should_freeze else 0
            ))

        # Update loan status
        total_paid_overall = sum(a for a, _ in payments)
        remaining = float(loan['principal']) - total_paid_overall

        if remaining <= 0:
            cursor.execute("UPDATE loans SET status = 'Cleared' WHERE id = %s", (loan_id,))
        elif today > final_due:
            cursor.execute("UPDATE loans SET status = 'Overdue' WHERE id = %s", (loan_id,))

    db.commit()
    cursor.close()
    return skipped_for_review


def get_member_loan_balances(db, member_id, group_id):
    today_date = date.today()
    total_overdue_balance = 0
    total_loans_committed = 0
    total_rejesho = 0
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, principal, due_date, status FROM loans WHERE member_id=%s AND group_id=%s", 
        (member_id, group_id)
    )
    loans_rows = cursor.fetchall()

    for loan in loans_rows:
        loan_id = loan['id']
        loan_principal = loan['principal']
        loan_due_date_str = loan['due_date']
        
        try:
            loan_due_date = datetime.strptime(loan_due_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            loan_due_date = today_date + timedelta(days=1)
        
        total_loans_committed += loan_principal
        
        cursor.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", 
            (loan_id, group_id)
        )
        repaid_amount = get_single_value(cursor, 0)
        
        total_rejesho += repaid_amount
        remaining_balance = max(loan_principal - repaid_amount, 0)
        
        if remaining_balance > 0 and loan_due_date < today_date:
            total_overdue_balance += remaining_balance
    
    cursor.close()
    remaining_loans = max(total_loans_committed - total_rejesho, 0)
    
    return {
        "total_loans_committed": total_loans_committed,
        "total_rejesho": total_rejesho,
        "remaining_loans": remaining_loans,
        "total_overdue": total_overdue_balance
    }


def get_total_penalties_due_for_member(member_id, db, group_id):
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT SUM(GREATEST(0, amount - COALESCE(amount_paid, 0) - COALESCE(forgiven_amount, 0)))
        AS total_outstanding
        FROM penalties
        WHERE member_id = %s AND group_id = %s
    """, (member_id, group_id))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result


def get_total_penalties_for_member(member_id, group_id):
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT SUM(GREATEST(0, amount - COALESCE(amount_paid, 0) - COALESCE(forgiven_amount, 0)))
        AS total_outstanding
        FROM penalties
        WHERE member_id = %s AND group_id = %s
    """, (member_id, group_id))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result


def get_total_penalties_imposed(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(amount) AS total_imposed FROM penalties WHERE group_id = %s", 
        (group_id,)
    )
    result = get_single_value(cursor, 0)
    cursor.close()
    return result


def get_total_penalties_paid(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(COALESCE(amount_paid, 0)) AS total_paid FROM penalties WHERE group_id = %s",
        (group_id,)
    )
    result = get_single_value(cursor, 0)
    cursor.close()
    return result


def get_total_group_penalty_liability(db, group_id):
    cursor = get_cursor(db)
    # Remaining = original amount - already paid - forgiven portion
    cursor.execute("""
        SELECT SUM(
            GREATEST(0, amount - COALESCE(amount_paid, 0) - COALESCE(forgiven_amount, 0))
        ) AS total_liability
        FROM penalties
        WHERE group_id = %s
    """, (group_id,))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

# ==================== ROUTES ====================
@app.route("/")
def index():
    if session.get("user_id") and session.get("group_id"):
        role = session.get("role")
        if role in ("admin", "treasurer"):
            return redirect("/dashboard")
        return redirect("/member-portal")
    return redirect("/login")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    db = get_db()
    cursor = get_cursor(db)
    
    if request.method == "POST":
        name     = (request.form.get("name")     or "").strip()
        phone    = (request.form.get("phone")    or "").strip()
        email    = (request.form.get("email")    or "").strip()
        password = (request.form.get("password") or "").strip()

        if not name or not phone or not email or not password:
            cursor.close()
            return render_template("signup.html", error="All fields are required")

        if len(password) < 6:
            cursor.close()
            return render_template("signup.html", error="Password must be at least 6 characters")

        cursor.execute("""
            INSERT INTO members (name, phone, email, password, is_system, role, is_active, joined_date)
            VALUES (%s, %s, %s, %s, 0, 'admin', 1, CURRENT_DATE)
            RETURNING id
        """, (name, phone, email, generate_password_hash(password)))

        new_admin_id = cursor.fetchone()["id"]
        db.commit()

        session["user_id"]     = new_admin_id
        session["member_id"]   = new_admin_id
        session["role"]        = "admin"
        session["member_name"] = name
        cursor.close()
        return redirect("/create-group")

    cursor.close()
    return render_template("signup.html")



@app.route('/api/groups', methods=['POST'])
@require_admin
def create_group_api():
    db = get_db()
    data = request.get_json()

    group_name = data.get("group_name")
    admin_id = data.get("admin_id")

    if not group_name or not admin_id:
        return jsonify({"error": "group_name and admin_id required"}), 400

    group_id = create_new_group(db, group_name, admin_id)

    return jsonify({
        "status": "success",
        "group_id": group_id
    })


@app.route("/create-group", methods=["GET", "POST"])
def create_group():
    if not session.get("user_id"):
        return redirect("/login")

    db = get_db()
    error = None

    if request.method == "POST":
        group_name = (request.form.get("group_name") or "").strip()

        if not group_name:
            error = "Group name is required"
        else:
            try:
                group_id = create_new_group(db, group_name, session["user_id"])
                session["group_id"] = group_id
                session["role"] = "admin"
                session["show_group_id_banner"] = True
                generate_monthly_bill(db, group_id)
                return redirect("/dashboard")
            except Exception as e:
                error = "Unable to create the Kikoba. Please try again."

    return render_template("create_group.html", error=error)


@app.route("/dashboard")
def dashboard():
    user_id  = session.get("user_id")
    group_id = session.get("group_id")
    role     = session.get("role")

    if not user_id or not group_id:
        return redirect("/login")
    if role not in ("admin", "treasurer"):
        return redirect("/member-portal")

    db = get_db()
    cursor = get_cursor(db)

    cursor.execute(
        "SELECT name FROM members WHERE id = %s AND group_id = %s",
        (user_id, group_id)
    )
    admin = cursor.fetchone()

    cursor.execute(
        "SELECT * FROM groups WHERE id = %s",
        (group_id,)
    )
    group = cursor.fetchone()
    cursor.close()

    show_group_id_banner = session.pop("show_group_id_banner", False)
    return render_template(
        "dashboard.html",
        admin=admin,
        group=group,
        show_group_id_banner=show_group_id_banner
    )


@app.route('/api/dashboard', methods=['GET'])
@require_any_member
def get_dashboard_data():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    auto_insert_loan_penalties(db, group_id)

    profit_data = get_current_group_profit(db, group_id)
    settings = get_group_settings(db, group_id)
    admin_id = get_group_admin_member_id(db, group_id)

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT COUNT(id) FROM members WHERE group_id = %s",
        (group_id,)
    )
    total_members = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else None)(cursor.fetchone())
    
    total_imposed = get_total_penalties_imposed(db, group_id) 
    total_paid = get_total_penalties_paid(db, group_id)
    total_due = get_total_group_penalty_liability(db, group_id)

    total_units = get_total_hisa_units(db, group_id)
    
    cursor.execute(
        """
        SELECT SUM(amount)
        FROM contributions
        WHERE group_id = %s
          AND type IN ('hisa anzia', 'hisa', 'jamii')
        """,
        (group_id,)
    )
    total_contributions = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())
    cursor.close()

    return jsonify({
        "group_name": settings.get('group_name', 'Kikoba App'),
        "constitution_path": settings.get('constitution_path', None),
        "interest_rate": settings.get('interest_rate', '0.10'),
        "daily_penalty": settings.get('daily_penalty_amount', '1000'),
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "jamii_amount": settings.get('jamii_amount', '2000'),
        "jamii_frequency": settings.get('jamii_frequency', 'monthly'),
        "cycle_start_date": settings.get('cycle_start_date', ''),
        "cycle_end_date": settings.get('cycle_end_date', ''),
        "hisa_unit_price": settings.get('hisa_unit_price', '5000'),
        "total_members": total_members,
        "total_contributions_hisa": total_contributions,
        "total_hisa_units": total_units,  
        "loan_balance_due": get_total_outstanding_loans(db, group_id),
        "total_principal_loaned": get_total_principal_loaned(db, group_id),
        "total_interests": profit_data["total_interest"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_in_hand": profit_data["net_profit_pool"],
        "penalties_imposed": total_imposed,
        "penalties_paid": total_paid,
        "penalties_due_net": total_due,
        "total_jamii_collected": profit_data["total_jamii_collected"],
        "loan_tier1_amount": settings.get('loan_tier1_amount', '500000'),
        "loan_tier1_months": settings.get('loan_tier1_months', '1'),
        "loan_tier2_amount": settings.get('loan_tier2_amount', '1000000'),
        "loan_tier2_months": settings.get('loan_tier2_months', '3'),
        "loan_tier3_amount": settings.get('loan_tier3_amount', '2000000'),
        "loan_tier3_months": settings.get('loan_tier3_months', '6'),
        "loan_tier4_amount": settings.get('loan_tier4_amount', '5000000'),
        "loan_tier4_months": settings.get('loan_tier4_months', '9'),
        "loan_tier5_amount": settings.get('loan_tier5_amount', '10000000'),  
        "loan_tier5_months": settings.get('loan_tier5_months', '12'),
    })


# ==================== CONFIGURATION ROUTES ====================

@app.route('/api/loan_rules', methods=['GET'])
@require_treasurer_or_above
def get_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, min_principal, max_principal, days FROM loan_rules WHERE group_id = %s ORDER BY min_principal ASC",
        (group_id,)
    )
    rules = cursor.fetchall()
    cursor.close()
    
    return jsonify([dict(r) for r in rules])


@app.route('/api/loan_rules', methods=['POST'])
@require_admin
def save_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    rules = data.get('rules')
    
    if not rules or not isinstance(rules, list):
        return jsonify({"error": "Invalid rules data format"}), 400
    
    cursor = get_cursor(db)
    cursor.execute("DELETE FROM loan_rules WHERE group_id = %s", (group_id,))
    
    for rule in rules:
        try:
            min_p = float(rule['min_principal'])
            max_p = float(rule['max_principal'])
            days = int(rule['days'])
            
            cursor.execute(
                "INSERT INTO loan_rules (group_id, min_principal, max_principal, days) VALUES (%s, %s, %s, %s)",
                (group_id, min_p, max_p, days)
            )
        except Exception as e:
            db.rollback()
            cursor.close()
            return jsonify({"error": f"Invalid rule value provided: {e}"}), 400
            
    db.commit()
    cursor.close()
    return jsonify({"status": "success", "message": f"{len(rules)} loan rules saved."})


@app.route('/api/settings', methods=['GET', 'POST'])
@require_treasurer_or_above
def handle_settings():
    db = get_db()
    group_id = get_current_group_id()

    # Only admins can write settings
    if request.method == 'POST' and session.get('role') != 'admin':
        return jsonify({"error": "Admin access required to change settings"}), 403
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    if request.method == 'GET':
        settings = get_group_settings(db, group_id)
        return jsonify(settings)
    
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON data"}), 400

    # ── Mercy-rule settings: validate before saving anything ──
    mercy_enabled = data.get('mercy_enabled')
    if mercy_enabled is not None and mercy_enabled != "":
        mercy_enabled = '1' if str(mercy_enabled).strip().lower() in ('1', 'true', 'yes', 'on') else '0'

    mercy_vals = {}
    for k in ('mercy_forgive_percent', 'mercy_half_percent', 'mercy_half_rate_percent'):
        v = data.get(k)
        if v is not None and v != "":
            try:
                fv = float(v)
            except (TypeError, ValueError):
                return jsonify({"error": f"{k} must be a number"}), 400
            if not (0 <= fv <= 100):
                return jsonify({"error": f"{k} must be between 0 and 100"}), 400
            mercy_vals[k] = fv
    if ('mercy_forgive_percent' in mercy_vals and 'mercy_half_percent' in mercy_vals
            and mercy_vals['mercy_half_percent'] > mercy_vals['mercy_forgive_percent']):
        return jsonify({"error": "Half-penalty percent cannot be higher than the full-forgiveness percent"}), 400

    updates = [
        ('group_name', data.get('group_name')),
        ('interest_rate', data.get('interest_rate')),
        ('daily_penalty_amount', data.get('daily_penalty_amount')),
        ('mercy_enabled', mercy_enabled),
        ('mercy_forgive_percent', data.get('mercy_forgive_percent')),
        ('mercy_half_percent', data.get('mercy_half_percent')),
        ('mercy_half_rate_percent', data.get('mercy_half_rate_percent')),
        ('leadership_pay_amount', data.get('leadership_pay_amount')),
        ('jamii_amount', data.get('jamii_amount')),
        ('jamii_frequency', data.get('jamii_frequency')),
        ('cycle_start_date', data.get('cycle_start_date')),
        ('cycle_end_date', data.get('cycle_end_date')),
        ('hisa_unit_price', data.get('hisa_unit_price')),
        ('loan_tier1_amount', data.get('loan_tier1_amount')),
        ('loan_tier1_months', data.get('loan_tier1_months')),
        ('loan_tier2_amount', data.get('loan_tier2_amount')),
        ('loan_tier2_months', data.get('loan_tier2_months')),
        ('loan_tier3_amount', data.get('loan_tier3_amount')),
        ('loan_tier3_months', data.get('loan_tier3_months')),
        ('loan_tier4_amount', data.get('loan_tier4_amount')),
        ('loan_tier4_months', data.get('loan_tier4_months')),
        ('loan_tier5_amount', data.get('loan_tier5_amount')),  
        ('loan_tier5_months', data.get('loan_tier5_months')), 
    ]

    cursor = get_cursor(db)
    
    try:
        for key, value in updates:
            if value is not None and value != "":
                cursor.execute("""
                    INSERT INTO settings (group_id, key, value) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (group_id, key) 
                    DO UPDATE SET value = EXCLUDED.value
                """, (group_id, key, str(value)))
        
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "General settings updated."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/constitution/upload', methods=['POST'])
@require_admin
def upload_constitution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    if 'constitution_file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['constitution_file']

    if not file or file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename = f"group{group_id}_{int(datetime.now().timestamp())}_{secure_filename(file.filename)}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)

    cursor = get_cursor(db)
    cursor.execute("""
        INSERT INTO settings (group_id, key, value) 
        VALUES (%s, 'constitution_path', %s)
        ON CONFLICT (group_id, key) 
        DO UPDATE SET value = EXCLUDED.value
    """, (group_id, filename))
    db.commit()
    cursor.close()

    return jsonify({
        "status": "success",
        "message": "Constitution uploaded successfully.",
        "path": filename
    })


@app.route("/constitution/view")
@require_treasurer_or_above
def view_constitution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = %s",
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()

    if not row:
        return "No constitution uploaded", 404

    return send_from_directory(
        app.config["UPLOAD_FOLDER"],
        row["value"],
        as_attachment=False
    )


@app.route("/constitution/download")
@require_treasurer_or_above
def download_constitution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = %s",
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()

    if not row:
        return "No constitution uploaded", 404

    return send_from_directory(
        app.config["UPLOAD_FOLDER"],
        row["value"],
        as_attachment=True
    )


@app.route('/api/constitution/status', methods=['GET'])
@require_treasurer_or_above
def constitution_status():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = %s",
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()
    
    if not row or not row['value']:
        return jsonify({"uploaded": False}), 200
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], row['value'])
    if not os.path.exists(file_path):
        return jsonify({"uploaded": False}), 200
    
    return jsonify({
        "uploaded": True,
        "filename": row['value'],
        "view_url": "/constitution/view",
        "download_url": "/constitution/download"
    })


@app.route('/api/jamii_deduction', methods=['POST'])
@require_admin
def record_jamii_deduction():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    amount = float(data.get("amount", 0))
    admin_id = get_group_admin_member_id(db, group_id)
    
    if amount <= 0:
        return jsonify({"error": "Deduction amount must be positive"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id FROM members WHERE id = %s AND group_id = %s",
        (admin_id, group_id)
    )
    admin_exists = cursor.fetchone()
    
    if not admin_exists:
        cursor.close()
        return jsonify({
            "error": f"Group admin member (ID {admin_id}) does not exist for this group. Cannot record group expense."
        }), 400

    today_str = datetime.now().strftime("%Y-%m-%d")

    cursor.execute(
        "INSERT INTO contributions (group_id, member_id, type, amount, date) VALUES (%s, %s, 'jamii_deduction', %s, %s)",
        (group_id, admin_id, -amount, today_str)
    )
    db.commit()
    cursor.close()
    
    return jsonify({
        "status": "success",
        "message": f"{amount:,.0f} TZS recorded as Jamii deduction."
    })



# ==================== MEMBER AUTH ROUTES ====================


@app.route('/member-portal')
@require_any_member
def member_portal():
    """Member self-service portal — read-only view of own data."""
    # Admins using the main dashboard don't need this
    if session.get("role") in ("admin", "treasurer"):
        return redirect("/dashboard")
    member_id = session.get("member_id")
    if not member_id:
        return redirect("/login")
    return render_template("member_portal.html",
                           member_id=member_id,
                           member_name=session.get("member_name", ""))


@app.route('/api/member/me', methods=['GET'])
@require_any_member
def get_my_details():
    """API: member fetches their own details — same as get_member_details but
    enforces that non-admins can only see their own data."""
    member_id = session.get("member_id")
    role = session.get("role")

    # Admins/treasurers can pass ?member_id=X to see anyone in their group
    if role in ("admin", "treasurer"):
        requested_id = request.args.get("member_id", member_id)
        try:
            member_id = int(requested_id)
        except (TypeError, ValueError):
            member_id = session.get("member_id")
    # Regular members only see themselves
    else:
        member_id = session.get("member_id")

    if not member_id:
        return jsonify({"error": "Not authenticated"}), 401

    # Reuse the existing get_member_details logic
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group found"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, name, phone, joined_date FROM members WHERE id = %s AND group_id = %s AND is_system = 0",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404
    cursor.close()

    # Delegate directly to get_member_details (same function, internal call)
    return get_member_details(member_id)



@app.route('/api/member/set-role', methods=['POST'])
@require_admin
def set_member_role():
    """Admin assigns a role to a member without changing password."""
    db       = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data      = request.get_json()
    member_id = data.get("member_id")
    role      = (data.get("role") or "member").strip().lower()

    if not member_id:
        return jsonify({"error": "member_id required"}), 400
    if role not in ("admin", "treasurer", "member"):
        return jsonify({"error": "Invalid role. Must be admin, treasurer or member."}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, name, role FROM members WHERE id = %s AND group_id = %s",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404

    try:
        cursor.execute(
            "UPDATE members SET role = %s WHERE id = %s AND group_id = %s",
            (role, member_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success",
                        "message": f"{member['name']} is now {role}."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/member/set-password', methods=['POST'])
@require_admin
def set_member_password():
    """Admin sets or resets a member's password."""
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data          = request.get_json()
    member_id     = data.get("member_id")
    new_password  = (data.get("password") or "").strip()
    role          = (data.get("role") or "member").strip()
    email         = (data.get("email") or "").strip()

    if not member_id or not new_password:
        return jsonify({"error": "member_id and password are required"}), 400

    if len(new_password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    if role not in ("admin", "treasurer", "member"):
        return jsonify({"error": "Invalid role"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, is_system FROM members WHERE id = %s AND group_id = %s",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404
    # Allow self-update but not downgrading the only admin
    if member["is_system"] and role != "admin":
        # Check there's another admin in the group
        cursor.execute(
            "SELECT COUNT(*) FROM members WHERE group_id=%s AND role='admin' AND id!=%s",
            (group_id, member_id)
        )
        other_admins = get_single_value(cursor, 0)
        if not other_admins:
            cursor.close()
            return jsonify({"error": "Cannot change role — this is the only admin."}), 400

    try:
        if email:
            cursor.execute(
                "UPDATE members SET password = %s, role = %s, email = %s WHERE id = %s AND group_id = %s",
                (generate_password_hash(new_password), role, email, member_id, group_id)
            )
        else:
            cursor.execute(
                "UPDATE members SET password = %s, role = %s WHERE id = %s AND group_id = %s",
                (generate_password_hash(new_password), role, member_id, group_id)
            )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Password, role" + (" and email" if email else "") + " updated."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/member/toggle-active', methods=['POST'])
@require_admin
def toggle_member_active():
    """Admin activates or deactivates a member account."""
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data = request.get_json()
    member_id = data.get("member_id")
    is_active = 1 if data.get("is_active") else 0

    if not member_id:
        return jsonify({"error": "member_id required"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, is_system FROM members WHERE id = %s AND group_id = %s",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404
    if member["is_system"]:
        cursor.close()
        return jsonify({"error": "Cannot deactivate system admin"}), 400

    try:
        cursor.execute(
            "UPDATE members SET is_active = %s WHERE id = %s AND group_id = %s",
            (is_active, member_id, group_id)
        )
        db.commit()
        cursor.close()
        action = "activated" if is_active else "deactivated"
        return jsonify({"status": "success", "message": f"Member {action}."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500



# ==================== PASSWORD RESET ====================



@app.route('/api/subscription/mark-paid', methods=['POST'])
def mark_subscription_paid():
    """Owner manually marks a subscription as paid."""
    # Simple owner token check — replace with proper owner auth in Stage 4
    owner_token = request.headers.get("X-Owner-Token")
    if owner_token != os.environ.get("OWNER_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 403

    data             = request.get_json()
    group_id         = data.get("group_id")
    period_start     = data.get("period_start")
    amount_paid      = data.get("amount_paid", 0)
    payment_ref      = data.get("payment_reference", "")

    if not group_id or not period_start:
        return jsonify({"error": "group_id and period_start required"}), 400

    db = get_db()
    cursor = get_cursor(db)

    try:
        # Update subscription record
        cursor.execute(
            """UPDATE subscriptions
               SET amount_paid       = %s,
                   status            = 'paid',
                   payment_reference = %s,
                   paid_at           = NOW()
               WHERE group_id=%s AND period_start=%s""",
            (amount_paid, payment_ref, group_id, period_start)
        )

        # Extend group access by 1 month from period end
        cursor.execute(
            "SELECT period_end FROM subscriptions WHERE group_id=%s AND period_start=%s",
            (group_id, period_start)
        )
        sub = cursor.fetchone()
        if sub:
            period_end = sub["period_end"]
            if isinstance(period_end, str):
                period_end = datetime.strptime(period_end, "%Y-%m-%d").date()
            grace_until = period_end + timedelta(days=GRACE_DAYS)

            cursor.execute(
                """UPDATE groups
                   SET subscription_status  = 'active',
                       subscription_expires = %s,
                       grace_until          = %s
                   WHERE id = %s""",
                (period_end, grace_until, group_id)
            )

        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Payment recorded and access extended."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/subscription/toggle-free', methods=['POST'])
def toggle_group_free():
    """Owner manually marks a group as free (no billing)."""
    owner_token = request.headers.get("X-Owner-Token")
    if owner_token != os.environ.get("OWNER_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 403

    data     = request.get_json()
    group_id = data.get("group_id")
    is_free  = 1 if data.get("is_free") else 0

    if not group_id:
        return jsonify({"error": "group_id required"}), 400

    db = get_db()
    cursor = get_cursor(db)
    try:
        cursor.execute(
            """UPDATE groups
               SET is_free             = %s,
                   subscription_status = %s,
                   subscription_expires = CASE WHEN %s = 1 THEN '2099-12-31' ELSE subscription_expires END
               WHERE id = %s""",
            (is_free, 'free' if is_free else 'active', is_free, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": f"Group {'set to free' if is_free else 'set to paid'}."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/subscription/generate-bill', methods=['POST'])
def generate_bill():
    """Generate month-end bill for a group (or all groups)."""
    owner_token = request.headers.get("X-Owner-Token")
    if owner_token != os.environ.get("OWNER_TOKEN", ""):
        return jsonify({"error": "Unauthorized"}), 403

    data     = request.get_json()
    group_id = data.get("group_id")  # None = all groups

    db = get_db()
    cursor = get_cursor(db)

    if group_id:
        group_ids = [group_id]
    else:
        cursor.execute("SELECT id FROM groups")
        group_ids = [r["id"] for r in cursor.fetchall()]
    cursor.close()

    for gid in group_ids:
        generate_monthly_bill(db, gid)

    return jsonify({"status": "success", "message": f"Bills generated for {len(group_ids)} group(s)."})


@app.route('/api/subscription/status', methods=['GET'])
@require_any_member
def get_subscription_status():
    """Any logged-in user can check their group's subscription status."""
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    db  = get_db()
    sub = get_group_subscription_status(db, group_id)

    cursor = get_cursor(db)
    cursor.execute(
        """SELECT * FROM subscriptions
           WHERE group_id = %s
           ORDER BY period_start DESC
           LIMIT 6""",
        (group_id,)
    )
    history = cursor.fetchall()
    cursor.close()

    return jsonify({
        "subscription": sub,
        "history": [dict(h) for h in history]
    })


# ==================== MEMBER TRANSPARENCY PAGES ====================

@app.route('/member-members')
@require_any_member
def member_members_page():
    if session.get('role') in ('admin', 'treasurer'):
        return redirect('/members-page')
    return render_template('member_members.html')

@app.route('/member-contributions')
@require_any_member
def member_contributions_page():
    if session.get('role') in ('admin', 'treasurer'):
        return redirect('/contributions-page')
    return render_template('member_contributions.html')

@app.route('/member-loans')
@require_any_member
def member_loans_page():
    if session.get('role') in ('admin', 'treasurer'):
        return redirect('/loans-page')
    return render_template('member_loans.html')

@app.route('/member-penalties')
@require_any_member
def member_penalties_page():
    if session.get('role') in ('admin', 'treasurer'):
        return redirect('/penalties-page')
    return render_template('member_penalties.html')




# ==================== OWNER DASHBOARD (PLATFORM ADMIN) ====================
# Protected by OWNER_TOKEN header — only you can access this
# Access at /owner/dashboard

def require_owner(f):
    """Decorator: only platform owner can access."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = (
            request.headers.get("X-Owner-Token") or
            request.cookies.get("owner-token") or
            request.args.get("owner_token")
        )
        if token != os.environ.get("OWNER_TOKEN", ""):
            if request.path.startswith("/api/owner"):
                return jsonify({"error": "Unauthorized"}), 403
            return render_template("owner_login.html", error=None)
        return f(*args, **kwargs)
    return decorated


@app.route('/owner/login', methods=['GET', 'POST'])
def owner_login():
    if request.method == 'POST':
        token = (request.form.get("token") or "").strip()
        if token == os.environ.get("OWNER_TOKEN", ""):
            resp = redirect("/owner/dashboard")
            resp.set_cookie("owner-token", token, httponly=True,
                            samesite="Lax", max_age=86400 * 7)
            return resp
        return render_template("owner_login.html", error="Invalid access token.")
    return render_template("owner_login.html", error=None)


@app.route('/owner/logout')
def owner_logout():
    resp = redirect("/owner/login")
    resp.delete_cookie("owner-token")
    return resp


@app.route('/owner/dashboard')
@require_owner
def owner_dashboard():
    return render_template("owner_dashboard.html")


# ── Owner API ─────────────────────────────────────────────────────────

@app.route('/api/owner/stats', methods=['GET'])
@require_owner
def owner_stats():
    """Platform-wide statistics."""
    db = get_db()
    cursor = get_cursor(db)

    # Groups summary
    cursor.execute("""
        SELECT
            g.id, g.name, g.created_at,
            g.subscription_status, g.subscription_expires,
            g.is_free, g.profits_unlocked,
            COUNT(m.id) AS member_count,
            COUNT(m.id) FILTER (WHERE m.is_active = 1) AS active_members,
            (SELECT COALESCE(SUM(amount), 0) FROM contributions WHERE group_id = g.id) AS total_contributions,
            (SELECT COALESCE(SUM(principal), 0) FROM loans WHERE group_id = g.id) AS total_loans,
            (SELECT COALESCE(SUM(amount_paid), 0) FROM penalties WHERE group_id = g.id) AS penalties_collected
        FROM groups g
        LEFT JOIN members m ON m.group_id = g.id
        GROUP BY g.id, g.name, g.created_at, g.subscription_status,
                 g.subscription_expires, g.is_free, g.profits_unlocked
        ORDER BY g.id
    """)
    groups = [dict(r) for r in cursor.fetchall()]

    # Platform totals
    cursor.execute("SELECT COUNT(*) FROM groups")
    total_groups = get_single_value(cursor, 0)

    cursor.execute("SELECT COUNT(*) FROM members WHERE is_system = 0")
    total_members = get_single_value(cursor, 0)

    cursor.execute("SELECT COALESCE(SUM(amount_paid), 0) FROM subscriptions WHERE status = 'paid'")
    total_subscription_revenue = get_single_value(cursor, 0)

    cursor.execute("""
        SELECT COALESCE(SUM(amount_due), 0) FROM subscriptions
        WHERE status = 'unpaid'
          AND period_end < CURRENT_DATE
    """)
    total_outstanding_bills = get_single_value(cursor, 0)

    # Recent subscription records
    cursor.execute("""
        SELECT s.*, g.name AS group_name
        FROM subscriptions s
        JOIN groups g ON g.id = s.group_id
        ORDER BY s.created_at DESC
        LIMIT 20
    """)
    recent_bills = [dict(r) for r in cursor.fetchall()]

    cursor.close()
    return jsonify({
        "groups": groups,
        "totals": {
            "total_groups": total_groups,
            "total_members": total_members,
            "total_subscription_revenue": float(total_subscription_revenue),
            "total_outstanding_bills": float(total_outstanding_bills),
        },
        "recent_bills": recent_bills
    })


@app.route('/api/owner/group/<int:group_id>', methods=['GET'])
@require_owner
def owner_group_detail(group_id):
    """Detailed view of one group."""
    db = get_db()
    cursor = get_cursor(db)

    cursor.execute("""
        SELECT g.*, COUNT(m.id) AS member_count
        FROM groups g
        LEFT JOIN members m ON m.group_id = g.id
        WHERE g.id = %s
        GROUP BY g.id
    """, (group_id,))
    group = cursor.fetchone()
    if not group:
        cursor.close()
        return jsonify({"error": "Group not found"}), 404

    cursor.execute("""
        SELECT id, name, phone, email, role, is_active,
               CASE WHEN password IS NOT NULL THEN true ELSE false END AS has_password
        FROM members WHERE group_id = %s ORDER BY role, name
    """, (group_id,))
    members = [dict(r) for r in cursor.fetchall()]

    cursor.execute("""
        SELECT * FROM subscriptions WHERE group_id = %s ORDER BY period_start DESC
    """, (group_id,))
    bills = [dict(r) for r in cursor.fetchall()]

    cursor.close()
    return jsonify({"group": dict(group), "members": members, "bills": bills})


@app.route('/api/owner/group/<int:group_id>/toggle-free', methods=['POST'])
@require_owner
def owner_toggle_free(group_id):
    """Mark a group as free or paid."""
    db = get_db()
    data    = request.get_json()
    is_free = 1 if data.get("is_free") else 0
    cursor  = get_cursor(db)
    try:
        cursor.execute(
            """UPDATE groups SET is_free=%s,
               subscription_status=%s,
               subscription_expires=CASE WHEN %s=1 THEN '2099-12-31' ELSE subscription_expires END
               WHERE id=%s""",
            (is_free, 'free' if is_free else 'active', is_free, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback(); cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/owner/group/<int:group_id>/unlock-profits', methods=['POST'])
@require_owner
def owner_unlock_profits(group_id):
    """Unlock the profit distribution page for a group."""
    db = get_db()
    data   = request.get_json()
    unlock = 1 if data.get("unlock", True) else 0
    cursor = get_cursor(db)
    try:
        cursor.execute(
            "UPDATE groups SET profits_unlocked=%s WHERE id=%s",
            (unlock, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success",
                        "message": "Profits " + ("unlocked" if unlock else "locked") + "."})
    except Exception as e:
        db.rollback(); cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/owner/group/<int:group_id>/activate', methods=['POST'])
@require_owner
def owner_activate_group(group_id):
    """Manually activate/suspend a group."""
    db     = get_db()
    data   = request.get_json()
    active = data.get("active", True)
    cursor = get_cursor(db)
    try:
        if active:
            # Extend subscription by 1 month from today
            new_expiry  = date.today().replace(day=1)
            import calendar as cal
            last_day    = cal.monthrange(new_expiry.year, new_expiry.month)[1]
            new_expiry  = new_expiry.replace(day=last_day)
            grace_until = new_expiry + timedelta(days=GRACE_DAYS)
            cursor.execute(
                """UPDATE groups SET subscription_status='active',
                   subscription_expires=%s, grace_until=%s WHERE id=%s""",
                (new_expiry, grace_until, group_id)
            )
        else:
            cursor.execute(
                "UPDATE groups SET subscription_status='suspended' WHERE id=%s",
                (group_id,)
            )
        db.commit()
        cursor.close()
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback(); cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/owner/subscription/mark-paid', methods=['POST'])
@require_owner
def owner_mark_paid(group_id=None):
    """Mark a subscription bill as paid and extend access."""
    db   = get_db()
    data = request.get_json()
    gid  = data.get("group_id")
    period_start = data.get("period_start")
    amount_paid  = int(data.get("amount_paid", 0))
    ref          = data.get("payment_reference", "")

    if not gid or not period_start:
        return jsonify({"error": "group_id and period_start required"}), 400

    cursor = get_cursor(db)
    try:
        cursor.execute(
            """UPDATE subscriptions SET amount_paid=%s, status='paid',
               payment_reference=%s, paid_at=NOW()
               WHERE group_id=%s AND period_start=%s""",
            (amount_paid, ref, gid, period_start)
        )
        # Extend group access
        cursor.execute(
            "SELECT period_end FROM subscriptions WHERE group_id=%s AND period_start=%s",
            (gid, period_start)
        )
        row = cursor.fetchone()
        if row:
            pend = row["period_end"]
            if isinstance(pend, str):
                pend = datetime.strptime(pend, "%Y-%m-%d").date()
            grace = pend + timedelta(days=GRACE_DAYS)
            cursor.execute(
                """UPDATE groups SET subscription_status='active',
                   subscription_expires=%s, grace_until=%s WHERE id=%s""",
                (pend, grace, gid)
            )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Payment recorded and access extended."})
    except Exception as e:
        db.rollback(); cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/owner/subscription/generate-bills', methods=['POST'])
@require_owner
def owner_generate_bills():
    """Generate month-end bills for all non-free groups."""
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM groups")
    gids = [r["id"] for r in cursor.fetchall()]
    cursor.close()
    for gid in gids:
        generate_monthly_bill(db, gid)
    return jsonify({"status": "success", "message": f"Bills generated for {len(gids)} groups."})

# ==================== SUPABASE AUTH ROUTES ====================


@app.route('/api/auth/set-session', methods=['POST'])
def set_session():
    """
    Called by frontend after Supabase JS login.
    Verifies JWT, finds all memberships, optionally sets Flask session
    for a specific group_id.
    Returns list of groups the user belongs to.
    """
    data     = request.get_json() or {}
    token    = data.get("token", "")
    group_id = data.get("group_id")  # Optional — set if user picked a group

    if not token:
        return jsonify({"error": "token required"}), 400

    # Verify JWT
    if SUPABASE_AUTH_ENABLED:
        payload = verify_supabase_jwt(token)
    else:
        # Fallback: decode without verification
        try:
            import base64, json as _json
            parts   = token.split('.')
            padded  = parts[1] + '=' * (4 - len(parts[1]) % 4)
            payload = _json.loads(base64.urlsafe_b64decode(padded))
        except Exception:
            return jsonify({"error": "Invalid token"}), 401

    if not payload:
        return jsonify({"error": "Invalid or expired token"}), 401

    auth_user_id = payload.get("sub", "")
    jwt_phone    = payload.get("phone", "")
    jwt_email    = payload.get("email", "")

    # Find all memberships for this user
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT m.id, m.name, m.phone, m.email, m.group_id, m.role,
               COALESCE(m.is_active, 1) AS is_active,
               g.name AS group_name,
               m.is_system
        FROM members m
        JOIN groups g ON g.id = m.group_id
        WHERE (
            m.user_id = %s
            OR (m.phone IS NOT NULL AND m.phone != '' AND m.phone = %s)
            OR (m.email IS NOT NULL AND m.email != '' AND m.email = %s)
        )
        AND COALESCE(m.is_active, 1) = 1
        ORDER BY g.name
    """, (auth_user_id, jwt_phone, jwt_email))
    rows = cursor.fetchall()

    # Deduplicate by (member_id, group_id) — avoid showing same group twice
    seen   = set()
    groups = []
    for r in rows:
        key = (r['id'], r['group_id'])
        if key not in seen:
            seen.add(key)
            role = r['role'] or ('admin' if r['is_system'] else 'member')
            groups.append({
                "member_id":  r['id'],
                "group_id":   r['group_id'],
                "group_name": r['group_name'],
                "role":       role,
                "name":       r['name'],
            })
        # Auto-link user_id if not yet set
        if auth_user_id:
            try:
                cursor.execute(
                    "UPDATE members SET user_id = %s WHERE id = %s AND user_id IS NULL",
                    (auth_user_id, r['id'])
                )
            except Exception:
                pass

    db.commit()
    cursor.close()

    if not groups:
        return jsonify({"error": "No Kikoba membership found for this account."}), 404

    # If group_id provided (user picked one), set Flask session now
    if group_id:
        picked = next((g for g in groups if g['group_id'] == int(group_id)), None)
        if picked:
            session.clear()
            session['user_id']     = picked['member_id']
            session['member_id']   = picked['member_id']
            session['group_id']    = picked['group_id']
            session['role']        = picked['role']
            session['member_name'] = picked['name']
            return jsonify({
                "status":    "session_set",
                "role":      picked['role'],
                "group_id":  picked['group_id'],
                "redirect":  "/dashboard" if picked['role'] in ("admin","treasurer") else "/member-portal"
            })

    # Return all groups for the picker
    return jsonify({"status": "pick_group", "groups": groups})

@app.route('/api/auth/memberships', methods=['GET'])
def get_memberships():
    """Return all group memberships for the authenticated user."""
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    memberships = get_all_memberships(user["auth_user_id"])
    return jsonify({"memberships": memberships, "user": {
        "auth_user_id": user["auth_user_id"],
        "email": user.get("email", ""),
        "phone": user.get("phone", "")
    }})


@app.route('/api/auth/me', methods=['GET'])
def get_me():
    """Return current user + group context."""
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401
    return jsonify(user)


@app.route('/api/auth/switch-group', methods=['POST'])
def switch_group():
    """Switch active group for a user with multiple memberships."""
    user = get_current_user()
    if not user:
        return jsonify({"error": "Not authenticated"}), 401

    data     = request.get_json()
    group_id = data.get("group_id")
    if not group_id:
        return jsonify({"error": "group_id required"}), 400

    db = get_db()
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, role, is_active FROM members WHERE user_id = %s AND group_id = %s",
        (user["auth_user_id"], group_id)
    )
    membership = cursor.fetchone()
    cursor.close()

    if not membership:
        return jsonify({"error": "You are not a member of this group"}), 403
    if not membership["is_active"]:
        return jsonify({"error": "Your membership in this group is inactive"}), 403

    return jsonify({"status": "success", "group_id": group_id, "role": membership["role"]})


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """Password reset via Supabase Auth + Brevo email."""
    if request.method == 'GET':
        return render_template('forgot_password.html')

    email = (request.form.get('email') or '').strip()
    if not email:
        return render_template('forgot_password.html', error="Email is required.")

    # Use Supabase Auth password reset (sends email via Supabase)
    # We also send our branded Brevo email
    import requests as req
    try:
        # Trigger Supabase password reset
        res = req.post(
            f"{SUPABASE_URL}/auth/v1/recover",
            headers={
                "apikey": SUPABASE_ANON,
                "Content-Type": "application/json"
            },
            json={"email": email},
            timeout=10
        )

        # Also send branded Brevo email if we have the user's name
        db = get_db()
        cursor = get_cursor(db)
        cursor.execute("SELECT name FROM members WHERE email = %s LIMIT 1", (email,))
        member = cursor.fetchone()
        cursor.close()

        if member:
            reset_url = f"{request.host_url.rstrip('/')}reset-password"
            send_password_reset_email(email, member["name"], reset_url)
    except Exception as e:
        print(f"Password reset error: {e}")

    # Always show success (don't reveal if email exists)
    return render_template('forgot_password.html',
                           success="If that email is registered, a reset link has been sent.")


@app.route('/reset-password', methods=['GET'])
def reset_password_page():
    """Supabase handles the actual reset via its redirect URL."""
    return render_template('reset_password.html', token=None, error=None)


@app.route('/login', methods=['GET', 'POST'])
def login_page():
    """
    Unified login for all roles: admin, treasurer, member.
    GET  → serve the login page (Supabase JS handles auth)
    POST → legacy fallback using phone + group_id + password (session-based)
    """
    # Already logged in → redirect appropriately
    if session.get("user_id") and session.get("group_id"):
        role = session.get("role", "member")
        if role in ("admin", "treasurer"):
            return redirect("/dashboard")
        return redirect("/member-portal")

    # POST: form-based login — phone/email + password, no Group ID needed
    if request.method == "POST":
        identifier = (request.form.get("identifier") or
                      request.form.get("phone")      or
                      request.form.get("email")      or "").strip()
        password   =  request.form.get("password") or ""
        group_id   = (request.form.get("group_id") or "").strip()  # optional: from group picker

        if not identifier or not password:
            return render_template("login.html",
                                   supabase_url=SUPABASE_URL,
                                   supabase_anon=SUPABASE_ANON,
                                   error="Phone/email and password are required.")

        db = get_db()
        cursor = get_cursor(db)

        # Find ALL memberships matching phone or email
        # Normalise phone to try multiple formats
        phone_variants = [identifier]
        cleaned = identifier.strip().replace(' ', '').replace('-', '')
        if cleaned.startswith('+255'):
            phone_variants += [cleaned[4:], '0' + cleaned[4:], cleaned[1:]]
        elif cleaned.startswith('255'):
            phone_variants += ['+' + cleaned, cleaned[3:], '0' + cleaned[3:]]
        elif cleaned.startswith('0') and len(cleaned) >= 9:
            phone_variants += ['+255' + cleaned[1:], '255' + cleaned[1:]]
        # Deduplicate
        phone_variants = list(dict.fromkeys(phone_variants))

        cursor.execute("""
            SELECT m.id, m.name, m.password, m.role, m.is_active,
                   m.group_id, m.is_system, g.name AS group_name
            FROM members m
            JOIN groups g ON g.id = m.group_id
            WHERE (
                m.phone = ANY(%s::text[])
                OR m.email = %s
            )
            ORDER BY g.name
        """, (phone_variants, identifier))
        matches = cursor.fetchall()
        cursor.close()

        if not matches:
            return render_template("login.html",
                                   supabase_url=SUPABASE_URL,
                                   supabase_anon=SUPABASE_ANON,
                                   error="Account not found. Check your phone/email.")

        # Verify password against first match (same password across all groups)
        first = matches[0]
        if not first["password"] or not check_password_hash(first["password"], password):
            return render_template("login.html",
                                   supabase_url=SUPABASE_URL,
                                   supabase_anon=SUPABASE_ANON,
                                   error="Incorrect password.")

        # Build deduplicated group list
        seen   = set()
        groups = []
        for u in matches:
            if u['group_id'] in seen: continue
            seen.add(u['group_id'])
            if not u.get('is_active', 1): continue
            groups.append(u)

        if not groups:
            return render_template("login.html",
                                   supabase_url=SUPABASE_URL,
                                   supabase_anon=SUPABASE_ANON,
                                   error="Your account is inactive. Contact your admin.")

        # If only one group OR user already picked a group → set session
        def set_session_and_redirect(u):
            role = (u["role"] or ("admin" if u["is_system"] else "member")).strip().lower()
            session.clear()
            session["user_id"]     = u["id"]
            session["member_id"]   = u["id"]
            session["group_id"]    = u["group_id"]
            session["role"]        = role
            session["member_name"] = u["name"]
            return redirect("/dashboard" if role in ("admin","treasurer") else "/member-portal")

        if group_id:
            picked = next((u for u in groups if str(u['group_id']) == str(group_id)), None)
            if picked:
                return set_session_and_redirect(picked)

        if len(groups) == 1:
            return set_session_and_redirect(groups[0])

        # Multiple groups → show picker (re-render with groups list)
        return render_template("login.html",
                               supabase_url=SUPABASE_URL,
                               supabase_anon=SUPABASE_ANON,
                               show_picker=True,
                               picker_groups=groups,
                               identifier=identifier,
                               password=password)

    # GET → serve login page
    return render_template("login.html",
                           supabase_url=SUPABASE_URL,
                           supabase_anon=SUPABASE_ANON)


@app.route('/logout')
def logout():
    """Clear server-side session. Client clears Supabase session via JS."""
    session.clear()
    resp = redirect('/login')
    resp.delete_cookie('sb-access-token')
    resp.delete_cookie('sb-refresh-token')
    resp.delete_cookie('sb-group-id')
    return resp


# ==================== SMS REMINDERS ====================
#
# Optional per-group settings (settings table, key/value):
#   sms_reminders_enabled   1 / 0               default 1
#   sms_reminder_days       "3,1"               days BEFORE a due date
#   sms_overdue_days        "1"  or "off"       days AFTER a due date
#   sms_daily_cap           300                 max bulk texts per 24h
# =====================================================================

import sms_service
 
 
# ---------------------------------------------------------------- helpers
def _eat_today():
    """Today's date in Tanzania (UTC+3). Render servers run on UTC."""
    from datetime import timezone
    return (datetime.now(timezone.utc) + timedelta(hours=3)).date()
 
 
def _ensure_sms_log(db):
    cur = get_cursor(db)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sms_log (
            id         SERIAL PRIMARY KEY,
            group_id   INTEGER NOT NULL,
            member_id  INTEGER,
            loan_id    INTEGER,
            month_num  INTEGER,
            kind       TEXT NOT NULL,
            phone      TEXT,
            message    TEXT,
            status     TEXT NOT NULL,
            request_id TEXT,
            error      TEXT,
            sent_by    INTEGER,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_sms_log_sent_once
        ON sms_log (loan_id, month_num, kind) WHERE status = 'sent'
    """)
    db.commit()
    cur.close()
 
 
def _log_sms(db, group_id, kind, phone, message, result,
             member_id=None, loan_id=None, month_num=None, sent_by=None):
    cur = get_cursor(db)
    cur.execute("""
        INSERT INTO sms_log (group_id, member_id, loan_id, month_num, kind, phone,
                             message, status, request_id, error, sent_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (group_id, member_id, loan_id, month_num, kind, phone, message,
          'sent' if result.get('ok') else 'failed',
          result.get('request_id') or None, result.get('error'), sent_by))
    db.commit()
    cur.close()
 
 
def _sms_group_allowed(db, group_id):
    """Groups whose subscription is suspended don't get SMS sent for them."""
    return get_group_subscription_status(db, group_id)["status"] in ("active", "free", "grace")
 
 
def _sms_sent_last_24h(db, group_id):
    cur = get_cursor(db)
    cur.execute("""
        SELECT COUNT(*) FROM sms_log
        WHERE group_id = %s AND status = 'sent' AND created_at >= NOW() - INTERVAL '24 hours'
    """, (group_id,))
    n = int(get_single_value(cur, 0))
    cur.close()
    return n
 
 
def _parse_days(value, default):
    text = str(value if value is not None else "").strip().lower()
    if text in ("off", "none"):
        return []
    days = [int(p) for p in text.split(",") if p.strip().isdigit()]
    return days or list(default)
 
 
def _first_name(full):
    first = (str(full or "").strip().split() or [""])[0]
    return sms_service.ascii_safe(first)[:15].title() or "Mwanachama"
 
 
# Rejesho reminders are a paid add-on. Bulk SMS is open to everyone unless this is True.
SMS_ADDON_REQUIRED_FOR_BULK = False
_ADDONS_READY = False
 
 
def _ensure_addons(db):
    global _ADDONS_READY
    if _ADDONS_READY:
        return
    cur = get_cursor(db)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS group_addons (
            id                SERIAL PRIMARY KEY,
            group_id          INTEGER NOT NULL,
            addon             TEXT NOT NULL,
            active_from       DATE NOT NULL,
            active_until      DATE NOT NULL,
            amount_paid       NUMERIC DEFAULT 0,
            payment_reference TEXT,
            note              TEXT,
            created_at        TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_group_addons ON group_addons (group_id, addon, active_until)")
    db.commit()
    cur.close()
    _ADDONS_READY = True
 
 
def _addon_status(db, group_id, addon="sms_reminders", today=None):
    """Returns (is_active, active_until_date_or_None) for a paid add-on."""
    _ensure_addons(db)
    today = today or _eat_today()
    cur = get_cursor(db)
    cur.execute("""SELECT MAX(active_until) AS until FROM group_addons
                   WHERE group_id = %s AND addon = %s AND active_from <= %s AND active_until >= %s""",
                (group_id, addon, today, today))
    row = cur.fetchone()
    cur.close()
    until = row['until'] if row else None
    return (until is not None), until
 
 
def _msg_rejesho_before(name, group, amount, days, due):
    when = "leo" if days == 0 else "kesho" if days == 1 else f"baada ya siku {days}"
    return (f"Habari {_first_name(name)}, rejesho yako ya TZS {amount:,.0f} ya "
            f"{sms_service.ascii_safe(group)[:25]} inatakiwa {when} ({due:%d/%m/%Y}). "
            f"Tafadhali lipa kwa wakati. Asante.")
 
 
def _msg_rejesho_overdue(name, group, amount, due):
    return (f"Habari {_first_name(name)}, rejesho yako ya TZS {amount:,.0f} ya "
            f"{sms_service.ascii_safe(group)[:25]} ilitakiwa kulipwa tarehe {due:%d/%m/%Y} "
            f"na bado haijakamilika. Tafadhali lipa haraka. Asante.")
 
 
# ---------------------------------------------------------------- admin page + status + log
@app.route('/sms-page')
@require_admin
def sms_page():
    return render_template('sms.html')
 
 
@app.route('/api/sms/status', methods=['GET'])
@require_admin
def sms_status():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _ensure_sms_log(db)
    settings = get_group_settings(db, group_id)
    try:
        cap = int(float(settings.get("sms_daily_cap", 300)))
    except (TypeError, ValueError):
        cap = 300
    active, until = _addon_status(db, group_id)
    return jsonify({
        "configured":    sms_service.is_configured(),
        "sender_id":     sms_service.sender_id(),
        "sent_last_24h": _sms_sent_last_24h(db, group_id),
        "daily_cap":     cap,
        "max_chars":     sms_service.MAX_CHARS,
        "reminders_premium": {
            "active":  active,
            "until":   until.isoformat() if until else None,
            "contact": os.environ.get("SUPPORT_CONTACT", ""),
        },
    })
 
 
@app.route('/api/sms/log', methods=['GET'])
@require_admin
def sms_log_list():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _ensure_sms_log(db)
    cur = get_cursor(db)
    cur.execute("""
        SELECT l.id, l.created_at, l.kind, l.status, l.error, l.phone, l.message,
               m.name AS member_name
        FROM sms_log l
        LEFT JOIN members m ON m.id = l.member_id
        WHERE l.group_id = %s
        ORDER BY l.id DESC
        LIMIT 100
    """, (group_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    return jsonify({"log": rows})
 
 
# ---------------------------------------------------------------- bulk SMS
@app.route('/api/sms/bulk', methods=['POST'])
@require_admin
def sms_bulk():
    """
    Send one message to many members of the CURRENT group (admin only).
 
    Body: {
      "message": "Habari {name}, mkutano kesho saa 4 asubuhi. {group}",
      "all": true            // or "member_ids": [3, 5, 8]
      "preview": true        // default true: shows what WOULD happen
      "confirm": true        // must be true to really send
    }
    {name} = member's first name, {group} = group name.
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
 
    data      = request.get_json(silent=True) or {}
    template  = sms_service.ascii_safe(data.get("message", ""))
    preview   = data.get("preview", True) is not False
    confirm   = data.get("confirm", False) is True
    send_all  = data.get("all", False) is True
    raw_ids   = data.get("member_ids") or []
 
    if not template:
        return jsonify({"error": "Message is required"}), 400
    if len(template) > sms_service.MAX_CHARS:
        return jsonify({"error": f"Message too long (max {sms_service.MAX_CHARS} characters)"}), 400
    if not send_all and not raw_ids:
        return jsonify({"error": "Choose 'all members' or select members"}), 400
    try:
        member_ids = [int(x) for x in raw_ids]
    except (TypeError, ValueError):
        return jsonify({"error": "member_ids must be numbers"}), 400
 
    if not _sms_group_allowed(db, group_id):
        return jsonify({"error": "Group subscription is suspended - SMS is disabled"}), 403
    if SMS_ADDON_REQUIRED_FOR_BULK and not _addon_status(db, group_id)[0]:
        return jsonify({"error": "Bulk SMS is part of the SMS premium plan. Contact support to activate it."}), 403
 
    _ensure_sms_log(db)
    settings   = get_group_settings(db, group_id)
    group_name = sms_service.ascii_safe(settings.get("group_name", "Kikoba"))[:25]
    try:
        cap = int(float(settings.get("sms_daily_cap", 300)))
    except (TypeError, ValueError):
        cap = 300
 
    cur = get_cursor(db)
    if send_all:
        cur.execute("""
            SELECT id, name, phone FROM members
            WHERE group_id = %s AND is_system = 0 AND COALESCE(is_active, 1) = 1
            ORDER BY name
        """, (group_id,))
    else:
        cur.execute("""
            SELECT id, name, phone FROM members
            WHERE group_id = %s AND is_system = 0 AND COALESCE(is_active, 1) = 1
              AND id = ANY(%s)
            ORDER BY name
        """, (group_id, member_ids))
    members = cur.fetchall()
    cur.close()
 
    items, invalid_phone, too_long, seen = [], [], [], set()
    for m in members:
        phone = sms_service.normalize_phone(m['phone'])
        if not phone:
            invalid_phone.append(m['name'])
            continue
        text = sms_service.ascii_safe(
            template.replace("{name}", _first_name(m['name'])).replace("{group}", group_name)
        )
        if len(text) > sms_service.MAX_CHARS:
            too_long.append(m['name'])
            continue
        if (phone, text) in seen:          # two members sharing one phone number
            continue
        seen.add((phone, text))
        items.append({"member_id": m['id'], "phone": phone, "message": text})
 
    sent_24h  = _sms_sent_last_24h(db, group_id)
    sample    = items[0]["message"] if items else ""
    per_person = sms_service.segments(sample)
    summary = {
        "recipients":         len(items),
        "sms_parts_each":     per_person,
        "invalid_phone":      invalid_phone,
        "too_long":           too_long,
        "sample":             sample,
        "sent_last_24h":      sent_24h,
        "daily_cap":          cap,
        "would_exceed_cap":   sent_24h + len(items) > cap,
        "configured":         sms_service.is_configured(),
    }
 
    if preview or not confirm:
        return jsonify({"preview": True, **summary})
 
    # ---- real send ----
    if not items:
        return jsonify({"error": "No members with a valid phone number to send to", **summary}), 400
    if not sms_service.is_configured():
        return jsonify({"error": "SMS is not configured on the server "
                                 "(BEEM_API_KEY / BEEM_SECRET_KEY missing)", **summary}), 503
    if summary["would_exceed_cap"]:
        return jsonify({"error": f"This would pass the daily limit of {cap} texts "
                                 f"({sent_24h} already sent in the last 24 hours)", **summary}), 429
 
    sent_by = session.get("member_id")
    sent = failed = 0
    errors = {}
    results = sms_service.send_many([{"phone": it["phone"], "message": it["message"]} for it in items])
    for it, res in zip(items, results):
        _log_sms(db, group_id, "bulk", it["phone"], it["message"], res,
                 member_id=it["member_id"], sent_by=sent_by)
        if res["ok"]:
            sent += 1
        else:
            failed += 1
            errors[res["error"]] = errors.get(res["error"], 0) + 1
 
    return jsonify({"preview": False, "sent": sent, "failed": failed,
                    "errors": errors, **summary})
 
 
# ---------------------------------------------------------------- rejesho reminders (cron)
@app.route('/api/reminders/loan-due', methods=['POST'])
def send_loan_due_reminders():
    """
    Rejesho reminders. Call once a day from a scheduler.  Owner-token protected.
 
      ?dry_run=1        show who WOULD be texted, send nothing
      ?as_of=2026-11-28 (dry run only) pretend today is that date
      ?member_id=7      only look at that member's loans (handy for testing)
      ?limit=100        max texts per run (a rerun picks up the rest)
 
    Only groups with an active SMS reminders add-on are texted.
 
    Texts members BEFORE a due date (default 3 and 1 days) and AFTER it
    (default 1 day) - but only for installments not yet paid, and only the
    amount still owed on that installment.  Never sends the same text twice.
    """
    expected = os.environ.get("OWNER_TOKEN", "")
    if not expected or request.headers.get("X-Owner-Token", "") != expected:
        return jsonify({"error": "Unauthorized"}), 403
 
    payload = request.get_json(silent=True) or {}
    dry_run = str(request.args.get("dry_run") or payload.get("dry_run") or "").lower() in ("1", "true", "yes")
    try:
        limit = int(request.args.get("limit") or payload.get("limit") or 100)
    except (TypeError, ValueError):
        limit = 100
 
    try:
        only_member = int(request.args.get("member_id") or payload.get("member_id") or 0) or None
    except (TypeError, ValueError):
        only_member = None
    as_of_raw = request.args.get("as_of") or payload.get("as_of")
 
    if not dry_run and not sms_service.is_configured():
        return jsonify({"error": "SMS is not configured (BEEM_API_KEY / BEEM_SECRET_KEY missing)"}), 503
 
    db = get_db()
    _ensure_sms_log(db)
    _ensure_addons(db)
    cursor = get_cursor(db)
    today  = _eat_today()
    if as_of_raw:
        if not dry_run:
            return jsonify({"error": "as_of only works together with dry_run=1 "
                                     "(a real send would be marked as sent for a date that has not come)"}), 400
        try:
            today = datetime.strptime(str(as_of_raw), "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "as_of must look like 2026-11-28"}), 400
 
    cursor.execute("""
        SELECT l.id, l.group_id, l.member_id, l.principal, l.months, l.due_date,
               m.name AS member_name, m.phone, g.name AS group_name
        FROM loans l
        JOIN members m ON m.id = l.member_id
        JOIN groups  g ON g.id = l.group_id
        WHERE l.status IN ('Active', 'Overdue')
          AND m.phone IS NOT NULL AND m.phone <> ''
    """)
    loans = cursor.fetchall()
 
    group_cfg = {}
 
    def cfg_for(group_id):
        if group_id not in group_cfg:
            s = get_group_settings(db, group_id)
            group_cfg[group_id] = {
                "premium": _addon_status(db, group_id, today=today)[0],
                "enabled": str(s.get("sms_reminders_enabled", "1")).strip() == "1"
                           and _sms_group_allowed(db, group_id),
                "before":  [d for d in _parse_days(s.get("sms_reminder_days"), [3, 1])],
                "overdue": _parse_days(s.get("sms_overdue_days"), [1]),
            }
        return group_cfg[group_id]
 
    stats = {"sent": 0, "failed": 0, "skipped_paid": 0, "skipped_already_sent": 0,
             "skipped_group_off": 0, "skipped_not_premium": 0, "skipped_bad_phone": 0,
             "held_back_by_limit": 0}
    preview = []
    pending = []
    budget  = limit
 
    for loan in loans:
        if only_member and loan['member_id'] != only_member:
            continue
        cfg = cfg_for(loan['group_id'])
        if not cfg["premium"]:
            stats["skipped_not_premium"] += 1
            continue
        if not cfg["enabled"]:
            stats["skipped_group_off"] += 1
            continue
 
        phone = sms_service.normalize_phone(loan['phone'])
        if not phone:
            stats["skipped_bad_phone"] += 1
            continue
 
        months    = int(loan['months'])
        monthly   = float(loan['principal']) / months
        final_due = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        base_month = final_due.month - months
        base_year  = final_due.year
        if base_month <= 0:
            base_month += 12
            base_year  -= 1
 
        cursor.execute(
            "SELECT COALESCE(SUM(amount), 0) AS paid FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (loan['id'], loan['group_id'])
        )
        total_paid = float(get_single_value(cursor, 0))
 
        for mn in range(1, months + 1):
            dm, dy = base_month + mn, base_year
            if dm > 12:
                dm -= 12
                dy += 1
            due   = date(dy, dm, min(final_due.day, calendar.monthrange(dy, dm)[1]))
            delta = (due - today).days
 
            if delta >= 0 and delta in cfg["before"]:
                kind = f"rejesho_before_{delta}"
            elif delta < 0 and -delta in cfg["overdue"]:
                kind = f"rejesho_overdue_{-delta}"
            else:
                continue
 
            # Still owed on THIS installment (oldest-first allocation).
            target      = monthly * mn
            prev_target = monthly * (mn - 1)
            remaining   = min(monthly, target - max(total_paid, prev_target))
            if remaining <= 1.0:
                stats["skipped_paid"] += 1
                continue
 
            cursor.execute(
                "SELECT 1 FROM sms_log WHERE loan_id = %s AND month_num = %s AND kind = %s AND status = 'sent'",
                (loan['id'], mn, kind)
            )
            if cursor.fetchone():
                stats["skipped_already_sent"] += 1
                continue
 
            amount = round(remaining)
            if kind.startswith("rejesho_before"):
                text = _msg_rejesho_before(loan['member_name'], loan['group_name'], amount, delta, due)
            else:
                text = _msg_rejesho_overdue(loan['member_name'], loan['group_name'], amount, due)
 
            if budget <= 0:
                stats["held_back_by_limit"] += 1
                continue
            budget -= 1
 
            if dry_run:
                preview.append({"loan_id": loan['id'], "member": loan['member_name'],
                                "phone": "..." + phone[-3:], "month_num": mn, "kind": kind,
                                "amount": amount, "message": text})
                continue
 
            pending.append({"phone": phone, "message": text, "kind": kind,
                            "group_id": loan['group_id'], "member_id": loan['member_id'],
                            "loan_id": loan['id'], "month_num": mn})
 
    cursor.close()
 
    if pending:
        results = sms_service.send_many([{"phone": p["phone"], "message": p["message"]} for p in pending])
        for p, res in zip(pending, results):
            _log_sms(db, p["group_id"], p["kind"], p["phone"], p["message"], res,
                     member_id=p["member_id"], loan_id=p["loan_id"], month_num=p["month_num"])
            stats["sent" if res["ok"] else "failed"] += 1
 
    result = {"status": "success", "dry_run": dry_run, "today": today.isoformat(), **stats}
    if dry_run:
        result["would_send"] = preview
    return jsonify(result)
 
 
# ---------------------------------------------------------------- subscription reminders (cron)
@app.route('/api/reminders/subscription-due', methods=['POST'])
def send_subscription_due_reminders():
    """Text group admins 3 days before their subscription expires. Owner-token protected."""
    expected = os.environ.get("OWNER_TOKEN", "")
    if not expected or request.headers.get("X-Owner-Token", "") != expected:
        return jsonify({"error": "Unauthorized"}), 403
 
    payload = request.get_json(silent=True) or {}
    dry_run = str(request.args.get("dry_run") or payload.get("dry_run") or "").lower() in ("1", "true", "yes")
    if not dry_run and not sms_service.is_configured():
        return jsonify({"error": "SMS is not configured (BEEM_API_KEY / BEEM_SECRET_KEY missing)"}), 503
 
    db = get_db()
    _ensure_sms_log(db)
    cursor = get_cursor(db)
    warn   = _eat_today() + timedelta(days=3)
    sent = failed = skipped = 0
    preview = []
 
    cursor.execute("""
        SELECT g.id AS group_id, g.name, g.subscription_expires,
               m.id AS member_id, m.name AS admin_name, m.phone,
               (SELECT COUNT(*) FROM members
                 WHERE group_id = g.id AND is_system = 0 AND COALESCE(is_active, 1) = 1) AS member_count
        FROM groups g
        JOIN members m ON m.group_id = g.id AND m.role = 'admin'
        WHERE COALESCE(g.is_free, 0) = 0
          AND g.subscription_expires = %s
          AND m.phone IS NOT NULL AND m.phone <> ''
    """, (warn,))
 
    for row in cursor.fetchall():
        phone = sms_service.normalize_phone(row['phone'])
        if not phone:
            skipped += 1
            continue
 
        cursor.execute("""
            SELECT 1 FROM sms_log
            WHERE group_id = %s AND member_id = %s AND kind = 'subscription_before_3'
              AND status = 'sent' AND created_at >= NOW() - INTERVAL '2 days'
        """, (row['group_id'], row['member_id']))
        if cursor.fetchone():
            skipped += 1
            continue
 
        amount = int(row['member_count']) * RATE_PER_MEMBER
        expiry = row['subscription_expires']
        if isinstance(expiry, str):
            expiry = datetime.strptime(expiry[:10], "%Y-%m-%d").date()
        text = (f"Habari {_first_name(row['admin_name'])}, usajili wa "
                f"{sms_service.ascii_safe(row['name'])[:25]} unaisha tarehe {expiry:%d/%m/%Y}. "
                f"Kiasi: TZS {amount:,.0f}. Tafadhali lipa ili kuendelea kutumia mfumo. Asante.")
 
        if dry_run:
            preview.append({"group": row['name'], "admin": row['admin_name'],
                            "phone": "..." + phone[-3:], "message": text})
            continue
 
        res = sms_service.send_one(phone, text)
        _log_sms(db, row['group_id'], "subscription_before_3", phone, text, res,
                 member_id=row['member_id'])
        if res["ok"]:
            sent += 1
        else:
            failed += 1
 
    cursor.close()
    result = {"status": "success", "dry_run": dry_run, "sent": sent, "failed": failed, "skipped": skipped}
    if dry_run:
        result["would_send"] = preview
    return jsonify(result)
 
 
# ---------------------------------------------------------------- test button
@app.route('/api/sms/test', methods=['POST'])
@require_admin
def sms_test():
    """
    Send ONE sample reminder to the admin's own phone (or a number typed in),
    exactly as members would receive it.  Max 3 per group per 24 hours.
    Body (optional): { "phone": "0712345678" }
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    if not sms_service.is_configured():
        return jsonify({"ok": False, "error": "SMS is not configured on the server "
                                              "(BEEM_API_KEY / BEEM_SECRET_KEY missing)"}), 503
    _ensure_sms_log(db)
 
    body = request.get_json(silent=True) or {}
    cur = get_cursor(db)
    cur.execute("SELECT name, phone FROM members WHERE id = %s AND group_id = %s",
                (session.get("member_id"), group_id))
    me = cur.fetchone()
    cur.execute("""SELECT COUNT(*) AS c FROM sms_log
                   WHERE group_id = %s AND kind = 'test' AND status = 'sent'
                     AND created_at >= NOW() - INTERVAL '24 hours'""", (group_id,))
    used = int(get_single_value(cur, 0))
    cur.close()
    if used >= 3:
        return jsonify({"ok": False, "error": "You already sent 3 test messages in the last 24 hours."}), 429
 
    phone = sms_service.normalize_phone(body.get("phone") or (me['phone'] if me else None))
    if not phone:
        return jsonify({"ok": False, "error": "No valid phone number. Type one in, or save your phone number "
                                              "on your member profile."}), 400
 
    group_name = get_group_settings(db, group_id).get("group_name", "Kikoba")
    text = "[TEST] " + _msg_rejesho_before(me['name'] if me else "Mwanachama", group_name,
                                          500000, 3, _eat_today() + timedelta(days=3))
    res = sms_service.send_one(phone, text)
    _log_sms(db, group_id, "test", phone, text, res, member_id=session.get("member_id"),
             sent_by=session.get("member_id"))
    return jsonify({"ok": res["ok"], "error": res.get("error"), "phone_hint": "..." + phone[-3:], "message": text})
 
 
# ---------------------------------------------------------------- premium add-on (owner only)
@app.route('/api/owner/group/<int:group_id>/addon', methods=['POST'])
@require_owner
def owner_grant_addon(group_id):
    """
    Switch on, or extend, the SMS reminders add-on for a group after it has paid.
    Body: { "days": 30, "amount_paid": 15000, "payment_reference": "MPESA123", "note": "optional" }
    Extending adds the days on top of whatever time the group still has left.
    """
    db = get_db()
    _ensure_addons(db)
    data = request.get_json(silent=True) or {}
    try:
        days = int(data.get("days", 30))
        amount = float(data.get("amount_paid", 0) or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "days and amount_paid must be numbers"}), 400
    if days < 1 or days > 400:
        return jsonify({"error": "days must be between 1 and 400"}), 400
 
    cur = get_cursor(db)
    cur.execute("SELECT id, name FROM groups WHERE id = %s", (group_id,))
    group = cur.fetchone()
    if not group:
        cur.close()
        return jsonify({"error": "Group not found"}), 404
 
    today = _eat_today()
    cur.execute("""SELECT MAX(active_until) AS u FROM group_addons
                   WHERE group_id = %s AND addon = 'sms_reminders' AND active_until >= %s""", (group_id, today))
    current = cur.fetchone()['u']
    start = (current + timedelta(days=1)) if current else today
    until = start + timedelta(days=days - 1)
    cur.execute("""INSERT INTO group_addons (group_id, addon, active_from, active_until, amount_paid,
                                             payment_reference, note)
                   VALUES (%s, 'sms_reminders', %s, %s, %s, %s, %s)""",
                (group_id, start, until, amount, str(data.get("payment_reference") or "")[:80] or None,
                 str(data.get("note") or "")[:200] or None))
    db.commit()
    cur.close()
    return jsonify({"status": "success", "group": group['name'],
                    "active_from": start.isoformat(), "active_until": until.isoformat()})
 
 
@app.route('/api/owner/group/<int:group_id>/addon/stop', methods=['POST'])
@require_owner
def owner_stop_addon(group_id):
    """Switch the SMS reminders add-on off for a group, effective today."""
    db = get_db()
    _ensure_addons(db)
    yesterday = _eat_today() - timedelta(days=1)
    cur = get_cursor(db)
    cur.execute("""UPDATE group_addons SET active_until = %s
                   WHERE group_id = %s AND addon = 'sms_reminders' AND active_until > %s""",
                (yesterday, group_id, yesterday))
    stopped = cur.rowcount
    db.commit()
    cur.close()
    return jsonify({"status": "success", "stopped": stopped})
 
 
@app.route('/api/owner/addons', methods=['GET'])
@require_owner
def owner_list_addons():
    """Which groups have the SMS reminders add-on, and until when."""
    db = get_db()
    _ensure_addons(db)
    cur = get_cursor(db)
    cur.execute("SELECT id, name FROM groups ORDER BY id")
    groups = cur.fetchall()
    cur.close()
    out = []
    for g_row in groups:
        active, until = _addon_status(db, g_row['id'])
        out.append({"group_id": g_row['id'], "name": g_row['name'], "active": active,
                    "active_until": until.isoformat() if until else None})
    return jsonify({"groups": out})
 

# ==================== MEMBERS ====================
@app.route('/members-page')
@require_treasurer_or_above
def members_page():
    return render_template('members.html')

@app.route('/member-details/<int:member_id>')
@require_treasurer_or_above
def member_details_page(member_id):
    
    return render_template('member_details.html', member_id=member_id)


@app.route('/api/members/<int:member_id>/details', methods=['GET'])
@require_any_member
def get_member_details(member_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    
    # Get member basic info
    cursor.execute(
        "SELECT id, name, phone, joined_date FROM members WHERE id = %s AND group_id = %s AND is_system = 0",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404
    
    # Get contribution breakdown
    cursor.execute(
        """SELECT type, SUM(amount) as total FROM contributions 
           WHERE member_id=%s AND group_id=%s AND type != 'jamii_deduction' 
           GROUP BY type""",
        (member_id, group_id)
    )
    contribs = cursor.fetchall()
    contrib_dict = {c["type"]: c["total"] for c in contribs}
    total_contributions = sum(contrib_dict.values())
    
    # Get contribution history
    cursor.execute(
        """SELECT id, type, amount, date, transaction_date 
           FROM contributions 
           WHERE member_id = %s AND group_id = %s AND type != 'jamii_deduction'
           ORDER BY date DESC""",
        (member_id, group_id)
    )
    contribution_history = cursor.fetchall()
    
    # Calculate savings
    member_total_savings = (
        contrib_dict.get('hisa anzia', 0) + 
        contrib_dict.get('hisa', 0) + 
        contrib_dict.get('jamii', 0)
    )
    
    # Get HISA units
    settings = get_group_settings(db, group_id)
    hisa_data = get_member_hisa_units(db, member_id, group_id)
    member_units = hisa_data['units']
    
    # Get loan information
    loan_balances = get_member_loan_balances(db, member_id, group_id)
    
    # Get all loans with details
    cursor.execute(
        """SELECT id, principal, interest, net_amount, months, start_date, due_date, status 
           FROM loans WHERE member_id = %s AND group_id = %s ORDER BY start_date DESC""",
        (member_id, group_id)
    )
    loans = cursor.fetchall()
    
    loan_details = []
    for loan in loans:
        cursor.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (loan['id'], group_id)
        )
        repaid = get_single_value(cursor, 0)
        remaining = max(loan['principal'] - repaid, 0)
        monthly_rejesho = round(loan['principal'] / loan['months'], 2) if loan['months'] else 0

        # Rejesho payment history for this loan
        cursor.execute(
            """SELECT amount, date FROM rejesho
               WHERE loan_id = %s AND group_id = %s
               ORDER BY date ASC""",
            (loan['id'], group_id)
        )
        rejesho_rows = cursor.fetchall()
        rejesho_history = [{"amount": float(r['amount']), "date": r['date']} for r in rejesho_rows]

        loan_details.append({
            "id": loan['id'],
            "principal": loan['principal'],
            "interest": loan['interest'],
            "net_amount": loan['net_amount'],
            "months": loan['months'],
            "monthly_rejesho": monthly_rejesho,
            "start_date": loan['start_date'],
            "due_date": loan['due_date'],
            "status": loan['status'],
            "repaid": repaid,
            "remaining": remaining,
            "rejesho_history": rejesho_history
        })
    
    # Get penalties
    total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)
    
    cursor.execute(
        """SELECT id, type, amount, amount_paid, description, date 
           FROM penalties 
           WHERE member_id = %s AND group_id = %s 
           ORDER BY date DESC""",
        (member_id, group_id)
    )
    penalties = cursor.fetchall()
    
    penalty_details = []
    for p in penalties:
        remaining = max(p['amount'] - (p['amount_paid'] or 0), 0)
        penalty_details.append({
            "id": p['id'],
            "type": p['type'],
            "amount": p['amount'],
            "amount_paid": p['amount_paid'] or 0,
            "remaining": remaining,
            "description": p['description'],
            "date": p['date']
        })
    
    # Net position — contributions minus what's owed (no profit assumption,
    # since profit distribution method is chosen later at cycle end)
    net_contribution_position = (
        member_total_savings
        - loan_balances["remaining_loans"]
        - total_penalties_due
    )

    cursor.close()

    return jsonify({
        "member": {
            "id": member['id'],
            "name": member['name'],
            "phone": member['phone'],
            "joined_date": member['joined_date']
        },
        "summary": {
            "contributions": contrib_dict,
            "total_contributions": total_contributions,
            "total_savings": member_total_savings,
            "hisa_units": round(member_units, 2),
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": total_penalties_due,
            "net_contribution_position": net_contribution_position
        },
        "contribution_history": [dict(c) for c in contribution_history],
        "loans": loan_details,
        "penalties": penalty_details
    })

@app.route('/api/members', methods=['GET'])
@require_any_member
def get_members():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT * FROM members WHERE group_id = %s ORDER BY CASE role WHEN 'admin' THEN 0 WHEN 'treasurer' THEN 1 ELSE 2 END, id ASC", 
        (group_id,)
    )
    members = cursor.fetchall()
    
    # PRE-FETCH all contributions in one query
    cursor.execute("""
        SELECT member_id, SUM(amount) as total
        FROM contributions 
        WHERE group_id = %s AND type != 'jamii_deduction'
        GROUP BY member_id
    """, (group_id,))
    contributions_map = {row['member_id']: row['total'] for row in cursor.fetchall()}
    
    # PRE-FETCH all loans in one query
    cursor.execute("""
        SELECT member_id, SUM(principal) as total_principal
        FROM loans 
        WHERE group_id = %s
        GROUP BY member_id
    """, (group_id,))
    loans_map = {row['member_id']: row['total_principal'] for row in cursor.fetchall()}
    
    # PRE-FETCH all repayments in one query
    cursor.execute("""
        SELECT l.member_id, SUM(r.amount) as total_rejesho
        FROM rejesho r
        JOIN loans l ON r.loan_id = l.id
        WHERE l.group_id = %s
        GROUP BY l.member_id
    """, (group_id,))
    rejesho_map = {row['member_id']: row['total_rejesho'] for row in cursor.fetchall()}
    
    # PRE-FETCH all penalties in one query
    cursor.execute("""
        SELECT member_id, SUM(amount - COALESCE(amount_paid, 0)) as total_penalties
        FROM penalties 
        WHERE group_id = %s
        GROUP BY member_id
    """, (group_id,))
    penalties_map = {row['member_id']: row['total_penalties'] for row in cursor.fetchall()}

    # PRE-FETCH hisa-only contributions (for correct HISA units — matches member details page)
    cursor.execute("""
        SELECT member_id, SUM(amount) as total_hisa
        FROM contributions
        WHERE group_id = %s AND type = 'hisa'
        GROUP BY member_id
    """, (group_id,))
    hisa_only_map = {row['member_id']: row['total_hisa'] for row in cursor.fetchall()}

    cursor.close()
    
    result = []
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    
    for m in members:
        member_id = m["id"]
        
        total_contributions = contributions_map.get(member_id, 0)
        total_loans = loans_map.get(member_id, 0)
        total_rejesho = rejesho_map.get(member_id, 0)
        total_penalties = penalties_map.get(member_id, 0)
        total_hisa_only = hisa_only_map.get(member_id, 0)

        # HISA units use ONLY 'hisa' type contributions (matches member details page)
        hisa_units = total_hisa_only / unit_price if unit_price > 0 else 0
        remaining_loans = max(total_loans - total_rejesho, 0)

        result.append({
            "id": member_id,
            "name": m["name"],
            "phone": m["phone"],
            "role": m.get("role", "member"),
            "is_active": m.get("is_active", 1),
            "total_contributions": total_contributions,
            "hisa_units": hisa_units,
            "total_loans_committed": total_loans,
            "total_penalties": total_penalties,
            "total_outstanding": remaining_loans,
            "jamii_paid": 0,
            "jamii_expected": 0,
            "jamii_shortfall": 0
        })
    
    return jsonify(result)


@app.route('/api/members', methods=['POST'])
@require_admin
def add_member():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    name = data.get("name")
    phone = data.get("phone")
    
    if not name:
        return jsonify({"error": "Name is required"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "INSERT INTO members (group_id, name, phone, joined_date, is_system, role, is_active) VALUES (%s, %s, %s, %s, 0, 'member', 1)",
        (group_id, name, phone, datetime.now().strftime("%Y-%m-%d"))
    )
    db.commit()
    cursor.close()
    
    return jsonify({"status": "success"})


@app.route('/api/members/<int:member_id>', methods=['PUT', 'DELETE'])
@require_admin
def edit_member(member_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, is_system FROM members WHERE id = %s AND group_id = %s",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found in this group"}), 404
    
    if member['is_system'] == 1:
        cursor.close()
        return jsonify({"error": "Cannot modify system admin account"}), 400
    
    if request.method == 'DELETE':
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM contributions WHERE member_id = %s AND group_id = %s) +
                (SELECT COUNT(*) FROM loans WHERE member_id = %s AND group_id = %s) +
                (SELECT COUNT(*) FROM penalties WHERE member_id = %s AND group_id = %s) as total
        """, (member_id, group_id, member_id, group_id, member_id, group_id))
        has_records = cursor.fetchone()['total']
        
        if has_records > 0:
            cursor.close()
            return jsonify({
                "error": "Cannot delete member with existing contributions, loans, or penalties"
            }), 400
        
        try:
            cursor.execute("DELETE FROM members WHERE id = %s AND group_id = %s", (member_id, group_id))
            db.commit()
            cursor.close()
            return jsonify({"status": "success", "message": "Member deleted"})
        except Exception as e:
            db.rollback()
            cursor.close()
            return jsonify({"error": str(e)}), 500
    
    # PUT
    data = request.get_json()
    name = data.get('name', '').strip()
    phone = data.get('phone', '').strip()
    
    if not name:
        cursor.close()
        return jsonify({"error": "Name is required"}), 400
    
    try:
        cursor.execute(
            "UPDATE members SET name = %s, phone = %s WHERE id = %s AND group_id = %s",
            (name, phone, member_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Member updated"})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500



# ==================== CONTRIBUTIONS ====================

@app.route('/contributions-page')
@require_treasurer_or_above
def contributions_page():
    return render_template('contributions.html')


@app.route('/api/contributions', methods=['GET'])
@require_any_member
def get_contributions():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT c.id, c.member_id, c.type, c.amount, c.date, c.transaction_date, m.name as member_name
        FROM contributions c
        JOIN members m ON c.member_id = m.id
        WHERE c.group_id = %s
        ORDER BY c.date DESC
    """, (group_id,))
    contributions = cursor.fetchall()
    cursor.close()

    result = [dict(c) for c in contributions]
    return jsonify(result)


@app.route('/api/contributions', methods=['POST'])
@require_treasurer_or_above
def add_contribution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    member_id = data.get("member_id")
    ctype = data.get("type")
    amount = data.get("amount")
    entry_date = data.get("date")
    transaction_date = data.get("transaction_date")

    if not member_id or not ctype or not amount:
        return jsonify({"error": "All fields are required"}), 400

    if not entry_date:
        entry_date = datetime.now().strftime("%Y-%m-%d")
    
    if transaction_date is None:
        transaction_date = entry_date

    cursor = get_cursor(db)

    if ctype == "rejesho":
        cursor.execute(
            "SELECT * FROM loans WHERE member_id = %s AND group_id = %s AND status != 'Cleared' ORDER BY start_date DESC LIMIT 1",
            (member_id, group_id)
        )
        loan = cursor.fetchone()
        
        if not loan:
            cursor.close()
            return jsonify({"error": "No active loan found for this member"}), 400

        cursor.execute(
            "INSERT INTO rejesho (group_id, loan_id, amount, date) VALUES (%s, %s, %s, %s)",
            (group_id, loan["id"], amount, transaction_date)
        )
        
        db.commit()
        cursor.close()
        update_loan_status(db, loan["id"], group_id)
    else:
        cursor.execute(
            "INSERT INTO contributions (group_id, member_id, type, amount, date, transaction_date) VALUES (%s, %s, %s, %s, %s, %s)",
            (group_id, member_id, ctype, amount, entry_date, transaction_date)
        )
        db.commit()
        cursor.close()

    return jsonify({"status": "success"})


@app.route('/api/contributions/<int:contribution_id>', methods=['PUT', 'DELETE'])
@require_admin
def edit_contribution(contribution_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    
    if request.method == 'DELETE':
        cursor.execute(
            "SELECT type FROM contributions WHERE id = %s AND group_id = %s", 
            (contribution_id, group_id)
        )
        contrib = cursor.fetchone()
        
        if not contrib:
            cursor.close()
            return jsonify({"error": "Contribution not found"}), 404
        
        if contrib['type'] == 'jamii_deduction':
            cursor.close()
            return jsonify({
                "error": "Cannot delete system-generated Jamii deductions. Use Profits page to manage."
            }), 400
        
        try:
            cursor.execute("DELETE FROM contributions WHERE id = %s AND group_id = %s", (contribution_id, group_id))
            db.commit()
            cursor.close()
            return jsonify({"status": "success", "message": "Contribution deleted"})
        except Exception as e:
            db.rollback()
            cursor.close()
            return jsonify({"error": str(e)}), 500
    
    # PUT
    data = request.get_json()
    amount = float(data.get('amount', 0))
    ctype = data.get('type', '').strip()
    date_str = data.get('date', '').strip()
    transaction_date_str = data.get('transaction_date', '').strip()
    
    if amount <= 0:
        cursor.close()
        return jsonify({"error": "Amount must be positive"}), 400
    
    if ctype not in ['hisa', 'hisa anzia', 'jamii']:
        cursor.close()
        return jsonify({"error": "Invalid contribution type"}), 400
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        datetime.strptime(transaction_date_str, "%Y-%m-%d")
        
        cursor.execute(
            "UPDATE contributions SET amount = %s, type = %s, date = %s, transaction_date = %s WHERE id = %s AND group_id = %s",
            (amount, ctype, date_str, transaction_date_str, contribution_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Contribution updated"})
    except ValueError:
        cursor.close()
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500

# ==================== LOANS ====================

@app.route('/loans-page')
@require_treasurer_or_above
def loans_page():
    return render_template('loans.html')


@app.route('/api/loans', methods=['GET'])
@require_any_member
def get_loans():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    auto_insert_loan_penalties(db, group_id)

    cursor = get_cursor(db)
    cursor.execute("""
        SELECT l.*, m.name AS member_name 
        FROM loans l
        JOIN members m ON l.member_id = m.id
        WHERE l.group_id = %s
    """, (group_id,))
    loans = cursor.fetchall()

    result = []

    for l in loans:
        cursor.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (l["id"], group_id)
        )
        repaid = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())

        remaining = l["principal"] - repaid

        today = datetime.now().date()
        due_date = datetime.strptime(l["due_date"], "%Y-%m-%d").date()

        if remaining <= 0:
            status = "Cleared"
        elif today > due_date:
            status = "Overdue"
        else:
            status = "Active"

        monthly_rejesho = round(l["principal"] / l["months"], 2) if l["months"] > 0 else 0

        result.append({
            "loan_id": l["id"],
            "member_name": l["member_name"],
            "principal": l["principal"],
            "interest": l["interest"],
            "net_amount": l["net_amount"],
            "months": l["months"],
            "monthly_rejesho": monthly_rejesho,
            "total": l["principal"],
            "start_date": l["start_date"],
            "due_date": l["due_date"],
            "amount_returned": repaid,
            "remaining": remaining,
            "status": status
        })
        
        if l["status"] != status:
            cursor.execute("UPDATE loans SET status = %s WHERE id = %s AND group_id = %s", (status, l["id"], group_id))
            db.commit()

    cursor.close()
    return jsonify(result)



@app.route('/api/loans/preview', methods=['POST'])
@require_treasurer_or_above
def preview_loan():
    db = get_db()
    group_id = get_current_group_id()

    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data = request.get_json()
    try:
        principal = float(data.get("principal", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid principal amount"}), 400

    if principal <= 0:
        return jsonify({"error": "Principal must be greater than zero"}), 400

    settings = get_group_settings(db, group_id)
    interest_rate  = float(settings.get("interest_rate", 0.10))
    cycle_end_date = settings.get("cycle_end_date", "")

    rules = [
        (float(settings.get("loan_tier1_amount", 500000)),   int(settings.get("loan_tier1_months", 1))),
        (float(settings.get("loan_tier2_amount", 1500000)),  int(settings.get("loan_tier2_months", 3))),
        (float(settings.get("loan_tier3_amount", 3000000)),  int(settings.get("loan_tier3_months", 6))),
        (float(settings.get("loan_tier4_amount", 5000000)),  int(settings.get("loan_tier4_months", 9))),
        (float(settings.get("loan_tier5_amount", 10000000)), int(settings.get("loan_tier5_months", 12))),
    ]

    months = None
    for max_amount, duration in rules:
        if principal <= max_amount:
            months = duration
            break

    if months is None:
        return jsonify({"error": "Loan amount exceeds the maximum allowed by group rules"}), 400

    warning        = None
    original_months = months

    if cycle_end_date:
        try:
            cycle_end      = datetime.strptime(cycle_end_date, "%Y-%m-%d")
            today          = datetime.now()
            remaining_days = (cycle_end - today).days

            if remaining_days <= 0:
                return jsonify({"error": "Cannot issue loans — cycle has ended. Please start a new cycle."}), 400

            max_months_available = remaining_days // 30
            if months > max_months_available:
                months = max(1, max_months_available)
                warning = (f"Loan duration adjusted from {original_months} to {months} months "
                           f"to fit within cycle end date ({cycle_end_date})")
        except ValueError:
            pass

    today        = datetime.now()
    interest     = round(principal * interest_rate)
    net_amount   = principal - interest

    due_year  = today.year + (today.month + months - 1) // 12
    due_month = (today.month + months - 1) % 12 + 1
    due_day   = today.day
    max_day   = calendar.monthrange(due_year, due_month)[1]
    if due_day > max_day:
        due_day = max_day
    due_date = datetime(due_year, due_month, due_day)

    return jsonify({
        "start_date":      today.strftime("%Y-%m-%d"),
        "principal":       principal,
        "interest":        interest,
        "net_amount":      net_amount,
        "months":          months,
        "monthly_rejesho": round(principal / months, 2),
        "due_date":        due_date.strftime("%Y-%m-%d"),
        "warning":         warning
    })

@app.route('/api/loans', methods=['POST'])
@require_treasurer_or_above
def add_loan():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()

    try:
        member_id = data.get("member_id")
        principal = float(data.get("principal", 0))

        if not member_id or principal <= 0:
            return jsonify({"error": "Invalid member or principal amount"}), 400

        cursor = get_cursor(db)
        cursor.execute(
            "SELECT id FROM members WHERE id = %s AND group_id = %s",
            (member_id, group_id)
        )
        member = cursor.fetchone()
        
        if not member:
            cursor.close()
            return jsonify({"error": "Member not found in this group"}), 400

        settings = get_group_settings(db, group_id)
        interest_rate = float(settings.get("interest_rate", 0.10))
        cycle_end_date = settings.get('cycle_end_date', '')

        rules = [
            (float(settings.get("loan_tier1_amount", 500000)),
             int(settings.get("loan_tier1_months", 1))),
            (float(settings.get("loan_tier2_amount", 1500000)),
             int(settings.get("loan_tier2_months", 3))),
            (float(settings.get("loan_tier3_amount", 3000000)),
             int(settings.get("loan_tier3_months", 6))),
            (float(settings.get("loan_tier4_amount", 5000000)),
             int(settings.get("loan_tier4_months", 9))),
            (float(settings.get("loan_tier5_amount", 10000000)),
             int(settings.get("loan_tier5_months", 12))),
        ]

        months = None
        for max_amount, duration in rules:
            if principal <= max_amount:
                months = duration
                break

        if months is None:
            return jsonify({
                "error": "Loan amount exceeds the maximum allowed by group rules"
            }), 400

        warning_message = None
        original_months = months
        
        if cycle_end_date:
            try:
                cycle_end = datetime.strptime(cycle_end_date, "%Y-%m-%d")
                today = datetime.now()
                remaining_days = (cycle_end - today).days
                
                if remaining_days <= 0:
                    return jsonify({
                        "error": "Cannot issue loans - cycle has ended. Please start a new cycle."
                    }), 400
                
                max_months_available = remaining_days // 30
                
                if months > max_months_available:
                    months = max(1, max_months_available)
                    warning_message = (
                        f"⚠️ Loan duration adjusted from {original_months} to {months} months "
                        f"to fit within cycle end date ({cycle_end_date})"
                    )
            except ValueError:
                pass

        interest = round(principal * interest_rate)
        total = principal + interest
        net_amount = principal - interest

        start_date = datetime.now()
        
        # FIX: Calculate due date using same day next month (proper month arithmetic)
        # Add months to the start date
        due_year = start_date.year + (start_date.month + months - 1) // 12
        due_month = (start_date.month + months - 1) % 12 + 1
        due_day = start_date.day
        
        # Handle edge case: if start day doesn't exist in due month (e.g., Jan 31 -> Feb 31)
        # Use the last day of that month instead
        max_day_in_due_month = calendar.monthrange(due_year, due_month)[1]
        if due_day > max_day_in_due_month:
            due_day = max_day_in_due_month
        
        due_date = datetime(due_year, due_month, due_day)

        cursor.execute("""
            INSERT INTO loans (
                group_id, member_id, principal, interest, total,
                net_amount, start_date, due_date, months, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'Active')
        """, (
            group_id, member_id, principal, interest, total,
            net_amount, start_date.strftime("%Y-%m-%d"),
            due_date.strftime("%Y-%m-%d"), months
        ))

        db.commit()
        cursor.close()

        response_data = {
            "status": "success",
            "months": months,
            "principal": principal,
            "interest": interest,
            "net_amount": net_amount,
            "total": total,
            "monthly_rejesho": round(principal / months, 2),
            "start_date": start_date.strftime("%Y-%m-%d"),
            "due_date": due_date.strftime("%Y-%m-%d")
        }
        
        if warning_message:
            response_data["warning"] = warning_message
            response_data["original_months"] = original_months

        return jsonify(response_data)

    except Exception as e:
        db.rollback()
        print("Add loan error:", e)
        try:
            cursor.close()
        except:
            pass
        return jsonify({"error": "Failed to add loan"}), 500


@app.route('/api/loans/<int:loan_id>', methods=['PUT', 'DELETE'])
@require_admin
def edit_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id FROM loans WHERE id = %s AND group_id = %s",
        (loan_id, group_id)
    )
    loan = cursor.fetchone()
    
    if not loan:
        cursor.close()
        return jsonify({"error": "Loan not found in this group"}), 404
    
    if request.method == 'DELETE':
        cursor.execute(
            "SELECT COUNT(*) as count FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (loan_id, group_id)
        )
        repayments_count = cursor.fetchone()['count']
        
        cursor.execute(
            "SELECT COUNT(*) as count FROM penalties WHERE loan_id = %s AND group_id = %s",
            (loan_id, group_id)
        )
        penalties_count = cursor.fetchone()['count']
        
        if repayments_count > 0 or penalties_count > 0:
            cursor.close()
            return jsonify({
                "error": f"Cannot delete loan with existing records (Repayments: {repayments_count}, Penalties: {penalties_count}). Please delete those first or mark loan as 'Cleared'."
            }), 400
        
        try:
            cursor.execute("DELETE FROM loans WHERE id = %s AND group_id = %s", (loan_id, group_id))
            db.commit()
            cursor.close()
            return jsonify({"status": "success", "message": "Loan deleted successfully"})
        except Exception as e:
            db.rollback()
            cursor.close()
            return jsonify({"error": str(e)}), 500
    
    # PUT
    data = request.get_json()
    due_date_str = data.get('due_date', '').strip()
    status = data.get('status', '').strip()
    
    if status not in ['Active', 'Overdue', 'Cleared', 'Forgiven']:
        cursor.close()
        return jsonify({"error": "Invalid status"}), 400
    
    try:
        datetime.strptime(due_date_str, "%Y-%m-%d")
        
        cursor.execute(
            "UPDATE loans SET due_date = %s, status = %s WHERE id = %s AND group_id = %s",
            (due_date_str, status, loan_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Loan updated successfully"})
    except ValueError:
        cursor.close()
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/loans/<int:loan_id>/forgive', methods=['POST'])
@require_admin
def forgive_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    reason = (data.get("reason") or "").strip()
    forgiven_by = (data.get("forgiven_by") or "Admin").strip()
    if not reason:
        return jsonify({"error": "A forgiveness reason is required"}), 400
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, member_id, principal, status FROM loans WHERE id = %s AND group_id = %s",
        (loan_id, group_id)
    )
    loan = cursor.fetchone()
    if not loan:
        cursor.close()
        return jsonify({"error": "Loan not found"}), 404
    if loan['status'] == 'Forgiven':
        cursor.close()
        return jsonify({"error": "Loan is already forgiven"}), 400
    try:
        try:
            cursor.execute("ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven INTEGER DEFAULT 0")
            cursor.execute("ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiveness_reason TEXT")
            cursor.execute("ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_by TEXT")
            cursor.execute("ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_at TEXT")
            db.commit()
        except Exception:
            db.rollback()
        cursor.execute(
            "UPDATE loans SET status = 'Forgiven', forgiven = 1, forgiveness_reason = %s, forgiven_by = %s, forgiven_at = %s WHERE id = %s AND group_id = %s",
            (reason, forgiven_by, __import__('datetime').datetime.now().strftime("%Y-%m-%d"), loan_id, group_id)
        )
        cursor.execute(
            "DELETE FROM penalties WHERE loan_id = %s AND group_id = %s AND type = 'monthly_rejesho_late' AND COALESCE(amount_paid, 0) = 0",
            (loan_id, group_id)
        )
        cursor.execute(
            "INSERT INTO penalties (group_id, member_id, loan_id, type, amount, description, date) VALUES (%s, %s, %s, 'loan_forgiven', 0, %s, %s)",
            (group_id, loan['member_id'], loan_id,
             "Loan forgiven \u2014 " + reason + " (by " + forgiven_by + ")",
             __import__('datetime').datetime.now().strftime("%Y-%m-%d"))
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Loan has been forgiven and penalties cleared."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/loans/<int:loan_id>/unforgive', methods=['POST'])
@require_admin
def unforgive_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, due_date, status FROM loans WHERE id = %s AND group_id = %s",
        (loan_id, group_id)
    )
    loan = cursor.fetchone()
    if not loan:
        cursor.close()
        return jsonify({"error": "Loan not found"}), 404
    if loan['status'] != 'Forgiven':
        cursor.close()
        return jsonify({"error": "Loan is not currently forgiven"}), 400
    try:
        from datetime import datetime as _dt
        due_date = _dt.strptime(loan['due_date'], "%Y-%m-%d").date()
        new_status = 'Overdue' if _dt.now().date() > due_date else 'Active'
        cursor.execute(
            "UPDATE loans SET status = %s, forgiven = 0, forgiveness_reason = NULL, forgiven_by = NULL, forgiven_at = NULL WHERE id = %s AND group_id = %s",
            (new_status, loan_id, group_id)
        )
        cursor.execute(
            "DELETE FROM penalties WHERE loan_id = %s AND group_id = %s AND type = 'loan_forgiven'",
            (loan_id, group_id)
        )
        db.commit()
        cursor.close()
        auto_insert_loan_penalties(db, group_id)
        return jsonify({"status": "success", "message": "Loan forgiveness reversed. Status: " + new_status})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500



@app.route('/loans-page/download', methods=['GET'])
@require_treasurer_or_above
def download_loans_pdf():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba App")
    
    cursor = get_cursor(db)
    cursor.execute("""
        SELECT l.*, m.name AS member_name 
        FROM loans l
        JOIN members m ON l.member_id = m.id
        WHERE l.group_id = %s
        ORDER BY l.start_date DESC
    """, (group_id,))
    loans = cursor.fetchall()
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=landscape(A4),
        rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20
    )
    elements = []
    styles = getSampleStyleSheet()
    
    title = Paragraph(f"💰 {group_name} - Loans Report", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))
    
    report_date = datetime.now().strftime("%d %B %Y, %I:%M %p")
    subtitle = Paragraph(f"<i>Generated on: {report_date}</i>", styles['Normal'])
    elements.append(subtitle)
    elements.append(Spacer(1, 20))
    
    total_principal = sum(loan['principal'] for loan in loans)
    total_interest = sum(loan['interest'] for loan in loans)
    total_disbursed = sum(loan['net_amount'] for loan in loans)
    
    active_loans = [l for l in loans if l['status'] == 'Active']
    overdue_loans = [l for l in loans if l['status'] == 'Overdue']
    cleared_loans = [l for l in loans if l['status'] == 'Cleared']
    
    total_repaid = 0
    total_remaining = 0
    for loan in loans:
        cursor.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (loan['id'], group_id)
        )
        repaid = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())
        total_repaid += repaid
        total_remaining += max(loan['principal'] - repaid, 0)
    
    summary_data = [
        [Paragraph("<b>Summary</b>", styles['Normal']), ""],
        ["Total Loans Issued", f"{len(loans)}"],
        ["Total Principal Loaned", f"{total_principal:,.0f} TZS"],
        ["Total Interest Deducted", f"{total_interest:,.0f} TZS"],
        ["Total Repaid", f"{total_repaid:,.0f} TZS"],
        ["Total Outstanding", f"{total_remaining:,.0f} TZS"],
        ["", ""],
        ["Active Loans", f"{len(active_loans)}"],
        ["Overdue Loans", f"{len(overdue_loans)}"],
        ["Cleared Loans", f"{len(cleared_loans)}"],
    ]
    
    summary_table = Table(summary_data, colWidths=[200, 150])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#FFC107')),
        ('BACKGROUND', (0,1), (-1,-1), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONTSIZE', (0,0), (-1,-1), 11),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 30))
    
    headers = [
        "Member", "Principal", "Interest", "Net Disbursed", 
        "Months", "Monthly Rejesho", "Repaid", "Remaining", 
        "Start Date", "Due Date", "Status"
    ]
    data = [headers]
    
    for loan in loans:
        cursor.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s",
            (loan['id'], group_id)
        )
        repaid = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())
        
        remaining = max(loan['principal'] - repaid, 0)
        monthly_rejesho = round(loan['principal'] / loan['months'], 2) if loan['months'] > 0 else 0
        
        data.append([
            loan['member_name'],
            f"{loan['principal']:,.0f}",
            f"{loan['interest']:,.0f}",
            f"{loan['net_amount']:,.0f}",
            str(loan['months']),
            f"{monthly_rejesho:,.0f}",
            f"{repaid:,.0f}",
            f"{remaining:,.0f}",
            loan['start_date'],
            loan['due_date'],
            loan['status']
        ])
    
    loans_table = Table(data, repeatRows=1)
    loans_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#FFC107')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('GRID', (0,0), (-1,-1), 0.3, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8f9fa')]),
    ]))
    elements.append(Paragraph("<b>Detailed Loans Breakdown</b>", styles['Heading2']))
    elements.append(Spacer(1, 10))
    elements.append(loans_table)
    
    elements.append(Spacer(1, 20))
    footer_text = Paragraph(
        f"<font size=8>Report generated on {report_date} | {group_name}</font>",
        styles['Normal']
    )
    elements.append(footer_text)
    
    doc.build(elements)
    buffer.seek(0)
    cursor.close()
    
    filename = f"{group_name.replace(' ','_')}_Loans_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")


# ==================== REJESHO (REPAYMENTS) ====================

@app.route('/repayments-page')
@require_treasurer_or_above
def repayments_page():
    loan_id = request.args.get('loan_id')
    return render_template('repayments.html', loan_id=loan_id)


@app.route('/api/rejesho', methods=['POST'])
@require_treasurer_or_above
def add_rejesho():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    loan_id = data.get("loan_id")
    amount = data.get("amount")

    if not loan_id or not amount:
        return jsonify({"error": "Loan ID and amount are required"}), 400

    try:
        loan_id = int(loan_id)
        amount = float(amount)
    except ValueError:
        return jsonify({"error": "Invalid loan ID or amount format"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id FROM loans WHERE id = %s AND group_id = %s",
        (loan_id, group_id)
    )
    loan = cursor.fetchone()
    
    if not loan:
        cursor.close()
        return jsonify({"error": "Loan not found in this group"}), 404

    today_str = datetime.now().strftime("%Y-%m-%d")

    cursor.execute(
        "INSERT INTO rejesho (group_id, loan_id, amount, date) VALUES (%s, %s, %s, %s)",
        (group_id, loan_id, amount, today_str)
    )
    db.commit()
    cursor.close()
    
    update_loan_status(db, loan_id, group_id)
    
    return jsonify({"status": "success"})


@app.route('/api/rejesho/<int:loan_id>', methods=['GET'])
@require_treasurer_or_above
def get_rejesho_history(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT l.*, m.name as member_name FROM loans l JOIN members m ON l.member_id = m.id WHERE l.id = %s AND l.group_id = %s",
        (loan_id, group_id)
    )
    loan_info = cursor.fetchone()

    if not loan_info:
        cursor.close()
        return jsonify({"error": "Loan not found"}), 404
    
    cursor.execute(
        "SELECT id, amount, date FROM rejesho WHERE loan_id = %s AND group_id = %s ORDER BY date DESC",
        (loan_id, group_id)
    )
    repayments = cursor.fetchall()

    total_repaid = sum(r['amount'] for r in repayments)
    
    remaining = loan_info['principal'] - total_repaid
    monthly_rejesho = round(loan_info['principal'] / loan_info['months'], 2) if loan_info['months'] > 0 else 0

    cursor.close()

    return jsonify({
        "loan_info": {
            "id": loan_info['id'],
            "member_name": loan_info['member_name'],
            "principal": loan_info['principal'],
            "interest": loan_info['interest'],
            "net_amount": loan_info['net_amount'],
            "total": loan_info['principal'],
            "start_date": loan_info['start_date'],
            "due_date": loan_info['due_date'],
            "months": loan_info['months'],
            "status": loan_info['status'],
            "total_repaid": total_repaid,
            "remaining": remaining,
            "monthly_rejesho": monthly_rejesho
        },
        "repayments": [dict(r) for r in repayments]
    })


@app.route('/api/rejesho/<int:rejesho_id>', methods=['DELETE'])
@require_admin
def delete_rejesho(rejesho_id):
    db = get_db()
    group_id = get_current_group_id()

    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    cursor = get_cursor(db)
    # Fetch the rejesho record to confirm it belongs to this group
    cursor.execute(
        "SELECT id, loan_id FROM rejesho WHERE id = %s AND group_id = %s",
        (rejesho_id, group_id)
    )
    record = cursor.fetchone()

    if not record:
        cursor.close()
        return jsonify({"error": "Rejesho record not found"}), 404

    loan_id = record['loan_id']

    cursor.execute(
        "DELETE FROM rejesho WHERE id = %s AND group_id = %s",
        (rejesho_id, group_id)
    )
    db.commit()
    cursor.close()

    # Recalculate loan status and penalties after deletion
    update_loan_status(db, loan_id, group_id)
    auto_insert_loan_penalties(db, group_id)

    return jsonify({"status": "success", "message": "Rejesho deleted successfully"})


# ==================== PENALTIES ====================


@app.route('/penalties-page')
@require_treasurer_or_above
def penalties_page():
    return render_template('penalties.html')


@app.route('/api/penalties', methods=['GET'])
@require_any_member
def get_penalties():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    cursor = get_cursor(db)
    cursor.execute("""
        SELECT 
            p.id,
            p.member_id,
            m.name AS member_name,
            p.amount,
            p.type,
            COALESCE(p.amount_paid, 0) AS amount_paid,
            COALESCE(p.forgiven_amount, 0) AS forgiven_amount,
            COALESCE(p.is_frozen, 0) AS is_frozen,
            p.freeze_reason,
            p.description,
            p.date
        FROM penalties p
        JOIN members m ON p.member_id = m.id
        WHERE p.group_id = %s
        ORDER BY p.date DESC, p.id DESC
    """, (group_id,))
    ledger = cursor.fetchall()

    cursor.execute(
        "SELECT SUM(GREATEST(0, amount - COALESCE(amount_paid, 0) - COALESCE(forgiven_amount, 0))) FROM penalties WHERE group_id = %s",
        (group_id,)
    )
    total_due = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())
    
    cursor.close()
    
    return jsonify({
        "total_outstanding": total_due,
        "ledger": [dict(p) for p in ledger]
    })


@app.route('/api/penalties', methods=['POST'])
@require_treasurer_or_above
def add_penalty():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    member_id = data.get("member_id")
    ptype = data.get("type")
    amount = float(data.get("amount", 0))
    description = data.get("description", "")
    
    if not member_id or not ptype or amount <= 0:
        return jsonify({"error": "Member ID, type, and positive amount are required"}), 400

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id FROM members WHERE id = %s AND group_id = %s",
        (member_id, group_id)
    )
    member = cursor.fetchone()
    
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found in this group"}), 404

    try:
        cursor.execute(
            "INSERT INTO penalties (group_id, member_id, type, amount, description, date) VALUES (%s, %s, %s, %s, %s, %s)",
            (group_id, member_id, ptype, amount, description, datetime.now().strftime("%Y-%m-%d"))
        )
        db.commit()
        cursor.close()
        
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/record_penalty_payment/<int:penalty_id>', methods=['POST'])
@require_treasurer_or_above
def record_penalty_payment(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    amount_to_pay = float(data.get('amount', 0)) 
    
    if amount_to_pay <= 0:
        return jsonify({"error": "A valid payment amount is required."}), 400

    cursor = get_cursor(db)
    
    try:
        cursor.execute(
            "SELECT amount, COALESCE(amount_paid, 0) AS amount_paid, member_id FROM penalties WHERE id = %s AND group_id = %s",
            (penalty_id, group_id)
        )
        penalty = cursor.fetchone()

        if not penalty:
            cursor.close()
            return jsonify({"error": "Penalty record not found."}), 404

        remaining_due = penalty['amount'] - penalty['amount_paid']
        
        if remaining_due <= 0:
            cursor.close()
            return jsonify({"error": "Penalty is already fully paid."}), 400
             
        applied_amount = min(amount_to_pay, remaining_due) 

        new_paid_total = penalty['amount_paid'] + applied_amount
        cursor.execute(
            "UPDATE penalties SET amount_paid = %s WHERE id = %s AND group_id = %s",
            (new_paid_total, penalty_id, group_id)
        )
        
        cursor.execute(
            "INSERT INTO contributions (group_id, member_id, type, amount, date) VALUES (%s, %s, 'penalty_payment', %s, CURRENT_DATE)",
            (group_id, penalty['member_id'], applied_amount)
        )

        db.commit()
        cursor.close()
        
        remaining_after_payment = remaining_due - applied_amount
        message = f"Successfully recorded {applied_amount:,.0f} TZS payment for Penalty #{penalty_id}. Remaining: {remaining_after_payment:,.0f} TZS"
        return jsonify({"message": message})

    except Exception as e:
        db.rollback()
        cursor.close()
        print(f"Error recording penalty payment: {e}")
        return jsonify({"error": "Database error while recording payment."}), 500



@app.route('/api/penalties/<int:penalty_id>/freeze', methods=['POST'])
@require_treasurer_or_above
def freeze_penalty(penalty_id):
    """
    Freeze or unfreeze a penalty. When frozen, daily accrual stops.
    Body: { "freeze": true/false, "reason": "Illness" }
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data = request.get_json()
    freeze = bool(data.get("freeze", True))
    reason = (data.get("reason") or "").strip()

    if freeze and not reason:
        return jsonify({"error": "A reason is required when freezing a penalty"}), 400

    cursor = get_cursor(db)
    try:
        cursor.execute(
            "SELECT id FROM penalties WHERE id = %s AND group_id = %s",
            (penalty_id, group_id)
        )
        if not cursor.fetchone():
            cursor.close()
            return jsonify({"error": "Penalty not found"}), 404

        cursor.execute(
            """UPDATE penalties
               SET is_frozen = %s,
                   freeze_reason = %s
               WHERE id = %s AND group_id = %s""",
            (1 if freeze else 0, reason if freeze else None, penalty_id, group_id)
        )
        db.commit()
        cursor.close()
        action = "frozen" if freeze else "unfrozen"
        return jsonify({"status": "success", "message": f"Penalty {action} successfully."})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/penalties/<int:penalty_id>/forgive-amount', methods=['POST'])
@require_admin
def forgive_penalty_amount(penalty_id):
    """
    Record a partial forgiveness amount on a penalty.
    Body: { "forgiven_amount": 5000, "reason": "Group decision" }
    Stored in forgiven_amount column; subtracted from remaining due.
    The original amount is preserved for audit trail.
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    data = request.get_json()
    try:
        forgiven_amount = float(data.get("forgiven_amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "forgiven_amount must be a number"}), 400

    if forgiven_amount < 0:
        return jsonify({"error": "forgiven_amount cannot be negative"}), 400

    reason = (data.get("reason") or "").strip()

    cursor = get_cursor(db)
    try:
        cursor.execute(
            "SELECT id, amount, COALESCE(amount_paid, 0) AS amount_paid FROM penalties WHERE id = %s AND group_id = %s",
            (penalty_id, group_id)
        )
        penalty = cursor.fetchone()
        if not penalty:
            cursor.close()
            return jsonify({"error": "Penalty not found"}), 404

        max_forgivable = penalty['amount'] - penalty['amount_paid']
        if forgiven_amount > max_forgivable:
            cursor.close()
            return jsonify({
                "error": f"Cannot forgive more than the remaining due ({max_forgivable:,.0f} TZS)"
            }), 400

        forgiveness_note = f" | Forgiven {forgiven_amount:,.0f} TZS" + (f": {reason}" if reason else "")
        cursor.execute(
            """UPDATE penalties
               SET forgiven_amount = %s,
                   description = COALESCE(description, '') || %s
               WHERE id = %s AND group_id = %s""",
            (forgiven_amount, forgiveness_note, penalty_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({
            "status": "success",
            "message": f"Forgiven {forgiven_amount:,.0f} TZS from Penalty #{penalty_id}."
        })
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500


@app.route('/api/penalties/<int:penalty_id>', methods=['PUT', 'DELETE'])
@require_admin
def edit_penalty(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    
    if request.method == 'DELETE':
        cursor.execute(
            "SELECT type, loan_id FROM penalties WHERE id = %s AND group_id = %s", 
            (penalty_id, group_id)
        )
        penalty = cursor.fetchone()
        
        if not penalty:
            cursor.close()
            return jsonify({"error": "Penalty not found"}), 404
        
        if penalty['type'] == 'loan_late' and penalty['loan_id']:
            cursor.close()
            return jsonify({
                "error": "Cannot delete auto-generated loan penalties. Clear the loan instead."
            }), 400
        
        try:
            cursor.execute("DELETE FROM penalties WHERE id = %s AND group_id = %s", (penalty_id, group_id))
            db.commit()
            cursor.close()
            return jsonify({"status": "success", "message": "Penalty deleted"})
        except Exception as e:
            db.rollback()
            cursor.close()
            return jsonify({"error": str(e)}), 500
    
    # PUT
    data = request.get_json()
    amount = float(data.get('amount', 0))
    description = data.get('description', '').strip()
    
    if amount <= 0:
        cursor.close()
        return jsonify({"error": "Amount must be positive"}), 400
    
    try:
        cursor.execute(
            "SELECT amount_paid FROM penalties WHERE id = %s AND group_id = %s", 
            (penalty_id, group_id)
        )
        current = cursor.fetchone()
        
        if not current:
            cursor.close()
            return jsonify({"error": "Penalty not found"}), 404
        
        amount_paid = current['amount_paid'] or 0
        
        if amount < amount_paid:
            cursor.close()
            return jsonify({
                "error": f"Amount cannot be less than already paid: {amount_paid:,.0f} TZS"
            }), 400
        
        cursor.execute(
            "UPDATE penalties SET amount = %s, description = %s WHERE id = %s AND group_id = %s",
            (amount, description, penalty_id, group_id)
        )
        db.commit()
        cursor.close()
        return jsonify({"status": "success", "message": "Penalty updated"})
    except Exception as e:
        db.rollback()
        cursor.close()
        return jsonify({"error": str(e)}), 500
    

@app.route('/penalties-page/download', methods=['GET'])
@require_treasurer_or_above
def download_penalties_pdf():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return "No group selected", 400

    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba App")

    cursor = get_cursor(db)
    cursor.execute("""
        SELECT p.*, m.name AS member_name
        FROM penalties p
        JOIN members m ON p.member_id = m.id
        WHERE p.group_id = %s
        ORDER BY p.date DESC
    """, (group_id,))
    penalties = cursor.fetchall()

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4),
                            rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20)
    elements = []
    styles = getSampleStyleSheet()

    title = Paragraph(f"🚨 {group_name} - Penalties Report", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))

    report_date = datetime.now().strftime("%d %B %Y, %I:%M %p")
    subtitle = Paragraph(f"<i>Generated on: {report_date}</i>", styles['Normal'])
    elements.append(subtitle)
    elements.append(Spacer(1, 20))

    headers = ["Member", "Type", "Amount", "Amount Paid", "Remaining Due", "Description", "Date"]
    data = [headers]

    total_outstanding = 0
    for p in penalties:
        remaining = max(p['amount'] - p.get('amount_paid', 0), 0)
        total_outstanding += remaining
        data.append([
            p['member_name'],
            "Auto Loan" if p['type'] == "monthly_rejesho_late" else "Manual",
            f"{p['amount']:,.0f}",
            f"{p.get('amount_paid', 0):,.0f}",
            f"{remaining:,.0f}",
            p.get('description', ''),
            p['date']
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#FFC107')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.black),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('GRID', (0,0), (-1,-1), 0.3, colors.grey),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#f8f9fa')]),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 20))

    doc.build(elements)
    buffer.seek(0)
    cursor.close()

    filename = f"{group_name.replace(' ', '_')}_Penalties_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")


# ==================== PROFITS ====================

@app.route('/profits-page')
@require_admin
def profits_page():
    db       = get_db()
    group_id = get_current_group_id()
    cursor   = get_cursor(db)
    cursor.execute(
        "SELECT profits_unlocked FROM groups WHERE id = %s",
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()
    unlocked = row and row.get('profits_unlocked')
    if not unlocked:
        return render_template('profits_locked.html')
    return render_template('profits.html')

@app.route('/api/profits', methods=['POST'])
@require_admin
def calculate_profits():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    profit_data = get_current_group_profit(db, group_id)
    net_profit = profit_data["net_profit_pool"]

    total_units = get_total_hisa_units(db, group_id)
    
    if total_units == 0:
        return jsonify({
            "error": "No Hisa units available for profit distribution",
            "net_profit_to_distribute": 0,
            "breakdown": [],
            "leadership_pay_amount": profit_data["leadership_pay_amount"], 
            "gross_distributable_pool": profit_data["gross_distributable_pool"],
        })
    
    profit_per_unit = net_profit / total_units
    
    admin_id = get_group_admin_member_id(db, group_id)
    
    # FIX: Use cursor instead of db.execute()
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, name FROM members WHERE group_id = %s AND is_system = 0", 
        (group_id,)
    )
    members = cursor.fetchall()
    
    results = []

    for m in members:
        member_id = m['id']
        
        # Get HISA units (only Hisa + Jamii count toward units)
        hisa_data = get_member_hisa_units(db, member_id, group_id)
        member_units = hisa_data['units']
        
        # Total savings includes Hisa Anzia + Hisa + Jamii (all to be returned)
        cursor.execute(
            """
            SELECT SUM(amount) 
            FROM contributions 
            WHERE member_id = %s AND group_id = %s AND type IN ('hisa anzia', 'hisa', 'jamii')
            """,
            (member_id, group_id)
        )
        total_savings_member = get_single_value(cursor, 0)
        
        profit_share = round(member_units * profit_per_unit)

        # Calculate remaining loans
        cursor.execute(
            """
            SELECT SUM(principal) 
            FROM loans 
            WHERE member_id = %s AND group_id = %s
            """,
            (member_id, group_id)
        )
        total_principal_taken = get_single_value(cursor, 0)
        
        cursor.execute(
            """
            SELECT SUM(r.amount) 
            FROM rejesho r
            JOIN loans l ON r.loan_id = l.id
            WHERE l.member_id = %s AND l.group_id = %s
            """,
            (member_id, group_id)
        )
        total_repaid = get_single_value(cursor, 0)
        
        remaining_loan_balance = max(total_principal_taken - total_repaid, 0)

        total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)

        total_deductions = remaining_loan_balance + total_penalties_due
        
        final_payout = max((total_savings_member + profit_share) - total_deductions, 0)

        results.append({
            "member_name": m["name"],
            "hisa_units": round(member_units, 2),
            "savings": total_savings_member,
            "profit_share": profit_share,
            "loan_balance_due": remaining_loan_balance,
            "penalties_due": total_penalties_due,
            "total_deductions": total_deductions,
            "total_payout": final_payout
        })

    cursor.close()

    return jsonify({
        "total_interest": profit_data["total_interest"],
        "total_penalties": profit_data["total_penalties_imposed"],
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_to_distribute": net_profit,
        "total_hisa_units": round(total_units, 2),
        "profit_per_unit": round(profit_per_unit, 2),
        "breakdown": results
    })

# ==================== REPORTS ====================

@app.route('/reports-page')
@require_treasurer_or_above
def reports_page():
    return render_template("reports.html")

@app.route('/api/reports', methods=['GET'])
@require_treasurer_or_above
def get_report_data():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    admin_id = get_group_admin_member_id(db, group_id)

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, name FROM members WHERE group_id = %s",
        (group_id,)
    )
    members = cursor.fetchall()

    report_data = []

    for m in members:
        member_id = m["id"]
        
        cursor.execute(
            """SELECT type, SUM(amount) as total FROM contributions 
               WHERE member_id=%s AND group_id=%s AND type != 'jamii_deduction' 
               GROUP BY type""",
            (member_id, group_id)
        )
        contribs = cursor.fetchall()
        
        contrib_dict = {c["type"]: c["total"] for c in contribs}
        total_contributions = sum(contrib_dict.values())
        
        member_total_savings = (
            contrib_dict.get('hisa anzia', 0) + 
            contrib_dict.get('hisa', 0) + 
            contrib_dict.get('jamii', 0)
        )
        
        hisa_data = get_member_hisa_units(db, member_id, group_id)
        member_units = hisa_data['units']

        loan_balances = get_member_loan_balances(db, member_id, group_id)
        total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)
        
        net_contribution_position = (
            member_total_savings
            - loan_balances["remaining_loans"]
            - total_penalties_due
        )

        report_data.append({
            "member_name": m["name"],
            "contributions": contrib_dict,
            "total_contributions": total_contributions,
            "total_savings": member_total_savings,
            "hisa_units": round(member_units, 2),
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": total_penalties_due,
            "net_contribution_position": net_contribution_position,
        })

    cursor.close()
    return jsonify({"report": report_data})


@app.route('/reports-page/download', methods=['GET'])
@require_treasurer_or_above
def download_report_pdf():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    buffer = BytesIO()
    
    doc = SimpleDocTemplate(
        buffer, pagesize=landscape(A4),
        rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20
    )
    elements = []
    styles = getSampleStyleSheet()

    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba App")

    title = Paragraph(f"📊 {group_name} - Monthly Financial Report", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 12))
    report_date = datetime.now().strftime("%B %Y")
    subtitle = Paragraph(
        f"<i>Report Period: {report_date}</i>",
        styles['Normal']
    )
    elements.append(subtitle)
    elements.append(Spacer(1, 10))

    response = get_report_data() 
    report_json = response.get_json()
    report_data = report_json.get("report", [])

    total_contributions = sum(m['total_contributions'] for m in report_data)
    total_loans_due = sum(m['remaining_loans'] for m in report_data)

    summary_data = [
        [Paragraph("<b>Total Contributions</b>", styles['Normal']), f"{total_contributions:,.0f} TZS"],
        [Paragraph("<b>Total Loans Outstanding</b>", styles['Normal']), f"{total_loans_due:,.0f} TZS"],
    ]
    summary_table = Table(summary_data, colWidths=[150, 100])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
        ('FONTSIZE', (0,0), (-1,-1), 12)
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 20))

    headers = [
        "Member", "Units", "Hisa Anzia", "Hisa", "Jamii", "Total Savings",
        "Loans Taken", "Rejesho", "Loan Due", "Penalties"
    ]
    data = [headers]

    for m in report_data:
        data.append([
            m['member_name'],
            f"{m.get('hisa_units', 0):.2f}",
            f"{m['contributions'].get('hisa anzia',0):,.0f}",
            f"{m['contributions'].get('hisa',0):,.0f}",
            f"{m['contributions'].get('jamii',0):,.0f}",
            f"{m['total_savings']:,.0f}",
            f"{m['total_loans']:,.0f}",
            f"{m['total_rejesho']:,.0f}",
            f"{m['remaining_loans']:,.0f}",
            f"{m['total_penalties']:,.0f}",
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.green),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('FONTSIZE', (0,0), (-1,-1), 10),
        ('GRID', (0,0), (-1,-1), 0.3, colors.grey),
    ]))
    elements.append(table)

    generated_on = datetime.now().strftime("%d %b %Y")
    generated_text = Paragraph(
        f"<font size=8>Generated on: {generated_on}</font>",
        styles['Normal']
    )
    elements.append(Spacer(1, 6))
    elements.append(generated_text)

    doc.build(elements)
    buffer.seek(0)
    filename = f"{group_name.replace(' ','_')}_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")


# ==================== BACKUP & EXPORT ====================

@app.route('/api/backup/export', methods=['GET'])
@require_admin
def export_raw_backup():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    queries = {
        "members": "SELECT * FROM members WHERE group_id = %s AND is_system = 0",
        "contributions": """
            SELECT c.id, m.name as member_name, c.type, c.amount, c.date 
            FROM contributions c 
            JOIN members m ON c.member_id = m.id
            WHERE c.group_id = %s
        """,
        "loans": """
            SELECT l.id, m.name as member_name, l.principal, l.interest, l.total, l.start_date, l.due_date, l.status 
            FROM loans l 
            JOIN members m ON l.member_id = m.id
            WHERE l.group_id = %s
        """,
        "repayments": """
            SELECT r.id, m.name as member_name, r.loan_id, r.amount, r.date 
            FROM rejesho r 
            JOIN loans l ON l.id = r.loan_id
            JOIN members m ON l.member_id = m.id
            WHERE r.group_id = %s
        """,
        "penalties": """
            SELECT p.id, m.name as member_name, p.type, p.amount, p.amount_paid, p.date, p.description 
            FROM penalties p 
            JOIN members m ON p.member_id = m.id
            WHERE p.group_id = %s
        """,
        "settings": "SELECT * FROM settings WHERE group_id = %s"
    }

    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, sql in queries.items():
            cursor = get_cursor(db)
            cursor.execute(sql, (group_id,))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(column_names)

            for row in rows:
                data = [row[col] for col in column_names]
                writer.writerow(data)
            
            zip_file.writestr(f"{file_name}.csv", csv_buffer.getvalue())
            csv_buffer.close()
            cursor.close()

        # Balance sheet generation
        cursor = get_cursor(db)
        cursor.execute(
            "SELECT id, name FROM members WHERE group_id = %s AND is_system = 0", 
            (group_id,)
        )
        members = cursor.fetchall()
        
        balance_csv = StringIO()
        balance_writer = csv.writer(balance_csv)
        balance_writer.writerow([
            "Member Name", "Hisa Units", "Hisa (Savings)", "Jamii Paid", 
            "Jamii Shortfall", "Loan Balance", "Unpaid Penalties"
        ])

        for m in members:
            m_id = m['id']
            hisa_data = get_member_hisa_units(db, m_id, group_id)
            hisa = hisa_data['total_contributed']
            units = hisa_data['units']
            
            jamii_status = get_member_jamii_balance(db, m_id, group_id)
            loan_bal = get_member_loan_balances(db, m_id, group_id)['remaining_loans']
            penalty_bal = get_total_penalties_due_for_member(m_id, db, group_id)

            balance_writer.writerow([
                m['name'], f"{units:.2f}", hisa, jamii_status['total_paid'], 
                jamii_status['shortfall'], loan_bal, penalty_bal
            ])

        zip_file.writestr("Group_Balance_Sheet.csv", balance_csv.getvalue())
        balance_csv.close()
        cursor.close()

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f"Kikoba_Backup_Group_{group_id}_{datetime.now().strftime('%Y-%m-%d')}.zip"
    )

# =====================================================================
# ACCOUNTABILITY  -  "Money Check"

import json
 
_ACC_READY = False
 
_EXPENSE_CATEGORIES = {
    "leadership_pay": "Leadership pay",
    "meeting":        "Meeting / refreshments",
    "stationery":     "Stationery & printing",
    "transport":      "Transport",
    "charges":        "M-Koba / bank charges",
    "welfare":        "Welfare payout",
    "other":          "Other",
}
 
 
def _acc_today():
    """Today's date in Tanzania (UTC+3). Render servers run on UTC."""
    from datetime import timezone
    return (datetime.now(timezone.utc) + timedelta(hours=3)).date()
 
 
def _acc_ensure(db):
    """Create the tables on first use (safe to run every time)."""
    global _ACC_READY
    if _ACC_READY:
        return
    cur = get_cursor(db)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_checks (
            id               SERIAL PRIMARY KEY,
            group_id         INTEGER NOT NULL,
            as_of            TEXT NOT NULL,
            actual_balance   NUMERIC NOT NULL,
            expected_balance NUMERIC NOT NULL,
            difference       NUMERIC NOT NULL,
            tolerance        NUMERIC NOT NULL,
            status           TEXT NOT NULL,
            components       TEXT,
            statement_ref    TEXT,
            note             TEXT,
            entered_by       INTEGER,
            entered_by_name  TEXT,
            entered_by_role  TEXT,
            created_at       TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id          SERIAL PRIMARY KEY,
            group_id    INTEGER NOT NULL,
            actor_id    INTEGER,
            actor_name  TEXT,
            actor_role  TEXT,
            action      TEXT NOT NULL,
            summary     TEXT NOT NULL,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id                SERIAL PRIMARY KEY,
            group_id          INTEGER NOT NULL,
            spent_on          TEXT NOT NULL,
            category          TEXT NOT NULL,
            amount            NUMERIC NOT NULL CHECK (amount > 0),
            paid_to           TEXT NOT NULL,
            description       TEXT NOT NULL,
            approval          TEXT,
            receipt_ref       TEXT,
            recorded_by       INTEGER,
            recorded_by_name  TEXT,
            recorded_by_role  TEXT,
            voided            BOOLEAN NOT NULL DEFAULT FALSE,
            void_reason       TEXT,
            voided_by_name    TEXT,
            voided_at         TIMESTAMP,
            created_at        TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_balance_checks_group ON balance_checks (group_id, id DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_group ON audit_log (group_id, id DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_expenses_group ON expenses (group_id, id DESC)")
    db.commit()
    cur.close()
    _ACC_READY = True
 
 
def _n(value):
    return float(value or 0)
 
 
def _tzs(value):
    return f"{_n(value):,.0f} TZS"
 
 
def _audit_direct(db, group_id, action, summary):
    """Write one audit line straight away (used by routes that log themselves)."""
    cur = get_cursor(db)
    cur.execute("""
        INSERT INTO audit_log (group_id, actor_id, actor_name, actor_role, action, summary)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (group_id, session.get("member_id"), session.get("member_name"),
          session.get("role"), action, summary[:500]))
    cur.close()
 
 
# ---------------------------------------------------------------- the maths
def compute_expected_balance(db, group_id, as_of):
    """
    What the group's account SHOULD hold on `as_of` (YYYY-MM-DD), judging
    only by the records in the app.  Amounts are cast to NUMERIC first
    because several columns are `real` (float) and lose cents on big sums.
    """
    cur = get_cursor(db)
 
    cur.execute("""
        SELECT c.type, COALESCE(SUM(c.amount::numeric), 0) AS total
        FROM contributions c
        WHERE c.group_id = %s
          AND COALESCE(NULLIF(c.transaction_date::text, ''), c.date::text) <= %s
        GROUP BY c.type
    """, (group_id, as_of))
    by_type = {r['type']: _n(r['total']) for r in cur.fetchall()}
 
    cur.execute("""
        SELECT COALESCE(SUM(r.amount::numeric), 0) AS total
        FROM rejesho r
        WHERE r.group_id = %s AND r.date::text <= %s
    """, (group_id, as_of))
    rejesho = _n(get_single_value(cur, 0))
 
    cur.execute("""
        SELECT COALESCE(SUM(COALESCE(l.net_amount, l.principal - COALESCE(l.interest, 0))::numeric), 0) AS total,
               COUNT(*) AS n
        FROM loans l
        WHERE l.group_id = %s AND l.start_date::text <= %s
    """, (group_id, as_of))
    row = cur.fetchone()
    loans_out, loans_n = _n(row['total']), int(row['n'])
 
    cur.execute("""
        SELECT e.category, COALESCE(SUM(e.amount), 0) AS total
        FROM expenses e
        WHERE e.group_id = %s AND NOT e.voided AND e.spent_on <= %s
        GROUP BY e.category
    """, (group_id, as_of))
    exp_by_cat = {r['category']: _n(r['total']) for r in cur.fetchall()}
    cur.close()
 
    hisa_anzia = by_type.pop('hisa anzia', 0.0)
    hisa       = by_type.pop('hisa', 0.0)
    jamii      = by_type.pop('jamii', 0.0)
    penalties  = by_type.pop('penalty_payment', 0.0)
    jamii_ded  = -by_type.pop('jamii_deduction', 0.0)     # stored as negative amounts
    other      = sum(by_type.values())
 
    breakdown = []
    if jamii_ded:
        breakdown.append({"label": "Jamii deductions", "amount": round(jamii_ded)})
    for key, amount in sorted(exp_by_cat.items(), key=lambda kv: -kv[1]):
        breakdown.append({"label": _EXPENSE_CATEGORIES.get(key, key), "amount": round(amount), "key": key})
    expenses_total = jamii_ded + sum(exp_by_cat.values())
 
    money_in  = hisa_anzia + hisa + jamii + penalties + other + rejesho
    money_out = loans_out + expenses_total
 
    return {
        "as_of": as_of,
        "money_in": {
            "hisa_anzia": round(hisa_anzia), "hisa": round(hisa), "jamii": round(jamii),
            "penalty_payments": round(penalties), "other": round(other),
            "rejesho": round(rejesho), "total": round(money_in),
        },
        "money_out": {
            "loans_paid_out": round(loans_out), "loans_count": loans_n,
            "expenses": round(expenses_total), "expense_breakdown": breakdown,
            "total": round(money_out),
        },
        "expected_balance": round(money_in - money_out),
    }
 
 
def _acc_status(difference, tolerance):
    if abs(difference) <= tolerance:
        return "matched"
    return "short" if difference < 0 else "over"
 
 
def integrity_checks(db, group_id):
    """Checks the books against themselves. No M-Koba number needed."""
    checks = []
    cur = get_cursor(db)
 
    # 1. Penalty money is recorded in two places; they must agree.
    cur.execute("SELECT COALESCE(SUM(amount::numeric),0) AS t FROM contributions WHERE group_id=%s AND type='penalty_payment'", (group_id,))
    pay_in_cash = _n(get_single_value(cur, 0))
    cur.execute("SELECT COALESCE(SUM(COALESCE(amount_paid,0)::numeric),0) AS t FROM penalties WHERE group_id=%s", (group_id,))
    pay_in_penalties = _n(get_single_value(cur, 0))
    gap = pay_in_cash - pay_in_penalties
    ok = abs(gap) <= 1
    checks.append({"key": "penalty_ledger", "level": "ok" if ok else "bad",
        "label": "Penalty payments agree in both records",
        "detail": ("Cash received for penalties matches what is marked paid." if ok else
                   f"Cash records show {pay_in_cash:,.0f} TZS of penalty payments but penalties are marked paid for "
                   f"{pay_in_penalties:,.0f} TZS (difference {gap:,.0f} TZS).")})
 
    # 2 + 3. Loans: overpaid, or marked Cleared while money is still owed.
    cur.execute("""
        SELECT l.id, l.status, l.principal::numeric AS principal, COALESCE(r.paid, 0) AS paid, m.name
        FROM loans l
        JOIN members m ON m.id = l.member_id
        LEFT JOIN (SELECT loan_id, SUM(amount::numeric) AS paid FROM rejesho
                   WHERE group_id = %s GROUP BY loan_id) r ON r.loan_id = l.id
        WHERE l.group_id = %s
    """, (group_id, group_id))
    loans = cur.fetchall()
    overpaid = [x for x in loans if _n(x['paid']) > _n(x['principal']) + 1]
    cleared_unpaid = [x for x in loans if x['status'] == 'Cleared' and _n(x['paid']) < _n(x['principal']) - 1]
    checks.append({"key": "loans_overpaid", "level": "ok" if not overpaid else "warn",
        "label": "No loan has been paid more than it was",
        "detail": "All loans are within their amount." if not overpaid else
                  "Check these loans: " + ", ".join(f"{x['name']} (loan #{x['id']})" for x in overpaid[:5])})
    checks.append({"key": "cleared_but_unpaid", "level": "ok" if not cleared_unpaid else "bad",
        "label": "No loan is marked Cleared while money is still owed",
        "detail": "Every cleared loan is fully paid." if not cleared_unpaid else
                  "Marked Cleared but not fully paid: " + ", ".join(
                      f"{x['name']} (loan #{x['id']}, owes {_n(x['principal']) - _n(x['paid']):,.0f} TZS)"
                      for x in cleared_unpaid[:5])})
 
    # 4. Zero or negative amounts hide things.
    cur.execute("SELECT COUNT(*) AS c FROM contributions WHERE group_id=%s AND type != 'jamii_deduction' AND amount::numeric <= 0", (group_id,))
    bad_c = int(get_single_value(cur, 0))
    cur.execute("SELECT COUNT(*) AS c FROM rejesho WHERE group_id=%s AND amount::numeric <= 0", (group_id,))
    bad_r = int(get_single_value(cur, 0))
    checks.append({"key": "non_positive", "level": "ok" if not (bad_c or bad_r) else "bad",
        "label": "No zero or negative payments",
        "detail": "None found." if not (bad_c or bad_r) else f"{bad_c + bad_r} entries have a zero or negative amount."})
 
    # 5. Possible double entries (a warning only: two real payments can look alike).
    cur.execute("""SELECT COUNT(*) AS c FROM (SELECT 1 FROM rejesho WHERE group_id=%s
                   GROUP BY loan_id, amount, date HAVING COUNT(*) > 1) d""", (group_id,))
    dup_r = int(get_single_value(cur, 0))
    cur.execute("""SELECT COUNT(*) AS c FROM (SELECT 1 FROM contributions WHERE group_id=%s
                   GROUP BY member_id, type, amount, COALESCE(transaction_date::text, date::text)
                   HAVING COUNT(*) > 1) d""", (group_id,))
    dup_c = int(get_single_value(cur, 0))
    checks.append({"key": "duplicates", "level": "ok" if not (dup_r or dup_c) else "warn",
        "label": "No possible double entries",
        "detail": "None found." if not (dup_r or dup_c) else
                  f"{dup_r + dup_c} groups of identical entries (same person, amount and date). Check they are real."})
 
    # 6. Leadership pay must not be more than the group agreed in Settings.
    settings = get_group_settings(db, group_id)
    try:
        agreed = float(settings.get("leadership_pay_amount", 0) or 0)
    except (TypeError, ValueError):
        agreed = 0.0
    cur.execute("""SELECT COALESCE(SUM(amount), 0) AS t FROM expenses
                   WHERE group_id = %s AND category = 'leadership_pay' AND NOT voided""", (group_id,))
    paid_leaders = _n(get_single_value(cur, 0))
    ok = paid_leaders <= agreed + 1
    checks.append({"key": "leadership_pay", "level": "ok" if ok else "bad",
        "label": "Leadership pay is within what the group agreed",
        "detail": (f"Paid {paid_leaders:,.0f} TZS of the agreed {agreed:,.0f} TZS." if ok else
                   f"Leaders have been paid {paid_leaders:,.0f} TZS but the group agreed {agreed:,.0f} TZS "
                   f"(too much by {paid_leaders - agreed:,.0f} TZS).")})
 
    cur.close()
    return checks
 
 
def fairness_summary(db, group_id):
    cur = get_cursor(db)
    cur.execute("""
        SELECT COALESCE(SUM(amount::numeric),0) AS imposed,
               COALESCE(SUM(COALESCE(amount_paid,0)::numeric),0) AS paid,
               COALESCE(SUM(COALESCE(forgiven_amount,0)::numeric),0) AS forgiven,
               COUNT(*) FILTER (WHERE COALESCE(forgiven_amount,0) > 0) AS forgiven_count
        FROM penalties WHERE group_id = %s AND type != 'loan_forgiven'
    """, (group_id,))
    p = cur.fetchone()
    imposed, paid, forgiven = _n(p['imposed']), _n(p['paid']), _n(p['forgiven'])
 
    cur.execute("""
        SELECT m.id, m.name,
               GREATEST(l.principal::numeric - COALESCE(r.paid, 0), 0) AS outstanding
        FROM loans l
        JOIN members m ON m.id = l.member_id
        LEFT JOIN (SELECT loan_id, SUM(amount::numeric) AS paid FROM rejesho
                   WHERE group_id = %s GROUP BY loan_id) r ON r.loan_id = l.id
        WHERE l.group_id = %s AND l.status != 'Forgiven'
    """, (group_id, group_id))
    per_member = {}
    for row in cur.fetchall():
        d = per_member.setdefault(row['id'], {"name": row['name'], "owed": 0.0})
        d["owed"] += _n(row['outstanding'])
    ranked = sorted((v for v in per_member.values() if v["owed"] > 0), key=lambda v: -v["owed"])
    total_owed = sum(v["owed"] for v in ranked)
    top3 = ranked[:3]
    top3_share = (sum(v["owed"] for v in top3) / total_owed * 100) if total_owed else 0
 
    cur.execute("""
        SELECT action, COUNT(*) AS c FROM audit_log
        WHERE group_id = %s AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY action
    """, (group_id,))
    actions = {r['action']: int(r['c']) for r in cur.fetchall()}
    cur.close()
 
    return {
        "penalties": {"imposed": round(imposed), "collected": round(paid), "forgiven": round(forgiven),
                      "forgiven_pct": round(forgiven / imposed * 100, 1) if imposed else 0,
                      "outstanding": round(max(imposed - paid - forgiven, 0))},
        "loans": {"borrowers": len(ranked), "owed_total": round(total_owed),
                  "top3": [{"name": v["name"], "owed": round(v["owed"])} for v in top3],
                  "top3_share_pct": round(top3_share, 1)},
        "actions_30d": {
            "deletions": sum(actions.get(k, 0) for k in ("delete_rejesho", "edit_contribution:DELETE",
                             "edit_loan:DELETE", "edit_penalty:DELETE", "void_expense")),
            "edits": sum(actions.get(k, 0) for k in ("edit_contribution:PUT", "edit_loan:PUT", "edit_penalty:PUT",
                                                     "handle_settings")),
            "forgiveness": sum(actions.get(k, 0) for k in ("forgive_loan", "forgive_penalty_amount")),
            "total": sum(actions.values()),
        },
    }
 
 
# ---------------------------------------------------------------- routes: money check
def _check_row(r):
    return {
        "id": r['id'], "as_of": r['as_of'],
        "actual": _n(r['actual_balance']), "expected": _n(r['expected_balance']),
        "difference": _n(r['difference']), "status": r['status'],
        "tolerance": _n(r['tolerance']),
        "components": json.loads(r['components']) if r.get('components') else None,
        "statement_ref": r['statement_ref'], "note": r['note'],
        "entered_by": r['entered_by_name'], "entered_role": r['entered_by_role'],
        "created_at": r['created_at'],
    }
 
 
@app.route('/balance-check')
@require_any_member
def balance_check_page():
    return render_template('balance_check.html',
                           can_record=session.get('role') in ('admin', 'treasurer'))
 
 
@app.route('/api/reconciliation', methods=['GET'])
@require_any_member
def reconciliation_summary():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
 
    settings = get_group_settings(db, group_id)
    try:
        tolerance = float(settings.get("reconcile_tolerance", 1000))
    except (TypeError, ValueError):
        tolerance = 1000.0
    try:
        every_days = int(float(settings.get("reconcile_every_days", 30)))
    except (TypeError, ValueError):
        every_days = 30
 
    today = _acc_today()
    now = compute_expected_balance(db, group_id, today.isoformat())
 
    cur = get_cursor(db)
    cur.execute("SELECT * FROM balance_checks WHERE group_id = %s ORDER BY id DESC LIMIT 12", (group_id,))
    history = [_check_row(r) for r in cur.fetchall()]
    cur.execute("""SELECT id, actor_name, actor_role, action, summary, created_at
                   FROM audit_log WHERE group_id = %s ORDER BY id DESC LIMIT 50""", (group_id,))
    actions = [dict(r) for r in cur.fetchall()]
    cur.close()
 
    latest = history[0] if history else None
    days_since = None
    if latest:
        days_since = (today - datetime.strptime(latest["as_of"], "%Y-%m-%d").date()).days
 
    return jsonify({
        "today": today.isoformat(),
        "tolerance": tolerance,
        "every_days": every_days,
        "now": now,
        "latest": latest,
        "history": history,
        "days_since_last": days_since,
        "check_overdue": latest is None or days_since > every_days,
        "integrity": integrity_checks(db, group_id),
        "fairness": fairness_summary(db, group_id),
        "actions": actions,
        "can_record": session.get('role') in ('admin', 'treasurer'),
    })
 
 
@app.route('/api/reconciliation/check', methods=['POST'])
@require_treasurer_or_above
def reconciliation_record():
    """
    Record the real M-Koba balance. The app computes what the balance SHOULD
    be on that date and stores both, plus the difference. Entries are
    permanent: there is no edit or delete.
    Body: { "actual_balance": 8450000, "as_of": "2026-10-14" (optional),
            "statement_ref": "optional", "note": "optional" }
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
 
    data = request.get_json(silent=True) or {}
    try:
        actual = float(data.get("actual_balance"))
    except (TypeError, ValueError):
        return jsonify({"error": "Enter the M-Koba balance as a number"}), 400
    if actual < 0 or actual > 1e12:
        return jsonify({"error": "That balance does not look right"}), 400
 
    today = _acc_today()
    as_of = str(data.get("as_of") or today.isoformat()).strip()
    try:
        as_of_date = datetime.strptime(as_of, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Date must look like 2026-10-14"}), 400
    if as_of_date > today:
        return jsonify({"error": "The date cannot be in the future"}), 400
 
    note = str(data.get("note") or "").strip()[:300]
    ref  = str(data.get("statement_ref") or "").strip()[:60]
 
    settings = get_group_settings(db, group_id)
    try:
        tolerance = float(settings.get("reconcile_tolerance", 1000))
    except (TypeError, ValueError):
        tolerance = 1000.0
 
    comp = compute_expected_balance(db, group_id, as_of_date.isoformat())
    expected   = comp["expected_balance"]
    difference = round(actual - expected)
    status     = _acc_status(difference, tolerance)
 
    cur = get_cursor(db)
    cur.execute("""
        INSERT INTO balance_checks
            (group_id, as_of, actual_balance, expected_balance, difference, tolerance, status,
             components, statement_ref, note, entered_by, entered_by_name, entered_by_role)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """, (group_id, as_of_date.isoformat(), actual, expected, difference, tolerance, status,
          json.dumps(comp), ref or None, note or None,
          session.get("member_id"), session.get("member_name"), session.get("role")))
    row = _check_row(cur.fetchone())
    db.commit()
    cur.close()
    return jsonify({"status": "success", "check": row})
 
 
@app.route('/api/audit-log', methods=['GET'])
@require_any_member
def audit_log_list():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
    cur = get_cursor(db)
    cur.execute("""SELECT id, actor_name, actor_role, action, summary, created_at
                   FROM audit_log WHERE group_id = %s ORDER BY id DESC LIMIT 200""", (group_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    return jsonify({"log": rows})
 
 
# ---------------------------------------------------------------- PDF download
@app.route('/balance-check/download')
@require_any_member
def balance_check_download():
    """The whole Money Check as a PDF (anyone in the group can download it)."""
    from xml.sax.saxutils import escape as _x
 
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return "No group selected", 400
 
    d = reconciliation_summary().get_json()
    settings   = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba")
 
    def tzs(v):
        return f"{_n(v):,.0f} TZS"
 
    def when(s):
        try:
            t = datetime.strptime(str(s), "%a, %d %b %Y %H:%M:%S GMT") + timedelta(hours=3)
            return t.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            return str(s or "")
 
    styles = getSampleStyleSheet()
    small  = ParagraphStyle('mc_small', parent=styles['Normal'], fontSize=8, leading=10)
    cell   = lambda t: Paragraph(_x(str(t if t is not None else "")), small)
 
    def grid(data, widths, header_color='#198754', align_right=(), bold_rows=(), span_row=None):
        t = Table(data, colWidths=widths, repeatRows=1)
        style = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(header_color)),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.3, colors.grey),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f6f6f6')]),
        ]
        for c in align_right:
            style.append(('ALIGN', (c, 0), (c, -1), 'RIGHT'))
        for r in bold_rows:
            style.append(('FONTNAME', (0, r), (-1, r), 'Helvetica-Bold'))
            style.append(('BACKGROUND', (0, r), (-1, r), colors.HexColor('#e2efe8')))
        if span_row is not None:
            style.append(('SPAN', (0, span_row), (-1, span_row)))
        t.setStyle(TableStyle(style))
        return t
 
    now, latest = d["now"], d["latest"]
    if not latest:
        headline = "No M-Koba balance has been recorded yet."
    elif latest["status"] == "matched":
        headline = (f"MATCHED on {latest['as_of']}: M-Koba held {tzs(latest['actual'])}, "
                    f"the records say {tzs(latest['expected'])}.")
    elif latest["status"] == "short":
        headline = (f"{tzs(abs(latest['difference']))} MISSING on {latest['as_of']}: M-Koba held "
                    f"{tzs(latest['actual'])} but the records say {tzs(latest['expected'])}.")
    else:
        headline = (f"{tzs(latest['difference'])} EXTRA on {latest['as_of']}: M-Koba held "
                    f"{tzs(latest['actual'])} but the records only explain {tzs(latest['expected'])}.")
 
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4),
                            rightMargin=20, leftMargin=20, topMargin=30, bottomMargin=20)
    el = []
    el.append(Paragraph(f"{_x(group_name)} - Money Check", styles['Title']))
    el.append(Paragraph(f"<i>Generated {(datetime.utcnow() + timedelta(hours=3)).strftime('%d %B %Y, %H:%M')} (Tanzania time)</i>",
                        styles['Normal']))
    el.append(Spacer(1, 8))
    el.append(Paragraph(_x(headline), styles['Heading3']))
    el.append(Spacer(1, 6))
 
    i, o = now["money_in"], now["money_out"]
    rows = [["MONEY IN", ""],
            ["Hisa Anzia", tzs(i["hisa_anzia"])], ["Hisa", tzs(i["hisa"])], ["Jamii", tzs(i["jamii"])],
            ["Penalty payments", tzs(i["penalty_payments"])]]
    if i["other"]:
        rows.append(["Other", tzs(i["other"])])
    rows += [["Rejesho (loan repayments)", tzs(i["rejesho"])], ["Total in", tzs(i["total"])],
             ["MONEY OUT", ""], [f"Loans paid out ({o['loans_count']})", tzs(o["loans_paid_out"])]]
    for b in o["expense_breakdown"]:
        rows.append([f"Expense: {b['label']}", tzs(b["amount"])])
    rows += [["Total out", tzs(o["total"])],
             ["THE RECORDS SAY THE ACCOUNT SHOULD HOLD (today)", tzs(now["expected_balance"])]]
    bold = [n for n, r in enumerate(rows)
            if r[0] in ("MONEY IN", "MONEY OUT", "Total in", "Total out") or r[0].startswith("THE RECORDS SAY")]
    el.append(grid(rows, [320, 140], align_right=(1,), bold_rows=bold))
    el.append(Spacer(1, 10))
 
    el.append(Paragraph("History of M-Koba comparisons (permanent)", styles['Heading3']))
    hist = [["As of", "Records say", "M-Koba showed", "Difference", "Result", "Entered by", "Reference / note"]]
    result_text = {"matched": "Matched", "short": "Money missing", "over": "Extra money"}
    for h in d["history"]:
        hist.append([h["as_of"], tzs(h["expected"]), tzs(h["actual"]), tzs(h["difference"]),
                     result_text.get(h["status"], h["status"]),
                     cell(f"{h['entered_by'] or ''} ({h['entered_role'] or ''}) {when(h['created_at'])}"),
                     cell(" - ".join(x for x in (h["statement_ref"], h["note"]) if x))])
    if len(hist) == 1:
        hist.append(["Nothing recorded yet", "", "", "", "", "", ""])
    el.append(grid(hist, [60, 90, 90, 90, 80, 170, 200], align_right=(1, 2, 3),
                   span_row=1 if len(hist) == 2 and hist[1][0] == "Nothing recorded yet" else None))
    el.append(Spacer(1, 10))
 
    el.append(Paragraph("Do the books agree with themselves?", styles['Heading3']))
    label = {"ok": "OK", "warn": "Check", "bad": "PROBLEM"}
    integ = [["Result", "Check", "Detail"]]
    for c in d["integrity"]:
        integ.append([label.get(c["level"], c["level"]), cell(c["label"]), cell(c["detail"])])
    el.append(grid(integ, [60, 230, 440]))
    el.append(Spacer(1, 10))
 
    f = d["fairness"]
    el.append(Paragraph("Fairness at a glance", styles['Heading3']))
    top = ", ".join(f"{x['name']} {tzs(x['owed'])}" for x in f["loans"]["top3"]) or "none"
    el.append(Paragraph(_x(
        f"Penalties: charged {tzs(f['penalties']['imposed'])}, collected {tzs(f['penalties']['collected'])}, "
        f"forgiven {tzs(f['penalties']['forgiven'])} ({f['penalties']['forgiven_pct']}%), "
        f"still owed {tzs(f['penalties']['outstanding'])}."), styles['Normal']))
    el.append(Paragraph(_x(
        f"Loans: {f['loans']['borrowers']} members owe {tzs(f['loans']['owed_total'])}. "
        f"Biggest: {top} (together {f['loans']['top3_share_pct']}%)."), styles['Normal']))
    el.append(Paragraph(_x(
        f"Leader actions in the last 30 days: {f['actions_30d']['total']} in total "
        f"({f['actions_30d']['deletions']} deletions, {f['actions_30d']['edits']} edits, "
        f"{f['actions_30d']['forgiveness']} forgiveness)."), styles['Normal']))
    el.append(Spacer(1, 10))
 
    el.append(Paragraph("Latest leader actions (written automatically)", styles['Heading3']))
    acts = [["When (Tanzania time)", "Who", "What happened"]]
    for a in d["actions"]:
        acts.append([when(a["created_at"]), cell(f"{a['actor_name'] or ''} ({a['actor_role'] or ''})"), cell(a["summary"])])
    if len(acts) == 1:
        acts.append(["No leader actions recorded yet", "", ""])
    el.append(grid(acts, [110, 150, 470],
                   span_row=1 if len(acts) == 2 and acts[1][0].startswith("No leader actions") else None))
 
    doc.build(el)
    buffer.seek(0)
    filename = f"{group_name.replace(' ', '_')}_Money_Check_{_acc_today().isoformat()}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")
 
 
# ---------------------------------------------------------------- expenses ledger
@app.route('/expenses-page')
@require_any_member
def expenses_page():
    return render_template('expenses.html',
                           can_record=session.get('role') in ('admin', 'treasurer'),
                           can_void=session.get('role') == 'admin')
 
 
def _expense_setting(db, group_id, key, default):
    try:
        return float(get_group_settings(db, group_id).get(key, default))
    except (TypeError, ValueError):
        return float(default)
 
 
@app.route('/api/expenses', methods=['GET'])
@require_any_member
def expenses_list():
    """Everything that left the account other than loans: visible to every member."""
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
 
    cur = get_cursor(db)
    cur.execute("""SELECT id, spent_on, category, amount, paid_to, description, approval, receipt_ref,
                          recorded_by_name, recorded_by_role, voided, void_reason, voided_by_name, created_at
                   FROM expenses WHERE group_id = %s ORDER BY spent_on DESC, id DESC""", (group_id,))
    items = []
    for r in cur.fetchall():
        items.append({
            "id": r['id'], "source": "expense", "date": r['spent_on'],
            "category": r['category'], "category_label": _EXPENSE_CATEGORIES.get(r['category'], r['category']),
            "amount": _n(r['amount']), "paid_to": r['paid_to'], "description": r['description'],
            "approval": r['approval'], "receipt_ref": r['receipt_ref'],
            "recorded_by": r['recorded_by_name'], "recorded_role": r['recorded_by_role'],
            "voided": bool(r['voided']), "void_reason": r['void_reason'], "voided_by": r['voided_by_name'],
        })
    cur.execute("""SELECT c.id, COALESCE(NULLIF(c.transaction_date::text, ''), c.date::text) AS d, c.amount
                   FROM contributions c WHERE c.group_id = %s AND c.type = 'jamii_deduction'
                   ORDER BY 2 DESC, c.id DESC""", (group_id,))
    for r in cur.fetchall():
        items.append({
            "id": r['id'], "source": "jamii", "date": r['d'], "category": "jamii",
            "category_label": "Jamii deduction", "amount": abs(_n(r['amount'])),
            "paid_to": "", "description": "Jamii deduction recorded by a leader",
            "approval": None, "receipt_ref": None, "recorded_by": None, "recorded_role": None,
            "voided": False, "void_reason": None, "voided_by": None,
        })
    items.sort(key=lambda x: (x["date"] or "", x["id"]), reverse=True)
 
    totals = {}
    for x in items:
        if not x["voided"]:
            totals[x["category_label"]] = totals.get(x["category_label"], 0) + x["amount"]
 
    cur.execute("""SELECT COALESCE(SUM(amount), 0) AS t FROM expenses
                   WHERE group_id = %s AND category = 'leadership_pay' AND NOT voided""", (group_id,))
    paid_leaders = _n(get_single_value(cur, 0))
    cur.close()
 
    agreed = _expense_setting(db, group_id, "leadership_pay_amount", 0)
    return jsonify({
        "categories": [{"key": k, "label": v} for k, v in _EXPENSE_CATEGORIES.items()],
        "approval_above": _expense_setting(db, group_id, "expense_approval_above", 50000),
        "leadership": {"agreed": round(agreed), "paid": round(paid_leaders),
                       "remaining": round(max(agreed - paid_leaders, 0))},
        "total": round(sum(totals.values())),
        "by_category": [{"label": k, "amount": round(v)} for k, v in sorted(totals.items(), key=lambda kv: -kv[1])],
        "expenses": items,
        "can_record": session.get('role') in ('admin', 'treasurer'),
        "can_void": session.get('role') == 'admin',
    })
 
 
@app.route('/api/expenses', methods=['POST'])
@require_treasurer_or_above
def expenses_add():
    """
    Record money that left the account (not a loan).
    Body: { "category": "meeting", "amount": 30000, "paid_to": "Mama Neema",
            "description": "Tea for the AGM", "spent_on": "2026-10-14" (optional),
            "approval": "Agreed at the meeting of 12 Oct" (needed above the limit),
            "receipt_ref": "optional" }
    """
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
 
    data = request.get_json(silent=True) or {}
    category = str(data.get("category") or "").strip()
    if category not in _EXPENSE_CATEGORIES:
        return jsonify({"error": "Choose what the money was for"}), 400
    try:
        amount = float(data.get("amount"))
    except (TypeError, ValueError):
        return jsonify({"error": "Enter the amount as a number"}), 400
    if amount <= 0 or amount > 1e10:
        return jsonify({"error": "The amount does not look right"}), 400
 
    paid_to     = str(data.get("paid_to") or "").strip()[:80]
    description = str(data.get("description") or "").strip()[:300]
    approval    = str(data.get("approval") or "").strip()[:200]
    receipt     = str(data.get("receipt_ref") or "").strip()[:60]
    if len(paid_to) < 2:
        return jsonify({"error": "Say who the money was paid to"}), 400
    if len(description) < 5:
        return jsonify({"error": "Describe what the money was for (at least a few words)"}), 400
 
    today = _acc_today()
    spent_on = str(data.get("spent_on") or today.isoformat()).strip()
    try:
        spent_date = datetime.strptime(spent_on, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Date must look like 2026-10-14"}), 400
    if spent_date > today:
        return jsonify({"error": "The date cannot be in the future"}), 400
 
    limit = _expense_setting(db, group_id, "expense_approval_above", 50000)
    if (amount > limit or category == "leadership_pay") and len(approval) < 5:
        return jsonify({"error": "This needs an approval note: who or which meeting agreed to it "
                                 f"(required for leadership pay and for anything above {limit:,.0f} TZS)"}), 400
 
    if category == "leadership_pay":
        agreed = _expense_setting(db, group_id, "leadership_pay_amount", 0)
        cur = get_cursor(db)
        cur.execute("""SELECT COALESCE(SUM(amount), 0) AS t FROM expenses
                       WHERE group_id = %s AND category = 'leadership_pay' AND NOT voided""", (group_id,))
        already = _n(get_single_value(cur, 0))
        cur.close()
        if already + amount > agreed + 1:
            return jsonify({"error": f"The group agreed {agreed:,.0f} TZS for leadership pay and {already:,.0f} TZS "
                                     f"has already been paid. This would be too much. Change the agreed amount in "
                                     f"Settings first (that change is visible to all members)."}), 400
 
    cur = get_cursor(db)
    cur.execute("""
        INSERT INTO expenses (group_id, spent_on, category, amount, paid_to, description, approval, receipt_ref,
                              recorded_by, recorded_by_name, recorded_by_role)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
    """, (group_id, spent_date.isoformat(), category, amount, paid_to, description, approval or None, receipt or None,
          session.get("member_id"), session.get("member_name"), session.get("role")))
    new_id = cur.fetchone()['id']
    cur.close()
    _audit_direct(db, group_id, "add_expense",
                  f"Recorded an expense of {_tzs(amount)} ({_EXPENSE_CATEGORIES[category]}) paid to {paid_to}: {description}")
    db.commit()
    return jsonify({"status": "success", "id": new_id})
 
 
@app.route('/api/expenses/<int:expense_id>/void', methods=['POST'])
@require_admin
def expenses_void(expense_id):
    """Cancel an expense that was recorded by mistake. It stays on the list, marked void, with the reason."""
    db = get_db()
    group_id = get_current_group_id()
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    _acc_ensure(db)
 
    reason = str((request.get_json(silent=True) or {}).get("reason") or "").strip()[:200]
    if len(reason) < 5:
        return jsonify({"error": "Give a reason (a few words) for cancelling this expense"}), 400
 
    cur = get_cursor(db)
    cur.execute("SELECT amount, category, paid_to, voided FROM expenses WHERE id = %s AND group_id = %s",
                (expense_id, group_id))
    row = cur.fetchone()
    if not row:
        cur.close()
        return jsonify({"error": "Expense not found"}), 404
    if row['voided']:
        cur.close()
        return jsonify({"error": "This expense is already cancelled"}), 400
    cur.execute("""UPDATE expenses SET voided = TRUE, void_reason = %s, voided_by_name = %s, voided_at = NOW()
                   WHERE id = %s AND group_id = %s""", (reason, session.get("member_name"), expense_id, group_id))
    cur.close()
    _audit_direct(db, group_id, "void_expense",
                  f"Cancelled the expense of {_tzs(row['amount'])} ({_EXPENSE_CATEGORIES.get(row['category'], row['category'])}) "
                  f"paid to {row['paid_to']}. Reason: {reason}")
    db.commit()
    return jsonify({"status": "success"})
 
 
# ---------------------------------------------------------------- automatic audit trail
# Leader actions that change or hide money records. The key is the Flask
# endpoint name (the function name of the route).
_AUDITED = {
    'delete_rejesho', 'edit_contribution', 'edit_loan', 'forgive_loan', 'unforgive_loan',
    'edit_penalty', 'freeze_penalty', 'forgive_penalty_amount', 'add_penalty',
    'record_penalty_payment', 'record_jamii_deduction', 'add_loan',
    'set_member_role', 'toggle_member_active', 'handle_settings',
}
 
_AUDITED_SETTINGS = [
    'group_name', 'interest_rate', 'daily_penalty_amount', 'leadership_pay_amount',
    'mercy_enabled', 'mercy_forgive_percent', 'mercy_half_percent', 'mercy_half_rate_percent',
    'cycle_start_date', 'cycle_end_date', 'hisa_unit_price',
    'loan_tier1_amount', 'loan_tier1_months', 'loan_tier2_amount', 'loan_tier2_months',
    'loan_tier3_amount', 'loan_tier3_months', 'loan_tier4_amount', 'loan_tier4_months',
    'loan_tier5_amount', 'loan_tier5_months',
]
 
 
def _same_setting(a, b):
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return str(a) == str(b)
 
 
def _audit_describe(db, cur, ep, method, args, group_id, body):
    """Return a plain-language sentence about what is about to happen, or None."""
    if ep == 'handle_settings':
        old = get_group_settings(db, group_id)
        changes = [f"{k}: {old.get(k, '')} -> {body.get(k)}" for k in _AUDITED_SETTINGS
                   if body.get(k) not in (None, "") and not _same_setting(old.get(k, ""), body.get(k))]
        return ("Changed group settings - " + "; ".join(changes)) if changes else None
 
    if ep == 'delete_rejesho':
        cur.execute("""SELECT r.amount, r.date, l.id AS loan_id, m.name
                       FROM rejesho r JOIN loans l ON l.id = r.loan_id JOIN members m ON m.id = l.member_id
                       WHERE r.id = %s AND r.group_id = %s""", (args.get('rejesho_id'), group_id))
        r = cur.fetchone()
        return (f"Deleted a rejesho payment of {_tzs(r['amount'])} dated {r['date']} "
                f"({r['name']}, loan #{r['loan_id']})") if r else None
 
    if ep == 'edit_contribution':
        cur.execute("""SELECT c.type, c.amount, c.date, c.transaction_date, m.name
                       FROM contributions c JOIN members m ON m.id = c.member_id
                       WHERE c.id = %s AND c.group_id = %s""", (args.get('contribution_id'), group_id))
        r = cur.fetchone()
        if not r:
            return None
        if method == 'DELETE':
            return f"Deleted a {r['type']} contribution of {_tzs(r['amount'])} dated {r['transaction_date'] or r['date']} ({r['name']})"
        return (f"Edited a {r['type']} contribution of {r['name']}: was {_tzs(r['amount'])} dated "
                f"{r['transaction_date'] or r['date']}, now {_tzs(body.get('amount'))} dated {body.get('transaction_date') or body.get('date')}")
 
    if ep in ('edit_loan', 'forgive_loan', 'unforgive_loan'):
        cur.execute("""SELECT l.principal, l.status, l.due_date, m.name FROM loans l
                       JOIN members m ON m.id = l.member_id WHERE l.id = %s AND l.group_id = %s""",
                    (args.get('loan_id'), group_id))
        r = cur.fetchone()
        if not r:
            return None
        lid = args.get('loan_id')
        if ep == 'forgive_loan':
            return f"Forgave the loan of {r['name']} (loan #{lid}, {_tzs(r['principal'])}). Reason: {str(body.get('reason') or '')[:120]}"
        if ep == 'unforgive_loan':
            return f"Reversed the forgiveness of {r['name']}'s loan #{lid}"
        if method == 'DELETE':
            return f"Deleted the loan of {r['name']} (loan #{lid}, {_tzs(r['principal'])})"
        return (f"Changed loan #{lid} of {r['name']}: status {r['status']} -> {body.get('status')}, "
                f"due date {r['due_date']} -> {body.get('due_date')}")
 
    if ep in ('edit_penalty', 'freeze_penalty', 'forgive_penalty_amount', 'record_penalty_payment'):
        cur.execute("""SELECT p.type, p.amount, p.description, m.name FROM penalties p
                       JOIN members m ON m.id = p.member_id WHERE p.id = %s AND p.group_id = %s""",
                    (args.get('penalty_id'), group_id))
        r = cur.fetchone()
        if not r:
            return None
        pid = args.get('penalty_id')
        if ep == 'forgive_penalty_amount':
            return (f"Forgave {_tzs(body.get('forgiven_amount'))} of penalty #{pid} ({r['name']}). "
                    f"Reason: {str(body.get('reason') or '')[:120]}")
        if ep == 'freeze_penalty':
            return (f"{'Froze' if body.get('freeze', True) else 'Unfroze'} penalty #{pid} ({r['name']}). "
                    f"Reason: {str(body.get('reason') or '')[:120]}")
        if ep == 'record_penalty_payment':
            return f"Received a penalty payment of {_tzs(body.get('amount'))} for penalty #{pid} ({r['name']})"
        if method == 'DELETE':
            return f"Deleted penalty #{pid} of {r['name']}: {_tzs(r['amount'])}"
        return f"Changed penalty #{pid} of {r['name']}: {_tzs(r['amount'])} -> {_tzs(body.get('amount'))}"
 
    if ep in ('add_penalty', 'add_loan', 'set_member_role', 'toggle_member_active'):
        cur.execute("SELECT name FROM members WHERE id = %s AND group_id = %s", (body.get('member_id'), group_id))
        r = cur.fetchone()
        if not r:
            return None
        if ep == 'add_penalty':
            return f"Added a manual penalty of {_tzs(body.get('amount'))} to {r['name']}: {str(body.get('description') or '')[:100]}"
        if ep == 'add_loan':
            return f"Issued a loan of {_tzs(body.get('principal'))} to {r['name']}"
        if ep == 'set_member_role':
            return f"Changed the role of {r['name']} to {body.get('role')}"
        return f"{'Activated' if body.get('is_active') else 'Deactivated'} the member {r['name']}"
 
    if ep == 'record_jamii_deduction':
        return f"Recorded an expense (Jamii deduction) of {_tzs(body.get('amount'))}"
 
    return None
 
 
@app.before_request
def _audit_capture():
    """Before a leader action runs, note what it is about to change."""
    try:
        if request.method not in ("POST", "PUT", "DELETE"):
            return
        ep = request.endpoint
        if ep not in _AUDITED:
            return
        group_id = session.get("group_id")
        if not group_id:
            return
        db = get_db()
        _acc_ensure(db)
        cur = get_cursor(db)
        summary = _audit_describe(db, cur, ep, request.method, request.view_args or {}, group_id,
                                  request.get_json(silent=True) or {})
        cur.close()
        if summary:
            key = f"{ep}:{request.method}" if ep in ('edit_contribution', 'edit_loan', 'edit_penalty') else ep
            g._audit = {"group_id": group_id, "action": key, "summary": summary[:500]}
    except Exception as e:
        print(f"audit capture error: {e}")
        try:
            get_db().rollback()
        except Exception:
            pass
 
 
@app.after_request
def _audit_write(response):
    """If the action succeeded, write it to the permanent log."""
    try:
        entry = g.pop("_audit", None)
        if entry and response.status_code < 400:
            db = get_db()
            _audit_direct(db, entry["group_id"], entry["action"], entry["summary"])
            db.commit()
    except Exception as e:
        print(f"audit write error: {e}")
        try:
            get_db().rollback()
        except Exception:
            pass
    return response
 

if __name__ == "__main__":
    from models import init_db
    with app.app_context():
        init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)