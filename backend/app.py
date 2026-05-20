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

env = os.environ.get('FLASK_ENV', 'development')
CORS(app, origins=app.config['CORS_ORIGINS'])
init_db_app(app)

UPLOAD_FOLDER = app.config['UPLOAD_FOLDER']
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# ==================== HELPERS ====================
def get_single_value(cursor, default=None):
    result = cursor.fetchone()
    if not result:
        return default
    values = list(result.values())
    return values[0] if values and values[0] is not None else default

def get_cursor(db):
    return db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def get_current_group_id():
    return session.get("group_id")

def get_group_admin_member_id(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM members WHERE group_id = %s AND is_system = 1", (group_id,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        raise Exception(f"No system admin member found for group_id={group_id}")
    return row["id"]

def _ensure_penalty_columns(db):
    """Idempotently add columns missing from early schema versions."""
    cursor = get_cursor(db)
    for sql in [
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS amount_paid REAL DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS forgiven_amount REAL DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS is_frozen INTEGER DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS freeze_reason TEXT",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven INTEGER DEFAULT 0",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiveness_reason TEXT",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_by TEXT",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_at TEXT",
    ]:
        try:
            cursor.execute(sql)
            db.commit()
        except Exception:
            db.rollback()
    cursor.close()


# ==================== GROUP / SETTINGS ====================
def create_new_group(db, group_name, admin_id):
    cursor = get_cursor(db)
    cursor.execute("INSERT INTO groups (name, created_at) VALUES (%s, CURRENT_TIMESTAMP) RETURNING id", (group_name,))
    group_id = cursor.fetchone()["id"]
    cursor.execute("UPDATE members SET group_id = %s WHERE id = %s", (group_id, admin_id))
    defaults = [
        ('group_name', group_name), ('interest_rate', '0.10'),
        ('daily_penalty_amount', '1000'), ('leadership_pay_amount', '0'),
        ('jamii_amount', '2000'), ('jamii_frequency', 'monthly'),
        ('cycle_start_date', ''), ('cycle_end_date', ''),
        ('hisa_unit_price', '5000'),
        ('loan_tier5_amount', '10000000'), ('loan_tier5_months', '12'),
    ]
    for key, value in defaults:
        cursor.execute("INSERT INTO settings (group_id, key, value) VALUES (%s, %s, %s)", (group_id, key, value))
    db.commit()
    cursor.close()
    return group_id

def get_group_settings(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT key, value FROM settings WHERE group_id = %s", (group_id,))
    data = {s["key"]: s["value"] for s in cursor.fetchall()}
    cursor.close()
    defaults = {
        'group_name': 'Kikoba App', 'interest_rate': '0.10',
        'daily_penalty_amount': '1000', 'leadership_pay_amount': '0',
        'jamii_amount': '2000', 'jamii_frequency': 'monthly',
        'cycle_start_date': '', 'cycle_end_date': '',
        'hisa_unit_price': '5000',
        'loan_tier1_amount': '500000',   'loan_tier1_months': '1',
        'loan_tier2_amount': '1000000',  'loan_tier2_months': '3',
        'loan_tier3_amount': '2000000',  'loan_tier3_months': '6',
        'loan_tier4_amount': '5000000',  'loan_tier4_months': '9',
        'loan_tier5_amount': '10000000', 'loan_tier5_months': '12',
    }
    for k, v in defaults.items():
        if k not in data:
            data[k] = v
    if "constitution_path" in data:
        data["constitution_view_url"] = "/api/constitution/view"
        data["constitution_download_url"] = "/api/constitution/download"
    return data


# ==================== FINANCIAL HELPERS ====================
def get_member_hisa_units(db, member_id, group_id):
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(amount) FROM contributions WHERE member_id=%s AND group_id=%s AND type='hisa'",
        (member_id, group_id)
    )
    total_hisa = get_single_value(cursor, 0)
    cursor.close()
    return {"total_contributed": total_hisa, "units": total_hisa / unit_price if unit_price > 0 else 0, "unit_price": unit_price}

def get_total_hisa_units(db, group_id):
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    admin_id = get_group_admin_member_id(db, group_id)
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(amount) FROM contributions WHERE group_id=%s AND type='hisa' AND member_id!=%s",
        (group_id, admin_id)
    )
    total_hisa = get_single_value(cursor, 0)
    cursor.close()
    return total_hisa / unit_price if unit_price > 0 else 0

def get_member_jamii_balance(db, member_id, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(amount) FROM contributions WHERE member_id=%s AND group_id=%s AND type='jamii'",
        (member_id, group_id)
    )
    total_paid = get_single_value(cursor, 0)
    cursor.close()
    return {"total_paid": total_paid, "expected_total": 0, "shortfall": 0, "periods": 0}

def get_total_principal_loaned(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT SUM(principal) FROM loans WHERE group_id=%s", (group_id,))
    result = get_single_value(cursor)
    cursor.close()
    return result if result else 0

def get_total_outstanding_loans(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT SUM(principal) FROM loans WHERE group_id=%s AND status!='Cleared'", (group_id,))
    total_liability = get_single_value(cursor, 0)
    cursor.execute(
        "SELECT SUM(r.amount) FROM rejesho r JOIN loans l ON r.loan_id=l.id WHERE l.group_id=%s AND l.status!='Cleared'",
        (group_id,)
    )
    total_repaid = get_single_value(cursor, 0)
    cursor.close()
    return max(total_liability - total_repaid, 0)

def get_total_penalties_imposed(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT SUM(amount) FROM penalties WHERE group_id=%s", (group_id,))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

def get_total_penalties_paid(db, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT SUM(COALESCE(amount_paid,0)) FROM penalties WHERE group_id=%s", (group_id,))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

def get_total_group_penalty_liability(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(GREATEST(0, amount - COALESCE(amount_paid,0) - COALESCE(forgiven_amount,0))) FROM penalties WHERE group_id=%s",
        (group_id,)
    )
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

def get_total_penalties_due_for_member(member_id, db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT SUM(amount - COALESCE(amount_paid,0)) FROM penalties WHERE member_id=%s AND group_id=%s",
        (member_id, group_id)
    )
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

def get_total_penalties_for_member(member_id, group_id):
    db = get_db()
    return get_total_penalties_due_for_member(member_id, db, group_id)

def get_current_group_profit(db, group_id):
    settings = get_group_settings(db, group_id)
    LEADERSHIP_PAY = float(settings.get('leadership_pay_amount', 0))
    cursor = get_cursor(db)
    cursor.execute("SELECT SUM(total - principal) FROM loans WHERE group_id=%s", (group_id,))
    total_interest = get_single_value(cursor, 0)
    cursor.execute("SELECT SUM(amount) FROM contributions WHERE type='jamii' AND group_id=%s", (group_id,))
    total_jamii = get_single_value(cursor, 0)
    cursor.close()
    total_penalties_imposed = get_total_penalties_imposed(db, group_id)
    total_penalties_revenue = get_total_penalties_paid(db, group_id)
    gross = total_interest + total_penalties_imposed
    net = max(gross - LEADERSHIP_PAY, 0)
    return {
        "total_interest": total_interest,
        "total_penalties_imposed": total_penalties_imposed,
        "total_penalties_revenue": total_penalties_revenue,
        "total_jamii_collected": total_jamii,
        "leadership_pay_amount": LEADERSHIP_PAY,
        "gross_distributable_pool": gross,
        "net_profit_pool": net,
    }

def get_member_loan_balances(db, member_id, group_id):
    today_date = date.today()
    total_committed = total_rejesho = total_overdue = 0
    cursor = get_cursor(db)
    cursor.execute("SELECT id, principal, due_date FROM loans WHERE member_id=%s AND group_id=%s", (member_id, group_id))
    for loan in cursor.fetchall():
        total_committed += loan['principal']
        cursor.execute("SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan['id'], group_id))
        repaid = get_single_value(cursor, 0)
        total_rejesho += repaid
        remaining = max(loan['principal'] - repaid, 0)
        try:
            due = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        except Exception:
            due = today_date + timedelta(days=1)
        if remaining > 0 and due < today_date:
            total_overdue += remaining
    cursor.close()
    return {
        "total_loans_committed": total_committed,
        "total_rejesho": total_rejesho,
        "remaining_loans": max(total_committed - total_rejesho, 0),
        "total_overdue": total_overdue,
    }

def update_loan_status(db, loan_id, group_id):
    cursor = get_cursor(db)
    cursor.execute("SELECT * FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    loan = cursor.fetchone()
    if not loan or loan["status"] == "Forgiven":
        cursor.close()
        return
    cursor.execute("SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
    repaid = get_single_value(cursor, 0)
    remaining = loan["total"] - repaid
    if remaining <= 0:
        new_status = "Cleared"
    elif datetime.now().date() > datetime.strptime(loan["due_date"], "%Y-%m-%d").date():
        new_status = "Overdue"
    else:
        new_status = "Active"
    if loan["status"] != new_status:
        cursor.execute("UPDATE loans SET status=%s WHERE id=%s AND group_id=%s", (new_status, loan_id, group_id))
        db.commit()
    cursor.close()


# ==================== PENALTY ENGINE ====================
def _get_month_due_dates(loan):
    """Return list of due dates for each monthly instalment, oldest first."""
    final_due = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
    months = loan['months']
    base_day = final_due.day
    base_m = final_due.month - months
    base_y = final_due.year
    if base_m <= 0:
        base_m += 12
        base_y -= 1
    dues = []
    for n in range(1, months + 1):
        dm = base_m + n
        dy = base_y
        if dm > 12:
            dm -= 12
            dy += 1
        max_d = calendar.monthrange(dy, dm)[1]
        dues.append(date(dy, dm, min(base_day, max_d)))
    return dues


def auto_insert_loan_penalties(db, group_id):
    """
    For every active/overdue loan, check each monthly instalment slot.

    RULES (clean, no scaling):
      1. Paid >= 90% of that month ON TIME  → NO penalty (mercy). Delete if exists.
      2. Paid < 90% on time, slot FROZEN    → skip (freeze logic handles this).
      3. Paid < 90% on time, not frozen     → charge full daily_penalty × days_late.
         Once the slot is fully covered (even late), auto_freeze_settled_month_penalties
         will freeze it — accrual stops there.

    Pattern: 'Month {N} rejesho%' — anchored to avoid Month 1 matching Month 10.
    """
    _ensure_penalty_columns(db)
    settings = get_group_settings(db, group_id)
    daily_penalty = float(settings.get("daily_penalty_amount", 1000))
    MERCY_THRESHOLD = 0.90
    TOLERANCE = 1.0
    today = datetime.now().date()
    cursor = get_cursor(db)

    cursor.execute("""
        SELECT id, member_id, principal, months, start_date, due_date
        FROM loans WHERE group_id=%s AND status IN ('Active','Overdue')
    """, (group_id,))
    active_loans = cursor.fetchall()

    for loan in active_loans:
        loan_id = loan['id']
        member_id = loan['member_id']
        monthly_rejesho = loan['principal'] / loan['months']
        dues = _get_month_due_dates(loan)

        # Total ever paid on this loan (for freeze check)
        cursor.execute("SELECT COALESCE(SUM(amount),0) AS t FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
        total_paid_all_time = float(cursor.fetchone()['t'])

        for month_num, month_due in enumerate(dues, 1):
            if today <= month_due:
                continue  # not yet due

            days_late = (today - month_due).days
            if days_late <= 0:
                continue

            # Paid on or before this month's deadline
            cursor.execute("""
                SELECT COALESCE(SUM(amount),0) AS t FROM rejesho
                WHERE loan_id=%s AND group_id=%s AND date<=%s
            """, (loan_id, group_id, month_due.strftime("%Y-%m-%d")))
            paid_by_due = float(cursor.fetchone()['t'])

            expected_prev = monthly_rejesho * (month_num - 1)
            expected_this = monthly_rejesho * month_num

            # How much of THIS month was paid on time
            this_month_on_time = max(0.0, paid_by_due - expected_prev)
            pct_on_time = min(1.0, this_month_on_time / monthly_rejesho) if monthly_rejesho > 0 else 1.0

            # Find existing penalty record for this month (anchored LIKE)
            cursor.execute("""
                SELECT id, amount, COALESCE(is_frozen,0) AS is_frozen
                FROM penalties
                WHERE loan_id=%s AND group_id=%s
                  AND type='monthly_rejesho_late'
                  AND description LIKE %s
            """, (loan_id, group_id, f"Month {month_num} rejesho%"))
            existing = cursor.fetchone()

            # ── RULE 1: Mercy — ≥90% paid on time → no penalty ──────────
            if pct_on_time >= MERCY_THRESHOLD:
                if existing:
                    # Delete any previously created penalty (shouldn't exist, safety)
                    cursor.execute("DELETE FROM penalties WHERE id=%s", (existing['id'],))
                continue

            # ── RULE 2: Frozen → never touch ─────────────────────────────
            if existing and existing['is_frozen']:
                continue

            # ── RULE 3: Full daily penalty × days late ────────────────────
            penalty_amount = days_late * int(daily_penalty)

            desc = f"Month {month_num} rejesho overdue by {days_late} days"

            if existing:
                # Only increase, never decrease (preserves manually adjusted amounts)
                if penalty_amount > existing['amount']:
                    cursor.execute("""
                        UPDATE penalties SET amount=%s, description=%s, date=%s WHERE id=%s
                    """, (penalty_amount, desc, today.strftime("%Y-%m-%d"), existing['id']))
            else:
                cursor.execute("""
                    INSERT INTO penalties (group_id, member_id, loan_id, type, amount, description, date)
                    VALUES (%s,%s,%s,'monthly_rejesho_late',%s,%s,%s)
                """, (group_id, member_id, loan_id, penalty_amount, desc, today.strftime("%Y-%m-%d")))

        # Update loan status
        cursor.execute("SELECT COALESCE(SUM(amount),0) AS t FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
        total_paid = float(cursor.fetchone()['t'])
        if total_paid >= loan['principal'] - TOLERANCE:
            cursor.execute("UPDATE loans SET status='Cleared' WHERE id=%s", (loan_id,))
        elif today > datetime.strptime(loan['due_date'], "%Y-%m-%d").date():
            cursor.execute("UPDATE loans SET status='Overdue' WHERE id=%s", (loan_id,))

    db.commit()
    cursor.close()


def auto_freeze_settled_month_penalties(db, loan_id, group_id):
    """
    Called after every rejesho add or delete.

    For each monthly slot (oldest first):
      - If total_paid_all_time >= month_N cumulative target → FREEZE
      - If total_paid_all_time <  month_N cumulative target and frozen → UNFREEZE
        (handles rejesho deletions that un-cover a slot)

    Uses anchored LIKE 'Month {N} rejesho%' — no false matches.
    """
    _ensure_penalty_columns(db)
    cursor = get_cursor(db)
    cursor.execute("SELECT principal, months, due_date, status FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    loan = cursor.fetchone()
    if not loan or loan['status'] == 'Forgiven':
        cursor.close()
        return

    monthly_rejesho = float(loan['principal']) / int(loan['months'])
    TOLERANCE = 1.0

    cursor.execute("SELECT COALESCE(SUM(amount),0) AS t FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
    total_paid = float(cursor.fetchone()['t'])

    for month_num in range(1, int(loan['months']) + 1):
        expected_cumulative = monthly_rejesho * month_num

        cursor.execute("""
            SELECT id, COALESCE(is_frozen,0) AS is_frozen
            FROM penalties
            WHERE loan_id=%s AND group_id=%s
              AND type='monthly_rejesho_late'
              AND description LIKE %s
        """, (loan_id, group_id, f"Month {month_num} rejesho%"))
        row = cursor.fetchone()

        if not row:
            continue

        slot_paid = total_paid >= (expected_cumulative - TOLERANCE)

        if slot_paid and not row['is_frozen']:
            cursor.execute("""
                UPDATE penalties SET is_frozen=1, freeze_reason=%s WHERE id=%s
            """, (
                f"Auto-frozen: Month {month_num} settled "
                f"(paid {total_paid:,.0f} / needed {expected_cumulative:,.0f} TZS)",
                row['id']
            ))
        elif not slot_paid and row['is_frozen']:
            # Rejesho was deleted — unfreeze so accrual resumes
            cursor.execute("UPDATE penalties SET is_frozen=0, freeze_reason=NULL WHERE id=%s", (row['id'],))

    db.commit()
    cursor.close()


# ==================== ROUTES ====================
@app.route("/")
def index():
    if "admin_id" in session:
        return redirect("/create-group" if "group_id" not in session else "/login")
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("SELECT 1 FROM members WHERE is_system=1 LIMIT 1")
    exists = cursor.fetchone()
    cursor.close()
    return redirect("/signup" if not exists else "/login")

@app.route("/signup", methods=["GET","POST"])
def signup():
    db = get_db()
    cursor = get_cursor(db)
    if request.method == "POST":
        name = request.form.get("name")
        email = request.form.get("email")
        password = request.form.get("password")
        if not name or not email or not password:
            cursor.close()
            return render_template("signup.html", error="All fields are required")
        cursor.execute("SELECT * FROM members WHERE email=%s AND is_system=1", (email,))
        if cursor.fetchone():
            cursor.close()
            return render_template("signup.html", error="Email already registered")
        cursor.execute("""
            INSERT INTO members (name, email, password, is_system, joined_date)
            VALUES (%s,%s,%s,1,CURRENT_DATE) RETURNING id
        """, (name, email, generate_password_hash(password)))
        new_id = cursor.fetchone()["id"]
        db.commit()
        session["admin_id"] = new_id
        cursor.close()
        return redirect("/create-group")
    cursor.close()
    return render_template("signup.html")

@app.route("/login", methods=["GET","POST"])
def login():
    db = get_db()
    cursor = get_cursor(db)
    error = None
    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")
        cursor.execute("SELECT * FROM members WHERE email=%s AND is_system=1", (email,))
        admin = cursor.fetchone()
        if not admin:
            error = "Admin account not found"
        elif not check_password_hash(admin["password"], password):
            error = "Wrong password"
        else:
            session.clear()
            session["admin_id"] = admin["id"]
            if admin["group_id"]:
                session["group_id"] = admin["group_id"]
                cursor.close()
                return redirect("/dashboard")
            cursor.close()
            return redirect("/create-group")
    cursor.close()
    return render_template("login.html", error=error)

@app.route('/api/groups', methods=['POST'])
def create_group_api():
    db = get_db()
    data = request.get_json()
    if not data.get("group_name") or not data.get("admin_id"):
        return jsonify({"error": "group_name and admin_id required"}), 400
    group_id = create_new_group(db, data["group_name"], data["admin_id"])
    return jsonify({"status": "success", "group_id": group_id})

@app.route("/create-group", methods=["GET","POST"])
def create_group():
    if "admin_id" not in session:
        return redirect("/login")
    db = get_db()
    error = None
    if request.method == "POST":
        group_name = request.form.get("group_name")
        if not group_name:
            error = "Group name is required"
        else:
            group_id = create_new_group(db, group_name, session["admin_id"])
            session["group_id"] = group_id
            return redirect("/dashboard")
    return render_template("create_group.html", error=error)

@app.route("/dashboard")
def dashboard():
    if "admin_id" not in session: return redirect("/login")
    if "group_id" not in session: return redirect("/create-group")
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("SELECT name FROM members WHERE id=%s", (session["admin_id"],))
    admin = cursor.fetchone()
    cursor.execute("SELECT * FROM groups WHERE id=%s", (session["group_id"],))
    group = cursor.fetchone()
    cursor.close()
    return render_template("dashboard.html", admin=admin, group=group)

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    auto_insert_loan_penalties(db, group_id)
    profit_data = get_current_group_profit(db, group_id)
    settings = get_group_settings(db, group_id)
    admin_id = get_group_admin_member_id(db, group_id)
    cursor = get_cursor(db)
    cursor.execute("SELECT COUNT(id) FROM members WHERE group_id=%s AND is_system=0", (group_id,))
    total_members = get_single_value(cursor)
    cursor.execute(
        "SELECT SUM(amount) FROM contributions WHERE group_id=%s AND member_id!=%s AND type IN ('hisa anzia','hisa','jamii')",
        (group_id, admin_id)
    )
    total_contributions = get_single_value(cursor, 0)
    cursor.close()
    return jsonify({
        "group_name": settings.get('group_name','Kikoba App'),
        "constitution_path": settings.get('constitution_path', None),
        "interest_rate": settings.get('interest_rate','0.10'),
        "daily_penalty": settings.get('daily_penalty_amount','1000'),
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "jamii_amount": settings.get('jamii_amount','2000'),
        "jamii_frequency": settings.get('jamii_frequency','monthly'),
        "cycle_start_date": settings.get('cycle_start_date',''),
        "cycle_end_date": settings.get('cycle_end_date',''),
        "hisa_unit_price": settings.get('hisa_unit_price','5000'),
        "total_members": total_members,
        "total_contributions_hisa": total_contributions,
        "total_hisa_units": get_total_hisa_units(db, group_id),
        "loan_balance_due": get_total_outstanding_loans(db, group_id),
        "total_principal_loaned": get_total_principal_loaned(db, group_id),
        "total_interests": profit_data["total_interest"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_in_hand": profit_data["net_profit_pool"],
        "penalties_imposed": get_total_penalties_imposed(db, group_id),
        "penalties_paid": get_total_penalties_paid(db, group_id),
        "penalties_due_net": get_total_group_penalty_liability(db, group_id),
        "total_jamii_collected": profit_data["total_jamii_collected"],
        "loan_tier1_amount": settings.get('loan_tier1_amount','500000'),
        "loan_tier1_months": settings.get('loan_tier1_months','1'),
        "loan_tier2_amount": settings.get('loan_tier2_amount','1000000'),
        "loan_tier2_months": settings.get('loan_tier2_months','3'),
        "loan_tier3_amount": settings.get('loan_tier3_amount','2000000'),
        "loan_tier3_months": settings.get('loan_tier3_months','6'),
        "loan_tier4_amount": settings.get('loan_tier4_amount','5000000'),
        "loan_tier4_months": settings.get('loan_tier4_months','9'),
        "loan_tier5_amount": settings.get('loan_tier5_amount','10000000'),
        "loan_tier5_months": settings.get('loan_tier5_months','12'),
    })


# ==================== SETTINGS ====================
@app.route('/api/loan_rules', methods=['GET'])
def get_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id,min_principal,max_principal,days FROM loan_rules WHERE group_id=%s ORDER BY min_principal", (group_id,))
    rules = cursor.fetchall()
    cursor.close()
    return jsonify([dict(r) for r in rules])

@app.route('/api/loan_rules', methods=['POST'])
def save_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    rules = data.get('rules')
    if not rules or not isinstance(rules, list): return jsonify({"error": "Invalid rules data"}), 400
    cursor = get_cursor(db)
    cursor.execute("DELETE FROM loan_rules WHERE group_id=%s", (group_id,))
    for rule in rules:
        try:
            cursor.execute("INSERT INTO loan_rules (group_id,min_principal,max_principal,days) VALUES (%s,%s,%s,%s)",
                           (group_id, float(rule['min_principal']), float(rule['max_principal']), int(rule['days'])))
        except Exception as e:
            db.rollback(); cursor.close()
            return jsonify({"error": str(e)}), 400
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": f"{len(rules)} loan rules saved."})

@app.route('/api/settings', methods=['GET','POST'])
def handle_settings():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    if request.method == 'GET':
        return jsonify(get_group_settings(db, group_id))
    data = request.get_json()
    if not data: return jsonify({"error": "Invalid JSON"}), 400
    keys = ['group_name','interest_rate','daily_penalty_amount','leadership_pay_amount',
            'jamii_amount','jamii_frequency','cycle_start_date','cycle_end_date','hisa_unit_price',
            'loan_tier1_amount','loan_tier1_months','loan_tier2_amount','loan_tier2_months',
            'loan_tier3_amount','loan_tier3_months','loan_tier4_amount','loan_tier4_months',
            'loan_tier5_amount','loan_tier5_months']
    cursor = get_cursor(db)
    try:
        for key in keys:
            value = data.get(key)
            if value is not None and value != "":
                cursor.execute("""
                    INSERT INTO settings (group_id,key,value) VALUES (%s,%s,%s)
                    ON CONFLICT (group_id,key) DO UPDATE SET value=EXCLUDED.value
                """, (group_id, key, str(value)))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Settings updated."})
    except Exception as e:
        db.rollback(); cursor.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/constitution/upload', methods=['POST'])
def upload_constitution():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    if 'constitution_file' not in request.files: return jsonify({"error": "No file"}), 400
    file = request.files['constitution_file']
    if not file or file.filename == '': return jsonify({"error": "No file selected"}), 400
    filename = f"group{group_id}_{int(datetime.now().timestamp())}_{secure_filename(file.filename)}"
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    cursor = get_cursor(db)
    cursor.execute("INSERT INTO settings (group_id,key,value) VALUES (%s,'constitution_path',%s) ON CONFLICT (group_id,key) DO UPDATE SET value=EXCLUDED.value", (group_id, filename))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": "Constitution uploaded.", "path": filename})

@app.route("/constitution/view")
def view_constitution():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return "No group selected", 400
    cursor = get_cursor(db)
    cursor.execute("SELECT value FROM settings WHERE key='constitution_path' AND group_id=%s", (group_id,))
    row = cursor.fetchone(); cursor.close()
    if not row: return "No constitution uploaded", 404
    return send_from_directory(app.config["UPLOAD_FOLDER"], row["value"], as_attachment=False)

@app.route("/constitution/download")
def download_constitution():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return "No group selected", 400
    cursor = get_cursor(db)
    cursor.execute("SELECT value FROM settings WHERE key='constitution_path' AND group_id=%s", (group_id,))
    row = cursor.fetchone(); cursor.close()
    if not row: return "No constitution uploaded", 404
    return send_from_directory(app.config["UPLOAD_FOLDER"], row["value"], as_attachment=True)

@app.route('/api/constitution/status', methods=['GET'])
def constitution_status():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT value FROM settings WHERE key='constitution_path' AND group_id=%s", (group_id,))
    row = cursor.fetchone(); cursor.close()
    if not row or not row['value']: return jsonify({"uploaded": False}), 200
    if not os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], row['value'])): return jsonify({"uploaded": False}), 200
    return jsonify({"uploaded": True, "filename": row['value'], "view_url": "/constitution/view", "download_url": "/constitution/download"})

@app.route('/api/jamii_deduction', methods=['POST'])
def record_jamii_deduction():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    amount = float(data.get("amount", 0))
    if amount <= 0: return jsonify({"error": "Amount must be positive"}), 400
    admin_id = get_group_admin_member_id(db, group_id)
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM members WHERE id=%s AND group_id=%s", (admin_id, group_id))
    if not cursor.fetchone():
        cursor.close()
        return jsonify({"error": "Group admin member not found"}), 400
    cursor.execute("INSERT INTO contributions (group_id,member_id,type,amount,date) VALUES (%s,%s,'jamii_deduction',%s,CURRENT_DATE)", (group_id, admin_id, -amount))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": f"{amount:,.0f} TZS recorded as Jamii deduction."})


# ==================== MEMBERS ====================
@app.route('/members-page')
def members_page():
    return render_template('members.html')

@app.route('/member-details/<int:member_id>')
def member_details_page(member_id):
    if "admin_id" not in session: return redirect("/login")
    if "group_id" not in session: return redirect("/create-group")
    return render_template('member_details.html', member_id=member_id)

@app.route('/api/members/<int:member_id>/details', methods=['GET'])
def get_member_details(member_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    cursor = get_cursor(db)
    cursor.execute("SELECT id,name,phone,joined_date FROM members WHERE id=%s AND group_id=%s AND is_system=0", (member_id, group_id))
    member = cursor.fetchone()
    if not member:
        cursor.close()
        return jsonify({"error": "Member not found"}), 404
    cursor.execute("""SELECT type, SUM(amount) as total FROM contributions
        WHERE member_id=%s AND group_id=%s AND type!='jamii_deduction' GROUP BY type""", (member_id, group_id))
    contribs = cursor.fetchall()
    contrib_dict = {c["type"]: c["total"] for c in contribs}
    total_contributions = sum(contrib_dict.values())
    cursor.execute("""SELECT id,type,amount,date,transaction_date FROM contributions
        WHERE member_id=%s AND group_id=%s AND type!='jamii_deduction' ORDER BY date DESC""", (member_id, group_id))
    contribution_history = cursor.fetchall()
    member_total_savings = contrib_dict.get('hisa anzia',0) + contrib_dict.get('hisa',0) + contrib_dict.get('jamii',0)
    hisa_data = get_member_hisa_units(db, member_id, group_id)
    member_units = hisa_data['units']
    loan_balances = get_member_loan_balances(db, member_id, group_id)
    cursor.execute("""SELECT id,principal,interest,net_amount,months,start_date,due_date,status
        FROM loans WHERE member_id=%s AND group_id=%s ORDER BY start_date DESC""", (member_id, group_id))
    loans = cursor.fetchall()
    loan_details = []
    for loan in loans:
        cursor.execute("SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan['id'], group_id))
        repaid = get_single_value(cursor, 0)
        cursor.execute("SELECT id,amount,date FROM rejesho WHERE loan_id=%s AND group_id=%s ORDER BY date ASC, id ASC", (loan['id'], group_id))
        rejesho_rows = cursor.fetchall()
        monthly_rejesho = round(loan['principal'] / loan['months'], 2) if loan['months'] else 0
        loan_details.append({
            "id": loan['id'], "principal": loan['principal'], "interest": loan['interest'],
            "net_amount": loan['net_amount'], "months": loan['months'],
            "monthly_rejesho": monthly_rejesho, "start_date": loan['start_date'],
            "due_date": loan['due_date'], "status": loan['status'],
            "repaid": repaid, "remaining": max(loan['principal'] - repaid, 0),
            "rejesho_history": [{"id": r["id"], "amount": r["amount"], "date": r["date"]} for r in rejesho_rows]
        })
    total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)
    cursor.execute("""SELECT id,type,amount,COALESCE(amount_paid,0) AS amount_paid,description,date
        FROM penalties WHERE member_id=%s AND group_id=%s ORDER BY date DESC""", (member_id, group_id))
    penalties = cursor.fetchall()
    penalty_details = [{"id":p['id'],"type":p['type'],"amount":p['amount'],
        "amount_paid":p['amount_paid'],"remaining":max(p['amount']-p['amount_paid'],0),
        "description":p['description'],"date":p['date']} for p in penalties]
    profit_data = get_current_group_profit(db, group_id)
    total_units = get_total_hisa_units(db, group_id)
    profit_per_unit = profit_data["net_profit_pool"] / total_units if total_units > 0 else 0
    expected_profit_share = round(member_units * profit_per_unit)
    net_position = member_total_savings - loan_balances["remaining_loans"] - total_penalties_due
    cursor.close()
    return jsonify({
        "member": {"id":member['id'],"name":member['name'],"phone":member['phone'],"joined_date":member['joined_date']},
        "summary": {
            "contributions": contrib_dict, "total_contributions": total_contributions,
            "total_savings": member_total_savings, "hisa_units": round(member_units,2),
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": total_penalties_due,
            "net_contribution_position": net_position,
            "expected_profit_share": expected_profit_share,
            "net_payout": net_position + expected_profit_share,
        },
        "contribution_history": [dict(c) for c in contribution_history],
        "loans": loan_details,
        "penalties": penalty_details,
    })

@app.route('/api/members', methods=['GET'])
def get_members():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT * FROM members WHERE group_id=%s AND is_system=0", (group_id,))
    members = cursor.fetchall()
    cursor.execute("SELECT member_id, SUM(amount) as total FROM contributions WHERE group_id=%s AND type!='jamii_deduction' GROUP BY member_id", (group_id,))
    contributions_map = {r['member_id']: r['total'] for r in cursor.fetchall()}
    cursor.execute("SELECT member_id, SUM(principal) as total FROM loans WHERE group_id=%s GROUP BY member_id", (group_id,))
    loans_map = {r['member_id']: r['total'] for r in cursor.fetchall()}
    cursor.execute("SELECT l.member_id, SUM(r.amount) as total FROM rejesho r JOIN loans l ON r.loan_id=l.id WHERE l.group_id=%s GROUP BY l.member_id", (group_id,))
    rejesho_map = {r['member_id']: r['total'] for r in cursor.fetchall()}
    cursor.execute("SELECT member_id, SUM(amount - COALESCE(amount_paid,0)) as total FROM penalties WHERE group_id=%s GROUP BY member_id", (group_id,))
    penalties_map = {r['member_id']: r['total'] for r in cursor.fetchall()}
    cursor.close()
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    result = []
    for m in members:
        mid = m["id"]
        total_loans = loans_map.get(mid, 0)
        total_rejs = rejesho_map.get(mid, 0)
        total_contribs = contributions_map.get(mid, 0)
        result.append({
            "id": mid, "name": m["name"], "phone": m["phone"],
            "total_contributions": total_contribs,
            "hisa_units": total_contribs / unit_price if unit_price > 0 else 0,
            "total_loans_committed": total_loans,
            "total_penalties": penalties_map.get(mid, 0),
            "total_outstanding": max(total_loans - total_rejs, 0),
            "jamii_paid": 0, "jamii_expected": 0, "jamii_shortfall": 0,
        })
    return jsonify(result)

@app.route('/api/members', methods=['POST'])
def add_member():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    name = data.get("name")
    if not name: return jsonify({"error": "Name is required"}), 400
    cursor = get_cursor(db)
    cursor.execute("INSERT INTO members (group_id,name,phone,joined_date,is_system) VALUES (%s,%s,%s,%s,0)",
                   (group_id, name, data.get("phone"), datetime.now().strftime("%Y-%m-%d")))
    db.commit(); cursor.close()
    return jsonify({"status": "success"})

@app.route('/api/members/<int:member_id>', methods=['PUT','DELETE'])
def edit_member(member_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id,is_system FROM members WHERE id=%s AND group_id=%s", (member_id, group_id))
    member = cursor.fetchone()
    if not member:
        cursor.close(); return jsonify({"error": "Member not found"}), 404
    if member['is_system'] == 1:
        cursor.close(); return jsonify({"error": "Cannot modify system admin"}), 400
    if request.method == 'DELETE':
        cursor.execute("""SELECT
            (SELECT COUNT(*) FROM contributions WHERE member_id=%s AND group_id=%s) +
            (SELECT COUNT(*) FROM loans WHERE member_id=%s AND group_id=%s) +
            (SELECT COUNT(*) FROM penalties WHERE member_id=%s AND group_id=%s) as total""",
            (member_id,group_id,member_id,group_id,member_id,group_id))
        if cursor.fetchone()['total'] > 0:
            cursor.close(); return jsonify({"error": "Cannot delete member with existing records"}), 400
        cursor.execute("DELETE FROM members WHERE id=%s AND group_id=%s", (member_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Member deleted"})
    data = request.get_json()
    name = data.get('name','').strip()
    if not name:
        cursor.close(); return jsonify({"error": "Name is required"}), 400
    cursor.execute("UPDATE members SET name=%s, phone=%s WHERE id=%s AND group_id=%s",
                   (name, data.get('phone','').strip(), member_id, group_id))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": "Member updated"})


# ==================== CONTRIBUTIONS ====================
@app.route('/contributions-page')
def contributions_page():
    return render_template('contributions.html')

@app.route('/api/contributions', methods=['GET'])
def get_contributions():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("""SELECT c.id,c.member_id,c.type,c.amount,c.date,c.transaction_date,m.name as member_name
        FROM contributions c JOIN members m ON c.member_id=m.id WHERE c.group_id=%s ORDER BY c.date DESC""", (group_id,))
    result = [dict(c) for c in cursor.fetchall()]
    cursor.close()
    return jsonify(result)

@app.route('/api/contributions', methods=['POST'])
def add_contribution():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    member_id = data.get("member_id")
    ctype = data.get("type")
    amount = data.get("amount")
    entry_date = data.get("date") or datetime.now().strftime("%Y-%m-%d")
    transaction_date = data.get("transaction_date") or entry_date
    if not member_id or not ctype or not amount:
        return jsonify({"error": "All fields are required"}), 400
    cursor = get_cursor(db)
    if ctype == "rejesho":
        cursor.execute("SELECT * FROM loans WHERE member_id=%s AND group_id=%s AND status!='Cleared' ORDER BY start_date DESC LIMIT 1", (member_id, group_id))
        loan = cursor.fetchone()
        if not loan:
            cursor.close(); return jsonify({"error": "No active loan found for this member"}), 400
        cursor.execute("INSERT INTO rejesho (group_id,loan_id,amount,date) VALUES (%s,%s,%s,%s)", (group_id, loan["id"], amount, transaction_date))
        db.commit(); cursor.close()
        update_loan_status(db, loan["id"], group_id)
        try:
            auto_freeze_settled_month_penalties(db, loan["id"], group_id)
        except Exception as e:
            print(f"[auto_freeze] error: {e}")
    else:
        cursor.execute("INSERT INTO contributions (group_id,member_id,type,amount,date,transaction_date) VALUES (%s,%s,%s,%s,%s,%s)",
                       (group_id, member_id, ctype, amount, entry_date, transaction_date))
        db.commit(); cursor.close()
    return jsonify({"status": "success"})

@app.route('/api/contributions/<int:contribution_id>', methods=['PUT','DELETE'])
def edit_contribution(contribution_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    if request.method == 'DELETE':
        cursor.execute("SELECT type FROM contributions WHERE id=%s AND group_id=%s", (contribution_id, group_id))
        contrib = cursor.fetchone()
        if not contrib:
            cursor.close(); return jsonify({"error": "Not found"}), 404
        if contrib['type'] == 'jamii_deduction':
            cursor.close(); return jsonify({"error": "Cannot delete Jamii deductions here"}), 400
        cursor.execute("DELETE FROM contributions WHERE id=%s AND group_id=%s", (contribution_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success"})
    data = request.get_json()
    amount = float(data.get('amount', 0))
    ctype = data.get('type','').strip()
    date_str = data.get('date','').strip()
    txn_date = data.get('transaction_date','').strip()
    if amount <= 0:
        cursor.close(); return jsonify({"error": "Amount must be positive"}), 400
    if ctype not in ['hisa','hisa anzia','jamii']:
        cursor.close(); return jsonify({"error": "Invalid type"}), 400
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        datetime.strptime(txn_date, "%Y-%m-%d")
        cursor.execute("UPDATE contributions SET amount=%s,type=%s,date=%s,transaction_date=%s WHERE id=%s AND group_id=%s",
                       (amount, ctype, date_str, txn_date, contribution_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success"})
    except ValueError:
        cursor.close(); return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback(); cursor.close(); return jsonify({"error": str(e)}), 500


# ==================== LOANS ====================
@app.route('/loans-page')
def loans_page():
    return render_template('loans.html')

@app.route('/api/loans', methods=['GET'])
def get_loans():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    auto_insert_loan_penalties(db, group_id)
    cursor = get_cursor(db)
    cursor.execute("SELECT l.*, m.name AS member_name FROM loans l JOIN members m ON l.member_id=m.id WHERE l.group_id=%s", (group_id,))
    loans = cursor.fetchall()
    result = []
    for l in loans:
        cursor.execute("SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", (l["id"], group_id))
        repaid = get_single_value(cursor, 0)
        remaining = l["principal"] - repaid
        today = datetime.now().date()
        due_date = datetime.strptime(l["due_date"], "%Y-%m-%d").date()
        status = "Cleared" if remaining <= 0 else ("Overdue" if today > due_date else "Active")
        monthly_rejesho = round(l["principal"] / l["months"], 2) if l["months"] > 0 else 0
        result.append({
            "loan_id": l["id"], "member_name": l["member_name"],
            "principal": l["principal"], "interest": l["interest"],
            "net_amount": l["net_amount"], "months": l["months"],
            "monthly_rejesho": monthly_rejesho, "total": l["principal"],
            "start_date": l["start_date"], "due_date": l["due_date"],
            "amount_returned": repaid, "remaining": remaining, "status": status,
        })
        if l["status"] != status:
            cursor.execute("UPDATE loans SET status=%s WHERE id=%s AND group_id=%s", (status, l["id"], group_id))
            db.commit()
    cursor.close()
    return jsonify(result)

@app.route('/api/loans/preview', methods=['POST'])
def preview_loan():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    try:
        principal = float(data.get("principal", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid principal"}), 400
    if principal <= 0: return jsonify({"error": "Principal must be positive"}), 400
    settings = get_group_settings(db, group_id)
    interest_rate = float(settings.get("interest_rate", 0.10))
    cycle_end_date = settings.get('cycle_end_date', '')
    rules = [
        (float(settings.get(f"loan_tier{i}_amount", d)), int(settings.get(f"loan_tier{i}_months", m)))
        for i, d, m in [(1,'500000','1'),(2,'1000000','3'),(3,'2000000','6'),(4,'5000000','9'),(5,'10000000','12')]
    ]
    months = next((m for max_a, m in rules if principal <= max_a), None)
    if months is None: return jsonify({"error": "Amount exceeds maximum loan tier"}), 400
    warning = None
    original_months = months
    today = datetime.now()
    if cycle_end_date:
        try:
            cycle_end = datetime.strptime(cycle_end_date, "%Y-%m-%d")
            remaining_days = (cycle_end - today).days
            if remaining_days <= 0: return jsonify({"error": "Cycle has ended"}), 400
            max_months = remaining_days // 30
            if months > max_months:
                months = max(1, max_months)
                warning = f"⚠️ Duration adjusted from {original_months} to {months} month(s) to fit cycle end ({cycle_end_date})"
        except ValueError:
            pass
    interest = round(principal * interest_rate)
    due_year = today.year + (today.month + months - 1) // 12
    due_month = (today.month + months - 1) % 12 + 1
    due_day = min(today.day, calendar.monthrange(due_year, due_month)[1])
    due_date = datetime(due_year, due_month, due_day)
    resp = {
        "start_date": today.strftime("%Y-%m-%d"), "months": months,
        "due_date": due_date.strftime("%Y-%m-%d"), "principal": principal,
        "interest": interest, "net_amount": principal - interest,
        "total": principal + interest, "monthly_rejesho": round(principal / months, 2),
        "cycle_end_date": cycle_end_date or None,
    }
    if warning:
        resp["warning"] = warning
        resp["original_months"] = original_months
    return jsonify(resp)

@app.route('/api/loans', methods=['POST'])
def add_loan():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    try:
        member_id = data.get("member_id")
        principal = float(data.get("principal", 0))
        if not member_id or principal <= 0:
            return jsonify({"error": "Invalid member or principal"}), 400
        cursor = get_cursor(db)
        cursor.execute("SELECT id FROM members WHERE id=%s AND group_id=%s", (member_id, group_id))
        if not cursor.fetchone():
            cursor.close(); return jsonify({"error": "Member not found"}), 400
        settings = get_group_settings(db, group_id)
        interest_rate = float(settings.get("interest_rate", 0.10))
        cycle_end_date = settings.get('cycle_end_date', '')
        rules = [
            (float(settings.get(f"loan_tier{i}_amount", d)), int(settings.get(f"loan_tier{i}_months", m)))
            for i, d, m in [(1,'500000','1'),(2,'1500000','3'),(3,'3000000','6'),(4,'5000000','9'),(5,'10000000','12')]
        ]
        months = next((m for max_a, m in rules if principal <= max_a), None)
        if months is None:
            cursor.close(); return jsonify({"error": "Amount exceeds maximum loan tier"}), 400
        warning = None
        original_months = months
        if cycle_end_date:
            try:
                cycle_end = datetime.strptime(cycle_end_date, "%Y-%m-%d")
                today_dt = datetime.now()
                remaining_days = (cycle_end - today_dt).days
                if remaining_days <= 0:
                    cursor.close(); return jsonify({"error": "Cycle has ended"}), 400
                max_months = remaining_days // 30
                if months > max_months:
                    months = max(1, max_months)
                    warning = f"⚠️ Duration adjusted from {original_months} to {months} months to fit cycle end ({cycle_end_date})"
            except ValueError:
                pass
        interest = round(principal * interest_rate)
        total = principal + interest
        net_amount = principal - interest
        start_date = datetime.now()
        due_year = start_date.year + (start_date.month + months - 1) // 12
        due_month = (start_date.month + months - 1) % 12 + 1
        due_day = min(start_date.day, calendar.monthrange(due_year, due_month)[1])
        due_date = datetime(due_year, due_month, due_day)
        cursor.execute("""INSERT INTO loans (group_id,member_id,principal,interest,total,net_amount,start_date,due_date,months,status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Active')""",
            (group_id, member_id, principal, interest, total, net_amount,
             start_date.strftime("%Y-%m-%d"), due_date.strftime("%Y-%m-%d"), months))
        db.commit(); cursor.close()
        resp = {"status":"success","start_date":start_date.strftime("%Y-%m-%d"),"months":months,
                "principal":principal,"interest":interest,"net_amount":net_amount,"total":total,
                "monthly_rejesho":round(principal/months,2),"due_date":due_date.strftime("%Y-%m-%d")}
        if warning:
            resp["warning"] = warning
            resp["original_months"] = original_months
        return jsonify(resp)
    except Exception as e:
        db.rollback()
        print("Add loan error:", e)
        return jsonify({"error": "Failed to add loan"}), 500

@app.route('/api/loans/<int:loan_id>', methods=['PUT','DELETE'])
def edit_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    if not cursor.fetchone():
        cursor.close(); return jsonify({"error": "Loan not found"}), 404
    if request.method == 'DELETE':
        cursor.execute("SELECT COUNT(*) as c FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
        rc = cursor.fetchone()['c']
        cursor.execute("SELECT COUNT(*) as c FROM penalties WHERE loan_id=%s AND group_id=%s", (loan_id, group_id))
        pc = cursor.fetchone()['c']
        if rc > 0 or pc > 0:
            cursor.close()
            return jsonify({"error": f"Cannot delete loan with existing records (Repayments:{rc}, Penalties:{pc})"}), 400
        cursor.execute("DELETE FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Loan deleted"})
    data = request.get_json()
    due_date_str = data.get('due_date','').strip()
    status = data.get('status','').strip()
    if status not in ['Active','Overdue','Cleared','Forgiven']:
        cursor.close(); return jsonify({"error": "Invalid status"}), 400
    try:
        datetime.strptime(due_date_str, "%Y-%m-%d")
        cursor.execute("UPDATE loans SET due_date=%s, status=%s WHERE id=%s AND group_id=%s", (due_date_str, status, loan_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Loan updated"})
    except ValueError:
        cursor.close(); return jsonify({"error": "Invalid date"}), 400
    except Exception as e:
        db.rollback(); cursor.close(); return jsonify({"error": str(e)}), 500

@app.route('/api/loans/<int:loan_id>/forgive', methods=['POST'])
def forgive_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    data = request.get_json()
    reason = (data.get("reason") or "").strip()
    forgiven_by = (data.get("forgiven_by") or "Admin").strip()
    if not reason: return jsonify({"error": "A reason is required"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id,member_id,status FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    loan = cursor.fetchone()
    if not loan:
        cursor.close(); return jsonify({"error": "Loan not found"}), 404
    if loan['status'] == 'Forgiven':
        cursor.close(); return jsonify({"error": "Already forgiven"}), 400
    try:
        cursor.execute("UPDATE loans SET status='Forgiven',forgiven=1,forgiveness_reason=%s,forgiven_by=%s,forgiven_at=%s WHERE id=%s AND group_id=%s",
                       (reason, forgiven_by, datetime.now().strftime("%Y-%m-%d"), loan_id, group_id))
        cursor.execute("DELETE FROM penalties WHERE loan_id=%s AND group_id=%s AND type='monthly_rejesho_late' AND COALESCE(amount_paid,0)=0", (loan_id, group_id))
        cursor.execute("INSERT INTO penalties (group_id,member_id,loan_id,type,amount,description,date) VALUES (%s,%s,%s,'loan_forgiven',0,%s,%s)",
                       (group_id, loan['member_id'], loan_id, f"Loan forgiven — {reason} (by {forgiven_by})", datetime.now().strftime("%Y-%m-%d")))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Loan forgiven and penalties cleared."})
    except Exception as e:
        db.rollback(); cursor.close(); return jsonify({"error": str(e)}), 500

@app.route('/api/loans/<int:loan_id>/unforgive', methods=['POST'])
def unforgive_loan(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id,due_date,status FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    loan = cursor.fetchone()
    if not loan:
        cursor.close(); return jsonify({"error": "Loan not found"}), 404
    if loan['status'] != 'Forgiven':
        cursor.close(); return jsonify({"error": "Loan is not forgiven"}), 400
    try:
        due_date = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        new_status = 'Overdue' if datetime.now().date() > due_date else 'Active'
        cursor.execute("UPDATE loans SET status=%s,forgiven=0,forgiveness_reason=NULL,forgiven_by=NULL,forgiven_at=NULL WHERE id=%s AND group_id=%s",
                       (new_status, loan_id, group_id))
        cursor.execute("DELETE FROM penalties WHERE loan_id=%s AND group_id=%s AND type='loan_forgiven'", (loan_id, group_id))
        db.commit(); cursor.close()
        auto_insert_loan_penalties(db, group_id)
        return jsonify({"status": "success", "message": f"Forgiveness reversed. Status: {new_status}"})
    except Exception as e:
        db.rollback(); cursor.close(); return jsonify({"error": str(e)}), 500

@app.route('/loans-page/download', methods=['GET'])
def download_loans_pdf():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return "No group selected", 400
    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba App")
    cursor = get_cursor(db)
    cursor.execute("SELECT l.*, m.name AS member_name FROM loans l JOIN members m ON l.member_id=m.id WHERE l.group_id=%s ORDER BY l.start_date DESC", (group_id,))
    loans = cursor.fetchall()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20)
    elements = []
    styles = getSampleStyleSheet()
    report_date = datetime.now().strftime("%d %B %Y, %I:%M %p")
    elements.append(Paragraph(f"💰 {group_name} - Loans Report", styles['Title']))
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"<i>Generated on: {report_date}</i>", styles['Normal']))
    elements.append(Spacer(1, 20))
    headers = ["Member","Principal","Interest","Net Disbursed","Months","Monthly Rejesho","Repaid","Remaining","Start Date","Due Date","Status"]
    data = [headers]
    for loan in loans:
        cursor.execute("SELECT SUM(amount) FROM rejesho WHERE loan_id=%s AND group_id=%s", (loan['id'], group_id))
        repaid = get_single_value(cursor, 0)
        remaining = max(loan['principal'] - repaid, 0)
        monthly = round(loan['principal'] / loan['months'], 2) if loan['months'] > 0 else 0
        data.append([loan['member_name'], f"{loan['principal']:,.0f}", f"{loan['interest']:,.0f}",
                     f"{loan['net_amount']:,.0f}", str(loan['months']), f"{monthly:,.0f}",
                     f"{repaid:,.0f}", f"{remaining:,.0f}", loan['start_date'], loan['due_date'], loan['status']])
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#FFC107')),
        ('ALIGN',(1,0),(-1,-1),'RIGHT'),('ALIGN',(0,0),(0,-1),'LEFT'),
        ('FONTSIZE',(0,0),(-1,-1),9),('GRID',(0,0),(-1,-1),0.3,colors.grey),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f8f9fa')]),
    ]))
    elements.append(table)
    elements.append(Spacer(1,20))
    elements.append(Paragraph(f"<font size=8>Generated on {report_date} | {group_name}</font>", styles['Normal']))
    doc.build(elements)
    buffer.seek(0); cursor.close()
    return send_file(buffer, as_attachment=True, download_name=f"{group_name.replace(' ','_')}_Loans_{datetime.now().strftime('%Y-%m-%d')}.pdf", mimetype="application/pdf")


# ==================== REJESHO ====================
@app.route('/repayments-page')
def repayments_page():
    return render_template('repayments.html', loan_id=request.args.get('loan_id'))

@app.route('/api/rejesho', methods=['POST'])
def add_rejesho():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    try:
        loan_id = int(data.get("loan_id"))
        amount = float(data.get("amount"))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid loan ID or amount"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM loans WHERE id=%s AND group_id=%s", (loan_id, group_id))
    if not cursor.fetchone():
        cursor.close(); return jsonify({"error": "Loan not found"}), 404
    cursor.execute("INSERT INTO rejesho (group_id,loan_id,amount,date) VALUES (%s,%s,%s,%s)",
                   (group_id, loan_id, amount, datetime.now().strftime("%Y-%m-%d")))
    db.commit(); cursor.close()
    update_loan_status(db, loan_id, group_id)
    try:
        auto_freeze_settled_month_penalties(db, loan_id, group_id)
    except Exception as e:
        print(f"[auto_freeze] error after rejesho add: {e}")
    return jsonify({"status": "success"})

@app.route('/api/rejesho/<int:loan_id>', methods=['GET'])
def get_rejesho_history(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT l.*,m.name as member_name FROM loans l JOIN members m ON l.member_id=m.id WHERE l.id=%s AND l.group_id=%s", (loan_id, group_id))
    loan_info = cursor.fetchone()
    if not loan_info:
        cursor.close(); return jsonify({"error": "Loan not found"}), 404
    cursor.execute("SELECT id,amount,date FROM rejesho WHERE loan_id=%s AND group_id=%s ORDER BY date DESC", (loan_id, group_id))
    repayments = cursor.fetchall()
    total_repaid = sum(r['amount'] for r in repayments)
    monthly = round(loan_info['principal'] / loan_info['months'], 2) if loan_info['months'] > 0 else 0
    cursor.close()
    return jsonify({
        "loan_info": {"id":loan_info['id'],"member_name":loan_info['member_name'],
            "principal":loan_info['principal'],"interest":loan_info['interest'],
            "net_amount":loan_info['net_amount'],"total":loan_info['principal'],
            "start_date":loan_info['start_date'],"due_date":loan_info['due_date'],
            "months":loan_info['months'],"status":loan_info['status'],
            "total_repaid":total_repaid,"remaining":loan_info['principal']-total_repaid,"monthly_rejesho":monthly},
        "repayments": [dict(r) for r in repayments],
    })

@app.route('/api/rejesho/<int:rejesho_id>', methods=['DELETE'])
def delete_rejesho(rejesho_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id,loan_id FROM rejesho WHERE id=%s AND group_id=%s", (rejesho_id, group_id))
    record = cursor.fetchone()
    if not record:
        cursor.close(); return jsonify({"error": "Not found"}), 404
    loan_id = record['loan_id']
    cursor.execute("DELETE FROM rejesho WHERE id=%s AND group_id=%s", (rejesho_id, group_id))
    db.commit(); cursor.close()
    update_loan_status(db, loan_id, group_id)
    auto_insert_loan_penalties(db, group_id)
    try:
        auto_freeze_settled_month_penalties(db, loan_id, group_id)
    except Exception as e:
        print(f"[auto_freeze] error after rejesho delete: {e}")
    return jsonify({"status": "success", "message": "Rejesho deleted"})


# ==================== PENALTIES ====================
@app.route('/penalties-page')
def penalties_page():
    return render_template('penalties.html')

@app.route('/api/penalties', methods=['GET'])
def get_penalties():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    cursor = get_cursor(db)
    cursor.execute("""SELECT p.id,p.member_id,m.name AS member_name,p.amount,p.type,
        COALESCE(p.amount_paid,0) AS amount_paid,COALESCE(p.forgiven_amount,0) AS forgiven_amount,
        COALESCE(p.is_frozen,0) AS is_frozen,p.freeze_reason,p.description,p.date
        FROM penalties p JOIN members m ON p.member_id=m.id WHERE p.group_id=%s ORDER BY p.date DESC,p.id DESC""", (group_id,))
    ledger = cursor.fetchall()
    cursor.execute("SELECT SUM(GREATEST(0,amount-COALESCE(amount_paid,0)-COALESCE(forgiven_amount,0))) FROM penalties WHERE group_id=%s", (group_id,))
    total_due = get_single_value(cursor, 0)
    cursor.close()
    return jsonify({"total_outstanding": total_due, "ledger": [dict(p) for p in ledger]})

@app.route('/api/penalties', methods=['POST'])
def add_penalty():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    data = request.get_json()
    member_id = data.get("member_id")
    ptype = data.get("type")
    amount = float(data.get("amount", 0))
    description = data.get("description", "")
    if not member_id or not ptype or amount <= 0:
        return jsonify({"error": "Member, type, and amount required"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM members WHERE id=%s AND group_id=%s", (member_id, group_id))
    if not cursor.fetchone():
        cursor.close(); return jsonify({"error": "Member not found"}), 404
    cursor.execute("INSERT INTO penalties (group_id,member_id,type,amount,description,date) VALUES (%s,%s,%s,%s,%s,%s)",
                   (group_id, member_id, ptype, amount, description, datetime.now().strftime("%Y-%m-%d")))
    db.commit(); cursor.close()
    return jsonify({"status": "success"})

@app.route('/api/record_penalty_payment/<int:penalty_id>', methods=['POST'])
def record_penalty_payment(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    data = request.get_json()
    amount_to_pay = float(data.get('amount', 0))
    if amount_to_pay <= 0: return jsonify({"error": "Valid amount required"}), 400
    cursor = get_cursor(db)
    try:
        cursor.execute("SELECT amount,COALESCE(amount_paid,0) AS amount_paid,member_id FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
        penalty = cursor.fetchone()
        if not penalty:
            cursor.close(); return jsonify({"error": "Penalty not found"}), 404
        remaining = penalty['amount'] - penalty['amount_paid']
        if remaining <= 0:
            cursor.close(); return jsonify({"error": "Already fully paid"}), 400
        applied = min(amount_to_pay, remaining)
        cursor.execute("UPDATE penalties SET amount_paid=%s WHERE id=%s AND group_id=%s", (penalty['amount_paid'] + applied, penalty_id, group_id))
        cursor.execute("INSERT INTO contributions (group_id,member_id,type,amount,date) VALUES (%s,%s,'penalty_payment',%s,CURRENT_DATE)",
                       (group_id, penalty['member_id'], applied))
        db.commit(); cursor.close()
        return jsonify({"message": f"Recorded {applied:,.0f} TZS. Remaining: {remaining-applied:,.0f} TZS"})
    except Exception as e:
        db.rollback(); cursor.close(); return jsonify({"error": str(e)}), 500

@app.route('/api/penalties/<int:penalty_id>/freeze', methods=['POST'])
def freeze_penalty(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    data = request.get_json()
    freeze = bool(data.get("freeze", True))
    reason = (data.get("reason") or "").strip()
    if freeze and not reason: return jsonify({"error": "Reason required when freezing"}), 400
    cursor = get_cursor(db)
    cursor.execute("SELECT id FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
    if not cursor.fetchone():
        cursor.close(); return jsonify({"error": "Penalty not found"}), 404
    cursor.execute("UPDATE penalties SET is_frozen=%s, freeze_reason=%s WHERE id=%s AND group_id=%s",
                   (1 if freeze else 0, reason if freeze else None, penalty_id, group_id))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": f"Penalty {'frozen' if freeze else 'unfrozen'}."})

@app.route('/api/penalties/<int:penalty_id>/forgive-amount', methods=['POST'])
def forgive_penalty_amount(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    _ensure_penalty_columns(db)
    data = request.get_json()
    try:
        forgiven_amount = float(data.get("forgiven_amount", 0))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid amount"}), 400
    if forgiven_amount < 0: return jsonify({"error": "Amount cannot be negative"}), 400
    reason = (data.get("reason") or "").strip()
    cursor = get_cursor(db)
    cursor.execute("SELECT id,amount,COALESCE(amount_paid,0) AS amount_paid FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
    penalty = cursor.fetchone()
    if not penalty:
        cursor.close(); return jsonify({"error": "Penalty not found"}), 404
    max_forgivable = penalty['amount'] - penalty['amount_paid']
    if forgiven_amount > max_forgivable:
        cursor.close(); return jsonify({"error": f"Cannot forgive more than remaining due ({max_forgivable:,.0f} TZS)"}), 400
    note = f" | Forgiven {forgiven_amount:,.0f} TZS" + (f": {reason}" if reason else "")
    cursor.execute("UPDATE penalties SET forgiven_amount=%s, description=COALESCE(description,'')||%s WHERE id=%s AND group_id=%s",
                   (forgiven_amount, note, penalty_id, group_id))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": f"Forgiven {forgiven_amount:,.0f} TZS from Penalty #{penalty_id}."})

@app.route('/api/penalties/<int:penalty_id>', methods=['PUT','DELETE'])
def edit_penalty(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    cursor = get_cursor(db)
    if request.method == 'DELETE':
        cursor.execute("SELECT type,loan_id FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
        penalty = cursor.fetchone()
        if not penalty:
            cursor.close(); return jsonify({"error": "Penalty not found"}), 404
        if penalty['type'] == 'loan_late' and penalty['loan_id']:
            cursor.close(); return jsonify({"error": "Cannot delete auto-generated loan penalties"}), 400
        cursor.execute("DELETE FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
        db.commit(); cursor.close()
        return jsonify({"status": "success", "message": "Penalty deleted"})
    data = request.get_json()
    amount = float(data.get('amount', 0))
    description = data.get('description', '').strip()
    if amount <= 0:
        cursor.close(); return jsonify({"error": "Amount must be positive"}), 400
    cursor.execute("SELECT amount_paid FROM penalties WHERE id=%s AND group_id=%s", (penalty_id, group_id))
    current = cursor.fetchone()
    if not current:
        cursor.close(); return jsonify({"error": "Penalty not found"}), 404
    if amount < (current['amount_paid'] or 0):
        cursor.close(); return jsonify({"error": f"Amount cannot be less than already paid: {current['amount_paid']:,.0f} TZS"}), 400
    cursor.execute("UPDATE penalties SET amount=%s, description=%s WHERE id=%s AND group_id=%s", (amount, description, penalty_id, group_id))
    db.commit(); cursor.close()
    return jsonify({"status": "success", "message": "Penalty updated"})

@app.route('/penalties-page/download', methods=['GET'])
def download_penalties_pdf():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return "No group selected", 400
    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name", "Kikoba App")
    cursor = get_cursor(db)
    cursor.execute("SELECT p.*,m.name AS member_name FROM penalties p JOIN members m ON p.member_id=m.id WHERE p.group_id=%s ORDER BY p.date DESC", (group_id,))
    penalties = cursor.fetchall()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20)
    elements = []
    styles = getSampleStyleSheet()
    report_date = datetime.now().strftime("%d %B %Y, %I:%M %p")
    elements.append(Paragraph(f"🚨 {group_name} - Penalties Report", styles['Title']))
    elements.append(Spacer(1,12))
    elements.append(Paragraph(f"<i>Generated on: {report_date}</i>", styles['Normal']))
    elements.append(Spacer(1,20))
    data = [["Member","Type","Amount","Paid","Remaining","Description","Date"]]
    for p in penalties:
        remaining = max(p['amount'] - p.get('amount_paid',0), 0)
        data.append([p['member_name'],
                     "Auto Loan" if p['type']=="monthly_rejesho_late" else "Manual",
                     f"{p['amount']:,.0f}", f"{p.get('amount_paid',0):,.0f}",
                     f"{remaining:,.0f}", p.get('description',''), p['date']])
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#FFC107')),
        ('ALIGN',(1,0),(-1,-1),'RIGHT'),('ALIGN',(0,0),(0,-1),'LEFT'),
        ('FONTSIZE',(0,0),(-1,-1),9),('GRID',(0,0),(-1,-1),0.3,colors.grey),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f8f9fa')]),
    ]))
    elements.append(table)
    doc.build(elements)
    buffer.seek(0); cursor.close()
    return send_file(buffer, as_attachment=True, download_name=f"{group_name.replace(' ','_')}_Penalties_{datetime.now().strftime('%Y-%m-%d')}.pdf", mimetype="application/pdf")


# ==================== PROFITS ====================
@app.route('/profits-page')
def profits_page():
    return render_template('profits.html')

@app.route('/api/profits', methods=['POST'])
def calculate_profits():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    profit_data = get_current_group_profit(db, group_id)
    net_profit = profit_data["net_profit_pool"]
    total_units = get_total_hisa_units(db, group_id)
    if total_units == 0:
        return jsonify({"error":"No Hisa units","net_profit_to_distribute":0,"breakdown":[],
                        "leadership_pay_amount":profit_data["leadership_pay_amount"],
                        "gross_distributable_pool":profit_data["gross_distributable_pool"]})
    profit_per_unit = net_profit / total_units
    cursor = get_cursor(db)
    cursor.execute("SELECT id,name FROM members WHERE group_id=%s AND is_system=0", (group_id,))
    members = cursor.fetchall()
    results = []
    for m in members:
        mid = m['id']
        hisa_data = get_member_hisa_units(db, mid, group_id)
        member_units = hisa_data['units']
        cursor.execute("SELECT SUM(amount) FROM contributions WHERE member_id=%s AND group_id=%s AND type IN ('hisa anzia','hisa','jamii')", (mid, group_id))
        total_savings = get_single_value(cursor, 0)
        profit_share = round(member_units * profit_per_unit)
        cursor.execute("SELECT SUM(principal) FROM loans WHERE member_id=%s AND group_id=%s", (mid, group_id))
        total_principal = get_single_value(cursor, 0)
        cursor.execute("SELECT SUM(r.amount) FROM rejesho r JOIN loans l ON r.loan_id=l.id WHERE l.member_id=%s AND l.group_id=%s", (mid, group_id))
        total_repaid = get_single_value(cursor, 0)
        loan_balance = max(total_principal - total_repaid, 0)
        penalties_due = get_total_penalties_due_for_member(mid, db, group_id)
        total_deductions = loan_balance + penalties_due
        results.append({
            "member_name": m["name"], "hisa_units": round(member_units,2),
            "savings": total_savings, "profit_share": profit_share,
            "loan_balance_due": loan_balance, "penalties_due": penalties_due,
            "total_deductions": total_deductions,
            "total_payout": max((total_savings + profit_share) - total_deductions, 0),
        })
    cursor.close()
    return jsonify({
        "total_interest": profit_data["total_interest"],
        "total_penalties": profit_data["total_penalties_imposed"],
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_to_distribute": net_profit,
        "total_hisa_units": round(total_units,2),
        "profit_per_unit": round(profit_per_unit,2),
        "breakdown": results,
    })


# ==================== REPORTS ====================
@app.route('/reports-page')
def reports_page():
    return render_template("reports.html")

@app.route('/api/reports', methods=['GET'])
def get_report_data():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    profit_data = get_current_group_profit(db, group_id)
    total_units = get_total_hisa_units(db, group_id)
    profit_per_unit = profit_data["net_profit_pool"] / total_units if total_units > 0 else 0
    cursor = get_cursor(db)
    cursor.execute("SELECT id,name FROM members WHERE group_id=%s AND is_system=0", (group_id,))
    members = cursor.fetchall()
    report_data = []
    for m in members:
        mid = m["id"]
        cursor.execute("SELECT type,SUM(amount) as total FROM contributions WHERE member_id=%s AND group_id=%s AND type!='jamii_deduction' GROUP BY type", (mid, group_id))
        contribs = cursor.fetchall()
        contrib_dict = {c["type"]: c["total"] for c in contribs}
        total_contributions = sum(contrib_dict.values())
        member_savings = contrib_dict.get('hisa anzia',0) + contrib_dict.get('hisa',0) + contrib_dict.get('jamii',0)
        hisa_data = get_member_hisa_units(db, mid, group_id)
        loan_balances = get_member_loan_balances(db, mid, group_id)
        penalties_due = get_total_penalties_due_for_member(mid, db, group_id)
        net_position = member_savings - loan_balances["remaining_loans"] - penalties_due
        profit_share = round(hisa_data['units'] * profit_per_unit)
        report_data.append({
            "member_name": m["name"], "contributions": contrib_dict,
            "total_contributions": total_contributions, "total_savings": member_savings,
            "hisa_units": round(hisa_data['units'],2),
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": penalties_due,
            "net_contribution_position": net_position,
            "expected_profit_share": profit_share,
            "net_payout": net_position + profit_share,
        })
    cursor.close()
    return jsonify({"report": report_data})

@app.route('/reports-page/download', methods=['GET'])
def download_report_pdf():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return "No group selected", 400
    settings = get_group_settings(db, group_id)
    group_name = settings.get("group_name","Kikoba App")
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20)
    elements = []
    styles = getSampleStyleSheet()
    elements.append(Paragraph(f"📊 {group_name} - Monthly Financial Report", styles['Title']))
    elements.append(Spacer(1,12))
    elements.append(Paragraph(f"<i>Report Period: {datetime.now().strftime('%B %Y')}</i>", styles['Normal']))
    elements.append(Spacer(1,10))
    report_json = get_report_data().get_json()
    report_data = report_json.get("report",[])
    summary_data = [
        [Paragraph("<b>Total Contributions</b>",styles['Normal']), f"{sum(m['total_contributions'] for m in report_data):,.0f} TZS"],
        [Paragraph("<b>Total Loans Outstanding</b>",styles['Normal']), f"{sum(m['remaining_loans'] for m in report_data):,.0f} TZS"],
    ]
    t = Table(summary_data, colWidths=[150,100])
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),colors.whitesmoke),('GRID',(0,0),(-1,-1),0.5,colors.grey),('FONTSIZE',(0,0),(-1,-1),12)]))
    elements.append(t)
    elements.append(Spacer(1,20))
    headers = ["Member","Units","Hisa Anzia","Hisa","Jamii","Total Savings","Loans Taken","Rejesho","Loan Due","Penalties"]
    data = [headers]
    for m in report_data:
        data.append([m['member_name'],f"{m.get('hisa_units',0):.2f}",
                     f"{m['contributions'].get('hisa anzia',0):,.0f}",
                     f"{m['contributions'].get('hisa',0):,.0f}",
                     f"{m['contributions'].get('jamii',0):,.0f}",
                     f"{m['total_savings']:,.0f}",f"{m['total_loans']:,.0f}",
                     f"{m['total_rejesho']:,.0f}",f"{m['remaining_loans']:,.0f}",
                     f"{m['total_penalties']:,.0f}"])
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.green),('TEXTCOLOR',(0,0),(-1,0),colors.white),
                                ('ALIGN',(1,0),(-1,-1),'RIGHT'),('FONTSIZE',(0,0),(-1,-1),10),('GRID',(0,0),(-1,-1),0.3,colors.grey)]))
    elements.append(table)
    elements.append(Spacer(1,6))
    elements.append(Paragraph(f"<font size=8>Generated on: {datetime.now().strftime('%d %b %Y')}</font>",styles['Normal']))
    doc.build(elements)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=f"{group_name.replace(' ','_')}_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf", mimetype="application/pdf")


# ==================== BACKUP ====================
@app.route('/api/backup/export', methods=['GET'])
def export_raw_backup():
    db = get_db()
    group_id = get_current_group_id()
    if not group_id: return jsonify({"error": "No group selected"}), 400
    queries = {
        "members": "SELECT * FROM members WHERE group_id=%s AND is_system=0",
        "contributions": "SELECT c.id,m.name as member_name,c.type,c.amount,c.date FROM contributions c JOIN members m ON c.member_id=m.id WHERE c.group_id=%s",
        "loans": "SELECT l.id,m.name as member_name,l.principal,l.interest,l.total,l.start_date,l.due_date,l.status FROM loans l JOIN members m ON l.member_id=m.id WHERE l.group_id=%s",
        "repayments": "SELECT r.id,m.name as member_name,r.loan_id,r.amount,r.date FROM rejesho r JOIN loans l ON l.id=r.loan_id JOIN members m ON l.member_id=m.id WHERE r.group_id=%s",
        "penalties": "SELECT p.id,m.name as member_name,p.type,p.amount,p.amount_paid,p.date,p.description FROM penalties p JOIN members m ON p.member_id=m.id WHERE p.group_id=%s",
        "settings": "SELECT * FROM settings WHERE group_id=%s",
    }
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zf:
        for file_name, sql in queries.items():
            cursor = get_cursor(db)
            cursor.execute(sql, (group_id,))
            rows = cursor.fetchall()
            col_names = [d[0] for d in cursor.description]
            csv_buf = StringIO()
            writer = csv.writer(csv_buf)
            writer.writerow(col_names)
            for row in rows:
                writer.writerow([row[c] for c in col_names])
            zf.writestr(f"{file_name}.csv", csv_buf.getvalue())
            csv_buf.close(); cursor.close()
        cursor = get_cursor(db)
        cursor.execute("SELECT id,name FROM members WHERE group_id=%s AND is_system=0", (group_id,))
        members = cursor.fetchall()
        bal_buf = StringIO()
        writer = csv.writer(bal_buf)
        writer.writerow(["Member Name","Hisa Units","Hisa (Savings)","Jamii Paid","Loan Balance","Unpaid Penalties"])
        for m in members:
            mid = m['id']
            hisa = get_member_hisa_units(db, mid, group_id)
            jamii = get_member_jamii_balance(db, mid, group_id)
            loan_bal = get_member_loan_balances(db, mid, group_id)['remaining_loans']
            pen_bal = get_total_penalties_due_for_member(mid, db, group_id)
            writer.writerow([m['name'], f"{hisa['units']:.2f}", hisa['total_contributed'], jamii['total_paid'], loan_bal, pen_bal])
        zf.writestr("Group_Balance_Sheet.csv", bal_buf.getvalue())
        bal_buf.close(); cursor.close()
    zip_buffer.seek(0)
    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True,
                     download_name=f"Kikoba_Backup_Group_{group_id}_{datetime.now().strftime('%Y-%m-%d')}.zip")


if __name__ == "__main__":
    from models import init_db
    with app.app_context():
        init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)