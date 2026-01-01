import zipfile
from flask import Flask, redirect, request, jsonify, render_template, send_file, send_from_directory, session
from flask_cors import CORS
from backend.db import get_db, close_db
from backend.models import init_db, calculate_due_date, calculate_penalty 
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


app = Flask(__name__)
CORS(app)

app.secret_key = "supersecretkey"

def get_current_group_id():
    """Extract group_id from session, default to None"""
    return session.get("group_id")

def get_group_admin_member_id(db, group_id):
    row = db.execute(
        """
        SELECT id
        FROM members
        WHERE group_id = ? AND is_system = 1
        """,
        (group_id,)
    ).fetchone()

    if not row:
        raise Exception(f"No system admin member found for group_id={group_id}")

    return row["id"]


UPLOAD_FOLDER = os.path.join(app.root_path, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# ==================== HELPER FUNCTIONS ====================
def create_new_group(db, group_name, admin_id):
    """Create a new group and associate it with an admin"""
    cursor = db.cursor()

    # Insert group with created_at
    cursor.execute("""
        INSERT INTO groups (name, created_at)
        VALUES (?, datetime('now'))
    """, (group_name,))

    group_id = cursor.lastrowid

    # Update admin to belong to this group
    cursor.execute("""
        UPDATE members SET group_id = ? WHERE id = ?
    """, (group_id, admin_id))

    # Create default settings for this group
    defaults = [
        ('group_name', group_name),
        ('interest_rate', '0.10'),
        ('daily_penalty_amount', '1000'),
        ('leadership_pay_amount', '0'),
        ('jamii_amount', '2000'),
        ('jamii_frequency', 'monthly'),
        ('cycle_start_date', ''),
        ('cycle_end_date', ''),
        ('hisa_unit_price', '5000')
    ]

    for key, value in defaults:
        cursor.execute("""
            INSERT INTO settings (group_id, key, value)
            VALUES (?, ?, ?)
        """, (group_id, key, value))

    db.commit()
    return group_id


def get_group_settings(db, group_id):
    settings = db.execute(
        "SELECT key, value FROM settings WHERE group_id = ?", 
        (group_id,)
    ).fetchall()
    
    data = {s["key"]: s["value"] for s in settings}

    defaults = {
        'group_name': 'Kikoba App',
        'interest_rate': '0.10',
        'daily_penalty_amount': '1000',
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
        'loan_tier4_months': '9'
    }
    
    for key, default_value in defaults.items():
        if key not in data:
            data[key] = default_value

    if "constitution_path" in data:
        data["constitution_view_url"] = "/api/constitution/view"
        data["constitution_download_url"] = "/api/constitution/download"

    return data

def calculate_cycle_weeks(start_date_str, end_date_str):
    """Calculate the number of weeks between two dates"""
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
    """Calculate the number of months between two dates"""
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
    """Calculate member's HISA units based on contributions and unit price"""
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    
    # Only count 'hisa' contributions (not hisa anzia)
    total_hisa = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE member_id = ? AND group_id = ? AND type = 'hisa'
        """,
        (member_id, group_id)
    ).fetchone()[0] or 0
    
    units = total_hisa / unit_price if unit_price > 0 else 0
    
    return {
        "total_contributed": total_hisa,
        "units": units,
        "unit_price": unit_price
    }

def get_total_hisa_units(db, group_id):
    """Get total HISA units in the group"""
    settings = get_group_settings(db, group_id)
    unit_price = float(settings.get('hisa_unit_price', 5000))
    admin_id = get_group_admin_member_id(db, group_id)
    
    total_hisa = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE group_id = ? AND type = 'hisa' AND member_id != ?
        """,
        (group_id, admin_id)
    ).fetchone()[0] or 0
    
    units = total_hisa / unit_price if unit_price > 0 else 0
    return units

def get_member_jamii_balance(db, member_id, group_id):
    """Calculate member's Jamii contribution status for a specific group."""
    settings = get_group_settings(db, group_id)
    
    jamii_amount = float(settings.get('jamii_amount', 2000))
    jamii_frequency = settings.get('jamii_frequency', 'monthly')
    cycle_start = settings.get('cycle_start_date', '')
    cycle_end = settings.get('cycle_end_date', '')
    
    # Calculate expected total based on frequency and cycle duration
    if jamii_frequency == 'weekly':
        periods = calculate_cycle_weeks(cycle_start, cycle_end)
    elif jamii_frequency == 'monthly':
        periods = calculate_cycle_months(cycle_start, cycle_end)
    else:  # one-time
        periods = 1
    
    expected_total = jamii_amount * periods
    
    total_paid = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE member_id = ? AND group_id = ? AND type = 'jamii'
        """,
        (member_id, group_id)
    ).fetchone()[0] or 0
    
    shortfall = max(expected_total - total_paid, 0)
    
    return {
        "total_paid": total_paid,
        "expected_total": expected_total,
        "shortfall": shortfall,
        "periods": periods
    }

def get_total_principal_loaned(db, group_id):
    """Calculates the total principal amount disbursed across all loans for a specific group."""
    result = db.execute(
        "SELECT SUM(principal) FROM loans WHERE group_id = ?",
        (group_id,)
    ).fetchone()[0]
    
    return result if result else 0

def get_current_group_profit(db, group_id):
    """Calculates the total profit available for distribution for a specific group."""
    settings = get_group_settings(db, group_id)
    LEADERSHIP_PAY_AMOUNT = float(settings.get('leadership_pay_amount', 0))
    
    admin_id = get_group_admin_member_id(db, group_id)

    # Count only non-system members
    total_members = db.execute(
        "SELECT COUNT(id) FROM members WHERE group_id = ? AND is_system = 0",
        (group_id,)
    ).fetchone()[0] or 0
    
    # Calculate expected jamii using new cycle-based calculation
    jamii_amount = float(settings.get('jamii_amount', 2000))
    jamii_frequency = settings.get('jamii_frequency', 'monthly')
    cycle_start = settings.get('cycle_start_date', '')
    cycle_end = settings.get('cycle_end_date', '')
    
    if jamii_frequency == 'weekly':
        periods = calculate_cycle_weeks(cycle_start, cycle_end)
    elif jamii_frequency == 'monthly':
        periods = calculate_cycle_months(cycle_start, cycle_end)
    else:  # one-time
        periods = 1
    
    expected_jamii_total = jamii_amount * periods * total_members

    total_interest = db.execute(
        "SELECT SUM(total - principal) FROM loans WHERE group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0
    
    total_penalties_imposed = get_total_penalties_imposed(db, group_id)
    total_penalties_revenue = get_total_penalties_paid(db, group_id)

    total_jamii_collected = db.execute(
        "SELECT SUM(amount) FROM contributions WHERE type='jamii' AND group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0

    historical_jamii_spent = db.execute(
        "SELECT SUM(amount) FROM contributions WHERE type='jamii_deduction' AND group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0
    
    unused_jamii = max(0, total_jamii_collected + historical_jamii_spent)
    
    gross_distributable_pool = total_interest + total_penalties_imposed + expected_jamii_total
    
    net_profit_pool = max(
        gross_distributable_pool 
        - LEADERSHIP_PAY_AMOUNT 
        - abs(historical_jamii_spent),
        0
    )
    
    return {
        "total_interest": total_interest,
        "total_penalties_imposed": total_penalties_imposed,
        "total_penalties_revenue": total_penalties_revenue,
        "expected_jamii_total": expected_jamii_total,
        "total_jamii_collected": total_jamii_collected,
        "historical_jamii_spent": abs(historical_jamii_spent),
        "unused_jamii_balance": unused_jamii,
        "leadership_pay_amount": LEADERSHIP_PAY_AMOUNT, 
        "gross_distributable_pool": gross_distributable_pool,
        "net_profit_pool": net_profit_pool
    }

def get_total_savings(db, group_id):
    """Get total member savings (Hisa only, excluding hisa anzia) for a specific group."""
    admin_id = get_group_admin_member_id(db, group_id)
    result = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE group_id = ? AND type = 'hisa' AND member_id != ?
        """,
        (group_id, admin_id)
    ).fetchone()[0]
    
    return result if result else 0

def get_total_outstanding_loans(db, group_id):
    """Calculates actual money owed by members of a specific group."""
    total_liability = db.execute(
        "SELECT SUM(total) FROM loans WHERE group_id = ? AND status != 'Cleared'",
        (group_id,)
    ).fetchone()[0] or 0

    total_repaid = db.execute(
        """
        SELECT SUM(r.amount) FROM rejesho r
        JOIN loans l ON r.loan_id = l.id
        WHERE l.group_id = ? AND l.status != 'Cleared'
        """,
        (group_id,)
    ).fetchone()[0] or 0

    return max(total_liability - total_repaid, 0)

def update_loan_status(db, loan_id, group_id):
    """Updates the loan status in the database based on repayments."""
    loan = db.execute(
        "SELECT * FROM loans WHERE id = ? AND group_id = ?", 
        (loan_id, group_id)
    ).fetchone()
    
    if not loan:
        return
    
    repaid = db.execute(
        "SELECT SUM(amount) FROM rejesho WHERE loan_id = ? AND group_id = ?",
        (loan_id, group_id)
    ).fetchone()[0] or 0
    
    remaining = loan["total"] - repaid
    
    if remaining <= 0:
        new_status = "Cleared"
    elif datetime.now().date() > datetime.strptime(loan["due_date"], "%Y-%m-%d").date():
        new_status = "Overdue"
    else:
        new_status = "Active"
    
    if loan["status"] != new_status:
        db.execute(
            "UPDATE loans SET status = ? WHERE id = ? AND group_id = ?",
            (new_status, loan_id, group_id)
        )
        db.commit()

def auto_insert_loan_penalties(group_id):
    """Finds overdue loans for a specific group and inserts penalties."""
    db = get_db()

    settings = get_group_settings(db, group_id)
    daily_penalty = float(settings.get("daily_penalty_amount", 1000))

    today = datetime.now().date()

    overdue_loans = db.execute("""
        SELECT l.id, l.member_id, l.due_date
        FROM loans l
        WHERE l.group_id = ? 
          AND l.status IN ('Active', 'Overdue')
          AND l.due_date < ?
    """, (group_id, today.strftime("%Y-%m-%d"))).fetchall()

    for loan in overdue_loans:
        due_date = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        days_late = (today - due_date).days

        if days_late <= 0:
            continue

        total_penalty = days_late * daily_penalty

        exists = db.execute("""
            SELECT 1 FROM penalties
            WHERE loan_id = ? AND group_id = ?
              AND type = 'loan_late'
        """, (loan['id'], group_id)).fetchone()

        if exists:
            continue

        db.execute("""
            INSERT INTO penalties (
                group_id,
                member_id,
                loan_id,
                type,
                amount,
                description,
                date
            ) VALUES (?, ?, ?, 'loan_late', ?, ?, ?)
        """, (
            group_id,
            loan['member_id'],
            loan['id'],
            total_penalty,
            f"Loan overdue by {days_late} days",
            today.strftime("%Y-%m-%d")
        ))

        db.execute(
            "UPDATE loans SET status = 'Overdue' WHERE id = ? AND group_id = ?",
            (loan['id'], group_id)
        )

    db.commit()

def get_member_loan_balances(db, member_id, group_id):
    """Calculates loan balances for a member within a specific group."""
    today_date = date.today()
    total_overdue_balance = 0
    total_loans_committed = 0
    total_rejesho = 0
    
    loans_rows = db.execute(
        "SELECT id, total, due_date, status FROM loans WHERE member_id=? AND group_id=?", 
        (member_id, group_id)
    ).fetchall()

    for loan in loans_rows:
        loan_id = loan['id']
        loan_total_committed = loan['total']
        loan_due_date_str = loan['due_date']
        
        try:
            loan_due_date = datetime.strptime(loan_due_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            loan_due_date = today_date + timedelta(days=1)
        
        total_loans_committed += loan_total_committed
        
        repaid_amount = db.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id=? AND group_id=?", 
            (loan_id, group_id)
        ).fetchone()[0] or 0
        
        total_rejesho += repaid_amount
        remaining_balance = max(loan_total_committed - repaid_amount, 0)
        
        if remaining_balance > 0 and loan_due_date < today_date:
            total_overdue_balance += remaining_balance
    
    remaining_loans = max(total_loans_committed - total_rejesho, 0)
    
    return {
        "total_loans_committed": total_loans_committed,
        "total_rejesho": total_rejesho,
        "remaining_loans": remaining_loans,
        "total_overdue": total_overdue_balance
    }

def get_total_penalties_due_for_member(member_id, db, group_id):
    """Returns the NET OUTSTANDING penalties (liability) for a member in a group."""
    row = db.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
        FROM penalties
        WHERE member_id = ? AND group_id = ?
    """, (member_id, group_id)).fetchone()

    return row["total_outstanding"] or 0

def calculate_penalty(loan, group_id):
    """Calculates overdue penalty based on specific group settings."""
    db = get_db()
    
    settings = get_group_settings(db, group_id)
    PENALTY_RATE = float(settings.get('daily_penalty_amount', 1000))

    try:
        due_date = datetime.strptime(loan["due_date"], "%Y-%m-%d")
    except:
        return 0

    today = datetime.now()
    overdue_days = (today - due_date).days

    return max(overdue_days * PENALTY_RATE, 0) if overdue_days > 0 else 0

def get_total_penalties_for_member(member_id, group_id):
    """Same as due_for_member but with auto-db access."""
    db = get_db()
    row = db.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
        FROM penalties
        WHERE member_id = ? AND group_id = ?
    """, (member_id, group_id)).fetchone()
    return row["total_outstanding"] or 0

def get_total_penalties_imposed(db, group_id):
    """Calculates the total gross amount of all penalties ever imposed for a group."""
    row = db.execute(
        "SELECT SUM(amount) AS total_imposed FROM penalties WHERE group_id = ?", 
        (group_id,)
    ).fetchone()
    return row["total_imposed"] or 0

def get_total_penalties_paid(db, group_id):
    """Calculates the total amount of penalties actually PAID in this group."""
    row = db.execute(
        "SELECT SUM(COALESCE(amount_paid, 0)) AS total_paid FROM penalties WHERE group_id = ?",
        (group_id,)
    ).fetchone()
    return row["total_paid"] or 0

def get_total_group_penalty_liability(db, group_id):
    """Calculates the total OUTSTANDING (unpaid) penalties for the entire group."""
    row = db.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_liability
        FROM penalties
        WHERE group_id = ?
    """, (group_id,)).fetchone()
    return row["total_liability"] or 0


# ==================== ROUTES ====================
@app.route("/")
def index():
    if "admin_id" in session:
        if "group_id" in session:
            return redirect("/dashboard")
        else:
            return redirect("/create-group")
    
    db = get_db()
    admin_exists = db.execute("SELECT 1 FROM members WHERE is_system=1").fetchone()
    if admin_exists:
        return redirect("/login")
    else:
        return redirect("/signup")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    db = get_db()
    if request.method == "POST":
        name = request.form.get("name")
        email = request.form.get("email")
        password = request.form.get("password")

        if not name or not email or not password:
            return render_template("signup.html", error="All fields are required")

        existing = db.execute(
            "SELECT * FROM members WHERE email=? AND is_system=1",
            (email,)
        ).fetchone()
        if existing:
            return render_template("signup.html", error="Email already registered")

        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO members (name, email, password, is_system, joined_date)
            VALUES (?, ?, ?, 1, date('now'))
        """, (name, email, generate_password_hash(password)))
        db.commit()
        
        # Get the newly created admin ID
        new_admin = db.execute(
            "SELECT id FROM members WHERE email=? AND is_system=1",
            (email,)
        ).fetchone()
        
        # Log them in automatically
        session["admin_id"] = new_admin["id"]

        return redirect("/create-group")

    return render_template("signup.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    db = get_db()
    error = None

    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        admin = db.execute(
            "SELECT * FROM members WHERE email = ? AND is_system = 1",
            (email,)
        ).fetchone()

        if admin is None:
            error = "Admin account not found"
        elif not check_password_hash(admin["password"], password):
            error = "Wrong password"
        else:
            session.clear()
            session["admin_id"] = admin["id"]

            if admin["group_id"]:
                session["group_id"] = admin["group_id"]
                return redirect("/dashboard")

            return redirect("/create-group")

    return render_template("login.html", error=error)

@app.route("/logout")
def logout():
    """Log out the current user"""
    session.clear()
    return redirect("/login")


@app.route('/api/groups', methods=['POST'])
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
    if "admin_id" not in session:
        return redirect("/login")

    if "group_id" not in session:
        return redirect("/create-group")

    db = get_db()

    admin = db.execute(
        "SELECT name FROM members WHERE id = ?",
        (session["admin_id"],)
    ).fetchone()

    group = db.execute(
        "SELECT * FROM groups WHERE id = ?",
        (session["group_id"],)
    ).fetchone()

    return render_template(
        "dashboard.html",
        admin=admin,
        group=group
    )

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    auto_insert_loan_penalties(group_id)

    profit_data = get_current_group_profit(db, group_id)
    settings = get_group_settings(db, group_id)
    admin_id = get_group_admin_member_id(db, group_id)

    total_members = db.execute(
        "SELECT COUNT(id) FROM members WHERE group_id = ? AND is_system = 0",
        (group_id,)
    ).fetchone()[0]

    members = db.execute(
        "SELECT id FROM members WHERE group_id = ? AND is_system = 0",
        (group_id,)
    ).fetchall()
    
    total_jamii_shortfall = sum(
        get_member_jamii_balance(db, m['id'], group_id)['shortfall']
        for m in members
    )
    
    total_imposed = get_total_penalties_imposed(db, group_id) 
    total_paid = get_total_penalties_paid(db, group_id)
    total_due = get_total_group_penalty_liability(db, group_id)

    total_units = get_total_hisa_units(db, group_id)

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
        "total_contributions_hisa": get_total_savings(db, group_id),
        "total_hisa_units": total_units,
        "loan_balance_due": get_total_outstanding_loans(db, group_id),
        "total_principal_loaned": get_total_principal_loaned(db, group_id),
        "total_interests": profit_data["total_interest"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_in_hand": profit_data["net_profit_pool"],
        "penalties_imposed": total_imposed,
        "penalties_paid": total_paid,
        "penalties_due_net": total_due,
        "expected_jamii_total": profit_data["expected_jamii_total"],
        "total_jamii_collected": profit_data["total_jamii_collected"],
        "jamii_fund_used": profit_data["historical_jamii_spent"],
        "unused_jamii_for_refund": profit_data["unused_jamii_balance"],
        "total_jamii_shortfall": total_jamii_shortfall,
        "loan_tier1_amount": settings.get('loan_tier1_amount', '500000'),
        "loan_tier1_months": settings.get('loan_tier1_months', '1'),
        "loan_tier2_amount": settings.get('loan_tier2_amount', '1000000'),
        "loan_tier2_months": settings.get('loan_tier2_months', '3'),
        "loan_tier3_amount": settings.get('loan_tier3_amount', '2000000'),
        "loan_tier3_months": settings.get('loan_tier3_months', '6'),
        "loan_tier4_amount": settings.get('loan_tier4_amount', '5000000'),
        "loan_tier4_months": settings.get('loan_tier4_months', '9'),
    })

# ==================== CONFIGURATION ROUTES ====================

@app.route('/api/loan_rules', methods=['GET'])
def get_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    rules = db.execute(
        "SELECT id, min_principal, max_principal, days FROM loan_rules WHERE group_id = ? ORDER BY min_principal ASC",
        (group_id,)
    ).fetchall()
    return jsonify([dict(r) for r in rules])

@app.route('/api/loan_rules', methods=['POST'])
def save_loan_rules_api():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    rules = data.get('rules')
    
    if not rules or not isinstance(rules, list):
        return jsonify({"error": "Invalid rules data format"}), 400
    
    db.execute("DELETE FROM loan_rules WHERE group_id = ?", (group_id,))
    
    for rule in rules:
        try:
            min_p = float(rule['min_principal'])
            max_p = float(rule['max_principal'])
            days = int(rule['days'])
            
            db.execute(
                "INSERT INTO loan_rules (group_id, min_principal, max_principal, days) VALUES (?, ?, ?, ?)",
                (group_id, min_p, max_p, days)
            )
        except Exception as e:
            db.rollback()
            return jsonify({"error": f"Invalid rule value provided: {e}"}), 400
            
    db.commit()
    return jsonify({"status": "success", "message": f"{len(rules)} loan rules saved."})

@app.route('/api/settings', methods=['GET', 'POST'])
def handle_settings():
    """Handle both GET (retrieve) and POST (save) for settings"""
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    if request.method == 'GET':
        settings = get_group_settings(db, group_id)
        return jsonify(settings)
    
    # POST - Save settings
    data = request.get_json()
    if not data:
        return jsonify({"error": "Invalid JSON data"}), 400

    updates = [
        ('group_name', data.get('group_name')),
        ('interest_rate', data.get('interest_rate')),
        ('daily_penalty_amount', data.get('daily_penalty_amount')),
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
    ]

    try:
        for key, value in updates:
            if value is not None and value != "":
                db.execute(
                    "INSERT OR REPLACE INTO settings (group_id, key, value) VALUES (?, ?, ?)",
                    (group_id, key, str(value))
                )
        db.commit()
        return jsonify({"status": "success", "message": "General settings updated."})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/constitution/upload', methods=['POST'])
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

    db.execute(
        "INSERT OR REPLACE INTO settings (group_id, key, value) VALUES (?, ?, ?)",
        (group_id, 'constitution_path', filename)
    )
    db.commit()

    return jsonify({
        "status": "success",
        "message": "Constitution uploaded successfully.",
        "path": filename
    })

@app.route("/constitution/view")
def view_constitution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    row = db.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = ?",
        (group_id,)
    ).fetchone()

    if not row:
        return "No constitution uploaded", 404

    return send_from_directory(
        app.config["UPLOAD_FOLDER"],
        row["value"],
        as_attachment=False
    )


@app.route("/constitution/download")
def download_constitution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return "No group selected", 400
    
    row = db.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = ?",
        (group_id,)
    ).fetchone()

    if not row:
        return "No constitution uploaded", 404

    return send_from_directory(
        app.config["UPLOAD_FOLDER"],
        row["value"],
        as_attachment=True
    )

@app.route('/api/constitution/status', methods=['GET'])
def constitution_status():
    """Check if constitution exists for the current group"""
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    row = db.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path' AND group_id = ?",
        (group_id,)
    ).fetchone()
    
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
def record_jamii_deduction():
    """Record a permanent Jamii deduction (group expense)."""
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    amount = float(data.get("amount", 0))
    admin_id = get_group_admin_member_id(db, group_id)
    
    if amount <= 0:
        return jsonify({"error": "Deduction amount must be positive"}), 400

    admin_exists = db.execute(
        "SELECT id FROM members WHERE id = ? AND group_id = ?",
        (admin_id, group_id)
    ).fetchone()
    
    if not admin_exists:
        return jsonify({
            "error": f"Group admin member (ID {admin_id}) does not exist for this group. Cannot record group expense."
        }), 400

    today_str = datetime.now().strftime("%Y-%m-%d")

    db.execute(
        "INSERT INTO contributions (group_id, member_id, type, amount, date) VALUES (?, ?, 'jamii_deduction', ?, ?)",
        (group_id, admin_id, -amount, today_str)
    )
    db.commit()
    
    return jsonify({
        "status": "success",
        "message": f"{amount:,.0f} TZS recorded as Jamii deduction."
    })


# ==================== MEMBERS ====================

@app.route('/members-page')
def members_page():
    return render_template('members.html')

@app.route('/api/members', methods=['GET'])
def get_members():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    members = db.execute(
        "SELECT * FROM members WHERE group_id = ? AND is_system = 0", 
        (group_id,)
    ).fetchall()
    result = []
    
    for m in members:
        member_id = m["id"]
        
        total_contributions = db.execute(
            "SELECT SUM(amount) FROM contributions WHERE member_id=? AND group_id=? AND type != 'jamii_deduction'",
            (member_id, group_id)
        ).fetchone()[0] or 0

        loan_balances = get_member_loan_balances(db, member_id, group_id)
        total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)
        jamii_status = get_member_jamii_balance(db, member_id, group_id)
        hisa_data = get_member_hisa_units(db, member_id, group_id)

        result.append({
            "id": member_id,
            "name": m["name"],
            "phone": m["phone"],
            "total_contributions": total_contributions,
            "hisa_units": hisa_data["units"],
            "total_loans_committed": loan_balances["total_loans_committed"],
            "total_penalties": total_penalties_due,
            "total_outstanding": loan_balances["remaining_loans"],
            "jamii_paid": jamii_status["total_paid"],
            "jamii_expected": jamii_status["expected_total"],
            "jamii_shortfall": jamii_status["shortfall"]
        })
    
    return jsonify(result)

@app.route('/api/members', methods=['POST'])
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
    
    db.execute(
        "INSERT INTO members (group_id, name, phone, joined_date, is_system) VALUES (?, ?, ?, ?, 0)",
        (group_id, name, phone, datetime.now().strftime("%Y-%m-%d"))
    )
    db.commit()
    
    return jsonify({"status": "success"})

@app.route('/api/members/<int:member_id>', methods=['PUT', 'DELETE'])
def edit_member(member_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    member = db.execute(
        "SELECT id, is_system FROM members WHERE id = ? AND group_id = ?",
        (member_id, group_id)
    ).fetchone()
    
    if not member:
        return jsonify({"error": "Member not found in this group"}), 404
    
    if member['is_system'] == 1:
        return jsonify({"error": "Cannot modify system admin account"}), 400
    
    if request.method == 'DELETE':
        has_records = db.execute("""
            SELECT 
                (SELECT COUNT(*) FROM contributions WHERE member_id = ? AND group_id = ?) +
                (SELECT COUNT(*) FROM loans WHERE member_id = ? AND group_id = ?) +
                (SELECT COUNT(*) FROM penalties WHERE member_id = ? AND group_id = ?) as total
        """, (member_id, group_id, member_id, group_id, member_id, group_id)).fetchone()['total']
        
        if has_records > 0:
            return jsonify({
                "error": "Cannot delete member with existing contributions, loans, or penalties"
            }), 400
        
        try:
            db.execute("DELETE FROM members WHERE id = ? AND group_id = ?", (member_id, group_id))
            db.commit()
            return jsonify({"status": "success", "message": "Member deleted"})
        except Exception as e:
            db.rollback()
            return jsonify({"error": str(e)}), 500
    
    # PUT - Update member
    data = request.get_json()
    name = data.get('name', '').strip()
    phone = data.get('phone', '').strip()
    
    if not name:
        return jsonify({"error": "Name is required"}), 400
    
    try:
        db.execute(
            "UPDATE members SET name = ?, phone = ? WHERE id = ? AND group_id = ?",
            (name, phone, member_id, group_id)
        )
        db.commit()
        return jsonify({"status": "success", "message": "Member updated"})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500


# ==================== CONTRIBUTIONS ====================

@app.route('/contributions-page')
def contributions_page():
    return render_template('contributions.html')

@app.route('/api/contributions', methods=['GET'])
def get_contributions():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    contributions = db.execute("""
        SELECT c.id, c.member_id, c.type, c.amount, c.date, m.name as member_name
        FROM contributions c
        JOIN members m ON c.member_id = m.id
        WHERE c.group_id = ?
        ORDER BY c.date DESC
    """, (group_id,)).fetchall()

    result = [dict(c) for c in contributions]
    return jsonify(result)

@app.route('/api/contributions', methods=['POST'])
def add_contribution():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    member_id = data.get("member_id")
    ctype = data.get("type")
    amount = data.get("amount")

    if not member_id or not ctype or not amount:
        return jsonify({"error": "All fields are required"}), 400

    today_str = datetime.now().strftime("%Y-%m-%d")

    if ctype == "rejesho":
        loan = db.execute(
            "SELECT * FROM loans WHERE member_id = ? AND group_id = ? AND status != 'Cleared' ORDER BY start_date DESC LIMIT 1",
            (member_id, group_id)
        ).fetchone()
        
        if not loan:
            return jsonify({"error": "No active loan found for this member"}), 400

        db.execute(
            "INSERT INTO rejesho (group_id, loan_id, amount, date) VALUES (?, ?, ?, ?)",
            (group_id, loan["id"], amount, today_str)
        )
        
        db.commit()
        update_loan_status(db, loan["id"], group_id)
    else:
        db.execute(
            "INSERT INTO contributions (group_id, member_id, type, amount, date) VALUES (?, ?, ?, ?, ?)",
            (group_id, member_id, ctype, amount, today_str)
        )
        db.commit()

    return jsonify({"status": "success"})

@app.route('/api/contributions/<int:contribution_id>', methods=['PUT', 'DELETE'])
def edit_contribution(contribution_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    if request.method == 'DELETE':
        contrib = db.execute(
            "SELECT type FROM contributions WHERE id = ? AND group_id = ?", 
            (contribution_id, group_id)
        ).fetchone()
        
        if not contrib:
            return jsonify({"error": "Contribution not found"}), 404
        
        if contrib['type'] == 'jamii_deduction':
            return jsonify({
                "error": "Cannot delete system-generated Jamii deductions. Use Profits page to manage."
            }), 400
        
        try:
            db.execute("DELETE FROM contributions WHERE id = ? AND group_id = ?", (contribution_id, group_id))
            db.commit()
            return jsonify({"status": "success", "message": "Contribution deleted"})
        except Exception as e:
            db.rollback()
            return jsonify({"error": str(e)}), 500
    
    # PUT - Update contribution
    data = request.get_json()
    amount = float(data.get('amount', 0))
    ctype = data.get('type', '').strip()
    date_str = data.get('date', '').strip()
    
    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400
    
    if ctype not in ['hisa', 'hisa anzia', 'jamii']:
        return jsonify({"error": "Invalid contribution type"}), 400
    
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        
        db.execute(
            "UPDATE contributions SET amount = ?, type = ?, date = ? WHERE id = ? AND group_id = ?",
            (amount, ctype, date_str, contribution_id, group_id)
        )
        db.commit()
        return jsonify({"status": "success", "message": "Contribution updated"})
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500


# ==================== LOANS ====================

@app.route('/loans-page')
def loans_page():
    return render_template('loans.html')

@app.route('/api/loans', methods=['GET'])
def get_loans():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    auto_insert_loan_penalties(group_id)

    loans = db.execute("""
        SELECT l.*, m.name AS member_name 
        FROM loans l
        JOIN members m ON l.member_id = m.id
        WHERE l.group_id = ?
    """, (group_id,)).fetchall()

    result = []

    for l in loans:
        repaid = db.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = ? AND group_id = ?",
            (l["id"], group_id)
        ).fetchone()[0] or 0

        remaining = l["total"] - repaid

        today = datetime.now().date()
        due_date = datetime.strptime(l["due_date"], "%Y-%m-%d").date()

        if remaining <= 0:
            status = "Cleared"
        elif today > due_date:
            status = "Overdue"
        else:
            status = "Active"

        result.append({
            "loan_id": l["id"],
            "member_name": l["member_name"],
            "principal": l["principal"],
            "interest": l["interest"],
            "total": l["total"],
            "start_date": l["start_date"],
            "due_date": l["due_date"],
            "amount_returned": repaid,
            "remaining": remaining,
            "status": status
        })
        
        if l["status"] != status:
             db.execute("UPDATE loans SET status = ? WHERE id = ? AND group_id = ?", (status, l["id"], group_id))
             db.commit()

    return jsonify(result)

@app.route('/api/loans', methods=['POST'])
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

        member = db.execute(
            "SELECT id FROM members WHERE id = ? AND group_id = ?",
            (member_id, group_id)
        ).fetchone()
        
        if not member:
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

        # ================= CAP LOAN DURATION TO CYCLE END =================
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

        start_date = datetime.now()
        due_date = start_date + timedelta(days=30 * months)

        db.execute("""
            INSERT INTO loans (
                group_id,
                member_id,
                principal,
                interest,
                total,
                start_date,
                due_date,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'Active')
        """, (
            group_id,
            member_id,
            principal,
            interest,
            total,
            start_date.strftime("%Y-%m-%d"),
            due_date.strftime("%Y-%m-%d")
        ))

        db.commit()

        response_data = {
            "status": "success",
            "months": months,
            "due_date": due_date.strftime("%Y-%m-%d"),
            "interest": interest,
            "total": total
        }
        
        if warning_message:
            response_data["warning"] = warning_message
            response_data["original_months"] = original_months

        return jsonify(response_data)

    except Exception as e:
        db.rollback()
        print("Add loan error:", e)
        return jsonify({"error": "Failed to add loan"}), 500


@app.route('/api/loans/<int:loan_id>', methods=['PUT'])
def edit_loan(loan_id):
    """SAFE EDIT: Only allows editing due_date and status."""
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    
    loan = db.execute(
        "SELECT id FROM loans WHERE id = ? AND group_id = ?",
        (loan_id, group_id)
    ).fetchone()
    
    if not loan:
        return jsonify({"error": "Loan not found in this group"}), 404
    
    due_date_str = data.get('due_date', '').strip()
    status = data.get('status', '').strip()
    
    if status not in ['Active', 'Overdue', 'Cleared']:
        return jsonify({"error": "Invalid status"}), 400
    
    try:
        datetime.strptime(due_date_str, "%Y-%m-%d")
        
        db.execute(
            "UPDATE loans SET due_date = ?, status = ? WHERE id = ? AND group_id = ?",
            (due_date_str, status, loan_id, group_id)
        )
        db.commit()
        return jsonify({"status": "success", "message": "Loan updated (date/status only)"})
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500


# ==================== REJESHO (REPAYMENTS) ====================

@app.route('/repayments-page')
def repayments_page():
    loan_id = request.args.get('loan_id')
    return render_template('repayments.html', loan_id=loan_id)

@app.route('/api/rejesho', methods=['POST'])
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

    loan = db.execute(
        "SELECT id FROM loans WHERE id = ? AND group_id = ?",
        (loan_id, group_id)
    ).fetchone()
    
    if not loan:
        return jsonify({"error": "Loan not found in this group"}), 404

    today_str = datetime.now().strftime("%Y-%m-%d")

    db.execute(
        "INSERT INTO rejesho (group_id, loan_id, amount, date) VALUES (?, ?, ?, ?)",
        (group_id, loan_id, amount, today_str)
    )
    db.commit()
    
    update_loan_status(db, loan_id, group_id)
    
    return jsonify({"status": "success"})

@app.route('/api/rejesho/<int:loan_id>', methods=['GET'])
def get_rejesho_history(loan_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    loan_info = db.execute(
        "SELECT l.*, m.name as member_name FROM loans l JOIN members m ON l.member_id = m.id WHERE l.id = ? AND l.group_id = ?",
        (loan_id, group_id)
    ).fetchone()

    if not loan_info:
        return jsonify({"error": "Loan not found"}), 404
    
    repayments = db.execute(
        "SELECT amount, date FROM rejesho WHERE loan_id = ? AND group_id = ? ORDER BY date DESC",
        (loan_id, group_id)
    ).fetchall()

    return jsonify({
        "loan_info": dict(loan_info),
        "repayments": [dict(r) for r in repayments]
    })


# ==================== PENALTIES ====================

@app.route('/penalties-page')
def penalties_page():
    return render_template('penalties.html')

@app.route('/api/penalties', methods=['GET'])
def get_penalties():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400

    ledger = db.execute("""
        SELECT 
            p.id,
            p.member_id,
            m.name AS member_name,
            p.amount,
            p.type,
            COALESCE(p.amount_paid, 0) AS amount_paid,
            p.description,
            p.date
        FROM penalties p
        JOIN members m ON p.member_id = m.id
        WHERE p.group_id = ?
        ORDER BY p.date DESC, p.id DESC
    """, (group_id,)).fetchall()

    total_due = db.execute(
        "SELECT SUM(amount - COALESCE(amount_paid, 0)) FROM penalties WHERE group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0
    
    return jsonify({
        "total_outstanding": total_due,
        "ledger": [dict(p) for p in ledger]
    })

@app.route('/api/penalties', methods=['POST'])
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

    member = db.execute(
        "SELECT id FROM members WHERE id = ? AND group_id = ?",
        (member_id, group_id)
    ).fetchone()
    
    if not member:
        return jsonify({"error": "Member not found in this group"}), 404

    try:
        db.execute(
            "INSERT INTO penalties (group_id, member_id, type, amount, description, date) VALUES (?, ?, ?, ?, ?, ?)",
            (group_id, member_id, ptype, amount, description, datetime.now().strftime("%Y-%m-%d"))
        )
        db.commit()
        
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/record_penalty_payment/<int:penalty_id>', methods=['POST'])
def record_penalty_payment(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json()
    amount_to_pay = float(data.get('amount', 0)) 
    
    if amount_to_pay <= 0:
        return jsonify({"error": "A valid payment amount is required."}), 400

    try:
        penalty = db.execute(
            "SELECT amount, COALESCE(amount_paid, 0) AS amount_paid, member_id FROM penalties WHERE id = ? AND group_id = ?",
            (penalty_id, group_id)
        ).fetchone()

        if not penalty:
            return jsonify({"error": "Penalty record not found."}), 404

        remaining_due = penalty['amount'] - penalty['amount_paid']
        
        if remaining_due <= 0:
             return jsonify({"error": "Penalty is already fully paid."}), 400
             
        applied_amount = min(amount_to_pay, remaining_due) 

        new_paid_total = penalty['amount_paid'] + applied_amount
        db.execute(
            "UPDATE penalties SET amount_paid = ? WHERE id = ? AND group_id = ?",
            (new_paid_total, penalty_id, group_id)
        )
        
        db.execute(
            "INSERT INTO contributions (group_id, member_id, type, amount, date) VALUES (?, ?, 'penalty_payment', ?, datetime('now'))",
            (group_id, penalty['member_id'], applied_amount)
        )

        db.commit()
        
        remaining_after_payment = remaining_due - applied_amount
        message = f"Successfully recorded {applied_amount:,.0f} TZS payment for Penalty #{penalty_id}. Remaining: {remaining_after_payment:,.0f} TZS"
        return jsonify({"message": message})

    except Exception as e:
        db.rollback()
        print(f"Error recording penalty payment: {e}")
        return jsonify({"error": "Database error while recording payment."}), 500

@app.route('/api/penalties/<int:penalty_id>', methods=['PUT', 'DELETE'])
def edit_penalty(penalty_id):
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    if request.method == 'DELETE':
        penalty = db.execute(
            "SELECT type, loan_id FROM penalties WHERE id = ? AND group_id = ?", 
            (penalty_id, group_id)
        ).fetchone()
        
        if not penalty:
            return jsonify({"error": "Penalty not found"}), 404
        
        if penalty['type'] == 'loan_late' and penalty['loan_id']:
            return jsonify({
                "error": "Cannot delete auto-generated loan penalties. Clear the loan instead."
            }), 400
        
        try:
            db.execute("DELETE FROM penalties WHERE id = ? AND group_id = ?", (penalty_id, group_id))
            db.commit()
            return jsonify({"status": "success", "message": "Penalty deleted"})
        except Exception as e:
            db.rollback()
            return jsonify({"error": str(e)}), 500
    
    # PUT - Update penalty
    data = request.get_json()
    amount = float(data.get('amount', 0))
    description = data.get('description', '').strip()
    
    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400
    
    try:
        current = db.execute(
            "SELECT amount_paid FROM penalties WHERE id = ? AND group_id = ?", 
            (penalty_id, group_id)
        ).fetchone()
        
        if not current:
            return jsonify({"error": "Penalty not found"}), 404
        
        amount_paid = current['amount_paid'] or 0
        
        if amount < amount_paid:
            return jsonify({
                "error": f"Amount cannot be less than already paid: {amount_paid:,.0f} TZS"
            }), 400
        
        db.execute(
            "UPDATE penalties SET amount = ?, description = ? WHERE id = ? AND group_id = ?",
            (amount, description, penalty_id, group_id)
        )
        db.commit()
        return jsonify({"status": "success", "message": "Penalty updated"})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500


# ==================== PROFITS ====================

@app.route('/profits-page')
def profits_page():
    return render_template('profits.html')

@app.route('/api/profits', methods=['POST'])
def calculate_profits():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    data = request.get_json() or {}
    
    temp_jamii_used = float(data.get("jamii_used", 0))

    profit_data = get_current_group_profit(db, group_id)
    settings = get_group_settings(db, group_id)

    total_jamii_expense = profit_data["historical_jamii_spent"] + temp_jamii_used
    
    net_profit = max(
        profit_data["gross_distributable_pool"]
        - profit_data["leadership_pay_amount"]
        - total_jamii_expense,
        0
    )

    # ========== HISA UNIT SYSTEM ==========
    total_units = get_total_hisa_units(db, group_id)
    
    if total_units == 0:
        return jsonify({
            "error": "No Hisa units available for profit distribution",
            "net_profit_to_distribute": 0,
            "breakdown": [],
            "leadership_pay_amount": profit_data["leadership_pay_amount"], 
            "gross_distributable_pool": profit_data["gross_distributable_pool"],
            "historical_jamii_spent": profit_data["historical_jamii_spent"],
        })
    
    profit_per_unit = net_profit / total_units
    
    admin_id = get_group_admin_member_id(db, group_id)
    members = db.execute(
        "SELECT id, name FROM members WHERE group_id = ? AND is_system = 0", 
        (group_id,)
    ).fetchall()
    
    results = []

    for m in members:
        member_id = m['id']
        
        hisa_data = get_member_hisa_units(db, member_id, group_id)
        member_units = hisa_data['units']
        total_savings_member = hisa_data['total_contributed']
        
        profit_share = round(member_units * profit_per_unit)

        loan_balances = get_member_loan_balances(db, member_id, group_id)
        remaining_loan_balance = loan_balances["remaining_loans"]

        total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)

        jamii_status = get_member_jamii_balance(db, member_id, group_id)
        jamii_shortfall = jamii_status['shortfall']

        total_deductions = remaining_loan_balance + total_penalties_due + jamii_shortfall
        
        final_payout = max((total_savings_member + profit_share) - total_deductions, 0)

        results.append({
            "member_name": m["name"],
            "hisa_units": round(member_units, 2),
            "savings": total_savings_member,
            "profit_share": profit_share,
            "loan_balance_due": remaining_loan_balance,
            "penalties_due": total_penalties_due,
            "jamii_shortfall": jamii_shortfall,
            "total_deductions": total_deductions,
            "total_payout": final_payout
        })

    return jsonify({
        "total_interest": profit_data["total_interest"],
        "total_penalties": profit_data["total_penalties_imposed"],
        "expected_jamii_total": profit_data["expected_jamii_total"],
        
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        
        "historical_jamii_spent": profit_data["historical_jamii_spent"],
        "jamii_deducted_current_proposal": temp_jamii_used,
        "total_jamii_expenses": total_jamii_expense,
        
        "net_profit_to_distribute": net_profit,
        "total_hisa_units": round(total_units, 2),
        "profit_per_unit": round(profit_per_unit, 2),
        "breakdown": results
    })

# ==================== REPORTS ====================

@app.route('/reports-page')
def reports_page():
    return render_template("reports.html")

@app.route('/api/reports', methods=['GET'])
def get_report_data():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    profit_data = get_current_group_profit(db, group_id)
    total_profit = profit_data["net_profit_pool"]
    
    total_units = get_total_hisa_units(db, group_id)
    profit_per_unit = total_profit / total_units if total_units > 0 else 0
    
    admin_id = get_group_admin_member_id(db, group_id)

    members = db.execute(
        "SELECT id, name FROM members WHERE group_id = ? AND is_system = 0", 
        (group_id,)
    ).fetchall()
    
    report_data = []

    for m in members:
        member_id = m["id"]
        
        contribs = db.execute(
            """SELECT type, SUM(amount) as total FROM contributions 
               WHERE member_id=? AND group_id=? AND type != 'jamii_deduction' 
               GROUP BY type""",
            (member_id, group_id)
        ).fetchall()
        
        contrib_dict = {c["type"]: c["total"] for c in contribs}
        total_contributions = sum(contrib_dict.values())
        
        member_total_savings = (contrib_dict.get('hisa anzia', 0) + contrib_dict.get('hisa', 0))
        
        hisa_data = get_member_hisa_units(db, member_id, group_id)
        member_units = hisa_data['units']

        loan_balances = get_member_loan_balances(db, member_id, group_id)
        total_penalties_due = get_total_penalties_due_for_member(member_id, db, group_id)
        jamii_status = get_member_jamii_balance(db, member_id, group_id)
        
        net_contribution_position = (
            member_total_savings 
            - loan_balances["remaining_loans"] 
            - total_penalties_due 
            - jamii_status["shortfall"]
        )
        
        expected_profit_share = round(member_units * profit_per_unit)
        net_payout = net_contribution_position + expected_profit_share

        report_data.append({
            "member_name": m["name"],
            "contributions": contrib_dict,
            "total_contributions": total_contributions,
            "hisa_units": round(member_units, 2),
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": total_penalties_due,
            "jamii_paid": jamii_status["total_paid"],
            "jamii_expected": jamii_status["expected_total"],
            "jamii_shortfall": jamii_status["shortfall"],
            "net_contribution_position": net_contribution_position,
            "expected_profit_share": expected_profit_share,
            "net_payout": net_payout,
        })

    return jsonify({"report": report_data})


@app.route('/reports-page/download', methods=['GET'])
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
    #total_profit_share = sum(m['expected_profit_share'] for m in report_data)

    summary_data = [
        [Paragraph("<b>Total Contributions</b>", styles['Normal']), f"{total_contributions:,.0f} TZS"],
        [Paragraph("<b>Total Loans Outstanding</b>", styles['Normal']), f"{total_loans_due:,.0f} TZS"],
        #[Paragraph("<b>Distributable Profit</b>", styles['Normal']), f"{total_profit_share:,.0f} TZS"]
    ]
    summary_table = Table(summary_data, colWidths=[150, 100])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.whitesmoke),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey)
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 20))

    headers = [
        "Member", "Units", "Hisa Anzia", "Hisa", "Jamii", "Total Contrib",
        "Loans Taken", "Rejesho", "Loan Due", "Penalties"] #"Profits Share", "Net Payout"]
    data = [headers]

    for m in report_data:
        data.append([
            m['member_name'],
            f"{m.get('hisa_units', 0):.2f}",
            f"{m['contributions'].get('hisa anzia',0):,.0f}",
            f"{m['contributions'].get('hisa',0):,.0f}",
            f"{m['contributions'].get('jamii',0):,.0f}",
            f"{m['total_contributions']:,.0f}",
            f"{m['total_loans']:,.0f}",
            f"{m['total_rejesho']:,.0f}",
            f"{m['remaining_loans']:,.0f}",
            f"{m['total_penalties']:,.0f}",
            #f"{m['expected_profit_share']:,.0f}",
            #f"{m['net_payout']:,.0f}"
        ])

    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.green),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('FONTSIZE', (0,0), (-1,-1), 7),
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
def export_raw_backup():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    queries = {
        "members": "SELECT * FROM members WHERE group_id = ? AND is_system = 0",
        "contributions": """
            SELECT c.id, m.name as member_name, c.type, c.amount, c.date 
            FROM contributions c 
            JOIN members m ON c.member_id = m.id
            WHERE c.group_id = ?
        """,
        "loans": """
            SELECT l.id, m.name as member_name, l.principal, l.interest, l.total, l.start_date, l.due_date, l.status 
            FROM loans l 
            JOIN members m ON l.member_id = m.id
            WHERE l.group_id = ?
        """,
        "repayments": """
            SELECT r.id, m.name as member_name, r.loan_id, r.amount, r.date 
            FROM rejesho r 
            JOIN members m ON (SELECT member_id FROM loans WHERE id = r.loan_id) = m.id
            WHERE r.group_id = ?
        """,
        "penalties": """
            SELECT p.id, m.name as member_name, p.type, p.amount, p.amount_paid, p.date, p.description 
            FROM penalties p 
            JOIN members m ON p.member_id = m.id
            WHERE p.group_id = ?
        """,
        "settings": "SELECT * FROM settings WHERE group_id = ?"
    }

    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, sql in queries.items():
            cursor = db.execute(sql, (group_id,))
            rows = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(column_names)

            for row in rows:
                data = list(row)
                writer.writerow(data)
            
            zip_file.writestr(f"{file_name}.csv", csv_buffer.getvalue())
            csv_buffer.close()

        admin_id = get_group_admin_member_id(db, group_id)
        members = db.execute(
            "SELECT id, name FROM members WHERE group_id = ? AND is_system = 0", 
            (group_id,)
        ).fetchall()
        
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

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f"Kikoba_Backup_Group_{group_id}_{datetime.now().strftime('%Y-%m-%d')}.zip"
    )

if __name__ == "__main__":
    from backend.models import init_db
    with app.app_context():
        init_db()
    app.run(debug=True)