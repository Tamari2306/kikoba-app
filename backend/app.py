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

app = Flask(__name__)
app.config.from_object(Config)

# Load configuration
env = os.environ.get('FLASK_ENV', 'development')

# Initialize CORS
CORS(app, origins=app.config['CORS_ORIGINS'])

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


def get_group_admin_member_id(db, group_id):
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT id
        FROM members
        WHERE group_id = %s AND is_system = 1
        """,
        (group_id,)
    )
    row = cursor.fetchone()
    cursor.close()

    if not row:
        raise Exception(f"No system admin member found for group_id={group_id}")

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
    admin_id = get_group_admin_member_id(db, group_id)
    
    cursor = get_cursor(db)
    cursor.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE group_id = %s AND type IN ('hisa') AND member_id != %s
        """,
        (group_id, admin_id)
    )
    total_hisa = get_single_value(cursor, 0)
    cursor.close()
    
    units = total_hisa / unit_price if unit_price > 0 else 0
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
        "SELECT COUNT(id) FROM members WHERE group_id = %s AND is_system = 0",
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
    NEW PENALTY SYSTEM: Finds overdue MONTHLY REJESHO payments and charges 1000/day per late payment.
    Penalties start the day AFTER the due date (same day next month).
    """

    settings = get_group_settings(db, group_id)
    daily_penalty = float(settings.get("daily_penalty_amount", 1000))

    today = datetime.now().date()

    cursor = get_cursor(db)

    cursor.execute("""
        SELECT l.id, l.member_id, l.principal, l.months, l.start_date, l.due_date
        FROM loans l
        WHERE l.group_id = %s 
          AND l.status IN ('Active', 'Overdue')
    """, (group_id,))
    active_loans = cursor.fetchall()

    for loan in active_loans:
        loan_id = loan['id']
        member_id = loan['member_id']
        monthly_rejesho = loan['principal'] / loan['months']
        final_due_date = datetime.strptime(loan['due_date'], "%Y-%m-%d").date()
        
        base_day = final_due_date.day
        
        base_month = final_due_date.month - loan['months']
        base_year = final_due_date.year
        if base_month <= 0:
            base_month += 12
            base_year -= 1
        
        for month_num in range(1, loan['months'] + 1):
            due_month = base_month + month_num
            due_year = base_year
            if due_month > 12:
                due_month -= 12
                due_year += 1
            
            max_day_in_due_month = calendar.monthrange(due_year, due_month)[1]
            due_day = min(base_day, max_day_in_due_month)
            
            month_due_date = datetime(due_year, due_month, due_day).date()
            
            if today <= month_due_date:
                continue
            
            cursor.execute("""
                SELECT SUM(amount) FROM rejesho 
                WHERE loan_id = %s AND group_id = %s
            """, (loan_id, group_id))
            total_paid = get_single_value(cursor, 0)
            
            expected_by_this_month = monthly_rejesho * month_num
            
            if total_paid < expected_by_this_month:
                days_late = (today - month_due_date).days
                
                if days_late <= 0:
                    continue
                
                penalty_amount = days_late * daily_penalty
                
                cursor.execute("""
                    SELECT id, amount FROM penalties
                    WHERE loan_id = %s AND group_id = %s 
                      AND type = 'monthly_rejesho_late'
                      AND description LIKE %s
                """, (loan_id, group_id, f"%Month {month_num}%"))
                exists = cursor.fetchone()
                
                if exists:
                    cursor.execute("""
                        UPDATE penalties 
                        SET amount = %s, description = %s, date = %s
                        WHERE id = %s
                    """, (penalty_amount, f"Month {month_num} rejesho overdue by {days_late} days", today.strftime("%Y-%m-%d"), exists['id']))
                else:
                    cursor.execute("""
                        INSERT INTO penalties (
                            group_id, member_id, loan_id, type, amount,
                            description, date
                        ) VALUES (%s, %s, %s, 'monthly_rejesho_late', %s, %s, %s)
                    """, (
                        group_id, member_id, loan_id, penalty_amount,
                        f"Month {month_num} rejesho overdue by {days_late} days",
                        today.strftime("%Y-%m-%d")
                    ))
        
        cursor.execute("""
            SELECT SUM(amount) FROM rejesho WHERE loan_id = %s AND group_id = %s
        """, (loan_id, group_id))
        total_paid_overall = get_single_value(cursor, 0)
        
        remaining = loan['principal'] - total_paid_overall
        
        if remaining <= 0:
            cursor.execute("UPDATE loans SET status = 'Cleared' WHERE id = %s", (loan_id,))
        elif today > datetime.strptime(loan['due_date'], "%Y-%m-%d").date():
            cursor.execute("UPDATE loans SET status = 'Overdue' WHERE id = %s", (loan_id,))

    db.commit()
    cursor.close()


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
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
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
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
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
    cursor.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_liability
        FROM penalties
        WHERE group_id = %s
    """, (group_id,))
    result = get_single_value(cursor, 0)
    cursor.close()
    return result

# ==================== ROUTES ====================
@app.route("/")
def index():
    if "admin_id" in session:
        if "group_id" in session:
            return redirect("/login")
        else:
            return redirect("/create-group")
    
    db = get_db()
    cursor = get_cursor(db)
    cursor.execute("SELECT 1 FROM members WHERE is_system=1 LIMIT 1")
    admin_exists = cursor.fetchone()
    cursor.close()
    
    if admin_exists:
        return redirect("/login")
    else:
        return redirect("/signup")


@app.route("/signup", methods=["GET", "POST"])
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

        cursor.execute(
            "SELECT * FROM members WHERE email=%s AND is_system=1",
            (email,)
        )
        existing = cursor.fetchone()
        
        if existing:
            cursor.close()
            return render_template("signup.html", error="Email already registered")

        cursor.execute("""
            INSERT INTO members (name, email, password, is_system, joined_date)
            VALUES (%s, %s, %s, 1, CURRENT_DATE)
            RETURNING id
        """, (name, email, generate_password_hash(password)))
        
        new_admin_id = cursor.fetchone()["id"]
        db.commit()
        
        session["admin_id"] = new_admin_id
        cursor.close()
        return redirect("/create-group")

    cursor.close()
    return render_template("signup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    db = get_db()
    cursor = get_cursor(db)  # ← FIXED: Use get_cursor()
    error = None

    if request.method == "POST":
        email = request.form.get("email")
        password = request.form.get("password")

        cursor.execute(
            "SELECT * FROM members WHERE email = %s AND is_system = 1",
            (email,)
        )
        admin = cursor.fetchone()

        if admin is None:
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
    cursor = get_cursor(db)

    cursor.execute(
        "SELECT name FROM members WHERE id = %s",
        (session["admin_id"],)
    )
    admin = cursor.fetchone()

    cursor.execute(
        "SELECT * FROM groups WHERE id = %s",
        (session["group_id"],)
    )
    group = cursor.fetchone()
    cursor.close()

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
    
    auto_insert_loan_penalties(db, group_id)

    profit_data = get_current_group_profit(db, group_id)
    settings = get_group_settings(db, group_id)
    admin_id = get_group_admin_member_id(db, group_id)

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT COUNT(id) FROM members WHERE group_id = %s AND is_system = 0",
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
          AND member_id != %s
          AND type IN ('hisa anzia', 'hisa', 'jamii')
        """,
        (group_id, admin_id)
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
def handle_settings():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    if request.method == 'GET':
        settings = get_group_settings(db, group_id)
        return jsonify(settings)
    
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


# ==================== MEMBERS ====================
@app.route('/members-page')
def members_page():
    return render_template('members.html')

@app.route('/member-details/<int:member_id>')
def member_details_page(member_id):
    if "admin_id" not in session:
        return redirect("/login")
    
    if "group_id" not in session:
        return redirect("/create-group")
    
    return render_template('member_details.html', member_id=member_id)


@app.route('/api/members/<int:member_id>/details', methods=['GET'])
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
        
        loan_details.append({
            "id": loan['id'],
            "principal": loan['principal'],
            "interest": loan['interest'],
            "net_amount": loan['net_amount'],
            "months": loan['months'],
            "start_date": loan['start_date'],
            "due_date": loan['due_date'],
            "status": loan['status'],
            "repaid": repaid,
            "remaining": remaining
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
    
    # Calculate profit share
    profit_data = get_current_group_profit(db, group_id)
    net_profit = profit_data["net_profit_pool"]
    total_units = get_total_hisa_units(db, group_id)
    profit_per_unit = net_profit / total_units if total_units > 0 else 0
    expected_profit_share = round(member_units * profit_per_unit)
    
    # Calculate net position
    net_contribution_position = (
        member_total_savings 
        - loan_balances["remaining_loans"]
        - total_penalties_due
    )
    net_payout = net_contribution_position + expected_profit_share
    
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
            "net_contribution_position": net_contribution_position,
            "expected_profit_share": expected_profit_share,
            "net_payout": net_payout
        },
        "contribution_history": [dict(c) for c in contribution_history],
        "loans": loan_details,
        "penalties": penalty_details
    })

@app.route('/api/members', methods=['GET'])
def get_members():
    db = get_db()
    group_id = get_current_group_id()
    
    if not group_id:
        return jsonify({"error": "No group selected"}), 400
    
    cursor = get_cursor(db)
    cursor.execute(
        "SELECT * FROM members WHERE group_id = %s AND is_system = 0", 
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
        
        # Calculate HISA units (simplified)
        hisa_units = total_contributions / unit_price if unit_price > 0 else 0
        remaining_loans = max(total_loans - total_rejesho, 0)

        result.append({
            "id": member_id,
            "name": m["name"],
            "phone": m["phone"],
            "total_contributions": total_contributions,
            "hisa_units": hisa_units,
            "total_loans_committed": total_loans,
            "total_penalties": total_penalties,
            "total_outstanding": remaining_loans,
            "jamii_paid": 0,  # Simplified for speed
            "jamii_expected": 0,
            "jamii_shortfall": 0
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
    
    cursor = get_cursor(db)
    cursor.execute(
        "INSERT INTO members (group_id, name, phone, joined_date, is_system) VALUES (%s, %s, %s, %s, 0)",
        (group_id, name, phone, datetime.now().strftime("%Y-%m-%d"))
    )
    db.commit()
    cursor.close()
    
    return jsonify({"status": "success"})


@app.route('/api/members/<int:member_id>', methods=['PUT', 'DELETE'])
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
def contributions_page():
    return render_template('contributions.html')


@app.route('/api/contributions', methods=['GET'])
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
def loans_page():
    return render_template('loans.html')


@app.route('/api/loans', methods=['GET'])
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
    
    if status not in ['Active', 'Overdue', 'Cleared']:
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


@app.route('/loans-page/download', methods=['GET'])
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
        "SELECT amount, date FROM rejesho WHERE loan_id = %s AND group_id = %s ORDER BY date DESC",
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

    cursor = get_cursor(db)
    cursor.execute("""
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
        WHERE p.group_id = %s
        ORDER BY p.date DESC, p.id DESC
    """, (group_id,))
    ledger = cursor.fetchall()

    cursor.execute(
        "SELECT SUM(amount - COALESCE(amount_paid, 0)) FROM penalties WHERE group_id = %s",
        (group_id,)
    )
    total_due = (lambda r: list(r.values())[0] if r and list(r.values())[0] is not None else 0)(cursor.fetchone())
    
    cursor.close()
    
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


@app.route('/api/penalties/<int:penalty_id>', methods=['PUT', 'DELETE'])
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
def profits_page():
    return render_template('profits.html')

@app.route('/api/profits', methods=['POST'])
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

    cursor = get_cursor(db)
    cursor.execute(
        "SELECT id, name FROM members WHERE group_id = %s AND is_system = 0", 
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
        
        expected_profit_share = round(member_units * profit_per_unit)
        net_payout = net_contribution_position + expected_profit_share

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
            "expected_profit_share": expected_profit_share,
            "net_payout": net_payout,
        })

    cursor.close()
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

if __name__ == "__main__":
    from models import init_db
    with app.app_context():
        init_db()
    app.run(debug=True)