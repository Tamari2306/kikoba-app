import io
import zipfile
from flask import Flask, request, jsonify, render_template, send_file, send_from_directory
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


app = Flask(__name__)
CORS(app)

def get_current_group_id():
    return request.headers.get("X-GROUP-ID", 1)


# Configuration Constants
GROUP_ADMIN_MEMBER_ID = 1 # Centralized configuration for group expenses


UPLOAD_FOLDER = os.path.join(app.root_path, "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


# ==================== HELPER FUNCTIONS ====================
def get_group_settings(db, group_id):
    # 1. Fetch settings specifically for this group_id
    # Fixed the syntax error in the SQL tuple below
    settings = db.execute(
        "SELECT key, value FROM settings WHERE group_id = ?", 
        (group_id,)
    ).fetchall()
    
    # 2. Convert rows into a dictionary
    data = {s["key"]: s["value"] for s in settings}

    # 3. Define defaults for any missing keys
    defaults = {
        'group_name': 'Kikoba App',
        'interest_rate': '0.10',
        'daily_penalty_amount': '1000',
        'leadership_pay_amount': '0',
        'monthly_jamii_amount': '2000',
        'cycle_months': '12',
        'loan_tier1_amount': '500000',
        'loan_tier1_months': '1',
        'loan_tier2_amount': '1000000',
        'loan_tier2_months': '3',
        'loan_tier3_amount': '2000000',
        'loan_tier3_months': '6'
    }
    
    # 4. Fill in defaults if the group hasn't customized them yet
    for key, default_value in defaults.items():
        if key not in data:
            data[key] = default_value

    # 5. Handle Constitution URLs
    if "constitution_path" in data:
        data["constitution_view_url"] = "/api/constitution/view"
        data["constitution_download_url"] = "/api/constitution/download"

    return data

def get_member_jamii_balance(db, member_id, group_id):
    """
    Calculate member's Jamii contribution status for a specific group.
    """
    settings = get_group_settings(db, group_id)
    
    # 2. Extract settings (using float/int for math)
    monthly_jamii = float(settings.get('monthly_jamii_amount', 2000))
    cycle_months = int(settings.get('cycle_months', 12))
    
    expected_total = monthly_jamii * cycle_months
    
    # 3. Get total Jamii paid, filtering by group_id for safety
    total_paid = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE member_id = ? AND group_id = ? AND type = 'jamii'
        """,
        (member_id, group_id)
    ).fetchone()[0] or 0
    
    # 4. Calculate shortfall
    shortfall = max(expected_total - total_paid, 0)
    
    return {
        "total_paid": total_paid,
        "expected_total": expected_total,
        "shortfall": shortfall
    }

def get_total_principal_loaned(db, group_id):
    """
    Calculates the total principal amount disbursed across all loans for a specific group.
    """
    result = db.execute(
        "SELECT SUM(principal) FROM loans WHERE group_id = ?",
        (group_id,)
    ).fetchone()[0]
    
    return result if result else 0

# ==================== MODIFIED HELPER FUNCTION ====================

def get_current_group_profit(db, group_id):
    """
    Calculates the total profit available for distribution for a specific group.
    """
    # 1. Fetch Dynamic Settings for this specific group
    settings = get_group_settings(db, group_id)
    LEADERSHIP_PAY_AMOUNT = float(settings.get('leadership_pay_amount', 0))
    monthly_jamii = float(settings.get('monthly_jamii_amount', 0))
    cycle_months = int(settings.get('cycle_months', 12))

    # 2. Calculate Expected Jamii (Total Mandatory Liability for THIS group)
    total_members = db.execute(
        "SELECT COUNT(id) FROM members WHERE id != ? AND group_id = ?",
        (GROUP_ADMIN_MEMBER_ID, group_id)
    ).fetchone()[0] or 0
    
    expected_jamii_total = monthly_jamii * cycle_months * total_members

    # 3. Income Components
    total_interest = db.execute(
        "SELECT SUM(total - principal) FROM loans WHERE group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0
    
    # Total Penalties (Helpers must now accept group_id)
    total_penalties_imposed = get_total_penalties_imposed(db, group_id)
    total_penalties_revenue = get_total_penalties_paid(db, group_id)

    # 4. Jamii Fund Management
    total_jamii_collected = db.execute(
        "SELECT SUM(amount) FROM contributions WHERE type='jamii' AND group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0

    historical_jamii_spent = db.execute(
        "SELECT SUM(amount) FROM contributions WHERE type='jamii_deduction' AND group_id = ?",
        (group_id,)
    ).fetchone()[0] or 0
    
    # Unused Jamii balance
    unused_jamii = max(0, total_jamii_collected + historical_jamii_spent)
    
    # 5. Gross Distributable Pool
    gross_distributable_pool = total_interest + total_penalties_imposed + expected_jamii_total
    
    # 6. Net Profit Pool
    net_profit_pool = max(
        gross_distributable_pool 
        - LEADERSHIP_PAY_AMOUNT 
        - abs(historical_jamii_spent),
        0
    )
    
    # 7. Return Detailed Breakdown
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
    """
    Get total member savings (Hisa only) for a specific group.
    Used for calculating profit distribution ratios.
    """
    result = db.execute(
        """
        SELECT SUM(amount) 
        FROM contributions 
        WHERE group_id = ? AND type IN ('hisa', 'hisa anzia')
        """,
        (group_id,)
    ).fetchone()[0]
    
    return result if result else 0

def get_total_outstanding_loans(db, group_id):
    """
    Calculates actual money owed by members of a specific group.
    (Total Loan Liability - Total Repaid Amount)
    """
    # 1. Total amount disbursed + interest for THIS group
    total_liability = db.execute(
        "SELECT SUM(total) FROM loans WHERE group_id = ? AND status != 'Cleared'",
        (group_id,)
    ).fetchone()[0] or 0

    # 2. Total amount repaid against these specific loans
    # We JOIN to ensure we are only looking at repayments for this group's loans
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
    """
    Updates the loan status in the database based on repayments.
    Integrates group_id for multi-tenant isolation.
    """
    # 1. Fetch loan (ensure it belongs to the current group)
    loan = db.execute(
        "SELECT * FROM loans WHERE id = ? AND group_id = ?", 
        (loan_id, group_id)
    ).fetchone()
    
    if not loan:
        return
    
    # 2. Sum repayments for this specific loan and group
    repaid = db.execute(
        "SELECT SUM(amount) FROM rejesho WHERE loan_id = ? AND group_id = ?",
        (loan_id, group_id)
    ).fetchone()[0] or 0
    
    remaining = loan["total"] - repaid
    
    # 3. Determine status
    if remaining <= 0:
        new_status = "Cleared"
    elif datetime.now().date() > datetime.strptime(loan["due_date"], "%Y-%m-%d").date():
        new_status = "Overdue"
    else:
        new_status = "Active"
    
    # 4. Update if changed
    if loan["status"] != new_status:
        db.execute(
            "UPDATE loans SET status = ? WHERE id = ? AND group_id = ?",
            (new_status, loan_id, group_id)
        )
        db.commit()

def auto_insert_loan_penalties(group_id):
    """
    Finds overdue loans for a specific group and inserts penalties.
    """
    db = get_db()

    # 1. Use the helper we updated earlier with group_id
    settings = get_group_settings(db, group_id)
    daily_penalty = float(settings.get("daily_penalty_amount", 1000))

    today = datetime.now().date()

    # 2. Filter overdue loans by group_id
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

        # 3. Prevent duplicate penalty for this loan and group
        exists = db.execute("""
            SELECT 1 FROM penalties
            WHERE loan_id = ? AND group_id = ?
              AND type = 'loan_late'
        """, (loan['id'], group_id)).fetchone()

        if exists:
            continue

        # 4. Insert the penalty with the group_id column
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

        # 5. Mark loan as overdue within the group scope
        db.execute(
            "UPDATE loans SET status = 'Overdue' WHERE id = ? AND group_id = ?",
            (loan['id'], group_id)
        )

    db.commit()

def get_member_loan_balances(db, member_id, group_id):
    """
    Calculates loan balances for a member within a specific group.
    """
    today_date = date.today()
    total_overdue_balance = 0
    total_loans_committed = 0
    total_rejesho = 0
    
    # Filter by group_id and member_id
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
        
        # Ensure repayments are only pulled for this group
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
    
    # Uses our multi-group settings helper
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

@app.route('/')
def dashboard_page():
    return render_template('dashboard.html')

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    db = get_db()
    auto_insert_loan_penalties()

    profit_data = get_current_group_profit(db)
    settings = get_group_settings(db)

    # Members (EXCLUDE Group Expense Fund)
    total_members = db.execute(
        "SELECT COUNT(id) FROM members WHERE id != ?",
        (GROUP_ADMIN_MEMBER_ID,)
    ).fetchone()[0]

    # Calculate total Jamii shortfall across all members
    members = db.execute(
        "SELECT id FROM members WHERE id != ?",
        (GROUP_ADMIN_MEMBER_ID,)
    ).fetchall()
    
    total_jamii_shortfall = sum(
        get_member_jamii_balance(db, m['id'])['shortfall']
        for m in members
    )
    
    # Get penalties data
    total_imposed = get_total_penalties_imposed(db) 
    total_paid = get_total_penalties_paid(db)
    total_due = get_total_group_penalty_liability(db)

    return jsonify({
        # Settings
        "group_name": settings.get('group_name', 'Kikoba App'),
        "constitution_path": settings.get('constitution_path', None),
        "interest_rate": settings.get('interest_rate', '0.10'),
        "daily_penalty": settings.get('daily_penalty_amount', '1000'),
        "leadership_pay_amount": profit_data["leadership_pay_amount"],
        "monthly_jamii_amount": settings.get('monthly_jamii_amount', '2000'),
        "cycle_months": settings.get('cycle_months', '12'),

        # Members
        "total_members": total_members,

        # Savings & Loans
        "total_contributions_hisa": get_total_savings(db),
        "loan_balance_due": get_total_outstanding_loans(db),
        "total_principal_loaned": get_total_principal_loaned(db),

        # Profit Metrics (Interest + Penalties + Expected Jamii)
        "total_interests": profit_data["total_interest"],
        "gross_distributable_pool": profit_data["gross_distributable_pool"],
        "net_profit_in_hand": profit_data["net_profit_pool"],

        # Penalties
        "penalties_imposed": total_imposed,
        "penalties_paid": total_paid,
        "penalties_due_net": total_due,

        # Jamii Breakdown
        "expected_jamii_total": profit_data["expected_jamii_total"],
        "total_jamii_collected": profit_data["total_jamii_collected"],
        "jamii_fund_used": profit_data["historical_jamii_spent"],
        "unused_jamii_for_refund": profit_data["unused_jamii_balance"],
        "total_jamii_shortfall": total_jamii_shortfall,
    })


# ==================== CONFIGURATION ROUTES ====================

@app.route('/api/loan_rules', methods=['GET'])
def get_loan_rules_api():
    db = get_db()
    rules = db.execute("SELECT id, min_principal, max_principal, days FROM loan_rules ORDER BY min_principal ASC").fetchall()
    return jsonify([dict(r) for r in rules])

@app.route('/api/loan_rules', methods=['POST'])
def save_loan_rules_api():
    db = get_db()
    data = request.get_json()
    rules = data.get('rules')
    
    if not rules or not isinstance(rules, list):
        return jsonify({"error": "Invalid rules data format"}), 400
        
    db.execute("DELETE FROM loan_rules")
    
    for rule in rules:
        try:
            min_p = float(rule['min_principal'])
            max_p = float(rule['max_principal'])
            days = int(rule['days'])
            
            db.execute(
                "INSERT INTO loan_rules (min_principal, max_principal, days) VALUES (?, ?, ?)",
                (min_p, max_p, days)
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
    
    if request.method == 'GET':
        settings = get_group_settings(db)
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
        ('monthly_jamii_amount', data.get('monthly_jamii_amount')),
        ('cycle_months', data.get('cycle_months')),
    ]

    try:
        for key, value in updates:
            if value is not None and value != "":
                db.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, str(value))
                )
        db.commit()
        return jsonify({"status": "success", "message": "General settings updated."})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/constitution/upload', methods=['POST'])
def upload_constitution():
    db = get_db()

    if 'constitution_file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['constitution_file']

    if not file or file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    filename = f"{int(datetime.now().timestamp())}_{secure_filename(file.filename)}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)

    db.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        ('constitution_path', filename)
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
    row = db.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path'"
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
    row = db.execute(
        "SELECT value FROM settings WHERE key = 'constitution_path'"
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
    """Fixed: Check if constitution exists properly"""
    db = get_db()
    row = db.execute("SELECT value FROM settings WHERE key = 'constitution_path'").fetchone()
    
    if not row or not row['value']:
        return jsonify({"uploaded": False}), 200
    
    # Check if file actually exists on disk
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
    data = request.get_json()
    amount = float(data.get("amount", 0))
    
    if amount <= 0:
        return jsonify({"error": "Deduction amount must be positive"}), 400

    admin_exists = db.execute(
        "SELECT id FROM members WHERE id = ?",
        (GROUP_ADMIN_MEMBER_ID,)
    ).fetchone()
    
    if not admin_exists:
        return jsonify({
            "error": f"Group admin member (ID {GROUP_ADMIN_MEMBER_ID}) does not exist. Cannot record group expense."
        }), 400

    today_str = datetime.now().strftime("%Y-%m-%d")

    db.execute(
        "INSERT INTO contributions (member_id, type, amount, date) VALUES (?, 'jamii_deduction', ?, ?)",
        (GROUP_ADMIN_MEMBER_ID, -amount, today_str)
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
    members = db.execute("SELECT * FROM members WHERE id != ?", (GROUP_ADMIN_MEMBER_ID,)).fetchall()
    result = []
    
    for m in members:
        member_id = m["id"]
        
        # Total Contributions
        total_contributions = db.execute(
            "SELECT SUM(amount) FROM contributions WHERE member_id=? AND type != 'jamii_deduction'",
            (member_id,)
        ).fetchone()[0] or 0

        # Fetch loan data using the function
        loan_balances = get_member_loan_balances(db, member_id)
        
        # Total Penalties
        total_penalties_due = get_total_penalties_due_for_member(member_id, db)
        
        # Jamii Balance (NEW)
        jamii_status = get_member_jamii_balance(db, member_id)

        result.append({
            "id": member_id,
            "name": m["name"],
            "phone": m["phone"],
            "total_contributions": total_contributions,
            "total_loans_committed": loan_balances["total_loans_committed"],
            "total_penalties": total_penalties_due,
            "total_outstanding": loan_balances["remaining_loans"],
            "jamii_paid": jamii_status["total_paid"],  # NEW
            "jamii_expected": jamii_status["expected_total"],  # NEW
            "jamii_shortfall": jamii_status["shortfall"]  # NEW
        })
    
    return jsonify(result)

@app.route('/api/members', methods=['POST'])
def add_member():
    db = get_db()
    data = request.get_json()
    name = data.get("name")
    phone = data.get("phone")
    
    if not name:
        return jsonify({"error": "Name is required"}), 400
    
    db.execute(
        "INSERT INTO members (name, phone, joined_date) VALUES (?, ?, ?)",
        (name, phone, datetime.now().strftime("%Y-%m-%d"))
    )
    db.commit()
    
    return jsonify({"status": "success"})

@app.route('/api/members/<int:member_id>', methods=['PUT', 'DELETE'])
def edit_member(member_id):
    db = get_db()
    
    if request.method == 'DELETE':
        # Safety check: Don't delete Group Admin
        if member_id == GROUP_ADMIN_MEMBER_ID:
            return jsonify({"error": "Cannot delete group admin account"}), 400
        
        # Check if member has any records
        has_records = db.execute("""
            SELECT 
                (SELECT COUNT(*) FROM contributions WHERE member_id = ?) +
                (SELECT COUNT(*) FROM loans WHERE member_id = ?) +
                (SELECT COUNT(*) FROM penalties WHERE member_id = ?) as total
        """, (member_id, member_id, member_id)).fetchone()['total']
        
        if has_records > 0:
            return jsonify({
                "error": "Cannot delete member with existing contributions, loans, or penalties"
            }), 400
        
        try:
            db.execute("DELETE FROM members WHERE id = ?", (member_id,))
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
            "UPDATE members SET name = ?, phone = ? WHERE id = ?",
            (name, phone, member_id)
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
    contributions = db.execute("""
        SELECT c.id, c.member_id, c.type, c.amount, c.date, m.name as member_name
        FROM contributions c
        JOIN members m ON c.member_id = m.id
        ORDER BY c.date DESC
    """).fetchall()

    result = [dict(c) for c in contributions]
    return jsonify(result)

@app.route('/api/contributions', methods=['POST'])
def add_contribution():
    db = get_db()
    data = request.get_json()
    member_id = data.get("member_id")
    ctype = data.get("type")
    amount = data.get("amount")

    if not member_id or not ctype or not amount:
        return jsonify({"error": "All fields are required"}), 400

    today_str = datetime.now().strftime("%Y-%m-%d")

    if ctype == "rejesho":
        loan = db.execute(
            "SELECT * FROM loans WHERE member_id = ? AND status != 'Cleared' ORDER BY start_date DESC LIMIT 1",
            (member_id,)
        ).fetchone()
        
        if not loan:
            return jsonify({"error": "No active loan found for this member"}), 400

        db.execute(
            "INSERT INTO rejesho (loan_id, amount, date) VALUES (?, ?, ?)",
            (loan["id"], amount, today_str)
        )
        
        db.commit()
        update_loan_status(db, loan["id"])
    else:
        db.execute(
            "INSERT INTO contributions (member_id, type, amount, date) VALUES (?, ?, ?, ?)",
            (member_id, ctype, amount, today_str)
        )
        db.commit()

    return jsonify({"status": "success"})

@app.route('/api/contributions/<int:contribution_id>', methods=['PUT', 'DELETE'])
def edit_contribution(contribution_id):
    db = get_db()
    
    if request.method == 'DELETE':
        # Safety check: Don't delete Jamii deductions (system records)
        contrib = db.execute(
            "SELECT type FROM contributions WHERE id = ?", 
            (contribution_id,)
        ).fetchone()
        
        if not contrib:
            return jsonify({"error": "Contribution not found"}), 404
        
        if contrib['type'] == 'jamii_deduction':
            return jsonify({
                "error": "Cannot delete system-generated Jamii deductions. Use Profits page to manage."
            }), 400
        
        try:
            db.execute("DELETE FROM contributions WHERE id = ?", (contribution_id,))
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
        # Validate date format
        datetime.strptime(date_str, "%Y-%m-%d")
        
        db.execute(
            "UPDATE contributions SET amount = ?, type = ?, date = ? WHERE id = ?",
            (amount, ctype, date_str, contribution_id)
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
    auto_insert_loan_penalties()

    loans = db.execute("""
        SELECT l.*, m.name AS member_name 
        FROM loans l
        JOIN members m ON l.member_id = m.id
    """).fetchall()

    result = []

    for l in loans:
        repaid = db.execute(
            "SELECT SUM(amount) FROM rejesho WHERE loan_id = ?",
            (l["id"],)
        ).fetchone()[0] or 0

        remaining = l["total"] - repaid

        # Determine current status
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
        
        # Update database status if needed
        if l["status"] != status:
             db.execute("UPDATE loans SET status = ? WHERE id = ?", (status, l["id"]))
             db.commit()

    return jsonify(result)

@app.route('/api/loans', methods=['POST'])
def add_loan():
    db = get_db()
    data = request.get_json()

    try:
        member_id = data.get("member_id")
        principal = float(data.get("principal", 0))

        if not member_id or principal <= 0:
            return jsonify({"error": "Invalid member or principal amount"}), 400

        # ================= SETTINGS =================
        settings = get_group_settings(db)

        # Interest rate (stored as decimal e.g. 0.10)
        interest_rate = float(settings.get("interest_rate", 0.10))

        # Loan rules (principal → months)
        rules = [
            (float(settings.get("loan_tier1_amount", 500000)),
             int(settings.get("loan_tier1_months", 1))),
            (float(settings.get("loan_tier2_amount", 1500000)),
             int(settings.get("loan_tier2_months", 3))),
            (float(settings.get("loan_tier3_amount", 3000000)),
             int(settings.get("loan_tier3_months", 6))),
            (float(settings.get("loan_tier1_amount", 5000000)),
             int(settings.get("loan_tier1_months", 9))),
        ]

        # ================= DETERMINE DURATION =================
        months = None
        for max_amount, duration in rules:
            if principal <= max_amount:
                months = duration
                break

        if months is None:
            return jsonify({
                "error": "Loan amount exceeds the maximum allowed by group rules"
            }), 400

        # ================= CALCULATIONS =================
        interest = round(principal * interest_rate)
        total = principal + interest

        start_date = datetime.now()
        due_date = start_date + timedelta(days=30 * months)


        # ================= INSERT =================
        db.execute("""
            INSERT INTO loans (
                member_id,
                principal,
                interest,
                total,
                start_date,
                due_date,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, 'Active')
        """, (
            member_id,
            principal,
            interest,
            total,
            start_date.strftime("%Y-%m-%d"),
            due_date.strftime("%Y-%m-%d")
        ))

        db.commit()

        return jsonify({
            "status": "success",
            "months": months,
            "due_date": due_date.strftime("%Y-%m-%d"),
            "interest": interest,
            "total": total
        })

    except Exception as e:
        db.rollback()
        print("Add loan error:", e)
        return jsonify({"error": "Failed to add loan"}), 500



# ==================== REJESHO (REPAYMENTS) ====================

@app.route('/repayments-page')
def repayments_page():
    loan_id = request.args.get('loan_id')
    return render_template('repayments.html', loan_id=loan_id)

@app.route('/api/rejesho', methods=['POST'])
def add_rejesho():
    db = get_db()
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

    today_str = datetime.now().strftime("%Y-%m-%d")

    db.execute(
        "INSERT INTO rejesho (loan_id, amount, date) VALUES (?, ?, ?)",
        (loan_id, amount, today_str)
    )
    db.commit()
    
    update_loan_status(db, loan_id)
    
    return jsonify({"status": "success"})

@app.route('/api/rejesho/<int:loan_id>', methods=['GET'])
def get_rejesho_history(loan_id):
    db = get_db()
    
    repayments = db.execute(
        "SELECT amount, date FROM rejesho WHERE loan_id = ? ORDER BY date DESC",
        (loan_id,)
    ).fetchall()
    
    loan_info = db.execute(
        "SELECT l.*, m.name as member_name FROM loans l JOIN members m ON l.member_id = m.id WHERE l.id = ?",
        (loan_id,)
    ).fetchone()

    if not loan_info:
        return jsonify({"error": "Loan not found"}), 404

    return jsonify({
        "loan_info": dict(loan_info),
        "repayments": [dict(r) for r in repayments]
    })

@app.route('/api/loans/<int:loan_id>', methods=['PUT'])
def edit_loan(loan_id):
    """
    SAFE EDIT: Only allows editing due_date and status.
    Principal, Interest, Total are NOT editable (accounting integrity).
    To fix wrong loan amounts: Delete loan and create new one.
    """
    db = get_db()
    data = request.get_json()
    
    due_date_str = data.get('due_date', '').strip()
    status = data.get('status', '').strip()
    
    if status not in ['Active', 'Overdue', 'Cleared']:
        return jsonify({"error": "Invalid status"}), 400
    
    try:
        # Validate date format
        datetime.strptime(due_date_str, "%Y-%m-%d")
        
        db.execute(
            "UPDATE loans SET due_date = ?, status = ? WHERE id = ?",
            (due_date_str, status, loan_id)
        )
        db.commit()
        return jsonify({"status": "success", "message": "Loan updated (date/status only)"})
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500


# ==================== PENALTIES ====================

@app.route('/penalties-page')
def penalties_page():
    return render_template('penalties.html')

@app.route('/api/penalties', methods=['GET'])
def get_penalties():
    db = get_db()

    # Query to fetch all penalties and their payment status
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
        ORDER BY p.date DESC, p.id DESC
    """).fetchall()

    # Calculate total outstanding penalty amount for the summary card
    total_due = db.execute("SELECT SUM(amount - COALESCE(amount_paid, 0)) FROM penalties").fetchone()[0] or 0
    
    return jsonify({
        "total_outstanding": total_due,
        "ledger": [dict(p) for p in ledger]
    })

@app.route('/api/penalties', methods=['POST'])
def add_penalty():
    db = get_db()
    data = request.get_json()
    member_id = data.get("member_id")
    ptype = data.get("type")
    amount = float(data.get("amount", 0))
    description = data.get("description", "")
    
    if not member_id or not ptype or amount <= 0:
        return jsonify({"error": "Member ID, type, and positive amount are required"}), 400

    try:
        db.execute(
            # amount_paid is NULL by default or set to 0 initially
            "INSERT INTO penalties (member_id, type, amount, description, date) VALUES (?, ?, ?, ?, ?)",
            (member_id, ptype, amount, description, datetime.now().strftime("%Y-%m-%d"))
        )
        db.commit()
        
        return jsonify({"status": "success"})
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

@app.route('/api/record_penalty_payment/<int:penalty_id>', methods=['POST'])
def record_penalty_payment(penalty_id):
    db = get_db()
    data = request.get_json()
    # Frontend passes the proposed amount in the prompt
    amount_to_pay = float(data.get('amount', 0)) 
    
    if amount_to_pay <= 0:
        return jsonify({"error": "A valid payment amount is required."}), 400

    try:
        # 1. Fetch the penalty record and its current paid status
        penalty = db.execute(
            "SELECT amount, COALESCE(amount_paid, 0) AS amount_paid, member_id FROM penalties WHERE id = ?",
            (penalty_id,)
        ).fetchone()

        if not penalty:
            return jsonify({"error": "Penalty record not found."}), 404

        remaining_due = penalty['amount'] - penalty['amount_paid']
        
        if remaining_due <= 0:
             return jsonify({"error": "Penalty is already fully paid."}), 400
             
        # Actual amount to apply to prevent over-paying
        applied_amount = min(amount_to_pay, remaining_due) 

        # 2. Update the penalty record's amount_paid column
        new_paid_total = penalty['amount_paid'] + applied_amount
        db.execute(
            "UPDATE penalties SET amount_paid = ? WHERE id = ?",
            (new_paid_total, penalty_id)
        )
        
        # 3. Log the transaction (e.g., in contributions/transactions table) for audit
        db.execute(
            "INSERT INTO contributions (member_id, type, amount, date) VALUES (?, 'penalty_payment', ?, datetime('now'))",
            (penalty['member_id'], applied_amount)
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
    
    if request.method == 'DELETE':
        # Safety check: Don't delete auto-generated loan penalties
        penalty = db.execute(
            "SELECT type, loan_id FROM penalties WHERE id = ?", 
            (penalty_id,)
        ).fetchone()
        
        if not penalty:
            return jsonify({"error": "Penalty not found"}), 404
        
        if penalty['type'] == 'loan_late' and penalty['loan_id']:
            return jsonify({
                "error": "Cannot delete auto-generated loan penalties. Clear the loan instead."
            }), 400
        
        try:
            db.execute("DELETE FROM penalties WHERE id = ?", (penalty_id,))
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
        # Get current amount_paid to preserve payment history
        current = db.execute(
            "SELECT amount_paid FROM penalties WHERE id = ?", 
            (penalty_id,)
        ).fetchone()
        
        if not current:
            return jsonify({"error": "Penalty not found"}), 404
        
        amount_paid = current['amount_paid'] or 0
        
        # Don't allow setting amount below what's already been paid
        if amount < amount_paid:
            return jsonify({
                "error": f"Amount cannot be less than already paid: {amount_paid:,.0f} TZS"
            }), 400
        
        db.execute(
            "UPDATE penalties SET amount = ?, description = ? WHERE id = ?",
            (amount, description, penalty_id)
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
    data = request.get_json() or {}
    
    # Proposed additional Jamii expense from user input
    temp_jamii_used = float(data.get("jamii_used", 0))
    method = data.get("method", "cash")

    profit_data = get_current_group_profit(db)

    # Total Jamii Expenses = Historical + Proposed
    total_jamii_expense = profit_data["historical_jamii_spent"] + temp_jamii_used
    
    # Net Profit = Gross Pool - Leadership Pay - Total Jamii Expenses
    net_profit = max(
        profit_data["gross_distributable_pool"]
        - profit_data["leadership_pay_amount"]
        - total_jamii_expense,
        0
    )

    # Total HISA for profit distribution
    total_savings = get_total_savings(db)
    if total_savings == 0:
        return jsonify({
            "error": "No Hisa (savings) available for profit distribution base",
            "net_profit_to_distribute": 0,
            "breakdown": [],
            "leadership_pay_amount": profit_data["leadership_pay_amount"], 
            "gross_distributable_pool": profit_data["gross_distributable_pool"],
            "historical_jamii_spent": profit_data["historical_jamii_spent"],
        })

    members = db.execute(
        "SELECT id, name FROM members WHERE id != ?", 
        (GROUP_ADMIN_MEMBER_ID,)
    ).fetchall()
    results = []

    for m in members:
        member_id = m['id']
        
        # Only HISA contributions for profit share calculation
        savings = db.execute(
            "SELECT SUM(amount) FROM contributions WHERE member_id=? AND type IN ('hisa','hisa anzia')",
            (member_id,)
        ).fetchone()[0] or 0

        share_ratio = savings / total_savings if total_savings > 0 else 0
        profit_share = round(net_profit * share_ratio)

        # Loan balances
        loans_total = db.execute(
            "SELECT SUM(total) FROM loans WHERE member_id=?", 
            (member_id,)
        ).fetchone()[0] or 0
        repaid = db.execute(
            "SELECT SUM(r.amount) FROM rejesho r JOIN loans l ON r.loan_id = l.id WHERE l.member_id=?", 
            (member_id,)
        ).fetchone()[0] or 0
        remaining_loan_balance = max(loans_total - repaid, 0)

        # Penalties due
        total_penalties_due = get_total_penalties_due_for_member(member_id, db)

        # Jamii shortfall
        jamii_status = get_member_jamii_balance(db, member_id)
        jamii_shortfall = jamii_status['shortfall']

        # Total Deductions
        total_deductions = remaining_loan_balance + total_penalties_due + jamii_shortfall
        
        # Final Payout = Savings + Profit Share - Deductions
        final_payout = max((savings + profit_share) - total_deductions, 0)

        results.append({
            "member_name": m["name"],
            "savings": savings,
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
        "breakdown": results
    })

# ==================== REPORTS ====================

@app.route('/reports-page')
def reports_page():
    return render_template("reports.html")

@app.route('/api/reports', methods=['GET'])
def get_report_data():
    db = get_db()
    
    profit_data = get_current_group_profit(db)
    total_profit = profit_data["net_profit_pool"]
    total_savings = get_total_savings(db) 

    members = db.execute(
        "SELECT id, name FROM members WHERE id != ?", 
        (GROUP_ADMIN_MEMBER_ID,)
    ).fetchall()
    report_data = []

    for m in members:
        member_id = m["id"]
        
        # 1. Contributions by type
        contribs = db.execute(
            "SELECT type, SUM(amount) as total FROM contributions WHERE member_id=? AND type != 'jamii_deduction' GROUP BY type",
            (member_id,)
        ).fetchall()
        
        contrib_dict = {c["type"]: c["total"] for c in contribs}
        total_contributions = sum(contrib_dict.values()) 
        member_hisa_base = (contrib_dict.get('hisa anzia', 0) + contrib_dict.get('hisa', 0))

        # 2. Loans and Rejesho
        loan_balances = get_member_loan_balances(db, member_id)

        # 3. Penalties
        total_penalties_due = get_total_penalties_due_for_member(member_id, db)
        
        # 4. Jamii Status (NEW)
        jamii_status = get_member_jamii_balance(db, member_id)
        
        # 5. Net Contribution Position (NOW INCLUDES JAMII SHORTFALL)
        net_contribution_position = (
            member_hisa_base 
            - loan_balances["remaining_loans"] 
            - total_penalties_due 
            - jamii_status["shortfall"]  # NEW DEDUCTION
        )
        
        # 6. Expected profit share calculation
        expected_profit_share = round((member_hisa_base / total_savings) * total_profit) if total_savings > 0 else 0
        
        # 7. Final Net Payout
        net_payout = net_contribution_position + expected_profit_share

        report_data.append({
            "member_name": m["name"],
            "contributions": contrib_dict,
            "total_contributions": total_contributions,
            "total_loans": loan_balances["total_loans_committed"],
            "total_rejesho": loan_balances["total_rejesho"],
            "remaining_loans": loan_balances["remaining_loans"],
            "total_overdue": loan_balances["total_overdue"],
            "total_penalties": total_penalties_due,
            "jamii_paid": jamii_status["total_paid"],  # NEW
            "jamii_expected": jamii_status["expected_total"],  # NEW
            "jamii_shortfall": jamii_status["shortfall"],  # NEW
            "net_contribution_position": net_contribution_position,
            "expected_profit_share": expected_profit_share,
            "net_payout": net_payout,
        })

    return jsonify({"report": report_data})


@app.route('/reports-page/download', methods=['GET'])
def download_report_pdf():
    db = get_db()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=landscape(A4),
        rightMargin=20, leftMargin=20, topMargin=40, bottomMargin=20
    )
    elements = []
    styles = getSampleStyleSheet()

    # --- Group Name & Title ---
    settings = get_group_settings(db)

    group_name = settings.get("group_name", "Kikoba App")

    title = Paragraph(f"📊 {group_name} - Monthly Financial Report", styles['Title'])
    elements.append(title)
    elements.append(Spacer(1, 6))

    # --- Date ---
    date_paragraph = Paragraph(f"Date: {datetime.now().strftime('%Y-%m-%d')}", styles['Normal'])
    elements.append(date_paragraph)
    elements.append(Spacer(1, 12))

    # --- Get report data ---
    response = get_report_data()  # returns Flask Response
    report_json = response.get_json()
    report_data = report_json.get("report", [])

    # --- Totals ---
    total_contributions = sum(m['total_contributions'] for m in report_data)
    total_loans_taken = sum(m['total_loans'] for m in report_data)
    total_loans_due = sum(m['remaining_loans'] for m in report_data)
    total_penalties = sum(m['total_penalties'] for m in report_data)
    total_profit_share = sum(m['expected_profit_share'] for m in report_data)

    # --- Dashboard-style summary cards ---
    summary_data = [
        ["Total Contributions", f"{total_contributions:,.0f} TZS", colors.blue],
        ["Total Loans Taken", f"{total_loans_taken:,.0f} TZS", colors.orange],
        ["Total Loans Due", f"{total_loans_due:,.0f} TZS", colors.red],
        ["Total Penalties", f"{total_penalties:,.0f} TZS", colors.darkred],
        ["Net Profit Share", f"{total_profit_share:,.0f} TZS", colors.green]
    ]
    summary_rows = []
    for label, value, _ in summary_data:
        summary_rows.append([Paragraph(f"<b>{label}</b>", styles['Normal']),
                             Paragraph(value, styles['Normal'])])
    summary_table = Table(summary_rows, colWidths=[150, 100])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.whitesmoke),
        ('TEXTCOLOR', (0,0), (-1,-1), colors.black),
        ('ALIGN', (1,0), (1,-1), 'RIGHT'),
        ('FONTNAME', (0,0), (-1,-1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 9),
        ('BOX', (0,0), (-1,-1), 0.5, colors.grey),
        ('INNERGRID', (0,0), (-1,-1), 0.3, colors.grey)
    ]))
    elements.append(summary_table)
    elements.append(Spacer(1, 12))

    # --- Top Contributors & Largest Loaners ---
    top_contributors = sorted(report_data, key=lambda m: m['total_contributions'], reverse=True)[:3]
    top_loans = sorted(report_data, key=lambda m: m['total_loans'], reverse=True)[:3]

    # Prepare tables for mini-cards
    def mini_card_table(title, items, value_key, color):
        rows = [[Paragraph(f"<b>{title}</b>", styles['Normal']), "Amount (TZS)"]]
        for i, m in enumerate(items, start=1):
            rows.append([f"{i}. {m['member_name']}", f"{m[value_key]:,.0f}"])
        tbl = Table(rows, colWidths=[150, 80])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), color),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('ALIGN', (1,0), (1,-1), 'RIGHT'),
            ('ALIGN', (0,0), (0,-1), 'LEFT'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('GRID', (0,0), (-1,-1), 0.3, colors.grey)
        ]))
        return tbl

    elements.append(mini_card_table("Top Contributors", top_contributors, "total_contributions", colors.green))
    elements.append(Spacer(1, 6))
    elements.append(mini_card_table("Largest Loaners", top_loans, "total_loans", colors.orange))
    elements.append(Spacer(1, 12))

    # --- Main Table Headers ---
    headers = [
        "Member", "Hisa Anzia", "Hisa", "Jamii", "Total Contributions",
        "Loans Taken", "Rejesho Paid", "Loans Due",
        "Penalties", "Net Hisa", "Profit Share", "Profit %", "Net Payout"
    ]
    data = [headers]

    for m in report_data:
        profit_percent = (m['expected_profit_share'] / total_profit_share * 100) if total_profit_share else 0
        row = [
            m['member_name'],
            f"{m['contributions'].get('hisa anzia',0):,.0f}",
            f"{m['contributions'].get('hisa',0):,.0f}",
            f"{m['contributions'].get('jamii',0):,.0f}",
            f"{m['total_contributions']:,.0f}",
            f"{m['total_loans']:,.0f}",
            f"{m['total_rejesho']:,.0f}",
            f"{m['remaining_loans']:,.0f}",
            f"{m['total_penalties']:,.0f}",
            f"{m['net_contribution_position']:,.0f}",
            f"{m['expected_profit_share']:,.0f}",
            f"{profit_percent:.1f}%",
            f"{m['net_payout']:,.0f}"
        ]
        data.append(row)

    col_widths = [90, 60, 60, 60, 70, 60, 60, 60, 60, 70, 70, 50, 70]
    table = Table(data, repeatRows=1, colWidths=col_widths)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.green),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('ALIGN', (0,0), (0,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,0), 6),
        ('GRID', (0,0), (-1,-1), 0.3, colors.grey),
    ]))
    for i in range(1, len(data)):
        if i % 2 == 0:
            table.setStyle(TableStyle([('BACKGROUND', (0,i), (-1,i), colors.whitesmoke)]))

    elements.append(table)

    # --- Build PDF ---
    doc.build(elements)
    buffer.seek(0)
    filename = f"{group_name.replace(' ','_')}_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype="application/pdf")

@app.route('/api/backup/export', methods=['GET'])
def export_raw_backup():
    db = get_db()
    
    queries = {
        "members": "SELECT * FROM members",
        "contributions": """
            SELECT c.id, m.name as member_name, c.type, c.amount, c.date 
            FROM contributions c 
            JOIN members m ON c.member_id = m.id
        """,
        "loans": """
            SELECT l.id, m.name as member_name, l.principal, l.interest, l.total, l.start_date, l.due_date, l.status 
            FROM loans l 
            JOIN members m ON l.member_id = m.id
        """,
        "repayments": """
            SELECT r.id, m.name as member_name, l.id as loan_id, r.amount, r.date 
            FROM rejesho r 
            JOIN loans l ON r.loan_id = l.id 
            JOIN members m ON l.member_id = m.id
        """,
        "penalties": """
            SELECT p.id, m.name as member_name, p.type, p.amount, p.amount_paid, p.date, p.description 
            FROM penalties p 
            JOIN members m ON p.member_id = m.id
        """,
        "settings": "SELECT * FROM settings"
    }

    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        # --- PART 1: RAW TABLES ---
        for file_name, sql in queries.items():
            cursor = db.execute(sql)
            rows = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
            
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            writer.writerow(column_names)

            for row in rows:
                data = list(row)
                for i, col in enumerate(column_names):
                    # Robust Date Formatting for Excel
                    if 'date' in col.lower() and data[i]:
                        try:
                            # Extract only YYYY-MM-DD
                            clean_date = str(data[i]).split(' ')[0]
                            data[i] = clean_date
                        except:
                            pass
                writer.writerow(data)
            
            zip_file.writestr(f"{file_name}.csv", csv_buffer.getvalue())
            csv_buffer.close()

        # --- PART 2: THE BALANCE SHEET (Per Member Summary) ---
        members = db.execute("SELECT id, name FROM members WHERE id != ?", (GROUP_ADMIN_MEMBER_ID,)).fetchall()
        
        balance_csv = StringIO()
        balance_writer = csv.writer(balance_csv)
        balance_writer.writerow([
            "Member ID", "Member Name", "Total Hisa (Savings)", 
            "Total Jamii Paid", "Jamii Shortfall", "Outstanding Loan Balance", "Unpaid Penalties"
        ])

        for m in members:
            m_id = m['id']
            # Get Hisa
            hisa = db.execute("SELECT SUM(amount) FROM contributions WHERE member_id=? AND type IN ('hisa', 'hisa anzia')", (m_id,)).fetchone()[0] or 0
            # Get Jamii
            jamii_status = get_member_jamii_balance(db, m_id)
            # Get Loans
            loan_bal = get_member_loan_balances(db, m_id)['remaining_loans']
            # Get Penalties
            penalty_bal = get_total_penalties_due_for_member(m_id, db)

            balance_writer.writerow([
                m_id, m['name'], hisa, 
                jamii_status['total_paid'], jamii_status['shortfall'], loan_bal, penalty_bal
            ])

        zip_file.writestr("Balance_Sheet_Summary.csv", balance_csv.getvalue())
        balance_csv.close()

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f"Kikoba_Export_{datetime.now().strftime('%Y-%m-%d')}.zip"
    )

if __name__ == "__main__":
    from backend.models import init_db
    with app.app_context():
        init_db()
    app.run(debug=True)