from backend.db import get_db
from datetime import datetime, timedelta

# -----------------------------
# Initialize Database
# -----------------------------
def init_db():
    db = get_db()

    # --- 0. Groups Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        created_at TEXT NOT NULL,
        owner_email TEXT,
        is_active INTEGER DEFAULT 1
    )
    """)

    # --- 1. Settings Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        key TEXT NOT NULL,
        value TEXT,
        UNIQUE(group_id, key),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 2. Loan Rules Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS loan_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        min_principal REAL NOT NULL,
        max_principal REAL NOT NULL,
        days INTEGER NOT NULL,
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 3. Members Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        phone TEXT,
        email TEXT,
        password TEXT,
        joined_date TEXT NOT NULL,
        group_id INTEGER,
        is_system INTEGER DEFAULT 0,
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 4. Contributions Table (MODIFIED: added transaction_date) ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS contributions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        date TEXT NOT NULL,
        transaction_date TEXT,
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 5. Loans Table (MODIFIED: added net_amount for what member actually receives) ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        principal REAL NOT NULL,
        interest REAL NOT NULL,
        total REAL NOT NULL,
        net_amount REAL NOT NULL,
        start_date TEXT NOT NULL,
        due_date TEXT NOT NULL,
        months INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'Active',
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 6. Rejesho Table (MODIFIED: added due_date for monthly tracking) ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS rejesho (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        date TEXT NOT NULL,
        due_date TEXT,
        is_monthly_payment INTEGER DEFAULT 0,
        FOREIGN KEY(loan_id) REFERENCES loans(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 7. Penalties Table (MODIFIED: added rejesho_id for tracking which payment was late) ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS penalties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        loan_id INTEGER,
        rejesho_id INTEGER,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        amount_paid REAL DEFAULT 0,
        description TEXT,
        date TEXT NOT NULL,
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(loan_id) REFERENCES loans(id),
        FOREIGN KEY(rejesho_id) REFERENCES rejesho(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    db.commit()

# -----------------------------
# Helper Functions
# -----------------------------
def get_loan_rules(db, group_id):
    """Get loan rules for a specific group"""
    return db.execute(
        "SELECT min_principal, max_principal, days FROM loan_rules WHERE group_id = ? ORDER BY min_principal ASC",
        (group_id,)
    ).fetchall()

def calculate_due_date(principal, group_id):
    """Calculate due date based on principal amount and group-specific loan rules"""
    db = get_db()
    rules = get_loan_rules(db, group_id)
    
    for rule in rules:
        if rule["min_principal"] <= principal <= rule["max_principal"]:
            return (datetime.now() + timedelta(days=rule["days"])).strftime("%Y-%m-%d")
    
    # Default to 30 days if no rule matches
    return (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")

def calculate_penalty(loan, group_id):
    """
    NEW PENALTY LOGIC: Calculate penalty based on overdue monthly rejesho payments.
    Charges 1000 TZS per day after due date for each unpaid monthly rejesho amount.
    """
    db = get_db()
    
    penalty_row = db.execute(
        "SELECT value FROM settings WHERE key='daily_penalty_amount' AND group_id=?",
        (group_id,)
    ).fetchone()
    
    PENALTY_RATE = float(penalty_row["value"]) if penalty_row else 1000
    
    today = datetime.now().date()
    loan_id = loan["id"]
    
    # Calculate monthly rejesho amount
    monthly_rejesho = loan["principal"] / loan["months"]
    
    # Get all expected monthly payments
    start_date = datetime.strptime(loan["start_date"], "%Y-%m-%d").date()
    total_penalty = 0
    
    for month in range(loan["months"]):
        # Calculate due date for this month's payment
        due_date = start_date + timedelta(days=30 * (month + 1))
        
        if today <= due_date:
            continue  # Not yet due
        
        # Check if this month's payment has been made
        paid_for_month = db.execute("""
            SELECT SUM(amount) FROM rejesho 
            WHERE loan_id = ? AND group_id = ? 
            AND date <= ?
        """, (loan_id, group_id, due_date.strftime("%Y-%m-%d"))).fetchone()[0] or 0
        
        expected_by_now = monthly_rejesho * (month + 1)
        
        if paid_for_month < expected_by_now:
            # Calculate days overdue for this payment
            days_overdue = (today - due_date).days
            if days_overdue > 0:
                total_penalty += days_overdue * PENALTY_RATE
    
    return total_penalty

def get_total_penalties_for_member(member_id, group_id):
    """Get total outstanding penalties for a member in a specific group"""
    db = get_db()
    
    row = db.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
        FROM penalties
        WHERE member_id = ? AND group_id = ?
    """, (member_id, group_id)).fetchone()
    
    return row["total_outstanding"] or 0

def get_group_admin_member_id(db, group_id):
    """
    Get the system admin member ID for a specific group.
    Each group has its own system admin member with is_system=1.
    """
    admin = db.execute(
        "SELECT id FROM members WHERE is_system = 1 AND group_id = ?",
        (group_id,)
    ).fetchone()
    
    if not admin:
        raise Exception(f"No system admin member found for group_id={group_id}")
    
    return admin["id"]