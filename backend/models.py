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

    # --- 4. Contributions Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS contributions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        date TEXT NOT NULL,
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 5. Loans Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        principal REAL NOT NULL,
        interest REAL NOT NULL,
        total REAL NOT NULL,
        start_date TEXT NOT NULL,
        due_date TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'Active',
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 6. Rejesho Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS rejesho (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        loan_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        date TEXT NOT NULL,
        FOREIGN KEY(loan_id) REFERENCES loans(id),
        FOREIGN KEY(group_id) REFERENCES groups(id)
    )
    """)

    # --- 7. Penalties Table ---
    db.execute("""
    CREATE TABLE IF NOT EXISTS penalties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        group_id INTEGER NOT NULL,
        loan_id INTEGER,
        type TEXT NOT NULL,
        amount REAL NOT NULL,
        amount_paid REAL DEFAULT 0,
        description TEXT,
        date TEXT NOT NULL,
        FOREIGN KEY(member_id) REFERENCES members(id),
        FOREIGN KEY(loan_id) REFERENCES loans(id),
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
    """Calculate penalty for an overdue loan based on group settings"""
    db = get_db()
    
    penalty_row = db.execute(
        "SELECT value FROM settings WHERE key='daily_penalty_amount' AND group_id=?",
        (group_id,)
    ).fetchone()
    
    PENALTY_RATE = float(penalty_row["value"]) if penalty_row else 1000
    
    try:
        due_date = datetime.strptime(loan["due_date"], "%Y-%m-%d")
    except:
        return 0
    
    overdue_days = (datetime.now() - due_date).days
    return overdue_days * PENALTY_RATE if overdue_days > 0 else 0

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