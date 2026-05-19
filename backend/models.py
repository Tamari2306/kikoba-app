"""
Database Models and Schema - PostgreSQL Version
"""

from db import get_db
from datetime import datetime, timedelta
import psycopg2.extras


def init_db():
    """Initialize database schema for PostgreSQL"""
    db = get_db()
    cursor = db.cursor()

    try:
        # --- 0. Groups Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            owner_email TEXT,
            is_active INTEGER DEFAULT 1
        )
        """)

        # --- 1. Settings Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            id SERIAL PRIMARY KEY,
            group_id INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            UNIQUE(group_id, key),
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 2. Loan Rules Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS loan_rules (
            id SERIAL PRIMARY KEY,
            group_id INTEGER NOT NULL,
            min_principal REAL NOT NULL,
            max_principal REAL NOT NULL,
            days INTEGER NOT NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 3. Members Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS members (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            password TEXT,
            joined_date TEXT NOT NULL,
            group_id INTEGER,
            is_system INTEGER DEFAULT 0,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE SET NULL
        )
        """)

        # --- 4. Contributions Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contributions (
            id SERIAL PRIMARY KEY,
            member_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            date TEXT NOT NULL,
            transaction_date TEXT,
            FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 5. Loans Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            id SERIAL PRIMARY KEY,
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
            forgiven INTEGER DEFAULT 0,
            forgiveness_reason TEXT,
            forgiven_by TEXT,
            forgiven_at TEXT,
            FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 6. Rejesho Table ---
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejesho (
            id SERIAL PRIMARY KEY,
            loan_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            date TEXT NOT NULL,
            due_date TEXT,
            is_monthly_payment INTEGER DEFAULT 0,
            FOREIGN KEY(loan_id) REFERENCES loans(id) ON DELETE CASCADE,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 7. Penalties Table ---
        # FIX: amount_paid, forgiven_amount, is_frozen, freeze_reason are required by
        # app.py everywhere (get_penalties, get_member_details, auto_freeze, etc.).
        # They were previously missing from the schema and added ad-hoc via ALTER TABLE,
        # causing crashes on fresh databases and intermittent failures on existing ones.
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS penalties (
            id SERIAL PRIMARY KEY,
            member_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            loan_id INTEGER,
            rejesho_id INTEGER,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            amount_paid REAL DEFAULT 0,
            forgiven_amount REAL DEFAULT 0,
            is_frozen INTEGER DEFAULT 0,
            freeze_reason TEXT,
            description TEXT,
            date TEXT NOT NULL,
            FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE,
            FOREIGN KEY(loan_id) REFERENCES loans(id) ON DELETE SET NULL,
            FOREIGN KEY(rejesho_id) REFERENCES rejesho(id) ON DELETE SET NULL,
            FOREIGN KEY(group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
        """)

        # --- 8. Safety: add missing columns to existing databases ---
        # These ALTER TABLE statements are safe on fresh DBs (columns already exist)
        # and fix existing DBs that were created before the columns were added.
        _safe_add_columns(cursor, db)

        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_members_group_id ON members(group_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contributions_member_id ON contributions(member_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contributions_group_id ON contributions(group_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_member_id ON loans(member_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_loans_group_id ON loans(group_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_penalties_member_id ON penalties(member_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_penalties_group_id ON penalties(group_id)")

        db.commit()
        print("✅ Database schema created/verified successfully!")

    except Exception as e:
        db.rollback()
        print(f"❌ Error creating schema: {e}")
        raise
    finally:
        cursor.close()


def _safe_add_columns(cursor, db):
    """
    Idempotently add columns that were missing from early schema versions.
    Each ALTER TABLE is wrapped in its own try/except so one failure doesn't
    block the others. PostgreSQL raises an error if the column already exists,
    which we silently ignore.
    """
    migrations = [
        # penalties table — columns needed everywhere in app.py
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS amount_paid REAL DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS forgiven_amount REAL DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS is_frozen INTEGER DEFAULT 0",
        "ALTER TABLE penalties ADD COLUMN IF NOT EXISTS freeze_reason TEXT",
        # loans table — forgiveness columns
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven INTEGER DEFAULT 0",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiveness_reason TEXT",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_by TEXT",
        "ALTER TABLE loans ADD COLUMN IF NOT EXISTS forgiven_at TEXT",
    ]
    for sql in migrations:
        try:
            cursor.execute(sql)
            db.commit()
        except Exception:
            db.rollback()


# -----------------------------
# Helper Functions
# -----------------------------

def get_loan_rules(db, group_id):
    """Get loan rules for a specific group"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT min_principal, max_principal, days FROM loan_rules WHERE group_id = %s ORDER BY min_principal ASC",
        (group_id,)
    )
    results = cursor.fetchall()
    cursor.close()
    return results


def calculate_due_date(principal, group_id):
    """Calculate due date based on principal amount and group-specific loan rules"""
    db = get_db()
    rules = get_loan_rules(db, group_id)

    for rule in rules:
        if rule["min_principal"] <= principal <= rule["max_principal"]:
            return (datetime.now() + timedelta(days=rule["days"])).strftime("%Y-%m-%d")

    return (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")


def calculate_penalty(loan, group_id):
    """
    Calculate penalty based on overdue monthly rejesho payments.
    """
    db = get_db()
    cursor = db.cursor()

    cursor.execute(
        "SELECT value FROM settings WHERE key='daily_penalty_amount' AND group_id=%s",
        (group_id,)
    )
    penalty_row = cursor.fetchone()
    cursor.close()

    PENALTY_RATE = float(penalty_row["value"]) if penalty_row else 1000

    today = datetime.now().date()
    loan_id = loan["id"]
    monthly_rejesho = loan["principal"] / loan["months"]
    start_date = datetime.strptime(loan["start_date"], "%Y-%m-%d").date()
    total_penalty = 0

    cursor = db.cursor()

    for month in range(loan["months"]):
        due_date = start_date + timedelta(days=30 * (month + 1))

        if today <= due_date:
            continue

        cursor.execute("""
            SELECT SUM(amount) FROM rejesho
            WHERE loan_id = %s AND group_id = %s
            AND date <= %s
        """, (loan_id, group_id, due_date.strftime("%Y-%m-%d")))

        result = cursor.fetchone()
        paid_for_month = result[0] if result and result[0] else 0
        expected_by_now = monthly_rejesho * (month + 1)

        if paid_for_month < expected_by_now:
            days_overdue = (today - due_date).days
            if days_overdue > 0:
                total_penalty += days_overdue * PENALTY_RATE

    cursor.close()
    return total_penalty


def get_total_penalties_for_member(member_id, group_id):
    """Get total outstanding penalties for a member in a specific group"""
    db = get_db()
    cursor = db.cursor()

    cursor.execute("""
        SELECT SUM(amount - COALESCE(amount_paid, 0)) AS total_outstanding
        FROM penalties
        WHERE member_id = %s AND group_id = %s
    """, (member_id, group_id))

    row = cursor.fetchone()
    cursor.close()

    return row["total_outstanding"] or 0


def get_group_admin_member_id(db, group_id):
    """Get the system admin member ID for a specific group."""
    cursor = db.cursor()
    cursor.execute(
        "SELECT id FROM members WHERE is_system = 1 AND group_id = %s",
        (group_id,)
    )
    admin = cursor.fetchone()
    cursor.close()

    if not admin:
        raise Exception(f"No system admin member found for group_id={group_id}")

    return admin["id"]