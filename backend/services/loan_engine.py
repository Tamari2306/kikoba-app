# loan_engine.py - loan logic and utilities
from backend.db import get_db
from datetime import date, timedelta, datetime

def get_settings():
    db = get_db()
    cur = db.execute("SELECT key, value FROM settings")
    rows = cur.fetchall()
    return {r['key']: r['value'] for r in rows}

def get_brackets():
    db = get_db()
    cur = db.execute("SELECT min_amount, max_amount, months FROM loan_brackets ORDER BY min_amount")
    return [dict(r) for r in cur.fetchall()]

def find_bracket_for_amount(amount):
    for b in get_brackets():
        if b['min_amount'] <= amount <= b['max_amount']:
            return b
    return None

def calculate_interest(principal, interest_rate):
    # interest_rate string or float
    rate = float(interest_rate)
    return round(principal * rate, 2)

def calculate_due_date(start_date_str, months):
    # start_date_str in YYYY-MM-DD
    d = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    # approximate months as 30 days each for simplicity (works for your monthly deadlines)
    due = d + timedelta(days=30 * months)
    return due.isoformat()

def penalty_for_overdue(due_date_str, as_of_str=None, daily_penalty=None):
    as_of = datetime.strptime(as_of_str, '%Y-%m-%d').date() if as_of_str else date.today()
    due = datetime.strptime(due_date_str, '%Y-%m-%d').date()
    overdue_days = max(0, (as_of - due).days)
    if overdue_days == 0:
        return 0
    if daily_penalty is None:
        settings = get_settings()
        daily_penalty = int(settings.get('penalty_per_day', '1000'))
    return overdue_days * daily_penalty

def loan_summary(loan_row, as_of_str=None):
    settings = get_settings()
    interest_rate = settings.get('interest_rate', '0.10')
    principal = loan_row['principal']
    interest = calculate_interest(principal, interest_rate)
    payments_cur = get_db().execute("SELECT SUM(amount) as total FROM loan_payments WHERE loan_id=?", (loan_row['id'],)).fetchone()
    payments_total = payments_cur['total'] or 0
    remaining_principal = max(0, principal - payments_total)
    due_date = loan_row['due_date']
    penalty = penalty_for_overdue(due_date, as_of_str, int(settings.get('penalty_per_day', '1000')))
    total_due_now = round(interest + penalty + remaining_principal, 2)
    return {
        'id': loan_row['id'],
        'member_id': loan_row['member_id'],
        'principal': principal,
        'interest': interest,
        'payments_total': payments_total,
        'remaining_principal': remaining_principal,
        'due_date': due_date,
        'months': loan_row['months'],
        'penalty': penalty,
        'total_due_now': total_due_now,
        'status': loan_row['status']
    }
