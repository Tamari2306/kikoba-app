from datetime import datetime, timedelta
from .app import app
from .models import init_db, calculate_due_date
from .db import get_db

with app.app_context():
    db = get_db()
    # Initialize tables
    init_db()

    # Clear existing data (optional)
    db.execute("DELETE FROM contributions")
    db.execute("DELETE FROM loans")
    db.execute("DELETE FROM members")
    db.commit()

    # ---------------- Members ----------------
    members = [
        ("Alice", "0712345678"),
        ("Bob", "0712345679"),
        ("Charlie", "0712345680")
    ]
    for name, phone in members:
        db.execute(
            "INSERT INTO members (name, phone, joined_date) VALUES (?,?,?)",
            (name, phone, datetime.now().strftime("%Y-%m-%d"))
        )
    db.commit()

    member_ids = [row['id'] for row in db.execute("SELECT id FROM members").fetchall()]

    # ---------------- Contributions ----------------
    contributions = [
        (member_ids[0], "hisa", 200000),
        (member_ids[0], "jamii", 50000),
        (member_ids[1], "hisa", 300000),
        (member_ids[2], "rejesho", 100000)
    ]
    for member_id, ctype, amount in contributions:
        db.execute(
            "INSERT INTO contributions (member_id,type,amount,date) VALUES (?,?,?,?)",
            (member_id, ctype, amount, datetime.now().strftime("%Y-%m-%d"))
        )
    db.commit()

    # ---------------- Loans ----------------
    loans = [
        # member_id, principal, overdue_days
        (member_ids[0], 400000, 0),    # current
        (member_ids[1], 700000, 5),    # 5 days overdue
        (member_ids[2], 1_200_000, 10) # 10 days overdue
    ]
    for member_id, principal, overdue_days in loans:
        interest = principal * 0.1
        total = principal + interest
        # Set start_date and due_date to simulate overdue
        start_date = datetime.now() - timedelta(days=overdue_days + 1)
        due_date = start_date + timedelta(days=int(calculate_due_date(principal).split('-')[2]))  # keep original period
        # Alternatively, just set due_date in the past to ensure overdue
        due_date = datetime.now() - timedelta(days=overdue_days)
        db.execute(
            "INSERT INTO loans (member_id, principal, interest, total, start_date, due_date, status) VALUES (?,?,?,?,?,?,?)",
            (
                member_id,
                principal,
                interest,
                total,
                start_date.strftime("%Y-%m-%d"),
                due_date.strftime("%Y-%m-%d"),
                "active"
            )
        )
    db.commit()

    print("Seed data with overdue loans inserted successfully!")
