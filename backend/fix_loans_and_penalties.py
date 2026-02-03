from app import get_db, auto_insert_loan_penalties, get_cursor
from datetime import datetime
import calendar

def fix_loans_and_penalties():
    db = get_db()
    cursor = get_cursor(db)

    try:
        # --- Step 1: Recalculate due dates for all loans ---
        cursor.execute("SELECT id, start_date, months FROM loans")
        loans = cursor.fetchall()

        for loan in loans:
            start_date = datetime.strptime(loan['start_date'], "%Y-%m-%d")
            months = loan['months']

            # Calculate same-day-next-month due date
            due_year = start_date.year + (start_date.month + months - 1) // 12
            due_month = (start_date.month + months - 1) % 12 + 1
            due_day = start_date.day
            max_day_in_due_month = calendar.monthrange(due_year, due_month)[1]
            if due_day > max_day_in_due_month:
                due_day = max_day_in_due_month

            due_date = datetime(due_year, due_month, due_day)
            cursor.execute(
                "UPDATE loans SET due_date = %s WHERE id = %s",
                (due_date.strftime("%Y-%m-%d"), loan['id'])
            )

        db.commit()
        print("✅ All loan due dates recalculated.")

        # --- Step 2: Delete old wrong penalties ---
        cursor.execute("""
            DELETE FROM penalties
            WHERE type = 'monthly_rejesho_late'
        """)
        db.commit()
        print("✅ Old monthly_rejesho_late penalties deleted.")

        # --- Step 3: Regenerate penalties ---
        cursor.execute("SELECT id FROM groups")
        groups = [row['id'] for row in cursor.fetchall()]
        for group_id in groups:
            auto_insert_loan_penalties(db, group_id)
            print(f"✅ Penalties regenerated for group {group_id}.")

        print("✅ All tasks completed successfully.")

    except Exception as e:
        db.rollback()
        print("❌ Error:", e)

    finally:
        cursor.close()
        db.close()
