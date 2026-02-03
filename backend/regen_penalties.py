from app import get_db, auto_insert_loan_penalties, get_cursor
from datetime import datetime

def regenerate_penalties():
    db = get_db()
    cursor = get_cursor(db)
    
    try:
        # Backup old penalties
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS penalties_backup AS
            SELECT *
            FROM penalties
            WHERE type = 'monthly_rejesho_late'
        """)
        print("✅ penalties_backup table created / updated.")
        
        # Delete old/wrong penalties
        cursor.execute("""
            DELETE FROM penalties
            WHERE type = 'monthly_rejesho_late'
        """)
        print("✅ Old monthly_rejesho_late penalties deleted.")
        db.commit()
        
        # Get all group IDs
        cursor.execute("SELECT id FROM groups")
        groups = [row['id'] for row in cursor.fetchall()]
        
        # Regenerate penalties per group
        for group_id in groups:
            auto_insert_loan_penalties(db, group_id)
            print(f"✅ Penalties regenerated for group {group_id}.")
        
        print("✅ All penalties successfully regenerated.")
    
    except Exception as e:
        db.rollback()
        print("❌ Error during penalty regeneration:", e)
    
    finally:
        cursor.close()
        db.close()
