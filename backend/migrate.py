import sqlite3

def setup_authentication():
    # Targeted kikoba.db in your root folder
    db_path = 'kikoba.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("1. Creating 'admins' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)

        print("2. Adding 'admin_id' column to 'groups' table...")
        try:
            # We add this so each group knows which admin owns it
            cursor.execute("ALTER TABLE groups ADD COLUMN admin_id INTEGER;")
            print("   Column 'admin_id' added successfully.")
        except sqlite3.OperationalError:
            print("   Notice: 'admin_id' column already exists in 'groups'.")

        conn.commit()
        print("\nDatabase schema updated successfully!")

    except sqlite3.Error as e:
        print(f"\n[ERROR]: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    setup_authentication()