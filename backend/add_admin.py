import sqlite3

def insert_system_admin():
    # Targets kikoba.db in your root folder
    db_path = 'kikoba.db'
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # The SQL command you provided
        sql = """
        INSERT INTO members (
            group_id,
            name,
            phone,
            joined_date,
            is_system
        )
        VALUES (
            1,
            '__GROUP_ADMIN__',
            '',
            date('now'),
            1
        );
        """
        
        cursor.execute(sql)
        conn.commit()
        
        print("Successfully inserted '__GROUP_ADMIN__' as a system member.")
        print(f"New Member ID: {cursor.lastrowid}")

    except sqlite3.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    insert_system_admin()