"""
Clear all data from PostgreSQL database
Use this to reset database before re-importing
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from backend.db import get_db
from flask import Flask


def clear_all_data():
    """Delete all data from all tables (keeps schema)"""
    
    # Create minimal Flask app for context
    app = Flask(__name__)
    app.config.from_object('config.Config')
    
    with app.app_context():
        print("🗑️  CLEARING ALL DATA FROM DATABASE")
        print("=" * 60)
        print("⚠️  This will delete all data but keep table structure")
        
        # Ask for confirmation
        response = input("\nType 'YES' to confirm: ")
        
        if response != 'YES':
            print("❌ Operation cancelled")
            return False
        
        db = get_db()
        cursor = db.cursor()
        
        # Tables to clear (in reverse dependency order)
        tables = [
            'penalties',
            'rejesho',
            'loans',
            'contributions',
            'loan_rules',
            'settings',
            'members',
            'groups'
        ]
        
        try:
            print("\n🔄 Clearing tables...")
            print("=" * 60)
            
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                result = cursor.fetchone()
                count = result['count'] if isinstance(result, dict) else result[0]
                print(f"✅ {table:20} - cleared (remaining: {count})")
            
            # Reset sequences
            print("\n🔄 Resetting ID sequences...")
            for table in tables:
                try:
                    cursor.execute(f"""
                        SELECT setval(
                            pg_get_serial_sequence('{table}', 'id'),
                            1,
                            false
                        )
                    """)
                    print(f"  ✓ {table}")
                except Exception as e:
                    print(f"  ⚠ {table} - {e}")
            
            db.commit()
            
            print("\n" + "=" * 60)
            print("🎉 Database cleared successfully!")
            print("=" * 60)
            
            cursor.close()
            return True
            
        except Exception as e:
            db.rollback()
            print(f"\n❌ Error clearing database: {e}")
            import traceback
            traceback.print_exc()
            cursor.close()
            return False


if __name__ == '__main__':
    clear_all_data()