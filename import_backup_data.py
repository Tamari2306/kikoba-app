"""
Import Backed-Up Data into PostgreSQL
Imports data from JSON backup file into new PostgreSQL database
"""

import json
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

from backend.db import get_db
from flask import Flask


def import_data_from_backup(backup_file):
    """Import data from JSON backup into PostgreSQL"""
    
    if not os.path.exists(backup_file):
        print(f"❌ Backup file not found: {backup_file}")
        return False
    
    # Create minimal Flask app for context
    app = Flask(__name__)
    app.config.from_object('config.Config')
    
    with app.app_context():
        print("🚀 IMPORTING DATA FROM BACKUP")
        print("=" * 60)
        print(f"📁 Source: {backup_file}")
        
        # Load backup data
        with open(backup_file, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        print(f"📅 Backup timestamp: {backup_data['backup_timestamp']}")
        print()
        
        db = get_db()
        cursor = db.cursor()
        
        # Import order (respects foreign key dependencies)
        import_order = [
            'groups',
            'members',
            'settings',
            'loan_rules',
            'contributions',
            'loans',
            'rejesho',
            'penalties'
        ]
        
        total_imported = 0
        
        try:
            for table_name in import_order:
                records = backup_data['tables'].get(table_name, [])
                
                if not records:
                    print(f"⊘  {table_name:20} - No data to import")
                    continue
                
                # Get column names from first record
                columns = list(records[0].keys())
                
                # Remove 'id' column to let PostgreSQL auto-generate
                if 'id' in columns:
                    columns.remove('id')
                
                # Build INSERT query
                placeholders = ', '.join(['%s'] * len(columns))
                columns_str = ', '.join(columns)
                
                query = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                """
                
                # Insert each record
                imported_count = 0
                for record in records:
                    values = [record.get(col) for col in columns]
                    cursor.execute(query, values)
                    imported_count += 1
                
                total_imported += imported_count
                print(f"✅ {table_name:20} - {imported_count:5} records imported")
            
            # Reset sequences for auto-increment columns
            print("\n🔄 Resetting ID sequences...")
            for table_name in import_order:
                try:
                    cursor.execute(f"""
                        SELECT setval(
                            pg_get_serial_sequence('{table_name}', 'id'),
                            COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                            true
                        )
                    """)
                    print(f"  ✓ {table_name}")
                except Exception as e:
                    print(f"  ⚠ {table_name} - {e}")
            
            db.commit()
            
            print("\n" + "=" * 60)
            print(f"🎉 IMPORT COMPLETE!")
            print(f"📊 Total records imported: {total_imported}")
            print("=" * 60)
            
            # Verify data
            print("\n🔍 Verifying imported data...")
            print("=" * 60)
            
            for table_name in import_order:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                result = cursor.fetchone()
                
                # Handle both dict and tuple results
                count = result['count'] if isinstance(result, dict) else result[0]
                expected = len(backup_data['tables'].get(table_name, []))
                
                status = "✅" if count == expected else "⚠️"
                print(f"{status} {table_name:20} - {count:5} records (expected: {expected})")
            
            print("=" * 60)
            
            cursor.close()
            return True
            
        except Exception as e:
            db.rollback()
            print(f"\n❌ Import failed: {e}")
            import traceback
            traceback.print_exc()
            cursor.close()
            return False


if __name__ == '__main__':
    # Find most recent backup file
    backup_dir = 'backups'
    
    if not os.path.exists(backup_dir):
        print(f"❌ Backup directory not found: {backup_dir}")
        exit(1)
    
    backup_files = [f for f in os.listdir(backup_dir) if f.startswith('kikoba_backup_') and f.endswith('.json')]
    
    if not backup_files:
        print(f"❌ No backup files found in {backup_dir}")
        exit(1)
    
    # Use most recent backup
    backup_files.sort(reverse=True)
    latest_backup = os.path.join(backup_dir, backup_files[0])
    
    print(f"📁 Using backup file: {latest_backup}\n")
    
    success = import_data_from_backup(latest_backup)
    
    if success:
        print("\n✅ Data migration completed successfully!")
    else:
        print("\n❌ Data migration failed!")
        exit(1)