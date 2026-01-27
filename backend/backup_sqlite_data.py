"""
SQLite Data Backup Script
Exports all data to JSON for safe migration to PostgreSQL
"""

import sqlite3
import json
from datetime import datetime
import os

def backup_sqlite_to_json(db_path='kikoba.db', output_dir='backups'):
    """Export all SQLite data to JSON files"""
    
    # Create backup directory
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"❌ ERROR: Database not found at: {db_path}")
        print(f"📂 Current directory: {os.getcwd()}")
        print(f"📂 Looking for: {os.path.abspath(db_path)}")
        return None, None
    
    print(f"✅ Found database at: {db_path}")
    print(f"📊 Database size: {os.path.getsize(db_path):,} bytes")
    
    # Connect to SQLite
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    backup_data = {
        'backup_timestamp': timestamp,
        'database_path': db_path,
        'tables': {}
    }
    
    # Tables to backup (in dependency order)
    tables = [
        'groups',
        'settings', 
        'loan_rules',
        'members',
        'contributions',
        'loans',
        'rejesho',
        'penalties'
    ]
    
    print(f"\n🔄 Starting backup...")
    print("=" * 60)
    
    total_records = 0
    
    for table in tables:
        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            
            # Convert rows to list of dicts
            table_data = []
            for row in rows:
                table_data.append(dict(row))
            
            backup_data['tables'][table] = table_data
            total_records += len(table_data)
            
            print(f"✅ {table:20} - {len(table_data):5} records")
            
        except sqlite3.OperationalError as e:
            print(f"⚠️  {table:20} - Table not found (skipping)")
            backup_data['tables'][table] = []
    
    # Save to JSON file
    output_file = os.path.join(output_dir, f'kikoba_backup_{timestamp}.json')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False, default=str)
    
    print("=" * 60)
    print(f"💾 Backup saved to: {output_file}")
    print(f"📊 Total records backed up: {total_records}")
    
    # Show data summary
    print("\n📋 DATA SUMMARY:")
    print("=" * 60)
    for table, records in backup_data['tables'].items():
        if len(records) > 0:
            print(f"  {table:20} - {len(records):5} records")
    print("=" * 60)
    
    conn.close()
    
    return output_file, backup_data


def verify_backup(backup_file):
    """Verify backup file integrity"""
    
    if not backup_file:
        return False
    
    print("\n🔍 Verifying backup...")
    print("=" * 60)
    
    try:
        with open(backup_file, 'r') as f:
            data = json.load(f)
        
        print(f"✓ Backup timestamp: {data['backup_timestamp']}")
        print(f"✓ Source database: {data['database_path']}")
        print()
        
        total = 0
        for table, records in data['tables'].items():
            if len(records) > 0:
                print(f"✓ {table:20} - {len(records):5} records")
                total += len(records)
        
        print("=" * 60)
        print(f"✅ Backup verified! Total records: {total}")
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False


if __name__ == '__main__':
    print("🚀 KIKOBA DATA BACKUP TOOL")
    print("=" * 60)
    
    # Run backup
    backup_file, backup_data = backup_sqlite_to_json()
    
    if backup_file:
        # Verify
        if verify_backup(backup_file):
            print("\n" + "=" * 60)
            print("🎉 BACKUP COMPLETE - YOUR DATA IS SAFE!")
            print("=" * 60)
            print(f"\n📁 Backup location: {os.path.abspath(backup_file)}")
            print("\n📝 Next steps:")
            print("1. ✅ Keep this backup file safe")
            print("2. 🔄 Proceed with PostgreSQL migration")
            print("3. 📥 Import data after schema creation")
        else:
            print("\n⚠️  Backup created but verification failed")
            print("Please check the backup file manually")
    else:
        print("\n❌ Backup failed! Please check the error messages above.")