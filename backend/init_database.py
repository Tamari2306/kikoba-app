"""
Initialize PostgreSQL Database Schema
Run this ONCE to create all tables
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import after loading .env
from db import get_db
from models import init_db
from flask import Flask

def create_schema():
    """Create database schema"""
    
    # Create minimal Flask app for context
    app = Flask(__name__)
    app.config.from_object('config.Config')
    
    with app.app_context():
        print("🚀 Initializing PostgreSQL Database Schema...")
        print("=" * 60)
        
        try:
            # Test connection
            db = get_db()
            cursor = db.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            
            # Handle both dict and tuple results
            version_str = version['version'] if isinstance(version, dict) else version[0]
            
            print(f"✅ Connected to PostgreSQL")
            print(f"   Version: {version_str[:80]}...")
            cursor.close()
            
            # Create schema
            print("\n📋 Creating tables...")
            init_db()
            
            # Verify tables were created
            cursor = db.cursor()
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = cursor.fetchall()
            cursor.close()
            
            print("\n✅ Tables created:")
            print("=" * 60)
            for table in tables:
                table_name = table['table_name'] if isinstance(table, dict) else table[0]
                print(f"  ✓ {table_name}")
            print("=" * 60)
            
            print(f"\n🎉 Database schema initialized successfully!")
            print(f"📊 Total tables: {len(tables)}")
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
            raise


if __name__ == '__main__':
    create_schema()