"""
Database Connection Management - PostgreSQL with Connection Pooling
"""

import psycopg2
import psycopg2.extras
from psycopg2 import pool
from flask import g
import os


# Global connection pool
connection_pool = None


def init_connection_pool(app):
    """Initialize connection pool on app startup"""
    global connection_pool
    
    database_url = app.config.get('DATABASE_URL')
    
    if not database_url:
        raise ValueError("DATABASE_URL not configured!")
    
    # Fix Render's postgres:// -> postgresql://
    if database_url.startswith('postgres://'):
        database_url = database_url.replace('postgres://', 'postgresql://', 1)
    
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=database_url
        )
        print("✅ Database connection pool created")
    except Exception as e:
        print(f"❌ Failed to create connection pool: {e}")
        raise


def get_db():
    """
    Get database connection from pool.
    Reuses existing connection from Flask g context or gets new one from pool.
    """
    if 'db' not in g:
        if connection_pool is None:
            raise RuntimeError("Connection pool not initialized. Call init_connection_pool() first.")
        
        try:
            g.db = connection_pool.getconn()
            g.db.autocommit = False
            
        except Exception as e:
            print(f"❌ Error getting connection from pool: {e}")
            raise
    
    return g.db


def close_db(e=None):
    """
    Return connection to pool (don't actually close it).
    """
    db = g.pop('db', None)
    
    if db is not None:
        if connection_pool:
            connection_pool.putconn(db)


def init_app(app):
    """
    Register database functions with Flask app.
    """
    # Initialize connection pool when app starts
    with app.app_context():
        init_connection_pool(app)
    
    # Register teardown
    app.teardown_appcontext(close_db)