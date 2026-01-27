"""
WSGI entry point for production deployment
"""

from backend.app import app

if __name__ == "__main__":
    app.run()