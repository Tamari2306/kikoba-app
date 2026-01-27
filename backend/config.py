import os

class Config:
    # Use Render's PostgreSQL URL if it exists, otherwise use local SQLite
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', 'sqlite:///kikoba.db')
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-123')
    CORS_ORIGINS = ["*"]  # Or your specific frontend URL
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')