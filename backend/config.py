import os

class Config:
    # 1. Pull the URL from Render's environment
    # We assign it to DATABASE_URL because your db.py uses app.config.get('DATABASE_URL')
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    # 2. Also keep this for standard Flask-SQLAlchemy compatibility if needed
    SQLALCHEMY_DATABASE_URI = DATABASE_URL or 'sqlite:///kikoba.db'
    
    # 3. Rest of your settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-123')
    CORS_ORIGINS = ["*"]  
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads')