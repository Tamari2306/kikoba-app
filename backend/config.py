import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "../data/kikoba.db")
DATABASE = f"sqlite:///{DB_PATH}"
