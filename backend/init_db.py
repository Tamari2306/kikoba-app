# init_db.py - create SQLite DB and seed settings/brackets + sample member
import sqlite3
from pathlib import Path
from datetime import date

DB = Path(__file__).parent / "kikoba.db"

schema = '''
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS members (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  phone TEXT,
  email TEXT,
  joined_date TEXT
);

CREATE TABLE IF NOT EXISTS contributions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  member_id INTEGER NOT NULL,
  type TEXT NOT NULL,
  amount INTEGER NOT NULL,
  date TEXT NOT NULL,
  note TEXT,
  FOREIGN KEY(member_id) REFERENCES members(id)
);

CREATE TABLE IF NOT EXISTS loans (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  member_id INTEGER NOT NULL,
  principal INTEGER NOT NULL,
  interest_rate REAL NOT NULL,
  date_issued TEXT NOT NULL,
  due_date TEXT NOT NULL,
  months INTEGER NOT NULL,
  status TEXT DEFAULT 'open',
  FOREIGN KEY(member_id) REFERENCES members(id)
);

CREATE TABLE IF NOT EXISTS loan_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  loan_id INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  date TEXT NOT NULL,
  note TEXT,
  FOREIGN KEY(loan_id) REFERENCES loans(id)
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS loan_brackets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  min_amount INTEGER NOT NULL,
  max_amount INTEGER NOT NULL,
  months INTEGER NOT NULL
);
'''

def init():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.executescript(schema)

    # default settings (interest and penalty)
    cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("interest_rate", "0.10"))
    cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", ("penalty_per_day", "1000"))

    # default brackets from your spec
    cur.execute("DELETE FROM loan_brackets")
    brackets = [
        (100000, 500000, 1),
        (500001, 1000000, 3),
        (1000001, 1500000, 6),
        (1500001, 2000000, 8),
    ]
    for b in brackets:
        cur.execute("INSERT INTO loan_brackets (min_amount, max_amount, months) VALUES (?, ?, ?)", b)

    # sample member for testing
    cur.execute("INSERT INTO members (name, phone, email, joined_date) VALUES (?, ?, ?, ?)",
                ("Admin User", "+255700000000", "admin@example.com", date.today().isoformat()))

    conn.commit()
    conn.close()
    print("DB initialized at", DB)

if __name__ == "__main__":
    init()
