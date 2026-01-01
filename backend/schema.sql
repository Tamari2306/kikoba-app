-- Members Table
CREATE TABLE IF NOT EXISTS members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    phone TEXT,
    joined_date TEXT
);

-- Contributions Table
CREATE TABLE IF NOT EXISTS contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id INTEGER,
    type TEXT,
    amount REAL,
    date TEXT,
    FOREIGN KEY(member_id) REFERENCES members(id)
);

-- Loans Table
CREATE TABLE IF NOT EXISTS loans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id INTEGER,
    principal REAL,
    interest REAL,
    total REAL,
    repayment_period INTEGER,
    start_date TEXT,
    next_due_date TEXT,
    paid REAL DEFAULT 0,
    FOREIGN KEY(member_id) REFERENCES members(id)
);

-- Penalties Table
CREATE TABLE IF NOT EXISTS penalties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    loan_id INTEGER,
    amount REAL,
    date TEXT,
    FOREIGN KEY(loan_id) REFERENCES loans(id)
);
