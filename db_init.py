import sqlite3

conn = sqlite3.connect("rc_units.db")
c = conn.cursor()

c.execute("""
CREATE TABLE IF NOT EXISTS rc_units (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    rfid TEXT,
    rid TEXT,
    rma TEXT,
    unit_serial TEXT,

    operator_name TEXT,

    family TEXT,
    model TEXT,
    customer TEXT,

    service_type TEXT,
    unit_type TEXT,
    country TEXT,

    complaint TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

conn.commit()
conn.close()

print("Database initialized")
