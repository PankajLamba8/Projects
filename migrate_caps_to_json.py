import sqlite3
import json

DB = "rc_tracking.db"

components = [
    "e1f", "lmp_cap", "eac_cap", "power_cap",
    "ret_cap", "rxo_cap", "fan",
    "upper_mb", "lower_mb", "hdmi_cap",
    "qsfp", "mecb"
]

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

rows = cur.execute(
    "SELECT * FROM visual_inspection WHERE caps_json IS NULL"
).fetchall()

for row in rows:
    caps = {}
    for comp in components:
        caps[comp] = {
            "status": row[f"{comp}_status"],
            "count": row[f"{comp}_count"] or 0
        }

    cur.execute(
        "UPDATE visual_inspection SET caps_json=? WHERE rid=?",
        (json.dumps(caps), row["rid"])
    )

conn.commit()
conn.close()

print("Migration completed successfully.")
