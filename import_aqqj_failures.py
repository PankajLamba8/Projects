import pandas as pd
import sqlite3

DB_PATH = "rc_tracking.db"
EXCEL_FILE = "AQQJ fail ID.xlsx"   # ✅ picked from your existing project
MODEL = "AQQJ"

# Excel sheet → test_name mapping (matches your app)
SHEET_TO_TEST = {
    "RAP": "RAP",
    "Unit": "UNIT",
    "TRX B Scan": "TRX_BSCAN",
    "TRX PA": "TRX_PA",
}

def import_failures():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    xls = pd.ExcelFile(EXCEL_FILE)

    for sheet, test_name in SHEET_TO_TEST.items():
        if sheet not in xls.sheet_names:
            print(f"⚠️ Sheet missing: {sheet}")
            continue

        df = xls.parse(sheet)

        # Strict cleaning
        df = df.dropna(subset=["Meas ID", "Meas Name"])

        print(f"\n▶ Importing {sheet} → {test_name}")

        for _, row in df.iterrows():
            meas_id = str(int(row["Meas ID"])).strip()   # ✅ TRUE numeric ID
            meas_name = str(row["Meas Name"]).strip()

            # Category logic (keep simple for now)
            category = meas_name.split("_")[0]

            cur.execute("""
                INSERT INTO failure_master (
                    model,
                    test_name,
                    category,
                    failure_id,
                    description
                )
                SELECT ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM failure_master
                    WHERE model = ?
                      AND test_name = ?
                      AND failure_id = ?
                )
            """, (
                MODEL,
                test_name,
                category,
                meas_id,
                meas_name,
                MODEL,
                test_name,
                meas_id
            ))

    conn.commit()
    conn.close()
    print("\n✅ AQQJ failures imported cleanly (Meas ID preserved)")

if __name__ == "__main__":
    import_failures()
