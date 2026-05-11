-- ===============================
-- UNIT MASTER
-- ===============================


CREATE TABLE units (
    rid TEXT PRIMARY KEY,
    rma TEXT,
    family TEXT,
    model TEXT,
    customer TEXT,
    is_overseas INTEGER,
    country TEXT,
    srr_count INTEGER,
    tat_date DATE,
    complaint TEXT,
    status TEXT,
    current_station TEXT
);

-- ===============================
-- RFID MAPPING
-- ===============================


CREATE TABLE rfid_map (
    rfid TEXT PRIMARY KEY,
    rid TEXT
);

-- ===============================
-- STATION HISTORY (AUDIT TRAIL)
-- ===============================


CREATE TABLE station_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rid TEXT,
    station TEXT,
    user TEXT,
    remarks TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


-- ===============================
-- VISUAL INSPECTION (RC IN → VI)
-- ===============================


CREATE TABLE visual_inspection (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rid TEXT,
    warranty_label TEXT,
    visual_check TEXT,
    missing_items TEXT,
    overall_result TEXT,
    remarks TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);





-- ===============================
-- FAILURE MASTER (MODEL + TEST)
-- ===============================


CREATE TABLE failure_master (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model TEXT NOT NULL,
    test_name TEXT NOT NULL,      -- RAP / UNIT / TRX / PA
    category TEXT NOT NULL,       -- BOOT / POWER / RF / etc
    failure_code TEXT NOT NULL,   -- short code (optional)
    failure_id TEXT NOT NULL,     -- e.g. 175-WriteLutTable
    description TEXT
);

CREATE INDEX idx_failure_lookup
ON failure_master (model, test_name, category);
