from pydoc import doc
import pip
from flask import Flask, render_template, request, send_file, redirect, url_for, abort, make_response
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from io import BytesIO

import qrcode
import json
import os
import json
from reportlab.lib.colors import black, grey, red
from datetime import datetime
import json



app = Flask(__name__)
DB_PATH = "rc_tracking.db"

# ==============================
# DB HELPER
# ==============================
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/favicon.ico')
def favicon():
    return app.send_static_file('img/favicon.ico')


# ==============================
# DASHBOARD
# ==============================
@app.route("/")
def dashboard():
    db = get_db()
    units = db.execute("SELECT * FROM units").fetchall()

    stats = {
        "total": db.execute("SELECT COUNT(*) FROM units").fetchone()[0],
        "in_progress": db.execute("SELECT COUNT(*) FROM units WHERE status='IN_PROGRESS'").fetchone()[0],
        "completed": db.execute("SELECT COUNT(*) FROM units WHERE status='COMPLETED'").fetchone()[0],
        "rc_in": db.execute("SELECT COUNT(*) FROM units WHERE current_station='RC IN'").fetchone()[0],
        "visual": db.execute("SELECT COUNT(*) FROM units WHERE current_station='Visual Inspection'").fetchone()[0],
        "diagnosis": db.execute("SELECT COUNT(*) FROM units WHERE current_station='Diagnosis'").fetchone()[0],
        "repair": db.execute("SELECT COUNT(*) FROM units WHERE current_station='Repair'").fetchone()[0],
        "qa": db.execute("SELECT COUNT(*) FROM units WHERE current_station='QA'").fetchone()[0],
    }

    return render_template("dashboard.html", units=units, stats=stats)


# ==============================
# RC IN
# ==============================
@app.route("/incoming", methods=["GET", "POST"])
def incoming():
    db = get_db()
    error = None

    # ------------------------------
    # DISPATCH DATE
    # ------------------------------
    dispatch_date_ui = (datetime.now() + timedelta(days=10)).strftime("%d.%m.%Y")
    dispatch_date_db = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")

    # ==============================
    # POST: REGISTER UNIT
    # ==============================
    if request.method == "POST":
        d = request.form

        rid = d.get("rid")
        rfid = d.get("rfid")
        unit_serial = d.get("unit_serial")

        # ------------------------------
        # BASIC VALIDATION
        # ------------------------------
        if not rid:
            error = "RID is required."
        else:
            # ------------------------------
            # DUPLICATE RID
            # ------------------------------
            if db.execute("SELECT 1 FROM units WHERE rid=?", (rid,)).fetchone():
                error = f"RID {rid} already exists."

            # ------------------------------
            # DUPLICATE SERIAL
            # ------------------------------
            elif unit_serial:
                row = db.execute(
                    "SELECT rid FROM units WHERE unit_serial=?",
                    (unit_serial,)
                ).fetchone()
                if row:
                    error = f"Unit Serial already exists (RID {row['rid']})"

            # ------------------------------
            # DUPLICATE RFID
            # ------------------------------
            elif rfid:
                row = db.execute(
                    "SELECT rid FROM rfid_map WHERE rfid=?",
                    (rfid,)
                ).fetchone()
                if row:
                    error = f"RFID already mapped to RID {row['rid']}"

        # ❌ Stop if validation failed
        if error:
            return render_template(
                "incoming.html",
                error=error,
                dispatch_date=dispatch_date_ui,
                unit=None,
                success=False,
                last_rid=None      # ✅ ADD (prevents undefined access)
            )

        # ------------------------------
        # INSERT UNIT
        # ------------------------------
        try:
            db.execute("""
                INSERT INTO units (
                    rid, rma, unit_serial,
                    family, model, customer,
                    is_overseas, country,
                    srr_count, dispatch_date,
                    complaint, status, current_station
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rid,
                d.get("rma"),
                unit_serial,
                d.get("family"),
                d.get("model"),
                d.get("customer"),
                1 if d.get("unit_type") == "Overseas" else 0,
                d.get("country"),
                int(d.get("srr_count") or 0),
                dispatch_date_db,
                d.get("complaint"),
                "IN_PROGRESS",
                "Visual Inspection"
            ))

            # ------------------------------
            # RFID MAP
            # ------------------------------
            if rfid:
                db.execute(
                    "INSERT INTO rfid_map (rfid, rid) VALUES (?,?)",
                    (rfid, rid)
                )

            # ------------------------------
            # STATION HISTORY
            # ------------------------------
            db.execute("""
                INSERT INTO station_history (rid, station, user, remarks)
                VALUES (?,?,?,?)
            """, (
                rid,
                "Incoming",
                d.get("user"),
                "Unit registered"
            ))

            db.commit()

        except Exception as e:
            db.rollback()
            return render_template(
                "incoming.html",
                error=str(e),
                dispatch_date=dispatch_date_ui,
                unit=None,
                success=False,
                last_rid=None      # ✅ ADD
            )

        # ------------------------------
        # QR GENERATION
        # ------------------------------
        qr_payload = {
    "rid": rid,
    "rfid": rfid,
    "unitSerial": unit_serial,
    "family": d.get("family"),
    "model": d.get("model"),
    "customer": d.get("customer"),
    "complaint": d.get("complaint"),
    "unitType": "Overseas" if d.get("unit_type") == "Overseas" else "Domestic",
    "country": d.get("country"),
    "dispatchDate": dispatch_date_db
}

        qr_dir = os.path.join(app.static_folder, "qr")
        if not os.path.exists(qr_dir):
            os.makedirs(qr_dir)      # ✅ ADD                                                                                       


        qr_path = os.path.join(qr_dir, f"{rid}.png")
        qrcode.make(json.dumps(qr_payload)).save(qr_path)

        # ✅ PRG pattern (THIS is the main UI fix)
        return redirect(
            url_for(
                "incoming",
                success=1,
                rid=rid
            )
        )

    # ==============================
    # GET: LOAD PAGE
    # ==============================
    success = request.args.get("success") == "1"
    last_rid = request.args.get("rid")

    unit = None
    if success and last_rid:
        unit = db.execute(
            "SELECT * FROM units WHERE rid=?",
            (last_rid,)
        ).fetchone()

    return render_template(
        "incoming.html",
        success=success,
        error=error,
        unit=unit,
        last_rid=last_rid
    )



    



from datetime import datetime   # already imported, just confirming

@app.route("/incoming/preview/<rid>", endpoint="incoming_preview")
def incoming_preview(rid):
    db = get_db()

    unit = db.execute("""
    SELECT
        u.*,
        r.rfid AS rfid
    FROM units u
    LEFT JOIN rfid_map r ON r.rid = u.rid
    WHERE u.rid = ?
""", (rid,)).fetchone()


    history = db.execute(
        "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
        (rid,)
    ).fetchall()

    if not unit:
        abort(404)

    return render_template(
        "incoming_preview.html",
        unit=unit,
        history=history,
        generated_at=datetime.now()   # ✅ ADD THIS
    )



from playwright.sync_api import sync_playwright

@app.route("/incoming/preview/<rid>/download")
def download_preview_pdf(rid):
    url = url_for("incoming_preview", rid=rid, _external=True)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")

        pdf = page.pdf(
            format="A4",
            print_background=True,
            margin={
                "top": "20mm",
                "bottom": "20mm",
                "left": "20mm",
                "right": "20mm"
            }
        )

        browser.close()

    response = make_response(pdf)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = (
        f"attachment; filename=Incoming_Traveller_{rid}.pdf"
    )
    return response












# ==============================
# VISUAL INSPECTION
# ==============================
@app.route("/visual-inspection", methods=["GET", "POST"])
def visual_inspection():
    db = get_db()
    unit = None
    history = []
    error = None
    success = False  # ✅ ADD THIS  
    caps = {}        # ✅ ADD THIS


    if request.method == "POST":

        # ==========================
        # LOAD UNIT (SCAN)
        # ==========================
        if "rfid" in request.form and "save" not in request.form:
            rfid = request.form.get("rfid")

            unit = db.execute(
                "SELECT u.* FROM units u "
                "JOIN rfid_map r ON r.rid = u.rid "
                "WHERE r.rfid = ?",
                (rfid,)
            ).fetchone()

            if unit:
                history = db.execute(
                    "SELECT * FROM station_history "
                    "WHERE rid = ? "
                    "ORDER BY created_at",
                    (unit["rid"],)
                ).fetchall()
            else:
                error = "Invalid RFID. Unit not found."

        # ==========================
        # SAVE VISUAL INSPECTION
        # ==========================
        elif "save" in request.form:
            d = request.form
            rid = d.get("rid")

            ok, error = require_station(db, rid, "Visual Inspection")
            if not ok:
                unit = db.execute(
                    "SELECT * FROM units WHERE rid = ?",
                    (rid,)
                ).fetchone()

                history = db.execute(
                    "SELECT * FROM station_history "
                    "WHERE rid = ? ORDER BY created_at",
                    (rid,)
                ).fetchall()

                return render_template(
                    "visual_inspection.html",
                    unit=unit,
                    history=history,
                    error=error,
                    success=success # ✅ ADD THIS
                )

            existing = db.execute(
                "SELECT 1 FROM visual_inspection WHERE rid = ?",
                (rid,)
            ).fetchone()

            if existing:
                error = "Visual Inspection already completed for this unit."
                unit = db.execute(
                    "SELECT * FROM units WHERE rid = ?",
                    (rid,)
                ).fetchone()

                history = db.execute(
                    "SELECT * FROM station_history "
                    "WHERE rid = ? ORDER BY created_at",
                    (rid,)
                ).fetchall()

                return render_template(
                    "visual_inspection.html",
                    unit=unit,
                    history=history,
                    error=error,
                    success=success # ✅ ADD THIS
                )

            admin_decision = request.form.get("admin_decision", "None")

            def safe_int(val):
                try:
                    return int(val)
                except:
                    return 0

            # ==========================
            # BUILD CAPS JSON (NEW)
            # ==========================
            caps = {
                "e1f": {"status": d.get("e1f_status"), "count": safe_int(d.get("e1f_count"))},
                "lmp_cap": {"status": d.get("lmp_cap_status"), "count": safe_int(d.get("lmp_cap_count"))},
                "eac_cap": {"status": d.get("eac_cap_status"), "count": safe_int(d.get("eac_cap_count"))},
                "power_cap": {"status": d.get("power_cap_status"), "count": safe_int(d.get("power_cap_count"))},
                "ret_cap": {"status": d.get("ret_cap_status"), "count": safe_int(d.get("ret_cap_count"))},
                "rxo_cap": {"status": d.get("rxo_cap_status"), "count": safe_int(d.get("rxo_cap_count"))},
                "fan": {"status": d.get("fan_status"), "count": safe_int(d.get("fan_count"))},
                "upper_mb": {"status": d.get("upper_mb_status"), "count": safe_int(d.get("upper_mb_count"))},
                "lower_mb": {"status": d.get("lower_mb_status"), "count": safe_int(d.get("lower_mb_count"))},
                "hdmi_cap": {"status": d.get("hdmi_cap_status"), "count": safe_int(d.get("hdmi_cap_count"))},
                "qsfp": {"status": d.get("qsfp_status"), "count": safe_int(d.get("qsfp_count"))},
                "mecb": {"status": d.get("mecb_status"), "count": safe_int(d.get("mecb_count"))}
            }

            caps_json = json.dumps(caps)

            try:

                # ==================================================
                # ❌ OLD INSERT (KEPT, BUT DISABLED SAFELY)
                # ==================================================
                if False:
                    db.execute(""" 
                    INSERT INTO visual_inspection (
                        rid, engineer, visual_check, admin_decision,
                        e1f_status, e1f_count
                    ) VALUES (?,?,?,?,?,?)
                    """)

                # ==================================================
                # ✅ NEW INSERT (JSON-ONLY, CORRECT SCHEMA)
                # ==================================================
                db.execute("""
                INSERT INTO visual_inspection (
                    rid,
                    engineer,
                    visual_check,
                    warranty_label,
                    admin_decision,
                    caps_json,
                    pre_diag_note,
                    sanity_note,
                    incoming_ip_test,
                    remarks,
                    is_locked
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    rid,
                    d.get("user"),
                    d.get("visual_check"),
                    d.get("warranty_label"),
                    admin_decision,
                    caps_json,
                    d.get("pre_diag_note"),
                    d.get("sanity_note"),
                    d.get("incoming_ip_test"),
                    d.get("remarks"),
                    1
                ))

            except sqlite3.IntegrityError:
                error = "Visual Inspection already completed for this unit."

            else:
                db.execute("""
                    INSERT INTO station_history (rid, station, user, remarks)
                    VALUES (?, ?, ?, ?)
                """, (
                    rid,
                    "Visual Inspection",
                    d.get("user"),
                    "Completed"
                ))

                db.execute("""
                    UPDATE units SET current_station='Diagnosis'
                    WHERE rid=?
                """, (rid,))

                db.commit()
                success = True   # ✅ SET SUCCESS FLAG
                unit = db.execute(
                    "SELECT * FROM units WHERE rid = ?",
                    (rid,)
                ).fetchone()
                success = True   # ✅ ADD THIS


                history = db.execute(
                    "SELECT * FROM station_history "
                    "WHERE rid = ? ORDER BY created_at",
                    (rid,)
                ).fetchall()

    return render_template(
        "visual_inspection.html",
        unit=unit,
        history=history,
        error=error,
        success=success,
        caps=caps,
        qr_value=unit["rid"] if unit else None
    )


from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from io import BytesIO

def draw_line(c, y):
    c.setStrokeColor(grey)
    c.line(40, y, 550, y)






def row_val(row, key, default="NA"):
    try:
        return row[key]
    except Exception:
        return default


def generate_visual_pdf(unit, visual):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)

    y = 800

    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, "VISUAL INSPECTION REPORT")
    y -= 30
    y -= 10
    draw_line(c, y)
    y -= 20

    c.setFont("Helvetica", 10)
    c.drawString(40, y, f"RID: {unit['rid']}")
    y -= 15
    c.drawString(40, y, f"Model: {unit['model']}")
    y -= 15
    c.drawString(40, y, f"Customer: {unit['customer']}")
    y -= 25

    # ==========================
    # ✅ METADATA BLOCK (ADDED)
    # ==========================
    c.drawString(320, 770, f"Date: {datetime.now().strftime('%d-%m-%Y')}")
    c.drawString(320, 755, f"Engineer: {row_val(visual, 'engineer', 'NA')}")

    y -= 15
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, y, "Warranty Label:")
    c.setFont("Helvetica", 10)
    c.drawString(150, y, row_val(visual, "warranty_label", "NA"))

    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y, "Component Check")
    y -= 15

    draw_line(c, y)
    y -= 15

    components = [
        "e1f", "lmp_cap", "eac_cap", "power_cap",
        "ret_cap", "rxo_cap", "fan",
        "upper_mb", "lower_mb", "hdmi_cap",
        "qsfp", "mecb"
    ]

    # ==================================================
    # ✅ READ CAPS FROM JSON (ALREADY MIGRATED DB)
    # ==================================================
    caps = {}
    if "caps_json" in visual.keys() and visual["caps_json"]:
        try:
            caps = json.loads(visual["caps_json"])
        except Exception:
            caps = {}

    # ==========================
    # ✅ TABLE HEADER (ADDED)
    # ==========================
    c.setFont("Helvetica-Bold", 10)
    c.drawString(50, y, "COMPONENT")
    c.drawString(200, y, "STATUS")
    c.drawRightString(300, y, "QTY")
    y -= 10
    draw_line(c, y)
    y -= 12

    for comp in components:
        # ==========================
        # 🔁 HYBRID MODE (JSON → FALLBACK)
        # ==========================
        if caps:
            status = caps.get(comp, {}).get("status", "NA")
            count = caps.get(comp, {}).get("count", 0)
        else:
            
          status = row_val(visual, f"{comp}_status", "NA")
          count = row_val(visual, f"{comp}_count", 0)


        # ==========================
        # ✅ FAIL HIGHLIGHT (ADDED)
        # ==========================
        if status == "FAIL":
            c.setFont("Helvetica-Bold", 10)
            c.setFillColor(red)
        else:
            c.setFont("Helvetica", 10)
            c.setFillColor(black)

        c.drawString(50, y, comp.upper().replace("_", " "))
        c.drawString(200, y, status)
        c.drawRightString(300, y, str(count))

        y -= 14

    # ==========================
    # ✅ RESET COLOR
    # ==========================
    c.setFillColor(black)

    # ==========================
    # ✅ DECISION + TEST SUMMARY (ADDED)
    # ==========================
    y -= 10
    draw_line(c, y)
    y -= 15

    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, y, "Admin Decision:")
    c.setFont("Helvetica", 10)
    c.drawString(150, y, row_val(visual, "admin_decision"))

    y -= 15

    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, y, "Incoming IP Test:")
    c.setFont("Helvetica", 10)
    c.drawString(150, y, row_val(visual, "incoming_ip_test", "NA"))
    y -= 20

    # ==========================
    # ✅ REMARKS SECTION (ADDED)
    # ==========================
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, y, "Remarks:")
    y -= 12
    c.setFont("Helvetica", 10)

    remarks = row_val(visual, "remarks", "-")

    for line in remarks.splitlines():
        c.drawString(50, y, line)
        y -= 12

    # ==========================
    # ✅ FOOTER (ADDED)
    # ==========================
    y -= 10
    draw_line(c, y)
    y -= 15
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(40, y, "Generated by RC Tracking System")

    c.showPage()
    c.save()

    buffer.seek(0)
    return buffer







@app.route("/visual/<rid>/pdf")
def visual_pdf(rid):
    db = get_db()

    unit = db.execute(
        "SELECT * FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    visual = db.execute(
        "SELECT * FROM visual_inspection WHERE rid=?",
        (rid,)
    ).fetchone()

    if not unit or not visual:
        abort(404)

    pdf = generate_visual_pdf(unit, visual)

    return send_file(
        pdf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"VISUAL_{rid}.pdf"
    )


@app.route("/visual/<rid>/preview")
def visual_preview(rid):
    db = get_db()

    unit = db.execute(
        "SELECT * FROM units WHERE rid = ?",
        (rid,)
    ).fetchone()

    visual = db.execute(
        "SELECT * FROM visual_inspection WHERE rid = ?",
        (rid,)
    ).fetchone()

    if not unit or not visual:
        abort(404)

    # CAPS JSON SAFE LOAD
    caps = {}
    if visual["caps_json"]:
        try:
            caps = json.loads(visual["caps_json"])
        except:
            caps = {}

    history = db.execute(
        "SELECT * FROM station_history WHERE rid = ? ORDER BY created_at",
        (rid,)
    ).fetchall()

    return render_template(
        "visual_inspection_preview.html",
        unit=unit,
        visual=visual,
        caps=caps,
        history=history,
        generated_at=datetime.now()
    )
















# ==============================
# SCAN UNIT HELPER (REQUIRED)
# ==============================
# ==============================
# SCAN UNIT HELPER (SINGLE SOURCE)
# ==============================
def scan_unit(db, rfid):
    if not rfid:
        return None, []

    unit = db.execute("""
        SELECT u.*, r.rfid
        FROM units u
        JOIN rfid_map r ON r.rid = u.rid
        WHERE r.rfid = ?
    """, (rfid,)).fetchone()

    if not unit:
        return None, []

    history = db.execute("""
        SELECT *
        FROM station_history
        WHERE rid = ?
        ORDER BY created_at
    """, (unit["rid"],)).fetchall()

    return unit, history



# ==============================
# SCAN UNIT HELPER (SINGLE SOURCE)
# ==============================
def scan_unit(db, rfid):
    if not rfid:
        return None, []

    unit = db.execute("""
        SELECT u.*, r.rfid
        FROM units u
        JOIN rfid_map r ON r.rid = u.rid
        WHERE r.rfid = ?
    """, (rfid,)).fetchone()

    if not unit:
        return None, []

    history = db.execute("""
        SELECT *
        FROM station_history
        WHERE rid = ?
        ORDER BY created_at
    """, (unit["rid"],)).fetchall()

    return unit, history






































# ==============================
# DIAGNOSIS
# ==============================
# ==============================
# DIAGNOSIS
# ==============================
# ==============================
# DIAGNOSIS
# ==============================
@app.route("/diagnosis", methods=["GET", "POST"])
def diagnosis():
    db = get_db()

    unit = None
    visual = None
    history = []
    caps = {}
    error = None

    saved = request.args.get("saved") == "1"
    success = saved

    rap_failures = {}
    unit_failures = {}
    trx_failures = {}
    pa_failures = {}

    # =========================
    # GET FLOW LOAD (AFTER REDIRECT)
    # =========================
    rid = request.args.get("rid")

    if rid:
        unit = db.execute(
            "SELECT * FROM units WHERE rid=?",
            (rid,)
        ).fetchone()

    if unit:
        history = db.execute(
            "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
            (rid,)
        ).fetchall()

        visual = db.execute(
            "SELECT * FROM visual_inspection WHERE rid=?",
            (rid,)
        ).fetchone()

        if visual and visual["caps_json"]:
            try:
                caps = json.loads(visual["caps_json"])
            except Exception:
                caps = {}

    # =========================
    # POST HANDLING
    # =========================
    if request.method == "POST":

        # =========================
        # SAVE DIAGNOSIS
        # =========================
        if "save" in request.form:
            d = request.form
            rid = d.get("rid")

            ok, err = require_station(db, rid, "Diagnosis")
            if not ok:
                error = err
            else:
                # -------------------------
                # BASIC VALIDATION
                # -------------------------
                if not d.get("disassembly_status"):
                    error = "Please select Disassembly Status"
                elif not d.get("overall_result"):
                    error = "Please select Overall Result"
                else:
                    try:
                        # -------------------------
                        # SAVE DIAGNOSIS SUMMARY
                        # -------------------------
                        db.execute("""
                            INSERT INTO diagnosis_summary (
                                rid,
                                ic_test,
                                rap_result,
                                unit_result,
                                trx_result,
                                pa_result,
                                overall_result,
                                diagnosed_by,
                                pre_screening
                            ) VALUES (?,?,?,?,?,?,?,?,?)
                        """, (
                            rid,
                            d.get("ic_test"),
                            d.get("rap_result"),
                            d.get("unit_result"),
                            d.get("trx_result"),
                            d.get("pa_result"),
                            d.get("overall_result"),
                            d.get("user"),
                            d.get("pre_screening")
                        ))

                        # -------------------------
                        # SAVE MODULE RESULTS
                        # -------------------------
                        db.execute("""
                            INSERT INTO diagnosis_modules (
                                rid,
                                disassembly_status,
                                fan_result,
                                psu_result,
                                trx_module_result
                            ) VALUES (?,?,?,?,?)
                        """, (
                            rid,
                            d.get("disassembly_status"),
                            d.get("fan_result"),
                            d.get("psu_result"),
                            d.get("trx_module_result")
                        ))

                        # -------------------------
                        # SAVE FAILURE IDS
                        # -------------------------
                        def save_failures(test, ids):
                            for fid in ids or []:
                                fid = fid.strip()
                                if fid:
                                    db.execute("""
                                        INSERT INTO diagnosis_failures (rid, test_name, failure_id)
                                        SELECT ?, ?, ?
                                        WHERE NOT EXISTS (
                                            SELECT 1 FROM diagnosis_failures
                                            WHERE rid=? AND test_name=? AND failure_id=?
                                        )
                                    """, (rid, test, fid, rid, test, fid))

                        save_failures("RAP", d.getlist("rap_fail_ids[]"))
                        save_failures("UNIT", d.getlist("unit_fail_ids[]"))
                        save_failures("TRX_BSCAN", d.getlist("trx_fail_ids[]"))
                        save_failures("TRX_PA", d.getlist("pa_fail_ids[]"))

                        # -------------------------
                        # ✅ SAVE REGISTERED MODULES (FIXED – MATCHES YOUR HTML)
                        # -------------------------
                        def save_module_rows(module_type, part_list, serial_list):
                            for i in range(len(part_list)):
                                pn = (part_list[i] or "").strip()
                                sn = (serial_list[i] or "").strip()

                                if pn and sn:
                                    db.execute("""
                                        INSERT INTO modules
                                        (parent_rid, module_type, part_no, serial_no, registered_by)
                                        VALUES (?, ?, ?, ?, ?)
                                    """, (
                                        rid,
                                        module_type,
                                        pn,
                                        sn,
                                        d.get("user")
                                    ))

                        # FAN
                        save_module_rows(
                            "FAN",
                            d.getlist("fan_part_no[]"),
                            d.getlist("fan_serial_no[]")
                        )

                        # PSU
                        save_module_rows(
                            "PSU",
                            d.getlist("psu_part_no[]"),
                            d.getlist("psu_serial_no[]")
                        )

                        # TRX
                        save_module_rows(
                            "TRX",
                            d.getlist("trx_part_no[]"),
                            d.getlist("trx_serial_no[]")
                        )

                        # -------------------------
                        # STATION HISTORY
                        # -------------------------
                        db.execute("""
                            INSERT INTO station_history (rid, station, user, remarks)
                            VALUES (?,?,?,?)
                        """, (
                            rid,
                            "Diagnosis",
                            d.get("user"),
                            "Diagnosis completed"
                        ))

                        # -------------------------
                        # MOVE TO REPAIR
                        # -------------------------
                        db.execute("""
                            UPDATE units
                            SET current_station='Repair'
                            WHERE rid=?
                        """, (rid,))

                        db.commit()
                        return redirect(url_for("diagnosis", rid=rid, saved=1))

                    except Exception as e:
                        db.rollback()
                        error = f"Save failed: {str(e)}"

            # Reload after save or error
            if rid:
                unit = db.execute(
                    "SELECT * FROM units WHERE rid=?",
                    (rid,)
                ).fetchone()

                history = db.execute(
                    "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
                    (rid,)
                ).fetchall()

                visual = db.execute(
                    "SELECT * FROM visual_inspection WHERE rid=?",
                    (rid,)
                ).fetchone()

                if visual and visual["caps_json"]:
                    try:
                        caps = json.loads(visual["caps_json"])
                    except Exception:
                        caps = {}

        # =========================
        # SCAN UNIT FLOW
        # =========================
        else:
            rfid = request.form.get("rfid")

            if not rfid:
                error = "Please scan RFID"
            else:
                unit, history = scan_unit(db, rfid)

                if not unit:
                    error = "Invalid RFID"
                else:
                    visual = db.execute(
                        "SELECT * FROM visual_inspection WHERE rid=?",
                        (unit["rid"],)
                    ).fetchone()

                    if visual and visual["caps_json"]:
                        try:
                            caps = json.loads(visual["caps_json"])
                        except Exception:
                            caps = {}

    # =========================
    # FAILURE MASTER LOAD
    # =========================
    if unit:
        model = re.sub(r'[^A-Z0-9]', '', str(unit["model"]).upper())

        rap_failures  = get_failures_by_test(model, "RAP")
        unit_failures = get_failures_by_test(model, "UNIT")
        trx_failures  = get_failures_by_test(model, "TRX_BSCAN")
        pa_failures   = get_failures_by_test(model, "TRX_PA")

    # =========================
    # HARD SAFETY FOR JINJA
    # =========================
    rap_failures  = rap_failures  or {}
    unit_failures = unit_failures or {}
    trx_failures  = trx_failures  or {}
    pa_failures   = pa_failures   or {}

    # =========================
    # RENDER
    # =========================
    return render_template(
        "diagnosis.html",
        unit=unit,
        visual=visual,
        history=history,
        caps=caps,
        error=error,
        success=success,
        rap_failures=rap_failures,
        unit_failures=unit_failures,
        trx_failures=trx_failures,
        pa_failures=pa_failures
    )

@app.route("/api/register-modules-bulk", methods=["POST"])
def api_register_modules_bulk():
    data = request.get_json()
    db = get_db()

    parent_rid = data.get("parent_rid")
    user = data.get("user")
    modules = data.get("modules", [])

    if not parent_rid or not modules:
        return "Missing data", 400

    for m in modules:
        db.execute("""
            INSERT INTO modules (
              parent_rid,
              module_type,
              part_no,
              serial_no,
              registered_by
            ) VALUES (?,?,?,?,?)
        """, (
            parent_rid,
            m["module_type"],
            m["part_no"],
            m["serial_no"],
            user
        ))

    db.commit()
    return "OK", 200



# ==============================
# API: REGISTER MODULE UNDER PARENT
# ==============================
@app.route("/api/register-module", methods=["POST"])
def api_register_module():
    db = get_db()

    try:
        data = request.get_json(force=True)

        parent_rid  = data.get("parent_rid")
        module_type = data.get("module_type")
        part_no     = data.get("part_no")
        serial_no   = data.get("serial_no")

        if not parent_rid:
            return {"error": "Parent RID missing"}, 400

        if module_type not in ("FAN", "PSU", "TRX"):
            return {"error": "Invalid module type"}, 400

        if not part_no or not serial_no:
            return {"error": "Part or Serial missing"}, 400

        parent = db.execute(
            "SELECT rid FROM units WHERE rid=?",
            (parent_rid,)
        ).fetchone()

        if not parent:
            return {"error": "Parent unit not found"}, 404

        db.execute("""
            INSERT INTO modules (
                parent_rid,
                module_type,
                product_id,
                part_no,
                serial_no,
                location,
                registered_by
            ) VALUES (?,?,?,?,?,?,?)
        """, (
            parent_rid,
            module_type,
            data.get("product_id"),
            part_no,
            serial_no,
            data.get("location"),
            data.get("user")
        ))

        db.commit()

        return {
            "ok": True,
            "message": f"{module_type} module registered",
            "serial": serial_no
        }

    except sqlite3.IntegrityError:
        return {"error": "Module serial already registered"}, 409

    except Exception as e:
        db.rollback()
        return {"error": str(e)}, 500




import re
from collections import defaultdict


def derive_category(text: str) -> str:
    """
    Improved category derivation WITHOUT breaking existing behavior.

    Logic:
    - Split by "_"
    - Build category progressively from left
    - Stop when tokens start varying meaningfully (Freq / Param)
    """

    if not text:
        return "Misc"

    tokens = text.strip().split("_")

    category_tokens = []

    for t in tokens:
        # Stop at frequency tokens
        if re.match(r"\d+MHz", t, re.I):
            break

        # Stop at measurement / result fields
        if t.lower() in {
            "txlevel", "txdsa", "fbpower",
            "pacurrent", "patemperature",
            "aclroffset1lower", "aclroffset1upper",
            "aclroffset2lower", "aclroffset2upper",
            "ccdf0dot01", "dsaripple",
            "rxbfpower", "bfcaldsa", "temperature"
        }:
            break

        category_tokens.append(t)

    # SAFETY: fallback to old logic if category too small
    if len(category_tokens) < 2:
        token = tokens[0]
        token = re.sub(r"\d+$", "", token)
        return token or "Misc"

    return "_".join(category_tokens)








import re

from collections import defaultdict

def get_failures_by_test(model, test_name):
    db = get_db()

    rows = db.execute("""
        SELECT
            category,
            failure_id,
            description
        FROM failure_master
        WHERE UPPER(TRIM(model)) = UPPER(TRIM(?))
          AND UPPER(TRIM(test_name)) = UPPER(TRIM(?))
        ORDER BY category
    """, (model, test_name)).fetchall()

    failures = {}

    for r in rows:
        category = (r["category"] or "").strip()
        raw_id = (r["failure_id"] or "").strip()
        desc = (r["description"] or "").strip()

        if not category or not raw_id:
            continue

        num_part = raw_id.split("_", 1)[0].split("-", 1)[0].strip()

        if num_part.isdigit():
            numeric_id = num_part

            # ✅ BEST TEXT SELECTION
            if desc:
                text_id = desc
            else:
                text_id = raw_id[len(num_part):].lstrip("_- ").strip()

            if not text_id:
                text_id = "UNKNOWN"

            label = f"{numeric_id} - {text_id}"
        else:
            numeric_id = raw_id
            label = raw_id

        failures.setdefault(category, {})
        failures[category][numeric_id] = label

    final = {}

    for cat, items in failures.items():
        numeric = []
        text = []

        for k, v in items.items():
            if str(k).isdigit():
                numeric.append((int(k), v))
            else:
                text.append(v)

        numeric.sort(key=lambda x: x[0])
        text.sort(key=lambda x: x.lower())

        final[cat] = [v for _, v in numeric] + text

    return final


def register_module(db, payload):
    try:
        db.execute("""
          INSERT INTO modules
          (parent_rid, module_type, product_id, part_no, serial_no, location, registered_by)
          VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
          payload["parent_rid"],
          payload["module_type"],
          payload["product_id"],
          payload["part_no"],
          payload["serial_no"],
          payload["location"],
          payload["user"]
        ))
        db.commit()
    except sqlite3.IntegrityError:
        raise Exception("Module serial already registered")




from datetime import datetime
from collections import defaultdict

from datetime import datetime

@app.route("/diagnosis/preview/<rid>")
def diagnosis_preview(rid):
    db = get_db()

    # =============================
    # LOAD UNIT
    # =============================
    unit = db.execute(
        "SELECT * FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    if not unit:
        abort(404)

    # =============================
    # LOAD DIAGNOSIS SUMMARY
    # =============================
    summary = db.execute("""
        SELECT *
        FROM diagnosis_summary
        WHERE rid=?
    """, (rid,)).fetchone()

    if not summary:
        abort(404)

    # =============================
    # LOAD MODULE RESULTS
    # =============================
    modules = db.execute("""
        SELECT *
        FROM diagnosis_modules
        WHERE rid=?
    """, (rid,)).fetchone()

    # =============================
    # LOAD FAILURE IDS
    # =============================
    rows = db.execute("""
        SELECT test_name, failure_id
        FROM diagnosis_failures
        WHERE rid=?
        ORDER BY test_name, failure_id
    """, (rid,)).fetchall()

    failures = {}
    for r in rows:
        failures.setdefault(r["test_name"], []).append(r["failure_id"])

    # =============================
    # LOAD REGISTERED MODULES
    # =============================
    registered_modules = db.execute("""
        SELECT *
        FROM modules
        WHERE parent_rid=?
        ORDER BY module_type, serial_no
    """, (rid,)).fetchall()

    # =============================
    # LOAD VISUAL INSPECTION (OPTIONAL)
    # =============================
    visual = db.execute(
        "SELECT * FROM visual_inspection WHERE rid=?",
        (rid,)
    ).fetchone()

    caps = {}
    if visual and "caps_json" in visual.keys() and visual["caps_json"]:

        try:
            caps = json.loads(visual["caps_json"])
        except Exception:
            caps = {}

    # =============================
    # LOAD STATION HISTORY
    # =============================
    history = db.execute("""
        SELECT *
        FROM station_history
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()

    # =============================
    # RENDER PREVIEW
    # =============================
    return render_template(
        "diagnosis_preview.html",
        unit=unit,
        diagnosis=summary,          # IMPORTANT (template expects this)
        summary=summary,            # future-proof
        modules=modules,
        failures=failures,
        registered_modules=registered_modules,
        visual=visual,
        caps=caps,
        history=history,
        generated_at=datetime.now()
    )



















def derive_result(fails):
    return "FAIL" if fails and len(fails) > 0 else "PASS"


def build_diagnosis_history(diag):
    def safe_list(val):
        try:
            return json.loads(val) if val else []
        except:
            return []

    trx_parts = safe_list(diag["trx_part_no"])
    trx_serials = safe_list(diag["trx_serial_no"])
    trx_locs = safe_list(diag["trx_location"])

    fan_parts = safe_list(diag["fan_part_no"])
    fan_serials = safe_list(diag["fan_serial_no"])

    psu_parts = safe_list(diag["psu_part_no"])
    psu_serials = safe_list(diag["psu_serial_no"])

    return {
        "meta": {
            "engineer": diag["user"],
            "created_at": diag["created_at"],
            "overall": diag["overall_result"],
            "pre_screening": diag["pre_screening"]
        },
        "results": {
            "RAP": diag["rap_result"],
            "UNIT": diag["unit_result"],
            "TRX_BSCAN": diag["trx_result"],
            "TRX_PA": diag["pa_result"],
            "FAN": diag["fan_result"],
            "PSU": diag["psu_result"]
        },
        "failures": {
            "RAP": safe_list(diag["rap_fail_ids"]),
            "UNIT": safe_list(diag["unit_fail_ids"]),
            "TRX_BSCAN": safe_list(diag["trx_fail_ids"]),
            "TRX_PA": safe_list(diag["pa_fail_ids"])
        },
        "modules": {
            "trx": list(zip(trx_parts, trx_serials, trx_locs)) if trx_parts else [],
            "fan": list(zip(fan_parts, fan_serials)) if fan_parts else [],
            "psu": list(zip(psu_parts, psu_serials)) if psu_parts else []
        },
        "remarks": diag["remarks"]
    }




def build_diagnosis_history_from_new_tables(db, rid, summary, modules_row, fail_map, registered_modules):
    if not summary:
        return None

    # -------------------------
    # MODULE MAPPING
    # -------------------------
    trx_list = []
    fan_list = []
    psu_list = []

    for m in registered_modules or []:
        if m["module_type"] == "TRX":
            trx_list.append((m["part_no"], m["serial_no"], m["location"]))
        elif m["module_type"] == "FAN":
            fan_list.append((m["part_no"], m["serial_no"]))
        elif m["module_type"] == "PSU":
            psu_list.append((m["part_no"], m["serial_no"]))

    # -------------------------
    # SAFE ACCESSOR
    # -------------------------
    def safe(row, key):
        return row[key] if row and key in row.keys() else None

    # -------------------------
    # CREATED_AT FROM HISTORY
    # -------------------------
    created_at = get_diagnosis_created_at(db, rid)

    return {
        "meta": {
            "engineer": safe(summary, "diagnosed_by"),
            "created_at": created_at,
            "overall": safe(summary, "overall_result"),
            "pre_screening": safe(summary, "ic_test")
        },
        "results": {
            "RAP": safe(summary, "rap_result"),
            "UNIT": safe(summary, "unit_result"),
            "TRX_BSCAN": safe(summary, "trx_result"),
            "TRX_PA": safe(summary, "pa_result"),
            "FAN": safe(modules_row, "fan_result"),
            "PSU": safe(modules_row, "psu_result")
        },
        "failures": {
            "RAP": fail_map.get("RAP", []),
            "UNIT": fail_map.get("UNIT", []),
            "TRX_BSCAN": fail_map.get("TRX_BSCAN", []),
            "TRX_PA": fail_map.get("TRX_PA", [])
        },
        "modules": {
            "trx": trx_list,
            "fan": fan_list,
            "psu": psu_list
        },
        "remarks": None
    }



import re

def get_diagnosis_created_at(db, rid):
    row = db.execute("""
        SELECT created_at
        FROM station_history
        WHERE rid=? AND station='Diagnosis'
        ORDER BY created_at DESC
        LIMIT 1
    """, (rid,)).fetchone()

    return row["created_at"] if row else None

# ==============================










   
# REPAIR
# ==============================
# ==============================
# REPAIR (SAFE + FULLY WIRED)
# ==============================
# ==============================
# REPAIR
# ==============================
# ==============================
# FAILURE MASTER LOADER (SAFE)
# ==============================

def load_failure_maps(db):
    rap = {}
    unit = {}
    trx_bscan = {}
    trx_pa = {}

    rows = db.execute("""
        SELECT category, failure_id, description, meas_id, test_name
        FROM failure_master
    """).fetchall()

    for r in rows:
        cat = (r["category"] or "").strip()
        fid = (r["failure_id"] or "").strip()
        desc = (r["description"] or "").strip()
        meas = r["meas_id"]
        test = (r["test_name"] or "").upper()

        # =========================
        # BUILD PROPER LABEL
        # =========================
        if meas and desc:
            label = f"{meas} – {desc}"
        elif desc:
            label = f"{fid} – {desc}"
        else:
            label = fid  # last fallback

        entry = {
            "id": fid,
            "text": label
        }

        if "RAP" in test:
            rap.setdefault(cat, []).append(entry)
        elif "UNIT" in test:
            unit.setdefault(cat, []).append(entry)
        elif "BSCAN" in test:
            trx_bscan.setdefault(cat, []).append(entry)
        elif "PA" in test:
            trx_pa.setdefault(cat, []).append(entry)

    return rap, unit, trx_bscan, trx_pa




@app.route("/repair", methods=["GET", "POST"])
def repair():
    print("\n========== REPAIR ROUTE HIT ==========")
    print("METHOD:", request.method)
    print("FORM KEYS:", list(request.form.keys()))
    print("FORM DATA:", dict(request.form))
    print("=====================================\n")

    db = get_db()

    unit = None
    history = []
    visual = None
    caps = {}

    diag_summary = None
    diag_fail_map = {}
    diagnosis_history = None
    registered_modules = []

    error = None
    success = request.args.get("success") == "1"

    # ===============================
    # LOAD FAILURE MASTER MAPS
    # ===============================
    rap_out_failures, unit_out_failures, trx_bscan_failures, trx_pa_failures = load_failure_maps(db)

    # ===============================
    # POST HANDLING
    # ===============================
    if request.method == "POST":

        # =========================
        # SAVE REPAIR
        # =========================
        if "save" in request.form:
            print("---- SAVE REPAIR FLOW ----")

            d = request.form
            rid = d.get("rid")
            user = d.get("user")

            print("RID:", rid)
            print("USER:", user)

            ok, err = require_station(db, rid, "Repair")
            if not ok:
                print("❌ STATION ERROR:", err)
                error = err
            else:
                try:
                    print(">>> INSERTING INTO REPAIR TABLE")

                    db.execute("""
                        INSERT INTO repair (
                            rid,
                            fault_code, sub_fault_code,
                            repair_action,
                            fan_result, psu_result, trx_module_result,
                            trx_bscan_result, trx_pa_result,
                            assembly_result,
                            rap_outgoing_result, unit_outgoing_result,
                            sui_applicable, sui_trx, sui_pa, sui_psu, sui_unit, sui_verified,
                            repair_status, remarks,
                            user
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        rid,
                        d.get("fault_code"),
                        d.get("sub_fault_code"),
                        d.get("repair_action"),

                        d.get("fan_result"),
                        d.get("psu_result"),
                        d.get("trx_module_result"),

                        d.get("trx_result"),
                        d.get("pa_result"),

                        d.get("assembly_result"),

                        d.get("rap_outgoing_result"),
                        d.get("unit_outgoing_result"),

                        d.get("sui_applicable"),
                        d.get("sui_trx"),
                        d.get("sui_pa"),
                        d.get("sui_psu"),
                        d.get("sui_unit"),
                        d.get("sui_verified"),

                        d.get("repair_status"),
                        d.get("remarks"),

                        user
                    ))

                    print(">>> MAIN REPAIR INSERT OK")

                    # -------------------------
                    # INSERT REPAIR MODULES
                    # -------------------------
                    modules = d.getlist("module_name[]")
                    old_parts = d.getlist("old_part_no[]")
                    old_serials = d.getlist("old_serial_no[]")
                    old_locs = d.getlist("old_location[]")
                    new_parts = d.getlist("new_part_no[]")
                    new_serials = d.getlist("new_serial_no[]")
                    new_locs = d.getlist("new_location[]")

                    print("MODULES:", modules)

                    max_len = max(
                        len(modules),
                        len(old_parts),
                        len(old_serials),
                        len(old_locs),
                        len(new_parts),
                        len(new_serials),
                        len(new_locs)
                    )

                    for i in range(max_len):
                        module = modules[i] if i < len(modules) else None
                        if not module:
                            continue

                        print(f">>> INSERT MODULE ROW {i} ->", module)

                        db.execute("""
                            INSERT INTO repair_modules
                            (rid, module_type, old_part_no, old_serial_no, old_location,
                             new_part_no, new_serial_no, new_location)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            rid,
                            module,
                            old_parts[i] if i < len(old_parts) else None,
                            old_serials[i] if i < len(old_serials) else None,
                            old_locs[i] if i < len(old_locs) else None,
                            new_parts[i] if i < len(new_parts) else None,
                            new_serials[i] if i < len(new_serials) else None,
                            new_locs[i] if i < len(new_locs) else None
                        ))

                    print(">>> REPAIR MODULES INSERTED")

                    # -------------------------
                    # INSERT REPAIR FAILURES
                    # -------------------------
                    print("TRX FAIL IDS:", d.getlist("trx_fail_ids[]"))
                    print("PA FAIL IDS:", d.getlist("pa_fail_ids[]"))
                    print("RAP OUT FAIL IDS:", d.getlist("rap_out_fail_ids[]"))
                    print("UNIT OUT FAIL IDS:", d.getlist("unit_out_fail_ids[]"))

                    for fid in d.getlist("trx_fail_ids[]"):
                        db.execute("INSERT INTO repair_failures (rid, stage, failure_id) VALUES (?, 'TRX_BSCAN', ?)", (rid, fid))

                    for fid in d.getlist("pa_fail_ids[]"):
                        db.execute("INSERT INTO repair_failures (rid, stage, failure_id) VALUES (?, 'TRX_PA', ?)", (rid, fid))

                    for fid in d.getlist("rap_out_fail_ids[]"):
                        db.execute("INSERT INTO repair_failures (rid, stage, failure_id) VALUES (?, 'RAP', ?)", (rid, fid))

                    for fid in d.getlist("unit_out_fail_ids[]"):
                        db.execute("INSERT INTO repair_failures (rid, stage, failure_id) VALUES (?, 'UNIT', ?)", (rid, fid))

                    print(">>> REPAIR FAILURES INSERTED")

                    # -------------------------
                    # STATION HISTORY
                    # -------------------------
                    db.execute("""
                        INSERT INTO station_history (rid, station, user, remarks)
                        VALUES (?, ?, ?, ?)
                    """, (rid, "Repair", user, "Repair completed"))

                    print(">>> STATION HISTORY INSERTED")

                    # -------------------------
                    # MOVE TO QA
                    # -------------------------
                    db.execute("""
                        UPDATE units
                        SET current_station='QA'
                        WHERE rid=?
                    """, (rid,))

                    print(">>> UNIT MOVED TO QA")

                    print(">>> COMMITTING DB")
                    db.commit()
                    print(">>> COMMIT DONE")

                    print(">>> REDIRECTING NOW")
                    return redirect(url_for("repair", rid=rid, success=1))

                except Exception as e:
                    db.rollback()
                    print("❌ REPAIR SAVE FAILED:", str(e))
                    error = str(e)

        # =========================
        # SCAN UNIT
        # =========================
        else:
            print("---- SCAN FLOW ----")

            rfid = request.form.get("rfid")
            user = request.form.get("user")

            print("RFID:", rfid)
            print("USER:", user)

            if not rfid:
                error = "Please scan RFID"
            else:
                unit, history = scan_unit(db, rfid)

                if not unit:
                    print("❌ INVALID RFID")
                    error = "Invalid RFID"
                else:
                    rid = unit["rid"]
                    print("✅ UNIT FOUND:", rid)

                    visual = db.execute(
                        "SELECT * FROM visual_inspection WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    if visual and visual["caps_json"]:
                        try:
                            caps = json.loads(visual["caps_json"])
                        except:
                            caps = {}

                    diag_summary = db.execute(
                        "SELECT * FROM diagnosis_summary WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    rows = db.execute("""
                        SELECT test_name, failure_id
                        FROM diagnosis_failures
                        WHERE rid=?
                    """, (rid,)).fetchall()

                    diag_fail_map = {}
                    for r in rows:
                        diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

                    registered_modules = db.execute("""
                        SELECT *
                        FROM modules
                        WHERE parent_rid=?
                        ORDER BY module_type, serial_no
                    """, (rid,)).fetchall()

                    modules_row = None
                    if diag_summary:
                        modules_row = db.execute("""
                            SELECT *
                            FROM diagnosis_modules
                            WHERE rid=?
                        """, (rid,)).fetchone()

                    diagnosis_history = build_diagnosis_history_from_new_tables(
                        db, rid, diag_summary, modules_row, diag_fail_map, registered_modules
                    )

                    print(">>> SCAN FLOW COMPLETE")

    # ===============================
    # GET FLOW (AFTER REDIRECT)
    # ===============================
    rid = request.args.get("rid")
    if rid:
        print("---- GET FLOW LOAD RID:", rid)

        unit = db.execute("SELECT * FROM units WHERE rid=?", (rid,)).fetchone()

        if unit:
            history = db.execute(
                "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
                (rid,)
            ).fetchall()

            visual = db.execute(
                "SELECT * FROM visual_inspection WHERE rid=?",
                (rid,)
            ).fetchone()

            if visual and visual["caps_json"]:
                try:
                    caps = json.loads(visual["caps_json"])
                except:
                    caps = {}

            diag_summary = db.execute(
                "SELECT * FROM diagnosis_summary WHERE rid=?",
                (rid,)
            ).fetchone()

            rows = db.execute("""
                SELECT test_name, failure_id
                FROM diagnosis_failures
                WHERE rid=?
            """, (rid,)).fetchall()

            diag_fail_map = {}
            for r in rows:
                diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

            registered_modules = db.execute("""
                SELECT *
                FROM modules
                WHERE parent_rid=?
                ORDER BY module_type, serial_no
            """, (rid,)).fetchall()

            modules_row = None
            if diag_summary:
                modules_row = db.execute("""
                    SELECT *
                    FROM diagnosis_modules
                    WHERE rid=?
                """, (rid,)).fetchone()

            diagnosis_history = build_diagnosis_history_from_new_tables(
                db, rid, diag_summary, modules_row, diag_fail_map, registered_modules
            )

            print(">>> GET FLOW LOAD COMPLETE")

    return render_template(
        "repair.html",
        unit=unit,
        history=history,
        visual=visual,
        caps=caps,
        diag_summary=diag_summary,
        diag_fail_map=diag_fail_map,
        diagnosis_history=diagnosis_history,
        registered_modules=registered_modules,
        rap_out_failures=rap_out_failures,
        unit_out_failures=unit_out_failures,
        trx_bscan_failures=trx_bscan_failures,
        trx_pa_failures=trx_pa_failures,
        success=success,
        error=error
    )


def get_trx_failures_dict(db):
    rows = db.execute("SELECT category, failure_id FROM trx_failures_master").fetchall()
    out = {}
    for r in rows:
        out.setdefault(r["category"], []).append(r["failure_id"])
    return out


def get_diag_summary(db, rid):
    return db.execute("SELECT * FROM diagnosis WHERE rid=?", (rid,)).fetchone()


def get_diag_fail_map(db, rid):
    rows = db.execute("SELECT test_name, failure_id FROM diagnosis_failures WHERE rid=?", (rid,)).fetchall()
    out = {"RAP": [], "UNIT": [], "TRX_BSCAN": [], "TRX_PA": []}
    for r in rows:
        out.setdefault(r["test_name"], []).append(r["failure_id"])
    return out


def get_diagnosis_history(db, rid):
    row = db.execute("SELECT * FROM diagnosis WHERE rid=?", (rid,)).fetchone()
    if not row:
        return None

    modules = {
        "trx": db.execute("SELECT part_no, serial_no, location FROM diagnosis_modules WHERE rid=? AND module='TRX'", (rid,)).fetchall(),
        "fan": db.execute("SELECT part_no, serial_no FROM diagnosis_modules WHERE rid=? AND module='FAN'", (rid,)).fetchall(),
        "psu": db.execute("SELECT part_no, serial_no FROM diagnosis_modules WHERE rid=? AND module='PSU'", (rid,)).fetchall(),
    }

    return {
        "modules": modules,
        "remarks": row["remarks"]
    }



@app.route("/repair/preview/<rid>")
def repair_preview(rid):
    db = get_db()

    # =============================
    # LOAD UNIT
    # =============================
    unit = db.execute(
        "SELECT * FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    if not unit:
        abort(404)

    # =============================
    # LOAD STATION HISTORY
    # =============================
    history = db.execute("""
        SELECT *
        FROM station_history
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()

    # =============================
    # LOAD VISUAL INSPECTION
    # =============================
    visual = db.execute(
        "SELECT * FROM visual_inspection WHERE rid=?",
        (rid,)
    ).fetchone()

    caps = {}
    if visual and "caps_json" in visual.keys() and visual["caps_json"]:
        try:
            caps = json.loads(visual["caps_json"])
        except Exception:
            caps = {}

    # =============================
    # LOAD DIAGNOSIS SUMMARY
    # =============================
    diag_summary = db.execute("""
        SELECT *
        FROM diagnosis_summary
        WHERE rid=?
    """, (rid,)).fetchone()

    # =============================
    # LOAD DIAGNOSIS MODULE RESULTS
    # =============================
    diag_modules = db.execute("""
        SELECT *
        FROM diagnosis_modules
        WHERE rid=?
    """, (rid,)).fetchone()

    # =============================
    # LOAD DIAGNOSIS FAILURES
    # =============================
    rows = db.execute("""
        SELECT test_name, failure_id
        FROM diagnosis_failures
        WHERE rid=?
        ORDER BY test_name, failure_id
    """, (rid,)).fetchall()

    diag_fail_map = {}
    for r in rows:
        diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

    # =============================
    # LOAD REGISTERED MODULES
    # =============================
    registered_modules = db.execute("""
        SELECT *
        FROM modules
        WHERE parent_rid=?
        ORDER BY module_type, serial_no
    """, (rid,)).fetchall()

    # =============================
    # LOAD REPAIR RECORDS
    # =============================
    repair_records = db.execute("""
        SELECT *
        FROM repair
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()

    # =============================
    # BUILD DIAGNOSIS HISTORY OBJECT (OPTIONAL, IF YOU NEED)
    # =============================
    diagnosis_history = None
    if diag_summary:
        diagnosis_history = build_diagnosis_history_from_new_tables(
            db,
            rid,
            diag_summary,
            diag_modules,
            diag_fail_map,
            registered_modules
        )

    # =============================
    # RENDER TEMPLATE
    # =============================
    return render_template(
        "repair_preview.html",
        unit=unit,
        history=history,
        visual=visual,
        caps=caps,

        diag_summary=diag_summary,
        diag_modules=diag_modules,
        diag_fail_map=diag_fail_map,
        registered_modules=registered_modules,

        repair_records=repair_records,
        diagnosis_history=diagnosis_history,

        generated_at=datetime.now()
    )

















@app.route("/qa", methods=["GET", "POST"])
def qa():
    print("\n========== QA ROUTE HIT ==========")
    print("METHOD:", request.method)
    print("FORM KEYS:", list(request.form.keys()))
    print("FORM DATA:", dict(request.form))
    print("=================================\n")

    db = get_db()

    unit = None
    history = []
    visual = None
    caps = {}
    diag_summary = None
    diag_fail_map = {}
    registered_modules = []

    error = None
    success = False

    # ===============================
    # LOAD FAILURE MASTER MAPS
    # ===============================
    print(">>> Loading failure master maps")
    rap_out_failures, unit_out_failures, trx_bscan_failures, trx_pa_failures = load_failure_maps(db)
    print(">>> Failure maps loaded")

    # ===============================
    # POST HANDLING
    # ===============================
    if request.method == "POST":

        # ===========================
        # 1️⃣ SCAN FLOW
        # ===========================
        if "rfid" in request.form and "save" not in request.form:
            print("---- SCAN FLOW ----")

            rfid = request.form.get("rfid")
            user = request.form.get("user")

            print("RFID:", rfid)
            print("USER:", user)

            if not rfid:
                error = "Please scan RFID"
                print("❌ ERROR:", error)
            else:
                print(">>> Calling scan_unit()")
                unit, history = scan_unit(db, rfid)

                if not unit:
                    error = "Invalid RFID. Unit not found."
                    print("❌ ERROR:", error)
                else:
                    rid = unit["rid"]
                    print("✅ UNIT FOUND:", rid)

                    # ===========================
                    # LOAD VISUAL INSPECTION
                    # ===========================
                    print(">>> Loading visual inspection")
                    visual = db.execute(
                        "SELECT * FROM visual_inspection WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    print("VISUAL:", dict(visual) if visual else None)

                    if visual and "caps_json" in visual.keys() and visual["caps_json"]:
                        try:
                            caps = json.loads(visual["caps_json"])
                            print(">>> CAPS LOADED:", caps)
                        except Exception as e:
                            print("❌ CAPS JSON ERROR:", str(e))
                            caps = {}

                    # ===========================
                    # LOAD DIAGNOSIS SUMMARY
                    # ===========================
                    print(">>> Loading diagnosis_summary")
                    diag_summary = db.execute(
                        "SELECT * FROM diagnosis_summary WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    print("DIAG SUMMARY:", dict(diag_summary) if diag_summary else None)

                    # ===========================
                    # LOAD DIAGNOSIS FAILURES
                    # ===========================
                    print(">>> Loading diagnosis_failures")
                    rows = db.execute("""
                        SELECT test_name, failure_id
                        FROM diagnosis_failures
                        WHERE rid=?
                    """, (rid,)).fetchall()

                    diag_fail_map = {}
                    for r in rows:
                        diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

                    print("DIAG FAIL MAP:", diag_fail_map)

                    # ===========================
                    # LOAD REGISTERED MODULES
                    # ===========================
                    print(">>> Loading modules")
                    registered_modules = db.execute("""
                        SELECT *
                        FROM modules
                        WHERE parent_rid=?
                        ORDER BY module_type, serial_no
                    """, (rid,)).fetchall()

                    print("REGISTERED MODULES:", [dict(m) for m in registered_modules])

        # ===========================
        # 2️⃣ SAVE QA FLOW
        # ===========================
        elif "save" in request.form:
            print("---- SAVE QA FLOW ----")

            rid = request.form.get("rid")
            user = request.form.get("user")

            print("RID:", rid)
            print("USER:", user)

            if not rid:
                error = "RID missing. Please rescan unit."
                print("❌ ERROR:", error)
            else:
                try:
                    print(">>> Inserting into qa_final")

                    db.execute("""
                        INSERT INTO qa_final (
                            rid, user,
                            unit_final_wo_ant, afm_disassembly,
                            rap_final_with_ant, ota_test,
                            lbts_test, sw_load_test,
                            ip_test, check_seal,
                            dis_test_100, ota_test_100,
                            final_qc, ground_test,
                            qc_sign, tool_sign,
                            ulr_mac, ulr_ec, ulr_s3, ulr_s5, ulr_eq,
                            fc_code, fault_code_details,
                            version_upgrade,
                            ogi_label, ogi_ground_screw, ogi_conn_protection,
                            ogi_caps, ogi_power_cap, ogi_fan_cap,
                            ogi_ip_cap, ogi_warning_label, ogi_mounting_bracket,
                            ogi_guide_pin, ogi_surface_clean, ogi_screws_washer,
                            ogi_gap, ogi_fan_cable, ogi_ter_chain,
                            ogi_top_cover, ogi_no_deform, ogi_serial_match,
                            energy_sign, tl_sign, quality_check, rc_tool_sign,
                            remarks
                        )
                        VALUES (
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?,
                            ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?,
                            ?, ?, ?, ?,
                            ?
                        )
                    """, (
                        rid, user,
                        request.form.get("unit_final_wo_ant"),
                        request.form.get("afm_disassembly"),
                        request.form.get("rap_final_with_ant"),
                        request.form.get("ota_test"),
                        request.form.get("lbts_test"),
                        request.form.get("sw_load_test"),
                        request.form.get("ip_test"),
                        request.form.get("check_seal"),
                        request.form.get("dis_test_100"),
                        request.form.get("ota_test_100"),
                        request.form.get("final_qc"),
                        request.form.get("ground_test"),
                        request.form.get("qc_sign"),
                        request.form.get("tool_sign"),
                        request.form.get("ulr_mac"),
                        request.form.get("ulr_ec"),
                        request.form.get("ulr_s3"),
                        request.form.get("ulr_s5"),
                        request.form.get("ulr_eq"),
                        request.form.get("fc_code"),
                        request.form.get("fault_code_details"),
                        request.form.get("version_upgrade"),
                        request.form.get("ogi_label"),
                        request.form.get("ogi_ground_screw"),
                        request.form.get("ogi_conn_protection"),
                        request.form.get("ogi_caps"),
                        request.form.get("ogi_power_cap"),
                        request.form.get("ogi_fan_cap"),
                        request.form.get("ogi_ip_cap"),
                        request.form.get("ogi_warning_label"),
                        request.form.get("ogi_mounting_bracket"),
                        request.form.get("ogi_guide_pin"),
                        request.form.get("ogi_surface_clean"),
                        request.form.get("ogi_screws_washer"),
                        request.form.get("ogi_gap"),
                        request.form.get("ogi_fan_cable"),
                        request.form.get("ogi_ter_chain"),
                        request.form.get("ogi_top_cover"),
                        request.form.get("ogi_no_deform"),
                        request.form.get("ogi_serial_match"),
                        request.form.get("energy_sign"),
                        request.form.get("tl_sign"),
                        request.form.get("quality_check"),
                        request.form.get("rc_tool_sign"),
                        request.form.get("remarks"),
                    ))

                    print(">>> QA INSERT OK")

                    print(">>> Inserting station history")
                    db.execute(
                        "INSERT INTO station_history (rid, station, user, remarks) VALUES (?, ?, ?, ?)",
                        (rid, "QA", user, "Final QA completed")
                    )

                    print(">>> Updating unit station to RC OUT")
                    db.execute("""
                        UPDATE units
                        SET current_station='RC OUT'
                        WHERE rid=?
                    """, (rid,))

                    print(">>> Committing DB")
                    db.commit()

                    success = True
                    print(">>> QA SAVE SUCCESS")

                    # ===========================
                    # RELOAD DATA FOR UI
                    # ===========================
                    unit = db.execute("SELECT * FROM units WHERE rid=?", (rid,)).fetchone()

                    history = db.execute(
                        "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
                        (rid,)
                    ).fetchall()

                    visual = db.execute(
                        "SELECT * FROM visual_inspection WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    if visual and "caps_json" in visual.keys() and visual["caps_json"]:
                        try:
                            caps = json.loads(visual["caps_json"])
                        except:
                            caps = {}

                    diag_summary = db.execute(
                        "SELECT * FROM diagnosis_summary WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    rows = db.execute("""
                        SELECT test_name, failure_id
                        FROM diagnosis_failures
                        WHERE rid=?
                    """, (rid,)).fetchall()

                    diag_fail_map = {}
                    for r in rows:
                        diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

                    registered_modules = db.execute("""
                        SELECT *
                        FROM modules
                        WHERE parent_rid=?
                        ORDER BY module_type, serial_no
                    """, (rid,)).fetchall()

                except Exception as e:
                    db.rollback()
                    error = str(e)
                    print("❌ SAVE QA ERROR:", error)

    print(">>> Rendering QA page")
    return render_template(
        "qa.html",
        unit=unit,
        history=history,
        visual=visual,
        caps=caps,
        diag_summary=diag_summary,
        diag_fail_map=diag_fail_map,
        registered_modules=registered_modules,
        rap_out_failures=rap_out_failures,
        unit_out_failures=unit_out_failures,
        trx_bscan_failures=trx_bscan_failures,
        trx_pa_failures=trx_pa_failures,
        error=error,
        success=success
    )


def build_caps_from_visual(visual):
    caps = {}

    # Convert sqlite row to dict safely
    v = dict(visual)

    # List of possible cap fields we care about
    possible_fields = [
        "label",
        "label_status",
        "fan",
        "fan_status",
        "power",
        "power_status",
        "ip",
        "ip_status",
        "ground",
        "ground_status",
        "warning_label",
        "warranty_label"
    ]

    for key in possible_fields:
        if key in v:
            val = v.get(key)
            name = key.replace("_status", "").replace("_", " ").title()

            caps[name] = {
                "status": val if val else "NA",
                "count": 1 if val and val.upper() == "FAIL" else 0
            }

    return caps



def build_diag_fail_map(diag):
    return {
        "RAP": json.loads(diag["rap_fail_ids"] or "[]"),
        "UNIT": json.loads(diag["unit_fail_ids"] or "[]"),
        "TRX_BSCAN": json.loads(diag["trx_fail_ids"] or "[]"),
        "TRX_PA": json.loads(diag["pa_fail_ids"] or "[]"),
    }


@app.route("/qa_preview/<rid>")
def qa_preview(rid):
    print("\n========== QA PREVIEW ROUTE HIT ==========")
    print("RID:", rid)
    print("=========================================\n")

    db = get_db()

    # =========================
    # LOAD UNIT
    # =========================
    unit = db.execute(
        "SELECT * FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    if not unit:
        abort(404, "Unit not found")

    print(">>> UNIT:", dict(unit))

    # =========================
    # LOAD HISTORY
    # =========================
    history = db.execute("""
        SELECT station, user, remarks, created_at
        FROM station_history
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()

    print(">>> HISTORY:", len(history))

    # =========================
    # LOAD VISUAL
    # =========================
    visual = db.execute(
        "SELECT * FROM visual_inspection WHERE rid=?",
        (rid,)
    ).fetchone()

    print(">>> VISUAL:", dict(visual) if visual else None)

    # =========================
    # LOAD DIAG SUMMARY
    # =========================
    diag_summary = db.execute(
        "SELECT * FROM diagnosis_summary WHERE rid=?",
        (rid,)
    ).fetchone()

    print(">>> DIAG SUMMARY:", dict(diag_summary) if diag_summary else None)

    # =========================
    # LOAD DIAG FAILURES
    # =========================
    rows = db.execute("""
        SELECT test_name, failure_id
        FROM diagnosis_failures
        WHERE rid=?
    """, (rid,)).fetchall()

    diag_fail_map = {}
    for r in rows:
        diag_fail_map.setdefault(r["test_name"], []).append(r["failure_id"])

    print(">>> DIAG FAIL MAP:", diag_fail_map)

    # =========================
    # LOAD MODULES
    # =========================
    registered_modules = db.execute("""
        SELECT *
        FROM modules
        WHERE parent_rid=?
        ORDER BY module_type, serial_no
    """, (rid,)).fetchall()

    print(">>> MODULES:", [dict(m) for m in registered_modules])

    # =========================
    # LOAD QA FINAL
    # =========================
    qa_record = db.execute("""
        SELECT *
        FROM qa_final
        WHERE rid=?
        ORDER BY id DESC
        LIMIT 1
    """, (rid,)).fetchone()

    print(">>> QA RECORD:", dict(qa_record) if qa_record else None)

    generated_at = datetime.now()

    print(">>> Rendering QA Preview")

    return render_template(
        "qa_preview.html",
        unit=unit,
        history=history,
        visual=visual,
        diag_summary=diag_summary,
        diag_fail_map=diag_fail_map,
        registered_modules=registered_modules,
        qa_record=qa_record,
        generated_at=generated_at
    )



















    

# ==============================
# RC OUT
# ==============================
# ==============================
# RC OUT (FINAL – CLEAN + SAFE)
# ==============================
@app.route("/rc-out", methods=["GET", "POST"])
def rc_out():
    print("\n========== RC OUT ROUTE HIT ==========")
    print("METHOD:", request.method)
    print("FORM:", dict(request.form))
    print("=====================================\n")

    db = get_db()

    unit = None
    history = []
    error = None
    success = False

    if request.method == "POST":

        # =========================
        # 1️⃣ SCAN FLOW
        # =========================
        if "rfid" in request.form and "save" not in request.form:
            print("---- SCAN FLOW ----")

            rfid = request.form.get("rfid")
            user = request.form.get("user")

            print("RFID:", rfid)
            print("USER:", user)

            if not rfid:
                error = "Please scan RFID"
            else:
                unit, history = scan_unit(db, rfid)

                if not unit:
                    error = "Invalid RFID. Unit not found."
                else:
                    print("✅ UNIT FOUND:", unit["rid"])

        # =========================
        # 2️⃣ SAVE DISPATCH FLOW
        # =========================
        elif "save" in request.form:
            print("---- SAVE DISPATCH FLOW ----")

            rid = request.form.get("rid")
            user = request.form.get("user")
            dispatch_date = request.form.get("dispatch_date")
            courier = request.form.get("courier")
            awb_no = request.form.get("awb_no")
            package_condition = request.form.get("package_condition")
            remarks = request.form.get("remarks")

            print("RID:", rid)
            print("USER:", user)

            if not rid:
                error = "RID missing. Please rescan unit."
            else:
                try:
                    # =========================
                    # LOAD UNIT
                    # =========================
                    unit = db.execute(
                        "SELECT * FROM units WHERE rid=?",
                        (rid,)
                    ).fetchone()

                    if not unit:
                        raise Exception("Unit not found")

                    # =========================
                    # INSERT STATION HISTORY
                    # =========================
                    db.execute("""
                        INSERT INTO station_history (rid, station, user, remarks)
                        VALUES (?, ?, ?, ?)
                    """, (
                        rid,
                        "RC OUT",
                        user,
                        f"Dispatched | {courier} | AWB: {awb_no} | {package_condition} | {remarks}"
                    ))

                    print(">>> Station history inserted")

                    # =========================
                    # UPDATE UNIT STATUS
                    # =========================
                    db.execute("""
                        UPDATE units
                        SET status='COMPLETED',
                            current_station='RC OUT'
                        WHERE rid=?
                    """, (rid,))

                    print(">>> Unit marked COMPLETED")

                    # =========================
                    # DETACH RFID (IMPORTANT)
                    # =========================
                    print(">>> Detaching RFID")
                    db.execute("""
                        DELETE FROM rfid_map
                        WHERE rid=?
                    """, (rid,))

                    print(">>> RFID detached")

                    db.commit()
                    success = True
                    print(">>> RC OUT SUCCESS")

                    # =========================
                    # RELOAD DATA
                    # =========================
                    unit = db.execute("SELECT * FROM units WHERE rid=?", (rid,)).fetchone()
                    history = db.execute(
                        "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
                        (rid,)
                    ).fetchall()

                except Exception as e:
                    db.rollback()
                    error = str(e)
                    print("❌ RC OUT ERROR:", error)

    return render_template(
        "rc_out.html",
        unit=unit,
        history=history,
        error=error,
        success=success
    )






# ==============================
# API – DETACH RFID
# ==============================
@app.route("/api/detach-rfid", methods=["POST"])
def detach_rfid():
    db = get_db()

    try:
        data = request.get_json(force=True)
        rid = data.get("rid")

        if not rid:
            return {"success": False, "error": "RID missing"}, 400

        # check exists
        row = db.execute("SELECT 1 FROM rfid_map WHERE rid=?", (rid,)).fetchone()
        if not row:
            return {"success": False, "error": "RFID already detached"}, 400

        db.execute("DELETE FROM rfid_map WHERE rid=?", (rid,))
        db.commit()

        return {"success": True}

    except Exception as e:
        db.rollback()
        return {"success": False, "error": str(e)}, 500


# ==============================
# FULL TRAVELLER EXCEL EXPORT
# ==============================
@app.route("/rc-out/excel/<rid>")
def rc_out_excel(rid):
    db = get_db()
    from io import BytesIO
    import pandas as pd
    from datetime import datetime

    output = BytesIO()

    # =========================
    # 1️⃣ UNIT MASTER
    # =========================
    unit = db.execute("SELECT * FROM units WHERE rid=?", (rid,)).fetchone()
    df_unit = pd.DataFrame([dict(unit)]) if unit else pd.DataFrame()

    # =========================
    # 2️⃣ RFID MAP (if exists)
    # =========================
    rfid_row = db.execute("SELECT rfid FROM rfid_map WHERE rid=?", (rid,)).fetchone()
    df_rfid = pd.DataFrame([dict(rfid_row)]) if rfid_row else pd.DataFrame()

    # =========================
    # 3️⃣ VISUAL INSPECTION
    # =========================
    visual = db.execute("SELECT * FROM visual_inspection WHERE rid=?", (rid,)).fetchone()
    df_visual = pd.DataFrame([dict(visual)]) if visual else pd.DataFrame()

    # =========================
    # 4️⃣ DIAGNOSIS SUMMARY
    # =========================
    diag = db.execute("SELECT * FROM diagnosis_summary WHERE rid=?", (rid,)).fetchone()
    df_diag = pd.DataFrame([dict(diag)]) if diag else pd.DataFrame()

    # =========================
    # 5️⃣ DIAGNOSIS MODULE RESULTS
    # =========================
    diag_modules = db.execute("SELECT * FROM diagnosis_modules WHERE rid=?", (rid,)).fetchone()
    df_diag_modules = pd.DataFrame([dict(diag_modules)]) if diag_modules else pd.DataFrame()

    # =========================
    # 6️⃣ DIAGNOSIS FAILURES
    # =========================
    diag_fails = db.execute("""
        SELECT test_name, failure_id
        FROM diagnosis_failures
        WHERE rid=?
        ORDER BY test_name, failure_id
    """, (rid,)).fetchall()
    df_diag_fails = pd.DataFrame([dict(r) for r in diag_fails]) if diag_fails else pd.DataFrame()

    # =========================
    # 7️⃣ REGISTERED MODULES
    # =========================
    modules = db.execute("""
        SELECT parent_rid, module_type, product_id, part_no, serial_no, location, registered_by
        FROM modules
        WHERE parent_rid=?
        ORDER BY module_type, serial_no
    """, (rid,)).fetchall()
    df_modules = pd.DataFrame([dict(m) for m in modules]) if modules else pd.DataFrame()

    # =========================
    # 8️⃣ REPAIR MAIN RECORD
    # =========================
    repair = db.execute("""
        SELECT *
        FROM repair
        WHERE rid=?
    """, (rid,)).fetchall()
    df_repair = pd.DataFrame([dict(r) for r in repair]) if repair else pd.DataFrame()

    # =========================
    # 9️⃣ REPAIR MODULE CHANGES
    # =========================
    repair_modules = db.execute("""
        SELECT *
        FROM repair_modules
        WHERE rid=?
    """, (rid,)).fetchall()
    df_repair_modules = pd.DataFrame([dict(rm) for rm in repair_modules]) if repair_modules else pd.DataFrame()

    # =========================
    # 🔟 REPAIR FAILURES  ✅ FIXED (stage instead of module/test_name)
    # =========================
    repair_fails = db.execute("""
        SELECT stage, failure_id
        FROM repair_failures
        WHERE rid=?
        ORDER BY stage, failure_id
    """, (rid,)).fetchall()
    df_repair_fails = pd.DataFrame([dict(rf) for rf in repair_fails]) if repair_fails else pd.DataFrame()

    # =========================
    # 1️⃣1️⃣ QA FINAL
    # =========================
    qa = db.execute("""
        SELECT *
        FROM qa_final
        WHERE rid=?
        ORDER BY id DESC
        LIMIT 1
    """, (rid,)).fetchone()
    df_qa = pd.DataFrame([dict(qa)]) if qa else pd.DataFrame()

    # =========================
    # 1️⃣2️⃣ STATION HISTORY (FULL TIMELINE)
    # =========================
    history = db.execute("""
        SELECT station, user, remarks, created_at
        FROM station_history
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()
    df_history = pd.DataFrame([dict(h) for h in history]) if history else pd.DataFrame()

    # =========================
    # WRITE EXCEL – STRUCTURED
    # =========================
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:

        df_history.to_excel(writer, sheet_name="Timeline", index=False)
        df_unit.to_excel(writer, sheet_name="Unit Master", index=False)
        df_rfid.to_excel(writer, sheet_name="RFID Mapping", index=False)
        df_visual.to_excel(writer, sheet_name="Visual Inspection", index=False)
        df_diag.to_excel(writer, sheet_name="Diagnosis Summary", index=False)
        df_diag_modules.to_excel(writer, sheet_name="Diagnosis Modules", index=False)
        df_diag_fails.to_excel(writer, sheet_name="Diagnosis Failures", index=False)
        df_modules.to_excel(writer, sheet_name="Registered Modules", index=False)
        df_repair.to_excel(writer, sheet_name="Repair Main", index=False)
        df_repair_modules.to_excel(writer, sheet_name="Repair Module Changes", index=False)
        df_repair_fails.to_excel(writer, sheet_name="Repair Failures", index=False)
        df_qa.to_excel(writer, sheet_name="QA Final", index=False)

    output.seek(0)

    filename = f"RC_TRAVELLER_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )




def generate_full_traveller_excel(db, rid):
    import pandas as pd
    from io import BytesIO
    from datetime import datetime
    import os

    output = BytesIO()

    # =========================
    # UNIT
    # =========================
    unit = db.execute("SELECT * FROM units WHERE rid=?", (rid,)).fetchone()
    df_unit = pd.DataFrame([dict(unit)]) if unit else pd.DataFrame()

    # =========================
    # VISUAL
    # =========================
    visual = db.execute("SELECT * FROM visual_inspection WHERE rid=?", (rid,)).fetchone()
    df_visual = pd.DataFrame([dict(visual)]) if visual else pd.DataFrame()

    # =========================
    # DIAG SUMMARY
    # =========================
    diag = db.execute("SELECT * FROM diagnosis_summary WHERE rid=?", (rid,)).fetchone()
    df_diag = pd.DataFrame([dict(diag)]) if diag else pd.DataFrame()

    # =========================
    # DIAG FAILURES
    # =========================
    diag_fails = db.execute("""
        SELECT test_name, failure_id
        FROM diagnosis_failures
        WHERE rid=?
    """, (rid,)).fetchall()
    df_diag_fails = pd.DataFrame([dict(r) for r in diag_fails]) if diag_fails else pd.DataFrame()

    # =========================
    # MODULES
    # =========================
    modules = db.execute("""
        SELECT *
        FROM modules
        WHERE parent_rid=?
    """, (rid,)).fetchall()
    df_modules = pd.DataFrame([dict(m) for m in modules]) if modules else pd.DataFrame()

    # =========================
    # REPAIR
    # =========================
    repair = db.execute("""
        SELECT *
        FROM repair
        WHERE rid=?
    """, (rid,)).fetchall()
    df_repair = pd.DataFrame([dict(r) for r in repair]) if repair else pd.DataFrame()

    # =========================
    # QA
    # =========================
    qa = db.execute("""
        SELECT *
        FROM qa_final
        WHERE rid=?
    """, (rid,)).fetchall()
    df_qa = pd.DataFrame([dict(q) for q in qa]) if qa else pd.DataFrame()

    # =========================
    # HISTORY
    # =========================
    history = db.execute("""
        SELECT station, user, remarks, created_at
        FROM station_history
        WHERE rid=?
        ORDER BY created_at
    """, (rid,)).fetchall()
    df_history = pd.DataFrame([dict(h) for h in history]) if history else pd.DataFrame()

    # =========================
    # WRITE EXCEL
    # =========================
    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        df_unit.to_excel(writer, sheet_name="Unit", index=False)
        df_visual.to_excel(writer, sheet_name="Visual", index=False)
        df_diag.to_excel(writer, sheet_name="Diagnosis", index=False)
        df_diag_fails.to_excel(writer, sheet_name="Failures", index=False)
        df_modules.to_excel(writer, sheet_name="Modules", index=False)
        df_repair.to_excel(writer, sheet_name="Repair", index=False)
        df_qa.to_excel(writer, sheet_name="QA", index=False)
        df_history.to_excel(writer, sheet_name="History", index=False)

    output.seek(0)

    filename = f"RC_TRAVELLER_{rid}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join("/tmp", filename)

    with open(path, "wb") as f:
        f.write(output.read())

    return path





@app.route("/rc-out/download/<path:filename>")
def download_rc_out_file(filename):
    return send_file(filename, as_attachment=True)





# ==============================
# EXPORT
# ==============================
@app.route("/export")
def export_excel():
    db = get_db()
    rows = db.execute("SELECT * FROM units").fetchall()
    if not rows:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(rows, columns=rows[0].keys())
    file = f"rc_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    df.to_excel(file, index=False)
    return send_file(file, as_attachment=True)


@app.route("/export/full-traveller")
def export_full_traveller():
    conn = get_db()

    query = """
    SELECT
      u.rid,
      u.rma,
      u.unit_serial,
      u.family,
      u.model,
      u.customer,
      u.country,
      u.is_overseas,
      u.complaint,
      u.status,
      u.current_station
    FROM units u
    """

    df = pd.read_sql(query, conn)
    df["is_overseas"] = df["is_overseas"].map({1: "YES", 0: "NO"})

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Traveller Sheet")

    output.seek(0)
    return send_file(
        output,
        download_name="RC_Full_Traveller.xlsx",
        as_attachment=True,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ==============================
# MAP
# ==============================
@app.route("/map")
def map_view():
    db = get_db()
    data = db.execute("""
        SELECT country, COUNT(*) AS count
        FROM units
        WHERE is_overseas = 1
        GROUP BY country
    """).fetchall()
    return render_template("map.html", data=data)


# ==============================
# ANALYTICS
# ==============================
@app.route("/analytics")
def analytics():
    db = get_db()
    rows = db.execute("""
        SELECT rid,
               julianday('now') - julianday(dispatch_date) AS tat_days
        FROM units
        WHERE status='COMPLETED'
    """).fetchall()

    breached = [r for r in rows if r["tat_days"] is not None and float(r["tat_days"]) > 10]


    return render_template("analytics.html", rows=rows, breached=breached)


# ==============================
# ==============================
# RUN
# ==============================
def require_station(db, rid, expected_station):
    row = db.execute(
        "SELECT current_station FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    if not row:
        return False, "Unit not found"

    if row["current_station"] != expected_station:
        return False, f"⚠️ Unit is currently at {row['current_station']}"


    return True, None



def get_tests_for_unit(db, unit):
    return db.execute("""
        SELECT test_name, sequence
        FROM test_config
        WHERE family=? AND model=?
        ORDER BY sequence
    """, (unit["family"], unit["model"])).fetchall()


def get_next_pending_test(db, rid, tests):
    for t in tests:
        last = db.execute("""
            SELECT result
            FROM diagnostic_attempts
            WHERE rid=? AND test_name=?
            ORDER BY attempt_no DESC
            LIMIT 1
        """, (rid, t["test_name"])).fetchone()

        if not last or last["result"] == "FAIL":
            return t["test_name"]

    return None  # all passed


def get_attempt_no(db, rid, test_name):
    row = db.execute("""
        SELECT MAX(attempt_no) AS max_attempt
        FROM diagnostic_attempts
        WHERE rid=? AND test_name=?
    """, (rid, test_name)).fetchone()

    return (row["max_attempt"] or 0) + 1





@app.route("/incoming/preview/<rid>/pdf")
def incoming_preview_pdf(rid):
    db = get_db()

    unit = db.execute(
        "SELECT * FROM units WHERE rid=?",
        (rid,)
    ).fetchone()

    history = db.execute(
        "SELECT * FROM station_history WHERE rid=? ORDER BY created_at",
        (rid,)
    ).fetchall()

    if not unit:
        abort(404)

    path = f"/tmp/Incoming_Traveller_{rid}.pdf"
    generate_traveller_pdf(path, unit, history)

    return send_file(
        path,
        as_attachment=True,
        download_name=f"Incoming_Traveller_{rid}.pdf"
    )
def generate_traveller_pdf(path, unit, history):
    doc = SimpleDocTemplate(
        path,
        pagesize=A4,
        rightMargin=36,
        leftMargin=36,
        topMargin=36,
        bottomMargin=36
    )

    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph(
        "<b>Incoming Traveller Sheet</b>",
        styles["Title"]
    ))
    elements.append(Spacer(1, 12))

    # Unit summary
    unit_data = [
        ["RID", unit["rid"]],
        ["Family", unit["family"]],
        ["Model", unit["model"]],
        ["Customer", unit["customer"]],
        ["Complaint", unit["complaint"] or "—"],
        ["Current Station", unit["current_station"]],
    ]

    table = Table(unit_data, colWidths=[140, 360])
    table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("BACKGROUND", (0,0), (0,-1), colors.whitesmoke),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 16))

    # Traveller history
    elements.append(Paragraph("<b>Traveller History</b>", styles["Heading2"]))

    history_data = [["Station", "User", "Remarks", "Date"]]
    for h in history:
        history_data.append([
            h["station"],
            h["user"],
            h["remarks"],
            h["created_at"]
        ])

    hist_table = Table(history_data, colWidths=[90, 90, 200, 80])
    hist_table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
    ]))
    elements.append(hist_table)

    doc.build(elements)
    




from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import Image

#qr_path = os.path.join(app.static_folder, "qr", f"{unit['rid']}.png")
#<!--if os.path.exists(qr_path):
    #elements.append(Spacer(1, 12))
    #elements.append(Image(qr_path, width=120, height=120))-->



"""
<!--# Title
    elements.append(Paragraph("<b>Repair Center – Unit Traveller</b>", styles["Title"]))
    elements.append(Spacer(1, 12))

    # Unit summary
    unit_data = [
        ["RID", unit["rid"]],
        ["Family", unit["family"]],
        ["Model", unit["model"]],
        ["Customer", unit["customer"]],
       ["Complaint", unit["complaint"] or "—"],

        ["Current Station", unit["current_station"]],
    ]

    table = Table(unit_data, colWidths=[140, 360])
    table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("BACKGROUND", (0,0), (0,-1), colors.whitesmoke),
    ]))
    elements.append(table)
    elements.append(Spacer(1, 16))

    # Visual inspection (if exists)
    if visual:
        elements.append(Paragraph("<b>Visual Inspection</b>", styles["Heading2"]))
        elements.append(Spacer(1, 8))

        vi_data = [
            ["Engineer", visual["engineer"]],
            ["Admin Decision", visual["admin_decision"]],
            ["Visual Check", visual["visual_check"]],
            ["Incoming IP Test", visual["incoming_ip_test"]],
            ["Remarks", visual["remarks"]],
        ]

        vi_table = Table(vi_data, colWidths=[140, 360])
        vi_table.setStyle(TableStyle([
            ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ]))
        elements.append(vi_table)
        elements.append(Spacer(1, 16))

    # Traveller history
    elements.append(Paragraph("<b>Traveller History</b>", styles["Heading2"]))
    history_data = [["Station", "User", "Remarks", "Date"]]
    for h in history:
        history_data.append([
            h["station"], h["user"], h["remarks"], h["created_at"]
        ])

    hist_table = Table(history_data, colWidths=[90, 90, 200, 80])
    hist_table.setStyle(TableStyle([
        ("GRID", (0,0), (-1,-1), 0.5, colors.grey),
        ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
    ]))
    elements.append(hist_table)
    """

#<!--doc.build(elements)-->


if __name__ == "__main__":
    app.run(debug=True)
