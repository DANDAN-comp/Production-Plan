import pandas as pd
from flask import Flask, render_template, jsonify
from datetime import datetime
from io import BytesIO
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
import sqlite3
import threading
import os
import psycopg2
from sqlalchemy import create_engine

app = Flask(__name__)

# === SharePoint auth (set these correctly) ===
site_url = ""  # e.g. "https://yourtenant.sharepoint.com"
username = "daniel@donite.com"
password = "And096521"

# === SharePoint file details ===
file_url_pvt = (
    "/sites/Donite/Shared Documents/Quality/01-QMS/Records/"
    "DONITE Production Approvals/PPAR/Plan vs Actual - Daniel - Copy.xlsm"
)

# === Excel read parameters ===
sheet_name_pvt = "PVT - Planned Start Date"
sheet_name_stores = "Stores"
header_row = 11            # Excel row 12 (0-indexed 11)
header_row_stores = 11     # Excel row 12

# Column ranges
usecols_vacuum = "M:S"
usecols_trimming = "X:AD"
usecols_stores = "B:H"     # B: StartDate ... H: WO Status

# === Rename maps (exact Excel headers on the left) ===
# After renaming, we ALSO normalize to snake_case and lowercase.
column_rename_map_vacuum = {
    "Production Resources.ResourceDescription": "resource_description",
    "StartDate": "start_date",
    "WorksOrderNumber": "works_order_number",
    "Sum of TotalHours": "total_hours",
    "Part Number": "part_number",
    "Parts Qty": "parts_qty",
    "WO Status": "wo_status",
}
column_rename_map_trimming = column_rename_map_vacuum.copy()

column_rename_map_stores = {
    "StartDate": "start_date",
    "Work Order Number": "works_order_number",
    "Part Number": "part_number",
    "Total Hours Required": "total_hours",
    "Parts Qty": "parts_qty",
    "WO Status": "wo_status",
}

# === Database ===
DATABASE_URLL = os.getenv("DATABASE_URLL")  # must be set in Render
engine = create_engine(DATABASE_URLL)

def get_db_connection():
    # For Postgres on Render
    return psycopg2.connect(DATABASE_URLL, sslmode="require")

# === SharePoint download ===
def get_sharepoint_file(file_url):
    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
    file_stream = BytesIO()
    ctx.web.get_file_by_server_relative_url(file_url).download(file_stream).execute_query()
    file_stream.seek(0)
    return file_stream

# === Cleaning helper ===
def clean_and_prepare_df(df, rename_map):
    # strip, remove duplicate suffixes like ".3", ".4"
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r"\.\d+$", "", regex=True)
    # apply rename map
    df.rename(columns=rename_map, inplace=True)
    # final normalization: lowercase + spaces->underscores (safe for Postgres)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

# === Load Excel -> DB ===
def create_db_and_load_excel():
    try:
        file_stream = get_sharepoint_file(file_url_pvt)

        # Vacuum data
        df_vacuum = pd.read_excel(
            file_stream, sheet_name=sheet_name_pvt,
            header=header_row, usecols=usecols_vacuum, engine="openpyxl"
        )
        df_vacuum = clean_and_prepare_df(df_vacuum, column_rename_map_vacuum)

        file_stream.seek(0)
        # Trimming data
        df_trimming = pd.read_excel(
            file_stream, sheet_name=sheet_name_pvt,
            header=header_row, usecols=usecols_trimming, engine="openpyxl"
        )
        df_trimming = clean_and_prepare_df(df_trimming, column_rename_map_trimming)

        file_stream.seek(0)
        # Stores data (make sure we read the actual Stores sheet)
        df_stores = pd.read_excel(
            file_stream, sheet_name=sheet_name_stores,
            header=header_row_stores, usecols=usecols_stores, engine="openpyxl"
        )
        df_stores = clean_and_prepare_df(df_stores, column_rename_map_stores)

        # Optional: coerce dtypes before saving (helps later)
        for d in (df_vacuum, df_trimming, df_stores):
            if "start_date" in d.columns:
                d["start_date"] = pd.to_datetime(d["start_date"], errors="coerce")
            if "total_hours" in d.columns:
                d["total_hours"] = pd.to_numeric(d["total_hours"], errors="coerce")
            if "parts_qty" in d.columns:
                d["parts_qty"] = pd.to_numeric(d["parts_qty"], errors="coerce")

        conn = get_db_connection()
        df_vacuum.to_sql("vacuum_data", engine, if_exists="replace", index=False)
        df_trimming.to_sql("trimming_data", engine, if_exists="replace", index=False)
        df_stores.to_sql("stores_data", engine, if_exists="replace", index=False)
        conn.close()

        print(f"[{datetime.now()}] Database updated with latest Excel data.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating database: {e}")

# === Data providers ===
def get_stores_data():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM stores_data ORDER BY start_date DESC", engine)
    conn.close()

    today = datetime.today().date()
    work_orders = []
    for _, row in df.iterrows():
        start_date = row["startdate"].date() if pd.notnull(row["startdate"]) else None
        is_backlog = start_date != today
        work_orders.append({
            "start_date": row["startdate"].strftime("%d-%m-%y") if pd.notnull(row["startdate"]) else "",
            "work_order_number": row["works_order_number"],
"part_number": row["part_number"],
"total_hours_required": row["total_hours"],

            "parts_qty": row["parts_qty"],
            "wo_status": row["wo_status"],
            "printing_status": "Not Printed",
            "is_backlog": is_backlog
        })

    total_today = sum(1 for wo in work_orders if wo["start_date"] == today.strftime("%d-%m-%y"))
    total_backlog = len(work_orders) - total_today

    return {
        "total_work_orders": len(work_orders),
        "total_today": total_today,
        "total_backlog": total_backlog,
        "work_orders": work_orders
    }


def get_dashboard_data(resource_name, machine_type):
    conn = get_db_connection()
    table = "vacuum_data" if machine_type == "vacuum" else "trimming_data"
    placeholder = "?" if (DATABASE_URLL or "").startswith("sqlite") else "%s"

    # TRIM + ILIKE for Postgres, plain = for SQLite
    if (DATABASE_URLL or "").startswith("sqlite"):
        query = f"SELECT * FROM {table} WHERE TRIM(resource_description) = {placeholder}"
    else:
        query = f"SELECT * FROM {table} WHERE TRIM(resource_description) ILIKE {placeholder}"

    df = pd.read_sql_query(query, engine, params=(resource_name.strip(),))
    conn.close()

    if df.empty:
        return None

    df["startdate"] = pd.to_datetime(df["startdate"], errors="coerce")
    df["totalhours"] = pd.to_numeric(df["totalhours"], errors="coerce").fillna(0)
    df["parts_qty"] = pd.to_numeric(df["parts_qty"], errors="coerce").fillna(0)

    # Ensure dtypes
    if "start_date" in df.columns:
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    if "total_hours" in df.columns:
        df["total_hours"] = pd.to_numeric(df["total_hours"], errors="coerce").fillna(0)
    if "parts_qty" in df.columns:
        df["parts_qty"] = pd.to_numeric(df["parts_qty"], errors="coerce").fillna(0)

    today = datetime.today().date()
    total_work_orders = df.shape[0]
    total_today = df[df["start_date"].dt.date == today].shape[0]
    total_backlog = total_work_orders - total_today

    df = df.sort_values(by="start_date", ascending=False)

    work_orders = []
    for _, row in df.iterrows():
        printing_status = "Not Printed"
        if "printing_status" in df.columns and pd.notnull(row.get("printing_status")):
            printing_status = row["printing_status"]

        start_date = row["startdate"].date() if pd.notnull(row["startdate"]) else None
        is_backlog = start_date != today

        work_orders.append({
            "start_date": row["start_date"].strftime("%d-%m-%y") if pd.notnull(row.get("start_date")) else "",
            "work_order_number": row.get("works_order_number"),
            "part_number": row.get("part_number"),
            "total_hours_required": row.get("total_hours"),
            "parts_qty": row.get("parts_qty"),
            "wo_status": row.get("wo_status"),
            "printing_status": printing_status,
            "is_backlog": is_backlog
        })

    return {
        "total_work_orders": total_work_orders,
        "total_today": total_today,
        "total_backlog": total_backlog,
        "work_orders": work_orders
    }

# === Routes ===
@app.route("/stores.html")
def stores_dashboard():
    data = get_stores_data()
    return render_template("stores.html", **data)

def scheduled_refresh(interval_seconds=600):
    create_db_and_load_excel()
    threading.Timer(interval_seconds, scheduled_refresh, [interval_seconds]).start()

# Machine lists
vacuum_machines = ["Yellow Cannon", "CMS EIDOS", "Blue Cannon Shelley-Max 1450x915", "UNO 810x610", "Red Cannon"]
trimming_machines = ['CMS Ares "New" Prime', "CMS Ares 4618 Prime", "CMS Ares 3618 Prime", "Grimme 1", "Grimme 2"]

slug_to_excel_name = {
    "yellow-cannon": "Yellow Cannon",
    "uno": "UNO 810x610",
    "red-cannon": "Red Cannon",
    "grimme-2": "Grimme 2",
    "grimme-1": "Grimme 1",
    "eidos": "CMS EIDOS",
    "blue-cannon": "Blue Cannon Shelley-Max 1450x915",
    "ares-3": 'CMS Ares "New" Prime',
    "ares-2": "CMS Ares 4618 Prime",
    "ares-1": "CMS Ares 3618 Prime"
}

excel_to_html = {
    "Yellow Cannon": "Yellow Cannon.html",
    "UNO 810x610": "UNO.html",
    "Red Cannon": "Red Cannon.html",
    "Grimme 2": "Grimme 2.html",
    "Grimme 1": "Grimme 1.html",
    "CMS EIDOS": "Eidos.html",
    "Blue Cannon Shelley-Max 1450x915": "Blue Cannon.html",
    'CMS Ares "New" Prime': "Ares 3.html",
    "CMS Ares 4618 Prime": "Ares 2.html",
    "CMS Ares 3618 Prime": "Ares 1.html",
}

@app.route("/<machine_slug>")
def machine_dashboard(machine_slug):
    excel_name = slug_to_excel_name.get(machine_slug.lower())
    if not excel_name:
        return "Machine not found", 404

    machine_type = "trimming" if excel_name in trimming_machines else "vacuum"
    data = get_dashboard_data(excel_name, machine_type)
    if data is None:
        return jsonify({"error": f"No data found for {excel_name}"}), 404

    template_name = excel_to_html.get(excel_name, "default.html")
    return render_template(template_name, machine=excel_name, **data)

@app.route("/")
def index():
    display_name_map = {
        "Blue Cannon Shelley-Max 1450x915": "Blue Cannon",
        "UNO 810x610": "UNO",
        'CMS Ares "New" Prime': "Ares 3",
        "CMS Ares 4618 Prime": "Ares 2",
        "CMS Ares 3618 Prime": "Ares 1"
    }

    conn = get_db_connection()
    machine_data = []

    for machine_name in vacuum_machines + trimming_machines:
        table = "vacuum_data" if machine_name in vacuum_machines else "trimming_data"
        placeholder = "?" if (DATABASE_URLL or "").startswith("sqlite") else "%s"

        if (DATABASE_URLL or "").startswith("sqlite"):
            query = f"SELECT COUNT(DISTINCT works_order_number) FROM {table} WHERE TRIM(resource_description) = {placeholder}"
        else:
            query = f"SELECT COUNT(DISTINCT works_order_number) FROM {table} WHERE TRIM(resource_description) ILIKE {placeholder}"

        cur = conn.cursor()
        cur.execute(query, (machine_name.strip(),))
        result = cur.fetchone()
        cur.close()

        total_wos = result[0] if result else 0
        display_name = display_name_map.get(machine_name, machine_name)

        machine_data.append({
            "name": display_name,
            "category": "Vacuum Forming" if machine_name in vacuum_machines else "Trimming",
            "target": total_wos,
            "todo": "NA",
            "done": "NA",
            "url": "/" + [slug for slug, name in slug_to_excel_name.items() if name == machine_name][0]
        })

    # Add Stores card ONCE (not inside the loop)
    machine_data.append({
        "name": "Stores",
        "category": "Stores",
        "target": 0,
        "todo": "NA",
        "done": "NA",
        "url": "/stores.html"
    })

    conn.close()
    return render_template("index1.html", machines=machine_data)

# === Entrypoint ===
if __name__ == "__main__":
    print("Loading Excel data from SharePoint into DB initially...")
    create_db_and_load_excel()
    # optional: enable periodic refresh
    # scheduled_refresh(600)
    app.run(debug=True)
