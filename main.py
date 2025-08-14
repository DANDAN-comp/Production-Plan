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

# SharePoint authentication details
site_url = "https://donite1.sharepoint.com/sites/Donite"
username = "daniel@donite.com"
password = "And096521"

# SharePoint file details
file_url_pvt = (
    "/sites/Donite/Shared Documents/Quality/01-QMS/Records/"
    "DONITE Production Approvals/PPAR/Plan vs Actual - Daniel - Copy.xlsm"
)

# Excel read parameters
sheet_name_pvt = "PVT - Planned Start Date"
header_row = 11  # Excel row 12 (0-indexed 11)

# For vacuum machines
usecols_vacuum = "M:S"
column_rename_map_vacuum = {
    "Production Resources.ResourceDescription": "ResourceDescription",
    "StartDate": "StartDate",
    "WorksOrderNumber": "WorksOrderNumber",
    "Sum of TotalHours": "TotalHours",
    "Part Number": "PartNumber",
    "Parts Qty": "PartsQty",
    "WO Status": "WO Status",
}

# For trimming machines
usecols_trimming = "X:AD"
column_rename_map_trimming = {
    "Production Resources.ResourceDescription": "ResourceDescription",
    "StartDate": "StartDate",
    "WorksOrderNumber": "WorksOrderNumber",
    "Sum of TotalHours": "TotalHours",
    "Part Number": "PartNumber",
    "Parts Qty": "PartsQty",
    "WO Status": "WO Status",
}

# --- Local Testing (SQLite) ---
# Uncomment for local testing:
#DATABASE_URLL = "sqlite:///local.db"
#engine = create_engine(DATABASE_URLL)

#def get_db_connection():
    #if DATABASE_URLL.startswith("sqlite"):
        #return sqlite3.connect("local.db")
#else:
        #return psycopg2.connect(DATABASE_URLL, sslmode="require")

DATABASE_URLL = os.getenv("DATABASE_URLL")  # Set this in Render as an environment variable  # same var
engine = create_engine(DATABASE_URLL)

def get_db_connection():
    return psycopg2.connect(DATABASE_URLL, sslmode="require")


def get_sharepoint_file(file_url):
    ctx = ClientContext(site_url).with_credentials(UserCredential(username, password))
    file_stream = BytesIO()
    ctx.web.get_file_by_server_relative_url(file_url).download(file_stream).execute_query()
    file_stream.seek(0)
    return file_stream

def clean_and_prepare_df(df, rename_map):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\.\d+$', '', regex=True)
    df.rename(columns=rename_map, inplace=True)
    return df

def create_db_and_load_excel():
    try:
        file_stream = get_sharepoint_file(file_url_pvt)

        # Vacuum data
        df_vacuum = pd.read_excel(file_stream, sheet_name=sheet_name_pvt, header=header_row,
                                  usecols=usecols_vacuum, engine="openpyxl")
        df_vacuum = clean_and_prepare_df(df_vacuum, column_rename_map_vacuum)

        file_stream.seek(0)

        # Trimming data
        df_trimming = pd.read_excel(file_stream, sheet_name=sheet_name_pvt, header=header_row,
                                    usecols=usecols_trimming, engine="openpyxl")
        df_trimming = clean_and_prepare_df(df_trimming, column_rename_map_trimming)

        conn = get_db_connection()
        df_vacuum.to_sql("vacuum_data", engine, if_exists="replace", index=False)
        df_trimming.to_sql("trimming_data", engine, if_exists="replace", index=False)
        conn.close()
        print(f"[{datetime.now()}] Database updated with latest Excel data.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating database: {e}")

def scheduled_refresh(interval_seconds=600):
    create_db_and_load_excel()
    threading.Timer(interval_seconds, scheduled_refresh, [interval_seconds]).start()

def get_dashboard_data(resource_name, machine_type):
    conn = get_db_connection()
    table = "vacuum_data" if machine_type == "vacuum" else "trimming_data"
    placeholder = "?" if DATABASE_URLL.startswith("sqlite") else "%s"

    # TRIM + ILIKE for Postgres, just = for SQLite
    if DATABASE_URLL.startswith("sqlite"):
        query = f'SELECT * FROM {table} WHERE TRIM("ResourceDescription") = {placeholder}'
    else:
        query = f'SELECT * FROM {table} WHERE TRIM("ResourceDescription") ILIKE {placeholder}'

    df = pd.read_sql_query(
        query,
        engine,
        params=(resource_name.strip(),)
    )

    conn.close()

    if df.empty:
        return None

    df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
    df["TotalHours"] = pd.to_numeric(df["TotalHours"], errors="coerce").fillna(0)
    df["PartsQty"] = pd.to_numeric(df["PartsQty"], errors="coerce").fillna(0)

    today = datetime.today().date()
    total_work_orders = df.shape[0]
    total_today = df[df["StartDate"].dt.date == today].shape[0]
    total_backlog = total_work_orders - total_today

    # ✅ Sort DataFrame by StartDate descending
    df = df.sort_values(by="StartDate", ascending=False)

    work_orders = []
    for _, row in df.iterrows():
        printing_status = "Not Printed"
        if "Printing Status" in df.columns and pd.notnull(row.get("Printing Status")):
            printing_status = row["Printing Status"]

        start_date = row["StartDate"].date() if pd.notnull(row["StartDate"]) else None
        is_backlog = start_date != today

        work_orders.append({
            "start_date": row["StartDate"].strftime("%d-%m-%y") if pd.notnull(row["StartDate"]) else "",
            "work_order_number": row["WorksOrderNumber"],
            "part_number": row["PartNumber"],
            "total_hours_required": row["TotalHours"],
            "parts_qty": row["PartsQty"],
            "wo_status": row["WO Status"],
            "printing_status": printing_status,
            "is_backlog": is_backlog
        })

    return {
        "total_work_orders": total_work_orders,
        "total_today": total_today,
        "total_backlog": total_backlog,
        "work_orders": work_orders
    }

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
        placeholder = "?" if DATABASE_URLL.startswith("sqlite") else "%s"

        if DATABASE_URLL.startswith("sqlite"):
            query = f'SELECT COUNT(DISTINCT "WorksOrderNumber") FROM {table} WHERE TRIM("ResourceDescription") = {placeholder}'
        else:
            query = f'SELECT COUNT(DISTINCT "WorksOrderNumber") FROM {table} WHERE TRIM("ResourceDescription") ILIKE {placeholder}'

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

        machine_data.append({
            "name": "Stores",
            "category": "Stores",
            "target": 0,
            "todo": "NA",
            "done": "NA",
            "url": "/stores.html"  # Explicitly set URL
        })
    conn.close()
    return render_template("index1.html", machines=machine_data)


if __name__ == "__main__":
    print("Loading Excel data from SharePoint into local DB initially...")
    create_db_and_load_excel()
    app.run(debug=True)
