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
from sqlalchemy.engine.url import make_url









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

# For Stores
usecols_stores = "B:H"  # Columns B to H
header_row_stores = 11   # Excel row 12 (0-indexed 11)
column_rename_map_stores = {
    "StartDate": "StartDate",
    "WorksOrderNumber": "WorksOrderNumber",
    "Part Number": "PartNumber",
    "Sum of TotalHours": "TotalHours",
    "Parts Qty": "PartsQty",
    "WO Status": "WO Status",
    "Printing Status": "Printing Status"
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

# For psycopg2
def get_db_connection():
    url = make_url(DATABASE_URLL)
    return psycopg2.connect(
        dbname=url.database,
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port
    )

def get_stores_data():
    try:
        conn = get_db_connection()
        df_stores = pd.read_sql_query("SELECT * FROM stores_data", conn)
        conn.close()

        # Data cleaning
        relevant_cols = ["startdate", "worksordernumber", "partnumber", "totalhours", "partsqty", "wo status"]
        df_stores.dropna(subset=relevant_cols, how='all', inplace=True)

        df_stores["startdate"] = pd.to_datetime(df_stores["startdate"], errors="coerce")
        df_stores["totalhours"] = pd.to_numeric(df_stores["totalhours"], errors="coerce").fillna(0)
        df_stores["partsqty"] = pd.to_numeric(df_stores["partsqty"], errors="coerce").fillna(0)
        df_stores = df_stores.sort_values(by="startdate", ascending=False)

        today = datetime.today().date()
        total_work_orders = df_stores.shape[0]
        total_today = df_stores[df_stores["startdate"].dt.date == today].shape[0]
        total_backlog = total_work_orders - total_today

        work_orders = []
        for _, row in df_stores.iterrows():
            start_date = row["startdate"].date() if pd.notnull(row["startdate"]) else None
            is_backlog = start_date != today

            work_orders.append({
                "start_date": row["startdate"].strftime("%d-%m-%y") if pd.notnull(row["startdate"]) else "",
                "work_order_number": row["worksordernumber"],
                "part_number": row["partnumber"],
                "total_hours_required": row["totalhours"],
                "parts_qty": row["partsqty"],
                "wo_status": row["wo status"],
                "printing_status": row.get("printing status", "Not Printed"),
                "is_backlog": is_backlog
            })

        return {
            "total_work_orders": total_work_orders,
            "total_today": total_today,
            "total_backlog": total_backlog,
            "work_orders": work_orders
        }

    except Exception as e:
        print(f"[{datetime.now()}] Error fetching stores data: {e}")
        return None

@app.route("/stores")
def stores_dashboard():
    data = get_stores_data()
    if data is None:
        return jsonify({"error": "No data found for Stores"}), 404
    return render_template("stores.html", **data)

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
    df.columns = df.columns.str.lower()   # ✅ make all lowercase
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

        file_stream.seek(0)

        # Stores data
        df_stores = pd.read_excel(file_stream, sheet_name=sheet_name_pvt, header=header_row_stores,
                                  usecols=usecols_stores, engine="openpyxl")
        df_stores = clean_and_prepare_df(df_stores, column_rename_map_stores)

        # Save to PostgreSQL
        df_vacuum.to_sql("vacuum_data", engine, if_exists="replace", index=False, method="multi")
        df_trimming.to_sql("trimming_data", engine, if_exists="replace", index=False, method="multi")
        df_stores.to_sql("stores_data", engine, if_exists="replace", index=False, method="multi")

        print(f"[{datetime.now()}] Database updated with latest Excel data.")
    except Exception as e:
        print(f"[{datetime.now()}] Error updating database: {e}")

def scheduled_refresh(interval_seconds=600):
    create_db_and_load_excel()
    threading.Timer(interval_seconds, scheduled_refresh, [interval_seconds]).start()

def get_dashboard_data(resource_name, machine_type):
    table = "vacuum_data" if machine_type == "vacuum" else "trimming_data"
    conn = get_db_connection()
    query = f'SELECT * FROM {table} WHERE TRIM(resourcedescription) ILIKE %s'
    params = (f"%{resource_name.strip()}%",)
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        return None

    df["startdate"] = pd.to_datetime(df["startdate"], errors="coerce")
    df["totalhours"] = pd.to_numeric(df["totalhours"], errors="coerce").fillna(0)
    df["partsqty"] = pd.to_numeric(df["partsqty"], errors="coerce").fillna(0)
    df = df.sort_values(by="startdate", ascending=False)

    today = datetime.today().date()
    total_work_orders = df.shape[0]
    total_today = df[df["startdate"].dt.date == today].shape[0]
    total_backlog = total_work_orders - total_today

    work_orders = []
    for _, row in df.iterrows():
        printing_status = row["printing status"] if "printing status" in df.columns else "Not Printed"
        start_date = row["startdate"].date() if pd.notnull(row["startdate"]) else None
        is_backlog = start_date != today

        work_orders.append({
            "start_date": row["startdate"].strftime("%d-%m-%y") if pd.notnull(row["startdate"]) else "",
            "work_order_number": row["worksordernumber"],
            "part_number": row["partnumber"],
            "total_hours_required": row["totalhours"],
            "parts_qty": row["partsqty"],
            "wo_status": row["wo status"],
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

    # Stores first
    stores_data = get_stores_data()
    if stores_data:
        machine_data.append({
            "name": "Stores",
            "category": "Stores",
            "target": stores_data["total_work_orders"],
            "todo": "NA",
            "done": "NA",
            "url": "/stores"
        })

    for machine_name in vacuum_machines + trimming_machines:
        table = "vacuum_data" if machine_name in vacuum_machines else "trimming_data"
        query = f'SELECT COUNT(DISTINCT worksordernumber) FROM {table} WHERE TRIM(resourcedescription) ILIKE %s'

        cur = conn.cursor()
        cur.execute(query, (f"%{machine_name.strip()}%",))
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
            "url": f"/{machine_slug_from_name(machine_name)}"
        })

    conn.close()
    return render_template("index1.html", machines=machine_data)

def machine_slug_from_name(name):
    for slug, excel_name in slug_to_excel_name.items():
        if excel_name == name:
            return slug
    return name.lower().replace(" ", "-")


if __name__ == "__main__":
    print("Loading Excel data from SharePoint into local DB initially...")
    create_db_and_load_excel()
    app.run(debug=True)
