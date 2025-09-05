import pandas as pd
import requests
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
from msal import ConfidentialClientApplication
from urllib.parse import quote



app = Flask(__name__)

# SharePoint authentication details
site_url = "https://donite1.sharepoint.com/sites/Donite"

# --- 🔐 SharePoint Authentication (Program 1 style) ---
tenant_id = "fa65bc0e-19ae-4d1c-8474-e1a5c480afc4"
client_id = "fb01e8e3-4d48-4a21-bc7a-bc5210462897"
client_secret = "18Q8Q~11768wHF_cyG624qlHZnmGrCp2rU5awcfN"

file_url_pvt = "Quality/01-QMS/Records/DONITE Production Approvals/PPAR/Plan vs Actual - Daniel - Copy.xlsm"
file_url = "Quality/01-QMS/Records/DONITE Production Approvals/PPAR/KPI Plan vs Actual.xlsm"



msal_app = ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)

SCOPES = ["https://graph.microsoft.com/.default"]

def get_access_token():
    result = msal_app.acquire_token_silent(scopes=SCOPES, account=None)
    if not result:
        result = msal_app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in result:
        raise Exception(f"Unable to acquire token: {result.get('error_description')}")
    return result["access_token"]

def get_headers():
    return {"Authorization": f"Bearer {get_access_token()}"}

def fetch_site_and_drive():
    # Use the site identifier format: hostname:/site-path
    site_identifier = "donite1.sharepoint.com:/sites/Donite"

    # Site ID
    response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_identifier}", headers=get_headers())
    response.raise_for_status()
    site_id = response.json()["id"]

    # Drive ID
    response = requests.get(f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives", headers=get_headers())
    response.raise_for_status()
    drives = response.json().get("value", [])
    drive_id = next((d["id"] for d in drives if d["name"] in ["Documents", "Shared Documents"]), None)
    if not drive_id:
        raise Exception("Could not find desired drive")
    return site_id, drive_id
site_id, drive_id = fetch_site_and_drive()

# === Download file from SharePoint ===
def download_excel_from_sharepoint():
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Locate file (Graph path method)
    item_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{file_url}",
        headers=headers
    )
    item_resp.raise_for_status()
    item_id = item_resp.json()["id"]

    # Download content
    file_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{item_id}/content",
        headers=headers
    )
    file_resp.raise_for_status()

    return BytesIO(file_resp.content)

# === Process Excel & Update DB ===
def update_machine_utilization(engine):
    excel_bytes = download_excel_from_sharepoint()

    # Read sheet
    df = pd.read_excel(excel_bytes, sheet_name="Machine Utilisation_PVT",header=37)

    # Keep only the needed columns starting from row 38
    df = df.loc[37:, ["BookingWeek", "ResourceCode", "Max of AvailableHoursPerWeek", "Sum of Total actual time_Hrs"]]

    # Drop rows with NaN BookingWeek/ResourceCode
    df = df.dropna(subset=["BookingWeek", "ResourceCode"])

    # --- Fix: properly indented helper function ---
    def format_week(val):
        try:
            dt = pd.to_datetime(val, errors="coerce")
            if pd.isna(dt):
                return f"Week {int(val)}" if str(val).isdigit() else str(val)
            return f"Week {dt.isocalendar().week}"
        except Exception:
            return str(val)

    # Apply week formatting
    df["BookingWeek"] = df["BookingWeek"].apply(format_week)

    # Filter machines of interest
    machines = ["VAC_NO.1", "VAC_NO.2", "VAC_NO.3", "VAC_NO.5", "VAC_NO.7"]
    df = df[df["ResourceCode"].isin(machines)]

    # Aggregate
    agg_df = df.groupby(["BookingWeek", "ResourceCode"]).agg(
        Plan=("AvailableHoursPerWeek", "max"),
        Actual=("Total actual time_Hrs", "sum")
    ).reset_index()

    # Compute %
    agg_df["Percent"] = (agg_df["Actual"] / agg_df["Plan"] * 100).round(2)

    # Push to DB
    agg_df.to_sql("machine_utilization", engine, if_exists="replace", index=False)

    return agg_df


# === Flask Route ===
@app.route("/MU")
def mu():
    try:
        query = "SELECT * FROM machine_utilization ORDER BY BookingWeek, ResourceCode"
        df = pd.read_sql(query, engine)
    except Exception:
        # Table missing: create it
        df = update_machine_utilization(engine)

    pivot = df.pivot(index="BookingWeek", columns="ResourceCode", values=["Plan", "Actual", "Percent"])
    pivot = pivot.sort_index(axis=1, level=1)

    return render_template(
        "Machine Utilization.html",
        tables=[pivot.to_html(classes="table table-dark table-hover table-bordered align-middle text-center mb-5")]
    )




# --- 📂 File Download (like Program 1) ---
def download_sharepoint_file(drive_path):
    """Download file using Graph API like Program 1"""
    download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:{quote(drive_path)}:/content"
    response = requests.get(download_url, headers=get_headers())
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        raise Exception(f"Failed to download file: {response.status_code}, {response.text}")

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

# For Stores Goods in
usecols_stores_goods_in = "CK:CQ"  # Columns B to H
header_row_stores = 11   # Excel row 12 (0-indexed 11)
column_rename_map_stores_goods_in = {
    "FinishDate": "FinishDate",
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

bank = os.getenv("production-data-db")  # Set this in Render as an environment variable  # same var
engine = create_engine(bank)
update_machine_utilization(engine)

# For psycopg2
def get_db_connection():
    url = make_url(bank)
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

        print(f"Columns in stores_data: {df_stores.columns.tolist()}")  # Debug column names
        print(f"First few rows:\n{df_stores.head()}")  # Debug data

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

def get_stores_goods_in_data():
    try:
        conn = get_db_connection()
        df_stores_goods_in = pd.read_sql_query("SELECT * FROM stores_goods_in_data", conn)
        conn.close()

        print(f"Columns in stores_goods_in_data: {df_stores_goods_in.columns.tolist()}")  # Debug column names
        print(f"First few rows:\n{df_stores_goods_in.head()}")  # Debug data

        # Data cleaning
        relevant_cols = ["finishdate", "worksordernumber", "partnumber", "totalhours", "partsqty", "wo status"]
        df_stores_goods_in.dropna(subset=relevant_cols, how='all', inplace=True)

        df_stores_goods_in["finishdate"] = pd.to_datetime(df_stores_goods_in["finishdate"], errors="coerce")
        df_stores_goods_in["totalhours"] = pd.to_numeric(df_stores_goods_in["totalhours"], errors="coerce").fillna(0)
        df_stores_goods_in["partsqty"] = pd.to_numeric(df_stores_goods_in["partsqty"], errors="coerce").fillna(0)
        df_stores = df_stores_goods_in.sort_values(by="finishdate", ascending=False)

        today = datetime.today().date()
        total_work_orders = df_stores.shape[0]
        total_today = df_stores[df_stores["finishdate"].dt.date == today].shape[0]
        total_backlog = total_work_orders - total_today

        work_orders = []
        for _, row in df_stores.iterrows():
            start_date = row["finishdate"].date() if pd.notnull(row["finishdate"]) else None
            is_backlog = start_date != today

            work_orders.append({
                "finish_date": row["finishdate"].strftime("%d-%m-%y") if pd.notnull(row["finishdate"]) else "",
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

@app.route("/stores_goods_in")
def stores_goods_in_dashboard():
    data = get_stores_goods_in_data()
    if data is None:
        return jsonify({"error": "No data found for Stores"}), 404
    return render_template("stores goods in.html", **data)



def clean_and_prepare_df(df, rename_map):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(r'\.\d+$', '', regex=True)
    df.rename(columns=rename_map, inplace=True)
    df.columns = df.columns.str.lower()   # ✅ make all lowercase
    return df


def create_db_and_load_excel():
    try:
        drive_path = "/Quality/01-QMS/Records/DONITE Production Approvals/PPAR/Plan vs Actual - Daniel - Copy.xlsm"
        file_stream = download_sharepoint_file(drive_path)

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

        # Stores goods in data
        df_stores_goods_in = pd.read_excel(
            file_stream,
            sheet_name=sheet_name_pvt,
            header=header_row_stores,
            usecols=usecols_stores_goods_in,
            engine="openpyxl"
        )
        df_stores_goods_in = clean_and_prepare_df(df_stores_goods_in, column_rename_map_stores_goods_in)

        # Save to PostgreSQL
        df_vacuum.to_sql("vacuum_data", engine, if_exists="replace", index=False, method="multi")
        df_trimming.to_sql("trimming_data", engine, if_exists="replace", index=False, method="multi")
        df_stores.to_sql("stores_data", engine, if_exists="replace", index=False, method="multi")
        df_stores_goods_in.to_sql("stores_goods_in_data", engine, if_exists="replace", index=False, method="multi")


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
vacuum_machines = ["Yellow Cannon", "CMS EIDOS", "Blue Cannon Shelley-Max 1450x915", "UNO 810x610", "Red Shelley - Max 810x610"]
trimming_machines = ['CMS Ares "New" Prime', "CMS Ares 4618 Prime", "CMS Ares 3618 Prime", "Grimme 1", "Grimme 2"]

slug_to_excel_name = {
    "yellow-cannon": "Yellow Cannon",
    "uno": "UNO 810x610",
    "red-cannon": "Red Shelley - Max 810x610",
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
    "Red Shelley - Max 810x610": "Red Cannon.html",
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
        "Red Shelley - Max 810x610" : "Red Cannon",
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
            "name": "Stores-Prep",
            "category": "Stores",
            "target": stores_data["total_work_orders"],
            "todo": "NA",
            "done": "NA",
            "url": "/stores"
        })

    # Stores Goods In
    stores_goods_in_data = get_stores_goods_in_data()
    if stores_goods_in_data:
        machine_data.append({
            "name": "Stores-Goods In",
            "category": "Stores",
            "target": stores_goods_in_data["total_work_orders"],
            "todo": "NA",
            "done": "NA",
            "url": "/stores_goods_in"
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
    
# Reverse mapping (full name → short code)
machine_map = {
    "CMS EIDOS": "VAC_NO.1",
    "Yellow Cannon": "VAC_NO.2",
    "Blue Cannon Shelley-Max 1450x915": "VAC_NO.5",
    "UNO 810x610": "VAC_NO.7",
    "CMS Ares 3618 Prime": "CNC_NO.1",
    "CMS Ares 4618 Prime": "CNC_NO.2",
    "CMS Ares \"New\" Prime": "CNC_NO.3",
    "Grimme 1": "CNC_NO.4",
    "Grimme 2": "CNC_NO.5"
}

@app.template_filter("to_date")
def to_date(value):
    """Convert datetime or string → DD-MM-YYYY"""
    try:
        # Case 1: already a datetime object
        if isinstance(value, datetime):
            return value.strftime("%d-%m")

        # Case 2: string with time
        if isinstance(value, str) and " " in value:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return dt.strftime("%d-%m")

        # Case 3: string without time
        if isinstance(value, str):
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.strftime("%d-%m")

    except Exception:
        return value  # fallback (leave it unchanged)


@app.route("/complete")
def complete():
    conn = get_db_connection()
    vacuum_df = pd.read_sql_query("SELECT * FROM vacuum_data", conn)
    trimmer_df = pd.read_sql_query("SELECT * FROM trimming_data", conn)
    stores_prep_df = pd.read_sql_query("SELECT * FROM stores_data", conn)
    goods_in_df = pd.read_sql_query("SELECT * FROM stores_goods_in_data", conn)
    conn.close()

    return render_template(
        "complete.html",
        vacuum=vacuum_df.to_dict(orient="records"),
        trimmers=trimmer_df.to_dict(orient="records"),
        stores_prep=stores_prep_df.to_dict(orient="records"),
        goods_in=goods_in_df.to_dict(orient="records"),
        machine_map=machine_map
    )


if __name__ == "__main__":
    print("Loading Excel data from SharePoint into local DB initially...")
    create_db_and_load_excel()
    app.run(debug=True)
