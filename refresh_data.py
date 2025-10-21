# refresh_data.py
from main import create_db_and_load_excel, update_machine_utilization, engine

if __name__ == "__main__":
    print("Starting manual refresh from SharePoint...")

    create_db_and_load_excel()
    update_machine_utilization(engine)

    print("Refresh complete.")
