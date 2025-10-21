# refresh_data.py
from main import create_db_and_load_excel, update_machine_utilization
from sqlalchemy import create_engine

if __name__ == "__main__":
    print("Starting manual refresh from SharePoint...")

    # Step 1: Refresh Plan vs Actual data
    create_db_and_load_excel()

    # Step 2: Refresh Machine Utilization data
    # (Pass in the same engine instance your main uses)
    engine = create_engine("postgresql+psycopg2://username:password@localhost:5432/your_database")
    update_machine_utilization(engine)

    print("Refresh complete.")
