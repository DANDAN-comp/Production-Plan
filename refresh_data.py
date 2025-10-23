from main import create_db_and_load_excel, update_machine_utilization
from sqlalchemy import create_engine

if __name__ == "__main__":
    print("🚀 Starting automated SharePoint Excel refresh & DB update...")

    # Step 2: Once refresh completes, download and load data
    create_db_and_load_excel()

    # Step 3: Update machine utilization in the database
    engine = create_engine("postgresql+psycopg2://username:password@localhost:5432/your_database")
    update_machine_utilization(engine)

    print("✅ Full refresh and database update completed successfully.")
