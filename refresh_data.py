# refresh_data.py
from main import create_db_and_load_excel

if __name__ == "__main__":
    print("Starting manual refresh from SharePoint...")
    create_db_and_load_excel()
    print("Refresh complete.")
