import pandas as pd
from fastapi import FastAPI, HTTPException
import os
import glob
from datetime import datetime

apps = FastAPI()

# Base directory where CSV files are stored
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SampleData"))

def get_latest_file(file_type):
    files = glob.glob(os.path.join(base_dir, f"{file_type}_*.csv"))

    today = datetime.today().strftime("%Y%m%d")
    latest_file = None
    latest_date = "00000000"

    for file in files:
        filename = os.path.basename(file)
        file_date = filename[-12:-4]  # Extract date (YYYYMMDD)

        if file_date.isdigit() and len(file_date) == 8 and file_date <= today:
            if file_date > latest_date:  # Keep track of the latest valid date
                
                latest_date, latest_file = file_date, file

    return latest_file

@apps.get("/products")
def get_products():
    file_path = get_latest_file("products")  

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid products CSV file found.")

    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}

@apps.get("/customers")
def get_customers():
    file_path = get_latest_file("customers")  

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid customers CSV file found.")

    df = pd.read_csv(file_path)
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)  # Remove 'loyalty_tier' if exists

    return {"status": 200, "data": df.to_dict(orient="records")}

@apps.get("/suppliers")
def get_suppliers():
    file_path = get_latest_file("suppliers")  

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid suppliers CSV file found.")

    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}

