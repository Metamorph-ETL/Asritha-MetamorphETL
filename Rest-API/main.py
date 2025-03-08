import os
import glob
from datetime import datetime
import pandas as pd
from fastapi import FastAPI, HTTPException, Query

apps = FastAPI()

# Base directory where CSV files are stored.
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SampleData"))

def get_file(file_type: str, filename: str = None) -> str:
    # Retrieve all files matching the pattern <file_type>_*.csv.
    files = glob.glob(os.path.join(base_dir, f"{file_type}_*.csv"))
    if not files:
        return None  # No files found

    today = datetime.today().strftime("%Y%m%d")
    
    # If an exact filename is provided, check for an exact match with valid date.
    if filename:
        for file in files:
            if os.path.basename(file) == filename:
                base_filename = os.path.basename(file)
                file_date = base_filename[-12:-4]  # Extract date (YYYYMMDD)
                if file_date.isdigit() and len(file_date) == 8 and file_date <= today:
                    return file
        return None  # Exact filename not found or has a future date

    # find the latest file based on embedded 8-digit date.
    latest_file = None
    latest_date = "00000000"
    for file in files:
        base_filename = os.path.basename(file)
        file_date = base_filename[-12:-4]  # Expecting format: <file_type>_YYYYMMDD.csv
        if file_date.isdigit() and len(file_date) == 8 and file_date <= today:
            if file_date > latest_date:
                latest_date, latest_file = file_date, file

    # Return the latest valid file (or None if none found)
    return latest_file

# API endpoint to retrieve product data.
@apps.get("/products")
def get_products(filename: str = Query(None)):
    file_path = get_file("products", filename)
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid products CSV file found.")
    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}

# API endpoint to retrieve customers data.
@apps.get("/customers")
def get_customers(filename: str = Query(None)):
    file_path = get_file("customers", filename)
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid customers CSV file found.")
    df = pd.read_csv(file_path)
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)
    return {"status": 200, "data": df.to_dict(orient="records")}

# API endpoint to retrieve suppliers data.
@apps.get("/suppliers")
def get_suppliers(filename: str = Query(None)):
    file_path = get_file("suppliers", filename)
    if not file_path:
        raise HTTPException(status_code=404, detail="No valid suppliers CSV file found.")
    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}
