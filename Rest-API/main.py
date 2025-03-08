import pandas as pd
from fastapi import FastAPI, HTTPException, Query
import os
import glob
from datetime import datetime

apps = FastAPI()

# Base directory where CSV files are stored
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "SampleData"))

def get_file(file_type, filename=None):

    # Search for all CSV files matching the given file type pattern
    files = glob.glob(os.path.join(base_dir, f"{file_type}_*.csv"))
    if not files:
        return None  # No files found
    
    # Get today's date in YYYYMMDD format
    today = datetime.today().strftime("%Y%m%d")
    latest_file = None
    latest_date = "00000000"

    # If exact filename is requested, check if it exists in the list
    if filename:
        for file in files:
            if os.path.basename(file) == filename:
                return file
        return None  # Exact filename not found
    
    # Determine the most recent file by date
    for file in files:
        base_filename = os.path.basename(file) # Extracts  filename
        file_date = base_filename[-12:-4]  # Extract date (YYYYMMDD)

        if file_date.isdigit() and len(file_date) == 8 and file_date <= today:
            if file_date > latest_date:
                latest_date, latest_file = file_date, file

    
    
    # Default to the latest available file
    if latest_file:
        return latest_file
    elif files:
        return files[-1]  # Return the last stored file if no latest file found
    else:
        return None  # No files available


# API endpoint to retrieve product data from the latest or specified CSV file
@apps.get("/products")
def get_products(filename: str = Query(None)):
    file_path = get_file("products", filename=filename)

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid products CSV file found.")
    
    # Reads the CSV file into pandas dataframe
    df = pd.read_csv(file_path)

    #Returns JSON response with product data or an error message.
    return {"status": 200, "data": df.to_dict(orient="records")}


# API endpoint to retrieve customers data from the latest or specified CSV file
@apps.get("/customers")
def get_customers(filename: str = Query(None)):
    file_path = get_file("customers", filename=filename)

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid customers CSV file found.")

    df = pd.read_csv(file_path)
    df.drop(columns=["loyalty_tier"], errors="ignore", inplace=True)  # Removes 'loyalty_tier' column

    return {"status": 200, "data": df.to_dict(orient="records")}


# API endpoint to retrieve suppliers data from the latest or specified CSV file
@apps.get("/suppliers")
def get_suppliers(filename: str = Query(None)):
    file_path = get_file("suppliers", filename=filename)

    if not file_path:
        raise HTTPException(status_code=404, detail="No valid suppliers CSV file found.")

    df = pd.read_csv(file_path)
    return {"status": 200, "data": df.to_dict(orient="records")}
