from fastapi import FastAPI
import requests
import json

app = FastAPI()

@app.get("/get-data")
def get_data():
    url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    response = requests.get(url)
    data = response.json()["data"]

  
    with open("/opt/spark-data/data.json", "w") as f:
        json.dump(data, f)

    return {"message": "Data saved successfully", "records": len(data)}
