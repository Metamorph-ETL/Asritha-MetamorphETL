from fastapi import FastAPI
import requests

app = FastAPI()

@app.get("/get-data")
def get_data():
    url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    response = requests.get(url)
    data = response.json()["data"]
    return {"data": data}
