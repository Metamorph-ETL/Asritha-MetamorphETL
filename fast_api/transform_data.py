import pandas as pd
import json

# Read raw JSON
with open("/opt/spark-data/data.json", "r") as f:
    data = json.load(f)

df = pd.DataFrame(data)

df.rename(columns={
    "ID Nation": "id_nation",
    "Nation": "nation",
    "ID Year": "id_year",
    "Year": "year",
    "Population": "population",
    "Slug Nation": "slug_nation"
}, inplace=True)

# Save as transformed CSV
df.to_csv("/opt/spark-data/transformed_data.csv", index=False)
print("Transformed data saved to /opt/spark-data/transformed_data.csv")
