# dags/etl/extract.py
from google.cloud import storage
import pandas as pd
import io

def extract_data():
    client = storage.Client()
    bucket = client.bucket("beach-gcp-bucket")
    blob = bucket.blob("online_retail/raws/online_retail_sample.csv")

    data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(data))

    print("Extracted rows:", len(df))
    return df
