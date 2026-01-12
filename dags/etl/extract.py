# dags/etl/extract.py
from google.cloud import storage
import pandas as pd
import io

# https://storage.googleapis.com/beach-gcp-bucket/online_retail/raws/online_retail_sample.csv

BUCKET = "beach-gcp-bucket"
INPUT_PATH = "online_retail/online_retail_sample.csv"

def extract_data(run_dt: str):
    
    client = storage.Client()
    input_blob = client.bucket(BUCKET).blob(INPUT_PATH)
    data = input_blob.download_as_text()
    df = pd.read_csv(io.StringIO(data))

    output_path = f"online_retail/raw/online_retail_extract_{run_dt}.csv"
    client.bucket(BUCKET).blob(output_path).upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv",
    )

    print(f"Extract saved: gs://{BUCKET}/{output_path}")
