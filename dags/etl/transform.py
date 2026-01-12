# dags/etl/transform.py
from google.cloud import storage
import pandas as pd
import io

BUCKET = "beach-gcp-bucket"

def transform_data(run_dt: str):
    client = storage.Client()

    input_path = f"online_retail/raw/online_retail_extract_{run_dt}.csv"
    blob = client.bucket(BUCKET).blob(input_path)
    data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(data))

    df.columns = [c.upper() for c in df.columns]
    
    df = df[df['total_amount'] > 100]

    output_path = f"online_retail/raw/online_retail_transform_{run_dt}.csv"
    client.bucket(BUCKET).blob(output_path).upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv",
    )

    print(f"Transform saved: gs://{BUCKET}/{output_path}")
