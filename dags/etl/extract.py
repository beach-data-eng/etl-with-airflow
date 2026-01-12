# dags/etl/extract.py
from google.cloud import storage
import pandas as pd
import io

# https://storage.googleapis.com/beach-gcp-bucket/online_retail/raws/online_retail_sample.csv

INPUT_BUCKET = "beach-gcp-bucket"
INPUT_PATH = "online_retail/raws/online_retail_sample.csv"

OUTPUT_BUCKET = "beach-gcp-bucket"
OUTPUT_PATH = "online_retail/clean/online_retail_extract.csv"


def extract_data():
    client = storage.Client()

    # read input
    blob = client.bucket(INPUT_BUCKET).blob(INPUT_PATH)
    data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(data))

    print(f"Extract rows: {len(df)}")

    # write output
    out_blob = client.bucket(OUTPUT_BUCKET).blob(OUTPUT_PATH)
    out_blob.upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv"
    )

    print(f"Extract saved to gs://{OUTPUT_BUCKET}/{OUTPUT_PATH}")
