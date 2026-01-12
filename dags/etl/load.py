# dags/etl/load.py
from google.cloud import bigquery

BUCKET = "beach-gcp-bucket"
TABLE_ID = "stoked-summer-481406-b8.online_retail.sample_output"

def load_to_bq(run_dt: str):
    
    client = bigquery.Client()

    uri = f"online_retail/raw/online_retail_extract_{run_dt}.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
    job.result()

    print(f"Loaded {uri} → {TABLE_ID}")
