# dags/etl/load.py
from google.cloud import bigquery

def load_to_bq(df):
    client = bigquery.Client()
    table_id = "my_project.my_dataset.users"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print("Loaded to BigQuery")
