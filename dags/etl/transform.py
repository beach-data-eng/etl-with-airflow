# dags/etl/transform.py

def transform_data(df):
    df.columns = [c.upper() for c in df.columns]
    df = df[df['total_amount'] >= 100]
    return df
