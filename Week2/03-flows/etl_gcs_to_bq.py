from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    path = Path(f"../data/{gcs_path}")
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame, destination_table: str, project_id: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table,
        project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@task()
def bq_total_row(destination_table: str, project_id: str) -> int:
    """Total rows from BigQuery"""    
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    client = bigquery.Client(credentials=gcp_credentials_block.get_credentials_from_service_account(), project=project_id)
    query = f"SELECT COUNT(*) AS n_rows FROM {destination_table}"
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        n_rows = row.n_rows
        return n_rows


@flow(log_prints=True)
def el_gcs_to_bq(year: int, month: int, color: str) -> None:
    """EL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    destination_table="trips_data_all.ny_trip"
    project_id="atlantean-glyph-376012"
    write_bq(df, destination_table, project_id)
    total_row = bq_total_row(destination_table, project_id)
    print(f"rows: {total_row}")

@flow
def el_main_flow(
    months: list[int] = [2, 3],
    year: int = 2019,
    color: str = "yellow"
):
    for month in months:
        el_gcs_to_bq(year, month, color)
    


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    el_main_flow(months, year, color)
