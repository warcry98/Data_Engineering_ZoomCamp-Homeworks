from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


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
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="prefect-sbx-community-eng",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def el_gcs_to_bq(year: int, month: int, color: str) -> None:
    """EL flow to load data into Big Query"""
    df = extract_from_gcs(color, year, month)
    write_bq(df)
    print(f"rows: {len(df)}")

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
