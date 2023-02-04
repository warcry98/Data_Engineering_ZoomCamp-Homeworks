import os
from pathlib import Path
from urllib.parse import urlparse
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

PATH_DIRECTORY = Path().resolve()
DATA_DIRECTORY = PATH_DIRECTORY / "data"
exists = os.path.exists(DATA_DIRECTORY)
if not exists:
  os.mkdir(DATA_DIRECTORY)


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    parse_url = urlparse(dataset_url)
    dataset_file = os.path.basename(parse_url.path)
    if not os.path.isfile(f"{DATA_DIRECTORY}/{dataset_file}"):
        os.system(f"wget {dataset_url} -P {DATA_DIRECTORY}/")
    df = pd.read_csv(DATA_DIRECTORY/dataset_file)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    if not os.path.exists(DATA_DIRECTORY/color):
       os.mkdir(DATA_DIRECTORY/color)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2019
    months = [2, 3]
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, color, dataset_file)
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
