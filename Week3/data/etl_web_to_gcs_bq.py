# %%
import pathlib
import glob
import os
import json
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
import pandas_gbq
from google.cloud import storage, bigquery
from google.oauth2.service_account import Credentials

# %%
DIRECTORY = os.path.dirname(__file__)
PATH_DIRECTORY = pathlib.Path(DIRECTORY).resolve()
print(PATH_DIRECTORY)

if not os.path.exists(PATH_DIRECTORY/'parquet'):
  os.mkdir(PATH_DIRECTORY/'parquet')

year_month_list = [f"2019-{m:02}" for m in range(1, 13)]
print(year_month_list)

with open(PATH_DIRECTORY / "bq_config.json", "r") as f:
  bq_config = json.load(f)

credential = Credentials.from_service_account_file(bq_config['credential_file'])

dask_client = Client(n_workers=1, threads_per_worker=4, processes=False)

# %%
def load_files() -> dict:
  dataframe_collection = {}
  for month in year_month_list:
    if not os.path.isfile(f"{PATH_DIRECTORY}/fhv_tripdata_{month}.csv.gz"):
      os.system(f"wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{month}.csv.gz -P {PATH_DIRECTORY}/")
    dataframe_collection[month] = dd.DataFrame(dd.read_csv(f"{PATH_DIRECTORY}/fhv_tripdata_{month}.csv.gz"))
  
  return dataframe_collection

# %%
def print_dataframe(collection: dict) -> None:
  for key in collection.keys():
    print("\n" +"="*40)
    print(key)
    print("-"*40)
    print(collection[key])

# %%
def transform(collection: dict) -> dict:
  for key in collection.keys():
    df = collection[key]
    df["pickup_datetime"] = dd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = dd.to_datetime(df["dropOff_datetime"])
    collection[key] = df
  return collection

# %%
def write_local(collection: dict) -> None:
  for month in year_month_list:
    if not os.path.isfile(f"{PATH_DIRECTORY}/parquet/fhv_tripdata_{month}.parquet"):
      df: dd.DataFrame = collection[month]
      df.to_parquet(f"{PATH_DIRECTORY}/parquet/fhv_tripdata_{month}.parquet")

def write_gcs() -> None:
  client = storage.Client(credentials=credential, project=bq_config['project_id'])
  bucket = storage.Bucket(client, bq_config["bucket_id"])
  files = glob.glob(f"{PATH_DIRECTORY}/parquet/*.parquet")
  for file in files:
    filename = os.path.basename(file)
    blob = bucket.blob(f"data/{filename}", chunk_size=8388608)
    print(f"upload {filename}")
    blob.upload_from_filename(file)
    print(f"{filename} uploaded")


# %%
def web_to_bq(collection: dict) -> None:
  for key in collection.keys():
    pandas_gbq.to_gbq(
      collection[key], 
      "trips_data_all.data_trip", 
      project_id=bq_config["project_id"], 
      progress_bar=True,
      chunksize='500_000',
      if_exists='append', 
      credentials=credential
    )

# %%
def create_external_table() -> None:
  client = bigquery.Client(credentials=credential,project=bq_config['project_id'])
  dataset_id = "trips_data_all"
  table_id = "fvh_trip"
  bucket_id = bq_config['bucket_id']

  job_config = bigquery.QueryJobConfig()
  table_ref = client.dataset(dataset_id).table(table_id)
  job_config.destination = table_ref
  sql = f"""
         CREATE OR REPLACE EXTERNAL TABLE `{bq_config['project_id']}.{dataset_id}.{table_id}`
         OPTIONS (
            format = 'PARQUET',
            uris = ['gs://{bucket_id}/data/fvh_tripdata_2019-*.parquet]
         );
         """
  
  query_job = client.query(
    sql,
    job_config=job_config,
  )

  query_job.result()
  print(f"Query results loaded to table {table_ref.path}")

  dask_client.close()
  
# %%
if __name__ == '__main__':
  dataframe_collection = load_files()
  transform_collection = transform(dataframe_collection)
  write_local(transform_collection)
  write_gcs()
  web_to_bq(transform_collection)
  create_external_table()