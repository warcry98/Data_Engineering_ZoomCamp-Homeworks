# %%
import pathlib
import glob
import os
import json
import pandas as pd
import pandas_gbq
from google.oauth2.service_account import Credentials

# %%
DIRECTORY = os.path.dirname(__file__)
PATH_DIRECTORY = pathlib.Path(DIRECTORY).resolve()
print(PATH_DIRECTORY)

year_month_list = [f"2019-{m:02}" for m in range(1, 13)]
print(year_month_list)

with open(PATH_DIRECTORY / "bq_config.json", "r") as f:
  bq_config = json.load(f)

credential = Credentials.from_service_account_file(bq_config['credential_file'])

# %%
def load_files() -> dict:
  dataframe_collection = {}
  for month in year_month_list:
    dataframe_collection[month] = pd.DataFrame(pd.read_csv(f"{PATH_DIRECTORY}/fhv_tripdata_{month}.csv.gz"))
  
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
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
    collection[key] = df
  return collection
# %%
def web_to_bq(collection: dict) -> None:
  for key in collection.keys():
    pandas_gbq.to_gbq(
      collection[key], 
      "trips_data_all.data_trip", 
      project_id="atlantean-glyph-376012", 
      progress_bar=True,
      chunksize='500_000',
      if_exists='append', 
      credentials=credential
    )
  
# %%
if __name__ == '__main__':
  dataframe_collection = load_files()
  transform_collection = transform(dataframe_collection)
  web_to_bq(transform_collection)