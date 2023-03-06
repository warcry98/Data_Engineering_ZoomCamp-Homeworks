#!/bin/bash

BASE_URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv
YEAR=2019
for i in {01..12}; do
  wget $BASE_URL/fhv_tripdata_$YEAR-$i.csv.gz -P data/fhv/
done