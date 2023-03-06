#!/bin/bash

BASE_URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/
YEAR=2019
for i in {01..12}; do
  wget $BASE_URL/green_tripdata_$YEAR-$i.csv.gz -P data/green/
done
YEAR=2020
for i in {01..12}; do
  wget $BASE_URL/green_tripdata_$YEAR-$i.csv.gz -P data/green/
done