#!/bin/bash

BASE_URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/
YEAR=2019
for i in {01..12}; do
  wget $BASE_URL/yellow_tripdata_$YEAR-$i.csv.gz -P data/yellow/
done
YEAR=2020
for i in {01..12}; do
  wget $BASE_URL/yellow_tripdata_$YEAR-$i.csv.gz -P data/yellow/
done