#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
if [ ! -f $SCRIPT_DIR/*.gz ]; then
  wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz -P $SCRIPT_DIR/
fi
if [ ! -f $SCRIPT_DIR/*.csv ]; then
  wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -P $SCRIPT_DIR/
fi