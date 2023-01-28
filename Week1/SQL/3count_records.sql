SELECT count(vendorid)
FROM green_tripdata
WHERE
    date_trunc('day', lpep_pickup_datetime) = '2019-01-15'
    AND date_trunc('day', lpep_dropoff_datetime) = '2019-01-15'