SELECT
    date_trunc('day', lpep_pickup_datetime) as pickup_datetime,
    max(trip_distance) as trip -- sum(trip_distance) as total_trip # supposed to use max
FROM green_tripdata
WHERE
    date_trunc('day', lpep_pickup_datetime) = '2019-01-18'
    OR date_trunc('day', lpep_pickup_datetime) = '2019-01-28'
    OR date_trunc('day', lpep_pickup_datetime) = '2019-01-15'
    OR date_trunc('day', lpep_pickup_datetime) = '2019-01-10'
GROUP BY
    date_trunc('day', lpep_pickup_datetime) -- ORDER BY total_trip DESC
ORDER BY trip DESC