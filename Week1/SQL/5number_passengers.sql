SELECT
    COUNT(vendorid) as trip_count,
    date_trunc('day', lpep_pickup_datetime) as pickup_datetime,
    passenger_count
FROM green_tripdata
WHERE
    date_trunc('day', lpep_pickup_datetime) = '2019-01-01'
    AND (
        passenger_count = 2
        or passenger_count = 3
    )
GROUP BY
    date_trunc('day', lpep_pickup_datetime),
    passenger_count