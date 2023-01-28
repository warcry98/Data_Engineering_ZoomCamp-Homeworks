SELECT
    SUM(tip_amount) as tip,
    zone
FROM green_tripdata gt
    LEFT JOIN taxi_zone tz on gt.dolocationid = tz.locationid
WHERE
    zone = 'Central Park'
    OR zone = 'Jamaica'
    OR zone = 'South Ozone Park'
    OR zone = 'Long Island City/Queens Plaza'
GROUP BY zone
ORDER BY tip DESC