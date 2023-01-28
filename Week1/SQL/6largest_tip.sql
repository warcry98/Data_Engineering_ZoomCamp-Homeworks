SELECT
    SUM(tip_amount) as tip,
    zdo.zone as zone_do,
    zpu.zone as zone_pu
FROM green_tripdata gt
    LEFT JOIN taxi_zone zdo on gt.dolocationid = zdo.locationid
    LEFT JOIN taxi_zone zpu on gt.pulocationid = zpu.locationid
WHERE
    zpu.zone = 'Astoria'
    AND (
        zdo.zone = 'Central Park'
        OR zdo.zone = 'Jamaica'
        OR zdo.zone = 'South Ozone Park'
        OR zdo.zone = 'Long Island City/Queens Plaza'
    )
GROUP BY zdo.zone, zpu.zone
ORDER BY tip DESC