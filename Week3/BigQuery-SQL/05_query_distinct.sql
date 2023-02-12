# Query on Non Partioned Table (647.87 MB)
SELECT
    DISTINCT(Affiliated_base_number)
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`
WHERE
    DATE_TRUNC(pickup_datetime, DAY) BETWEEN '2019-03-01' AND '2019-03-31';

# Query on Partitioned Table (23.06 MB)
SELECT
    DISTINCT(Affiliated_base_number)
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip_partitioned_clustered_month`
WHERE
    DATE_TRUNC(pickup_datetime, DAY) BETWEEN '2019-03-01' AND '2019-03-31';