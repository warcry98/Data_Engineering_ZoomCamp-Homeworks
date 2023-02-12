# PARTITION BY expression must be timestamp
# Cluster on pickup_datetime Cluster on affiliated_base_number (Slot time consumed 5 min 25 sec Bytes shuffled 4.29 GB)
CREATE
OR
REPLACE
TABLE
    `atlantean-glyph-376012.trips_data_all.data_trip_clustered` CLUSTER BY pickup_datetime,
    Affiliated_base_number AS
SELECT *
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`;

# Partition by DATE(pickup_datetime) Cluster on affiliated_base_number (Slot time consumed 9 min 13 sec Bytes shuffled 4.93 GB )
CREATE
OR
REPLACE
TABLE
    `atlantean-glyph-376012.trips_data_all.data_trip_partitioned_clustered` PARTITION BY DATE(pickup_datetime) CLUSTER BY Affiliated_base_number AS
SELECT *
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`;

# Partition by PARTITION BY DATE_TRUNC(pickup_datetime, DAY) Cluster on affiliated_base_number (Slot time consumed 9 min 10 sec Bytes shuffled 4.93 GB )
CREATE
OR
REPLACE
TABLE
    `atlantean-glyph-376012.trips_data_all.data_trip_partitioned_clustered_day` PARTITION BY DATE_TRUNC(pickup_datetime, DAY) CLUSTER BY Affiliated_base_number AS
SELECT *
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`;

# Partition by PARTITION BY DATE_TRUNC(pickup_datetime, MONTH) Cluster on affiliated_base_number (Slot time consumed 6 min 41 sec Bytes shuffled 4.93 GB )
CREATE
OR
REPLACE
TABLE
    `atlantean-glyph-376012.trips_data_all.data_trip_partitioned_clustered_month` PARTITION BY DATE_TRUNC(pickup_datetime, MONTH) CLUSTER BY Affiliated_base_number AS
SELECT *
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`;