# Distinct Number of Affiliated_base_number on BQ Table
SELECT
    COUNT(
        DISTINCT(Affiliated_base_number)
    )
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`;

# Distinct Number of Affiliated_base_number on External Table 
SELECT
    COUNT(
        DISTINCT(Affiliated_base_number)
    )
FROM
    `atlantean-glyph-376012.trips_data_all.fvh_trip`;