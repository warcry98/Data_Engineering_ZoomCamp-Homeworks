SELECT COUNT(*)
FROM
    `atlantean-glyph-376012.trips_data_all.data_trip`
WHERE
    PUlocationID IS NULL
    AND DOlocationID IS NULL;