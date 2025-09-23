CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS dim_location (
    location_id int PRIMARY KEY,
    borough TEXT,
    zone TEXT
);

CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pu_location_id INT,
    do_location_id INT,
    passenger_count INT,
    trip_distance FLOAT,
    trip_duration_min FLOAT,
    fare_amount FLOAT
);