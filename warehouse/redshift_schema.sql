CREATE TABLE IF NOT EXISTS trips_fact ( 
pickup_date DATE, 
pu_location_id INT, 
do_location_id INT, 
total_trips INT, 
avg_passenger_count FLOAT, 
avg_trip_distance FLOAT, 
avg_duration_min FLOAT, 
total_fare FLOAT, 
avg_fare FLOAT 
); 