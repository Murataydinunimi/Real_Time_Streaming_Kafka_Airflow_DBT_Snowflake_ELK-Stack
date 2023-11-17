-- back compat for old kwarg name
  
  begin;
    

        insert into TAXI_APP.TRANSACTIONS.taxi_calls_table ("TRIP_ID", "BOOKING_SOURCE", "BOOKING_TIMESTAMP", "DAY_OF_WEEK", "DESTINATION_LOCATION", "DRIVER_ID", "DRIVERS_REVIEW", "ESTIMATED_ARRIVAL_TIME_IN_M", "ESTIMATED_FARE", "N_PASSENGERS", "ORDER_COMPLETION_TIME", "PASSENGER_PICK_UP_TIME", "PICKUP_LOCATION", "STATUS", "TAXI_ID", "TIME_OF_DAY", "TRAFFIC_CONDITIONS", "TRIP_DISTANCE", "TRIP_DURATION", "TRIP_RATING", "USER_ID", "VEHICLE_DETAILS", "WEATHER_CONDITIONS", "DRIVER_S_LAT", "DRIVER_S_LON", "PASSENGER_S_LAT", "PASSENGER_S_LON", "DT_INGESTION_TIMESTAMP")
        (
            select "TRIP_ID", "BOOKING_SOURCE", "BOOKING_TIMESTAMP", "DAY_OF_WEEK", "DESTINATION_LOCATION", "DRIVER_ID", "DRIVERS_REVIEW", "ESTIMATED_ARRIVAL_TIME_IN_M", "ESTIMATED_FARE", "N_PASSENGERS", "ORDER_COMPLETION_TIME", "PASSENGER_PICK_UP_TIME", "PICKUP_LOCATION", "STATUS", "TAXI_ID", "TIME_OF_DAY", "TRAFFIC_CONDITIONS", "TRIP_DISTANCE", "TRIP_DURATION", "TRIP_RATING", "USER_ID", "VEHICLE_DETAILS", "WEATHER_CONDITIONS", "DRIVER_S_LAT", "DRIVER_S_LON", "PASSENGER_S_LAT", "PASSENGER_S_LON", "DT_INGESTION_TIMESTAMP"
            from TAXI_APP.TRANSACTIONS.taxi_calls_table__dbt_tmp
        );
    commit;