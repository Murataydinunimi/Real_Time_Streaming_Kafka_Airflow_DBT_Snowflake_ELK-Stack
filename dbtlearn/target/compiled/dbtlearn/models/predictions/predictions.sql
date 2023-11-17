


with json_predictions as (
    SELECT
     var:trip_id::string AS trip_id,
     var:booking_source::string as booking_source,
     var:estimated_arrival_time_in_m::int as estimated_arrival_time_in_m,
     var:time_of_day::string as time_of_day,
     var:day_of_week::string as day_of_week,
     var:drivers_review::float as driver_s_review,
     var:traffic_conditions::string as traffic_conditions,
     var:weather_conditions::string as weather_conditions,
     var:prob_of_cancellation::float as prob_of_cancellation,
     DT_INGESTION_TIMESTAMP
FROM TAXI_APP.TRANSACTIONS_RAW.TAXI_CALLS_PREDICTIONS_JSON

)

SELECT * 
FROM json_predictions
WHERE DT_INGESTION_TIMESTAMP is not null

  AND DT_INGESTION_TIMESTAMP > (select max(DT_INGESTION_TIMESTAMP) from TAXI_APP.TRANSACTIONS.predictions)
