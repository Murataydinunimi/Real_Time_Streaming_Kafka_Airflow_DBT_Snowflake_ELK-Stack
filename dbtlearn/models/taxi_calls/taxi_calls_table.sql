{{
  config(
    materialized = 'incremental',
    on_schema_change='fail',
    unique='trip_id'
    )
}}


with json_calls_table as (
    SELECT
     var:trip_id::string AS trip_id,
    var:booking_source::string AS booking_source,
    var:booking_timestamp::string AS booking_timestamp,
    var:day_of_week::string AS day_of_week,
    var:destination_location::string AS destination_location,
    var:driver_id::string AS driver_id,
    var:drivers_review::float AS drivers_review,
    var:estimated_arrival_time_in_m::integer AS estimated_arrival_time_in_m,
    var:estimated_fare::float AS estimated_fare,
    var:n_passengers::integer AS n_passengers,
    var:order_completion_time::string AS order_completion_time,
    var:passenger_pick_up_time::string AS passenger_pick_up_time,
    var:pickup_location::string AS pickup_location,
    var:status::string AS status,
    var:taxi_id::string AS taxi_id,
    var:time_of_day::string AS time_of_day,
    var:traffic_conditions::string AS traffic_conditions,
    var:trip_distance::float AS trip_distance,
    var:trip_duration::string AS trip_duration,
    var:trip_rating::float AS trip_rating,
    var:user_id::string AS user_id,
    var:vehicle_details::string AS vehicle_details,
    var:weather_conditions::string AS weather_conditions,
    var:drivers_current_lat::float as driver_s_lat,
    var:drivers_current_lon::float as driver_s_lon,
    var:passenger_current_lat::float as passenger_s_lat,
    var:passenger_current_lon::float as passenger_s_lon,
    DT_INGESTION_TIMESTAMP
FROM {{source('TAXI_APP','TAXI_CALLS_JSON')}})


SELECT * from json_calls_table
WHERE DT_INGESTION_TIMESTAMP is not null
{% if is_incremental() %}
  AND DT_INGESTION_TIMESTAMP > (select max(DT_INGESTION_TIMESTAMP) from {{ this }})
{% endif %}

