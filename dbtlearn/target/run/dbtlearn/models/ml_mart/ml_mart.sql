
  create or replace   view TAXI_APP.TRANSACTIONS.ml_mart
  
   as (
    


with calls_table as (
    select  * from TAXI_APP.TRANSACTIONS.taxi_calls_table
)


SELECT
  c.dt_ingestion_timestamp,
  c.trip_id,
  c.booking_source,
  c.day_of_week,
  c.drivers_review,
  c.estimated_arrival_time_in_m,
  c.time_of_day,
  c.traffic_conditions,
  c.weather_conditions
FROM calls_table c
where c.dt_ingestion_timestamp = (select max(dt_ingestion_timestamp) from calls_table )
  );

