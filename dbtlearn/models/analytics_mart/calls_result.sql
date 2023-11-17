{{
  config(
    materialized = 'view'
    )
}}


with calls_table as (
    select  * from {{ref('taxi_calls_table')}}
)


SELECT
  c.dt_ingestion_timestamp::string as dt_ingestion_timestamp,
  c.trip_id,
  c.booking_timestamp,
  c.passenger_pick_up_time,
  c.order_completion_time,
  c.status
FROM calls_table c


