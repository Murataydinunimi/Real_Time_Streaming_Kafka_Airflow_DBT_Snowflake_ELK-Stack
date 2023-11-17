
  create or replace   view TAXI_APP.TRANSACTIONS.metrics_to_kibana
  
   as (
    




with trip_info as (select
tcb.trip_id,
tcb.driver_s_lat,
tcb.driver_s_lon,
tcb.passenger_s_lat,
tcb.passenger_s_lon,
loc.location as pickup_location,
get(split(user_id,'_'),1)::int as user_id_code,
get(split(driver_id,'_'),1)::int as driver_id_code
from TAXI_APP.TRANSACTIONS.taxi_calls_table tcb
join TAXI_APP.TRANSACTIONS_seeds.locations loc
where loc.loc_id = tcb.pickup_location),


all_fields as (select 
ps.*,
cr.status,
cr.booking_timestamp::timestamp as booking_timestamp,
cr.order_completion_time::timestamp as order_completion_time,
linf.driver_s_lat,
linf.driver_s_lon,
linf.passenger_s_lat,
linf.passenger_s_lon,
linf.pickup_location,
concat(dri.name,'_',dri.surname) as drivers_info,
concat(users.name,'_',users.surname) as users_info
from TAXI_APP.TRANSACTIONS.predictions ps
join TAXI_APP.TRANSACTIONS.calls_result cr
on cr.trip_id = ps.trip_id
join trip_info linf
on linf.trip_id = ps.trip_id
join TAXI_APP.TRANSACTIONS_seeds.users users
on users.id = linf.user_id_code
join TAXI_APP.TRANSACTIONS_seeds.drivers dri
on dri.id = driver_id_code),

distance_in_km as (SELECT
    trip_id,
    booking_source,
    driver_s_review,
    pickup_location,
    drivers_info,
    users_info,
    timestampdiff(minute, booking_timestamp, order_completion_time) AS wasted_time,
    6371 * ACOS(
        SIN(RADIANS(PASSENGER_S_LAT)) * SIN(RADIANS(DRIVER_S_LAT)) +
        COS(RADIANS(PASSENGER_S_LAT)) * COS(RADIANS(DRIVER_S_LAT)) *
        COS(RADIANS(PASSENGER_S_LON - DRIVER_S_LON))
    ) AS distance_in_km
FROM all_fields
WHERE prob_of_cancellation > 0.7 AND status = 'cancelled')


SELECT * FROM distance_in_km
  );

