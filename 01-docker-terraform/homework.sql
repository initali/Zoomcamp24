SELECT count(1) FROM public.green_taxi_data
where lpep_pickup_datetime >= TO_TIMESTAMP('2019-09-18', 'YYYY-MM-DD HH:MI:SS') 
and lpep_dropoff_datetime < TO_TIMESTAMP('2019-09-19', 'YYYY-MM-DD HH:MI:SS'); 


SELECT lpep_pickup_datetime, max(trip_distance) as longest FROM public.green_taxi_data
group by lpep_pickup_datetime
order by longest desc;


SELECT z."Borough", sum(g.total_amount) as total_sum 
FROM public.green_taxi_data as g
join zones as z on g."PULocationID" = z."LocationID"
where g.lpep_pickup_datetime >= TO_TIMESTAMP('2019-09-18', 'YYYY-MM-DD HH:MI:SS') 
and g.lpep_pickup_datetime < TO_TIMESTAMP('2019-09-19', 'YYYY-MM-DD HH:MI:SS')
and g."PULocationID" not in (264, 265)
group by z."Borough"
order by total_sum desc;



SELECT dz."Zone", max(g.tip_amount) as largest_tip
FROM public.green_taxi_data as g
join zones as pz on (g."PULocationID" = pz."LocationID" and pz."Zone" = 'Astoria')
join zones as dz on g."DOLocationID" = dz."LocationID"
where g.lpep_pickup_datetime >= TO_TIMESTAMP('2019-09-01', 'YYYY-MM-DD HH:MI:SS') 
and g.lpep_pickup_datetime < TO_TIMESTAMP('2019-10-01', 'YYYY-MM-DD HH:MI:SS')
group by dz."Zone"
order by largest_tip desc;
