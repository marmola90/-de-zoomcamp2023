--Question 3
SELECT 
	COUNT(1)
FROM
	green_taxi_data
WHERE date(lpep_pickup_datetime)=('2019-01-15') and date(lpep_dropoff_datetime)=('2019-01-15');

--Question 4
SELECT 
	date(lpep_pickup_datetime)date,
	MAX(trip_distance) maxs,
	SUM(trip_distance) sumax
FROM
	green_taxi_data
group by date(lpep_pickup_datetime)
order by maxs desc;

--Question 5
SELECT passenger_count,count(1)
FROM green_taxi_data
WHERE date(lpep_pickup_datetime)='2019-01-01' and passenger_count in (2,3)
group by passenger_count;

--Question 6
SELECT dol.zone,
MAX(gt.tip_amount)maxtip
FROM green_taxi_data gt
JOIN taxi_zone_lookup pu ON pu.locationid=pulocationid
JOIN taxi_zone_lookup dol ON dol.locationid=dolocationid
where pu.zone='Astoria'
GROUP BY dol.Zone
order by maxtip desc;
